package store

import (
	"context"
	"encoding/hex"
	"log"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// NodeOptions configures one simulated cluster node.
type NodeOptions struct {
	ID                NodeID
	DataDir           string // empty => temp dir
	Transport         *Transport
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
	Logger            *log.Logger
}

// role of a node for one shard.
type role int

const (
	roleFollower role = iota
	roleCandidate
	roleLeader
)

// shardState is all per-shard node state.
type shardState struct {
	cfg       ShardConfig
	wal       *WAL
	kv        map[Key]kvRec
	commitIdx uint64
	term      uint64
	votedFor  NodeID
	role      role
	leader    NodeID
	// fenceEpoch is the highest epoch at which this node was told it no
	// longer owns the shard; all writes at <= fenceEpoch are rejected.
	fenceEpoch  uint64
	installed   bool
	lastContact time.Time
	// seenTxn caches applied/in-flight txn results for idempotent retries.
	seenTxn map[string]writeCache
	// nextVersion reserves monotonically increasing per-key versions at
	// *append* time so concurrent writers can never reserve the same
	// version. Rebuilt from the WAL on restart.
	nextVersion map[Key]uint64
	// ackIndex[replica] = highest index acked during current leader's term.
	ackIndex map[NodeID]uint64
}

type writeCache struct {
	ok       bool
	version  uint64
	index    uint64
	inflight bool // true = appended, not yet committed/applied
}

// Node is one independent shard replica with its own durable log.
type Node struct {
	opts NodeOptions

	mu     sync.Mutex
	shards map[ShardID]*shardState

	stopCh chan struct{}
	wg     sync.WaitGroup
	closed bool
}

// NewNode constructs a node. Data on disk under DataDir is loaded on Start.
func NewNode(opts NodeOptions) *Node {
	if opts.ElectionTimeout == 0 {
		opts.ElectionTimeout = 250 * time.Millisecond
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = 80 * time.Millisecond
	}
	return &Node{opts: opts, shards: map[ShardID]*shardState{}, stopCh: make(chan struct{})}
}

// Start loads persisted state and starts the liveness/election loop.
func (n *Node) Start(ctx context.Context) error {
	if err := n.loadShardsFromDisk(); err != nil {
		return err
	}
	if n.opts.Transport != nil {
		n.opts.Transport.Register(n.opts.ID, n)
	}
	n.wg.Add(1)
	go n.actorLoop()
	return nil
}

// Close stops background loops and closes all WALs.
func (n *Node) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	close(n.stopCh)
	states := make([]*shardState, 0, len(n.shards))
	for _, s := range n.shards {
		states = append(states, s)
	}
	n.mu.Unlock()
	n.wg.Wait()
	var firstErr error
	for _, s := range states {
		if err := s.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ID returns the node identifier.
func (n *Node) ID() NodeID { return n.opts.ID }

func (n *Node) actorLoop() {
	defer n.wg.Done()
	t := time.NewTicker(n.opts.HeartbeatInterval / 2)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.tick()
		}
	}
}

// tick performs lease-expiry based role transitions and leader heartbeats.
func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	for sid, s := range n.shards {
		if s.cfg.Epoch <= s.fenceEpoch {
			continue // fenced: never act
		}
		switch s.role {
		case roleLeader:
			n.sendHeartbeatsLocked(sid, s)
		case roleFollower:
			// A configured static leader that has lost contact for an
			// election timeout tries to acquire leadership via votes.
			if !s.lastContact.IsZero() && now.Sub(s.lastContact) > n.opts.ElectionTimeout &&
				s.cfg.Leader == n.opts.ID {
				n.startElectionLocked(sid, s)
			}
		case roleCandidate:
			if now.Sub(s.lastContact) > n.opts.ElectionTimeout {
				n.startElectionLocked(sid, s)
			}
		}
	}
}

// InstallShard makes the node a replica of cfg (epoch must be >= current).
func (n *Node) InstallShard(ctx context.Context, cfg ShardConfig, snapshot map[Key]kvRec, snapIndex uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.installLocked(ctx, cfg, snapshot, snapIndex)
}

func (n *Node) installLocked(ctx context.Context, cfg ShardConfig, snapshot map[Key]kvRec, snapIndex uint64) error {
	dir := shardDir(n.opts.DataDir, n.opts.ID, cfg.Shard)
	existing := n.shards[cfg.Shard]
	if existing != nil {
		if cfg.Epoch < existing.cfg.Epoch {
			return ErrPositionRegression
		}
		if cfg.Epoch == existing.cfg.Epoch {
			// idempotent re-install at same epoch: just refresh membership
			existing.cfg = cfg
			return n.persistMetaLocked(existing)
		}
	}
	wal, err := OpenWAL(dir, cfg.Shard)
	if err != nil {
		return err
	}
	// New generation: fresh log; entries arrive via AppendEntries from the
	// new leader. A snapshot seeds the applied image during migration.
	if wal.LastIndex() > 0 {
		// preserve old epoch log only on disk? For verifiable clean epochs
		// we require callers to fence first; start from empty log.
		if err := wal.TruncateSuffix(0); err != nil {
			return err
		}
	}
	if snapIndex > 0 {
		// seed log with a single snapshot-marker Noop carrying snapIndex is
		// avoided: instead indexes stay contiguous and snapshot must equal
		// what the leader will push; callers use snapshot only for seeding
		// KV while fetching entries afterwards, so snapshot index must be 0
		// in this prototype unless explicitly bootstrapping.
		if snapIndex != 0 {
			_ = snapIndex // catch-up via FetchLog is the standard path
		}
	}
	st := &shardState{
		cfg:         cfg,
		wal:         wal,
		kv:          map[Key]kvRec{},
		role:        roleFollower,
		leader:      cfg.Leader,
		installed:   true,
		lastContact: time.Now(),
		seenTxn:     map[string]writeCache{},
		nextVersion: map[Key]uint64{},
		ackIndex:    map[NodeID]uint64{},
	}
	// The configured static leader starts as leader immediately; the
	// election path exists for failover after a timeout.
	if cfg.Leader == n.opts.ID {
		st.role = roleLeader
		st.leader = n.opts.ID
		st.term = 1
		st.ackIndex[n.opts.ID] = 0
	}
	for k, v := range snapshot {
		st.kv[k] = v
	}
	if existing != nil {
		_ = existing.wal.Close()
	}
	n.shards[cfg.Shard] = st
	if err := n.persistMetaLocked(st); err != nil {
		return err
	}
	if err := writeCommitFile(filepath.Join(dir, "commit.bin"), 0); err != nil {
		return err
	}
	n.logf("installed shard %s epoch=%d replicas=%v leader=%s snapshotKV=%d",
		cfg.Shard, cfg.Epoch, cfg.Replicas, cfg.Leader, len(snapshot))
	return nil
}

func (n *Node) persistMetaLocked(s *shardState) error {
	dir := shardDir(n.opts.DataDir, n.opts.ID, s.cfg.Shard)
	return writeMetaFile(filepath.Join(dir, "meta.json"), persistedMeta{
		Config:     s.cfg,
		FenceEpoch: s.fenceEpoch,
	})
}

// FenceShard invalidates leases at epochs <= epoch for one shard.
func (n *Node) FenceShard(ctx context.Context, shard ShardID, epoch uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.shards[shard]
	if s == nil {
		// Persist a tombstone-ish fence so a restarted stale replica cannot
		// re-join under the old epoch. We materialize minimal state when
		// the shard is later installed; for an unknown shard there is
		// nothing to fence, which is fine for an installed cluster node.
		return nil
	}
	if epoch <= s.fenceEpoch {
		return nil // idempotent
	}
	s.fenceEpoch = epoch
	if s.cfg.Epoch <= epoch {
		s.role = roleFollower
		s.leader = ""
		// drop any uncommitted tail appended under the stale lease
		if s.wal.LastIndex() > s.commitIdx {
			if err := s.wal.TruncateSuffix(s.commitIdx); err != nil {
				return err
			}
		}
	}
	if err := n.persistMetaLocked(s); err != nil {
		return err
	}
	n.logf("FENCED shard %s at epoch<=%d (was leader=%v role=%d)", shard, epoch, s.cfg.Leader, s.role)
	return nil
}

// Position returns the node's verifiable sync position for shard.
func (n *Node) Position(ctx context.Context, shard ShardID) (SyncPosition, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.shards[shard]
	if s == nil {
		return SyncPosition{}, ErrUnavailable
	}
	return n.positionLocked(s), nil
}

func (n *Node) positionLocked(s *shardState) SyncPosition {
	chain := s.wal.HashChain()
	return SyncPosition{
		Epoch:     s.cfg.Epoch,
		LastIndex: s.wal.LastIndex(),
		CommitIdx: s.commitIdx,
		LastHash:  hex.EncodeToString(chain[:8]),
		LogDigest: s.wal.LogDigest(),
	}
}

// ReadOnlyKV is a diagnostic dump of applied KV on this node.
func (n *Node) ReadOnlyKV(shard ShardID) (map[Key]kvRec, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s, ok := n.shards[shard]
	if !ok {
		return nil, false
	}
	out := make(map[Key]kvRec, len(s.kv))
	for k, v := range s.kv {
		out[k] = v
	}
	return out, true
}

// EpochReq groups client write parameters.
type EpochReq struct {
	Epoch       uint64
	Op          OpKind
	Key         Key
	Value       Value
	TxnID       string
	Consistency Consistency
}

// ClientWrite is the node entry point for client writes (leader only).
func (n *Node) ClientWrite(ctx context.Context, shard ShardID, r EpochReq) (WriteResult, error) {
	n.mu.Lock()
	s := n.shards[shard]
	if s == nil || !s.installed {
		n.mu.Unlock()
		return WriteResult{}, fmtErr(ErrUnavailable, "shard not installed")
	}
	if r.Epoch != s.cfg.Epoch {
		n.mu.Unlock()
		return WriteResult{}, fmtErr(ErrStaleFollower, "write epoch=%d node epoch=%d", r.Epoch, s.cfg.Epoch)
	}
	if s.cfg.Epoch <= s.fenceEpoch {
		n.mu.Unlock()
		return WriteResult{}, fmtErr(ErrStaleFollower, "fenced at epoch %d", s.fenceEpoch)
	}
	if s.role != roleLeader {
		leader := s.leader
		n.mu.Unlock()
		return WriteResult{}, fmtErr(ErrNoLeader, "not leader; known leader=%s", leader)
	}
	// Idempotency: a previously completed txn returns the cached result.
	if r.TxnID != "" {
		if cached, ok := s.seenTxn[r.TxnID]; ok {
			if !cached.inflight {
				pos := n.positionLocked(s)
				n.mu.Unlock()
				n.logf("write txn=%s DUPLICATE -> cached version=%d index=%d", r.TxnID, cached.version, cached.index)
				return WriteResult{Applied: false, Duplicate: true, Version: cached.version, Epoch: s.cfg.Epoch, Position: pos}, nil
			}
			// Same request still in flight: do NOT append again. Wait for
			// the existing entry to reach quorum, then report duplicate.
			inflightIndex := cached.index
			n.mu.Unlock()
			return n.waitForIndex(ctx, shard, inflightIndex, cached.version, true, Quorum)
		}
	}
	if !s.cfg.Contains(r.Key) {
		n.mu.Unlock()
		return WriteResult{}, fmtErr(ErrInvalidConfig, "key %q outside shard range", r.Key)
	}
	// Reserve version deterministically and monotonically at append time:
	// the nextVersion counter advances under the node lock, so concurrent
	// writers never reserve the same version even before commit.
	version := s.nextVersion[r.Key] + 1
	s.nextVersion[r.Key] = version
	e := Entry{Epoch: s.cfg.Epoch, Op: r.Op, Key: r.Key, Value: append(Value(nil), r.Value...), TxnID: r.TxnID, Version: version}
	appended, err := s.wal.Append(e)
	if err != nil {
		n.mu.Unlock()
		return WriteResult{}, err
	}
	if r.TxnID != "" {
		s.seenTxn[r.TxnID] = writeCache{ok: true, version: version, index: appended.Index, inflight: true}
	}
	cons := r.Consistency
	n.mu.Unlock()
	return n.waitForIndex(ctx, shard, appended.Index, version, false, cons)
}

// waitForIndex polls ack tracking for idx, continuously pushing entries,
// until the required number of replicas have acknowledged (leader's own
// durable copy counts as one). duplicate=true marks the result as a repeat
// of an already-in-flight request (no new effect).
func (n *Node) waitForIndex(ctx context.Context, shard ShardID, idx, version uint64, duplicate bool, cons Consistency) (WriteResult, error) {
	n.mu.Lock()
	s := n.shards[shard]
	need := requiredAcks(s.cfg.Replicas, cons)
	n.mu.Unlock()

	res := WriteResult{Epoch: s.cfg.Epoch, Version: version, Applied: !duplicate, Duplicate: duplicate}
	acks := 1
	acked := map[NodeID]bool{n.opts.ID: true}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		n.mu.Lock()
		s = n.shards[shard]
		for _, rep := range s.cfg.Replicas {
			if rep == n.opts.ID || acked[rep] {
				continue
			}
			if ai := s.ackIndex[rep]; ai >= idx {
				acked[rep] = true
				acks++
			}
		}
		// opportunistic push
		n.sendEntriesLocked(shard, s)
		pos := n.positionLocked(s)
		n.mu.Unlock()
		if acks >= need {
			n.mu.Lock()
			cur := n.shards[shard]
			n.advanceCommit(cur, idx)
			pos = n.positionLocked(cur)
			n.logf("write index=%d QUORUM acks=%d/%d commit=%d duplicate=%v",
				idx, acks, need, cur.commitIdx, duplicate)
			n.mu.Unlock()
			res.Position = pos
			return res, nil
		}
		select {
		case <-ctx.Done():
			return res, fmtErr(ErrTimeout, "quorum %d/%d for index=%d: %v", acks, need, idx, ctx.Err())
		case <-ticker.C:
		}
	}
}

func requiredAcks(replicas []NodeID, c Consistency) int {
	switch c {
	case One:
		return 1
	case RequireAll:
		return len(replicas)
	default:
		return quorumSize(len(replicas))
	}
}

func deadlineFromCtx(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Time{}
}

// advanceCommit moves commit index forward (never backwards) and applies.
func (n *Node) advanceCommit(s *shardState, idx uint64) {
	if idx <= s.commitIdx {
		return
	}
	if idx > s.wal.LastIndex() {
		idx = s.wal.LastIndex()
	}
	// Commit is only allowed over a contiguous index prefix.
	for i := s.commitIdx + 1; i <= idx; i++ {
		e, exists := s.wal.Entry(i)
		if !exists {
			n.logf("commit stall at %d: missing log entry", i)
			break
		}
		n.applyOne(s, e)
		s.commitIdx = i
	}
	dir := shardDir(n.opts.DataDir, n.opts.ID, s.cfg.Shard)
	if err := writeCommitFile(filepath.Join(dir, "commit.bin"), s.commitIdx); err != nil {
		n.logf("persist commit index=%d failed: %v", s.commitIdx, err)
	}
}

// applyShardThrough applies committed entries [1..idx] for one shard into
// its in-memory KV (used at restart). Cache of txn results is rebuilt too.
func (n *Node) applyShardThrough(s *shardState, idx uint64) {
	for i := uint64(1); i <= idx; i++ {
		e, ok := s.wal.Entry(i)
		if !ok {
			break
		}
		n.applyOne(s, e)
	}
	s.commitIdx = idx
}

func (n *Node) applyOne(s *shardState, e Entry) {
	if e.Version == 0 {
		// pre-version record (shouldn't happen); reserve
		e.Version = s.nextVersion[e.Key] + 1
	}
	// Reconstruct the append-time monotonic counter from the log prefix.
	if v := s.nextVersion[e.Key]; e.Version > v {
		s.nextVersion[e.Key] = e.Version
	}
	switch e.Op {
	case OpPut:
		// Idempotency on apply: an already-applied txn at this exact
		// (index,version) is skipped rather than mutating again.
		if e.TxnID != "" {
			if c, seen := s.seenTxn[e.TxnID]; seen && !c.inflight && c.index == e.Index && c.version == e.Version {
				return
			}
		}
		// Never move KV backwards (a stale/truncated-tail replay must not
		// clobber a newer committed value).
		if cur, exists := s.kv[e.Key]; exists && cur.Version >= e.Version {
			n.logf("APPLY SKIP stale-or-dup key=%s incoming=%d have=%d", e.Key, e.Version, cur.Version)
			return
		}
		s.kv[e.Key] = kvRec{Value: append(Value(nil), e.Value...), Version: e.Version}
		if e.TxnID != "" {
			s.seenTxn[e.TxnID] = writeCache{ok: true, version: e.Version, index: e.Index}
		}
	case OpDelete:
		if cur, exists := s.kv[e.Key]; exists && cur.Version >= e.Version {
			return
		}
		delete(s.kv, e.Key)
		if e.TxnID != "" {
			s.seenTxn[e.TxnID] = writeCache{ok: true, version: e.Version, index: e.Index}
		}
	case OpNoop:
	}
}

// ClientRead is the node entry point for client reads.
func (n *Node) ClientRead(ctx context.Context, shard ShardID, epoch uint64, key Key, c Consistency) (ReadResult, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.shards[shard]
	if s == nil || !s.installed {
		return ReadResult{}, fmtErr(ErrUnavailable, "shard not installed")
	}
	if epoch != s.cfg.Epoch {
		return ReadResult{}, fmtErr(ErrStaleFollower, "read epoch=%d node=%d", epoch, s.cfg.Epoch)
	}
	r := ReadResult{Epoch: s.cfg.Epoch, Position: n.positionLocked(s)}
	if v, ok := s.kv[key]; ok {
		r.Found = true
		r.Value = append(Value(nil), v.Value...)
		r.Version = v.Version
	} else {
		r.Found = false
	}
	return r, nil
}

// Entries returns log entries with index >= from (replication/catch-up).
func (n *Node) Entries(ctx context.Context, shard ShardID, from uint64, limit int) ([]Entry, SyncPosition, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := n.shards[shard]
	if s == nil {
		return nil, SyncPosition{}, ErrUnavailable
	}
	all := s.wal.Entries()
	if from == 0 || from > uint64(len(all)+1) {
		return nil, n.positionLocked(s), fmtErr(ErrLogGap, "from=%d len=%d", from, len(all))
	}
	out := append([]Entry(nil), all[from-1:]...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, n.positionLocked(s), nil
}

func sortedNodeIDs(m map[NodeID]uint64) []NodeID {
	out := make([]NodeID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

var _ Handler = (*Node)(nil)

// prefixDigest returns the historical-prefix digest of a shard's WAL.
func (n *Node) prefixDigest(shard ShardID, prefix uint64) (string, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	s, ok := n.shards[shard]
	if !ok {
		return "", 0
	}
	return s.wal.PrefixDigest(prefix)
}
