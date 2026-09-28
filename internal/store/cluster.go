package store

import (
	"context"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"
)

// Cluster is the simulated cluster: it owns shard membership metadata,
// performs quorum fan-out and serializes membership changes / migrations.
type Cluster struct {
	mu          sync.Mutex
	migrationMu sync.Mutex
	transport   *Transport
	nodes       map[NodeID]*Node
	shards      map[ShardID]ShardConfig
	view        ClusterView
	migrations  map[ShardID]*MigrationStatus
	cutover     map[ShardID]bool
	opts        ClusterOptions
}

// ClusterOptions tunes the control plane.
type ClusterOptions struct {
	// CutoverWindow is how long writes are rejected with ErrKeyMigrating
	// during the atomic epoch switch.
	CutoverWindow time.Duration
	// RPCDeadline bounds each single replica RPC made by the coordinator.
	RPCDeadline time.Duration
	// DataRoot is the parent directory for per-node on-disk state.
	DataRoot string
	Logger   *log.Logger
}

// NewCluster builds an empty cluster over transport t.
func NewCluster(t *Transport, opts ClusterOptions) *Cluster {
	if opts.CutoverWindow == 0 {
		opts.CutoverWindow = 50 * time.Millisecond
	}
	if opts.RPCDeadline == 0 {
		opts.RPCDeadline = 500 * time.Millisecond
	}
	return &Cluster{
		transport:  t,
		nodes:      map[NodeID]*Node{},
		shards:     map[ShardID]ShardConfig{},
		view:       ClusterView{Shards: map[ShardID]ShardConfig{}},
		migrations: map[ShardID]*MigrationStatus{},
		cutover:    map[ShardID]bool{},
		opts:       opts,
	}
}

func (c *Cluster) logf(format string, args ...any) {
	if c.opts.Logger != nil {
		c.opts.Logger.Printf("[cluster] "+format, args...)
	}
}

// AddNode creates and starts a node on the simulated network.
func (c *Cluster) AddNode(ctx context.Context, id NodeID, dataDir string) (*Node, error) {
	c.mu.Lock()
	if _, exists := c.nodes[id]; exists {
		c.mu.Unlock()
		return nil, fmtErr(ErrInvalidConfig, "node %s already exists", id)
	}
	if dataDir == "" {
		dataDir = c.opts.DataRoot
	}
	c.mu.Unlock()
	nd := NewNode(NodeOptions{
		ID:                id,
		DataDir:           dataDir,
		Transport:         c.transport,
		ElectionTimeout:   250 * time.Millisecond,
		HeartbeatInterval: 40 * time.Millisecond,
		Logger:            c.opts.Logger,
	})
	if err := nd.Start(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.nodes[id] = nd
	c.mu.Unlock()
	return nd, nil
}

// RemoveNode stops and removes a node. It must host no shard, otherwise
// ErrInvalidConfig is returned (migrate away first).
func (c *Cluster) RemoveNode(ctx context.Context, id NodeID) error {
	c.mu.Lock()
	nd, ok := c.nodes[id]
	if !ok {
		c.mu.Unlock()
		return fmtErr(ErrInvalidConfig, "node %s unknown", id)
	}
	for sid, cfg := range c.shards {
		for _, r := range cfg.Replicas {
			if r == id {
				c.mu.Unlock()
				return fmtErr(ErrInvalidConfig, "node %s still hosts shard %s epoch=%d", id, sid, cfg.Epoch)
			}
		}
	}
	delete(c.nodes, id)
	c.mu.Unlock()
	c.transport.Unregister(id)
	return nd.Close()
}

// Node returns a registered node.
func (c *Cluster) Node(id NodeID) (*Node, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	return n, ok
}

// CreateShard defines a new key range with its first replica set.
func (c *Cluster) CreateShard(ctx context.Context, shard ShardID, start, end Key, replicas []NodeID, leader NodeID) (ShardConfig, error) {
	c.mu.Lock()
	if _, exists := c.shards[shard]; exists {
		c.mu.Unlock()
		return ShardConfig{}, fmtErr(ErrInvalidConfig, "shard %s exists", shard)
	}
	if len(replicas) == 0 {
		c.mu.Unlock()
		return ShardConfig{}, fmtErr(ErrInvalidConfig, "empty replica set")
	}
	if !containsID(replicas, leader) {
		c.mu.Unlock()
		return ShardConfig{}, fmtErr(ErrInvalidConfig, "leader %s not in replicas", leader)
	}
	for _, r := range replicas {
		if _, ok := c.nodes[r]; !ok {
			c.mu.Unlock()
			return ShardConfig{}, fmtErr(ErrInvalidConfig, "replica %s not in cluster", r)
		}
	}
	if len(start) > 0 && len(end) > 0 && start >= end {
		c.mu.Unlock()
		return ShardConfig{}, fmtErr(ErrInvalidConfig, "empty range [%q,%q)", start, end)
	}
	cfg := ShardConfig{Shard: shard, Epoch: 1, Start: start, End: end, Replicas: append([]NodeID(nil), replicas...), Leader: leader}
	nodeSet := c.nodes
	c.mu.Unlock()

	for _, r := range cfg.Replicas {
		if err := c.sendAdmin(ctx, nodeSet[r], Message{
			Kind: MsgAdminInstallShard, Shard: shard, Epoch: 1, Config: cfg,
		}); err != nil {
			return ShardConfig{}, err
		}
	}
	c.mu.Lock()
	c.shards[shard] = cfg
	c.view.Shards[shard] = cfg
	c.mu.Unlock()
	c.logf("CREATE shard=%s range=[%q,%q) replicas=%v leader=%s", shard, start, end, replicas, leader)
	return cfg, nil
}

func containsID(xs []NodeID, x NodeID) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

func (c *Cluster) sendAdmin(ctx context.Context, n *Node, msg Message) error {
	if n == nil {
		return fmtErr(ErrUnavailable, "node missing")
	}
	msg.To = n.ID()
	msg.From = "coordinator"
	rctx, cancel := context.WithTimeout(ctx, c.opts.RPCDeadline)
	defer cancel()
	resp, err := c.transport.Send(rctx, msg)
	if err != nil {
		return err
	}
	if !resp.OK {
		return codeErr(resp.ErrCode, resp.RejectReason)
	}
	return nil
}

// View returns a snapshot copy of the current shard metadata.
func (c *Cluster) View() ClusterView {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := ClusterView{Shards: map[ShardID]ShardConfig{}}
	for k, v := range c.shards {
		v.Replicas = append([]NodeID(nil), v.Replicas...)
		out.Shards[k] = v
	}
	return out
}

// Route returns the shard owning key k.
func (c *Cluster) Route(k Key) (ShardConfig, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var hits []ShardConfig
	for _, cfg := range c.shards {
		if cfg.Contains(k) {
			hits = append(hits, cfg)
		}
	}
	switch len(hits) {
	case 0:
		return ShardConfig{}, fmtErr(ErrUnavailable, "no shard owns key %q", k)
	case 1:
		return hits[0], nil
	default:
		sort.Slice(hits, func(i, j int) bool { return hits[i].Shard < hits[j].Shard })
		return ShardConfig{}, fmtErr(ErrInvalidConfig, "overlapping shards for key %q: %v", k, []ShardID{hits[0].Shard, hits[1].Shard})
	}
}

// Put performs a replicated put with the requested consistency.
func (c *Cluster) Put(ctx context.Context, key Key, val Value, txnID string, w WriteOptions) (WriteResult, error) {
	return c.write(ctx, OpPut, key, val, txnID, w)
}

// Delete performs a replicated delete.
func (c *Cluster) Delete(ctx context.Context, key Key, txnID string, w WriteOptions) (WriteResult, error) {
	return c.write(ctx, OpDelete, key, nil, txnID, w)
}

func (c *Cluster) write(ctx context.Context, op OpKind, key Key, val Value, txnID string, w WriteOptions) (WriteResult, error) {
	if txnID == "" {
		txnID = NewTxnID()
	}
	cfg, err := c.Route(key)
	if err != nil {
		return WriteResult{}, err
	}
	if c.inCutover(cfg.Shard) {
		return WriteResult{}, fmtErr(ErrKeyMigrating, "shard %s cutover", cfg.Shard)
	}
	c.mu.Lock()
	c.mu.Unlock()

	msg := Message{
		Kind:        MsgClientWrite,
		To:          cfg.Leader,
		From:        "client",
		Shard:       cfg.Shard,
		Epoch:       cfg.Epoch,
		Op:          op,
		Key:         key,
		Value:       val,
		TxnID:       txnID,
		Consistency: w.Consistency,
	}
	rctx, cancel := context.WithTimeout(ctx, c.opts.RPCDeadline*3)
	defer cancel()
	resp, err := c.transport.Send(rctx, msg)
	if err == nil && !resp.OK {
		err = codeErr(resp.ErrCode, resp.RejectReason)
	}
	if err != nil {
		// Stale epoch: refresh routing once and retry with the same txnID.
		if isStaleEpoch(err) {
			if newCfg, rerr := c.Route(key); rerr == nil && newCfg.Epoch != cfg.Epoch {
				c.logf("write epoch=%d stale, refreshing to epoch=%d", cfg.Epoch, newCfg.Epoch)
				return c.write(ctx, op, key, val, txnID, w)
			}
		}
		return WriteResult{}, err
	}
	c.logf("WRITE key=%q op=%d txn=%s epoch=%d version=%d duplicate=%v quorum-pos=%+v",
		key, op, txnID, cfg.Epoch, resp.Version, resp.Duplicate, resp.Pos)
	return WriteResult{
		Applied:   !resp.Duplicate,
		Version:   resp.Version,
		Duplicate: resp.Duplicate,
		Epoch:     cfg.Epoch,
		Position:  resp.Pos,
	}, nil
}

func isStaleEpoch(err error) bool {
	return errIs(err, ErrStaleFollower)
}

func errIs(err, target error) bool {
	type unwrapper interface{ Unwrap() error }
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func (c *Cluster) inCutover(s ShardID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cutover[s]
}

// Get reads with the requested consistency.
func (c *Cluster) Get(ctx context.Context, key Key, r ReadOptions) (ReadResult, error) {
	cfg, err := c.Route(key)
	if err != nil {
		return ReadResult{}, err
	}
	c.mu.Lock()
	replicas := append([]NodeID(nil), cfg.Replicas...)
	nodes := c.nodes
	c.mu.Unlock()

	need := 1
	if r.Consistency == Quorum {
		need = quorumSize(len(replicas))
	} else if r.Consistency == RequireAll {
		need = len(replicas)
	}

	type vote struct {
		r    ReadResult
		node NodeID
		err  error
	}
	votesCh := make(chan vote, len(replicas))
	for _, rep := range replicas {
		go func(rep NodeID) {
			n := nodes[rep]
			if n == nil {
				votesCh <- vote{node: rep, err: ErrUnavailable}
				return
			}
			rctx, cancel := context.WithTimeout(ctx, c.opts.RPCDeadline)
			defer cancel()
			resp, err := c.transport.Send(rctx, Message{
				Kind: MsgClientRead, To: rep, From: "client", Shard: cfg.Shard, Epoch: cfg.Epoch, Key: key,
			})
			if err != nil {
				votesCh <- vote{node: rep, err: err}
				return
			}
			if !resp.OK {
				votesCh <- vote{node: rep, err: codeErr(resp.ErrCode, resp.RejectReason)}
				return
			}
			votesCh <- vote{node: rep, r: ReadResult{
				Found: resp.Found, Value: resp.Value, Version: resp.Version,
				Epoch: cfg.Epoch, Position: resp.Pos,
			}}
		}(rep)
	}
	var collected []vote
	okCount := 0
	deadline := ctxDeadline(ctx, c.opts.RPCDeadline)
	for okCount < need {
		if time.Now().After(deadline) {
			break
		}
		left := len(replicas) - len(collected)
		if left == 0 {
			break
		}
		select {
		case v := <-votesCh:
			collected = append(collected, v)
			if v.err == nil {
				okCount++
			}
		case <-ctx.Done():
			return ReadResult{}, fmtErr(ErrTimeout, "read: %v", ctx.Err())
		case <-time.After(time.Until(deadline)):
		}
	}
	if okCount < need {
		return ReadResult{}, fmtErr(ErrTimeout, "read quorum %d/%d", okCount, need)
	}

	// Reconcile: group successful reads by (found, version, value); pick
	// the highest-version value that has >= need votes.
	type cell struct {
		found   bool
		version uint64
		value   string
		r       ReadResult
	}
	groups := map[string]int{}
	// cellKey identifies one observed value for reconciliation.
	type cellT struct {
		k string
		r ReadResult
	}
	var sample cellT
	sampleCount := 0
	put := func(rr ReadResult) {
		k := cellKeyOf(rr)
		groups[k]++
		if groups[k] > sampleCount {
			sampleCount = groups[k]
			sample = cellT{k: k, r: rr}
		}
	}
	_ = put
	for _, v := range collected {
		if v.err != nil {
			continue
		}
		put(v.r)
	}
	if sampleCount < need {
		return ReadResult{}, fmtErr(ErrTimeout, "read disagreement: max %d/%d agree", sampleCount, need)
	}
	c.logf("READ key=%q found=%v version=%d quorum=%d/%d pos=%+v",
		key, sample.r.Found, sample.r.Version, sampleCount, need, sample.r.Position)
	return sample.r, nil
}

func cellKeyOf(r ReadResult) string {
	return boolStr(r.Found) + ":" + u64str(r.Version) + ":" + string(r.Value)
}

func boolStr(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func u64str(u uint64) string {
	return strconv.FormatUint(u, 10)
}

func ctxDeadline(ctx context.Context, fallback time.Duration) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(fallback)
}

// Close stops every node.
func (c *Cluster) Close() error {
	c.mu.Lock()
	nodes := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	c.nodes = map[NodeID]*Node{}
	c.mu.Unlock()
	var firstErr error
	for _, n := range nodes {
		if err := n.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// StopNode closes a node but keeps it out of c.nodes while retaining its
// on-disk state, so RestartNode can recover it to the same position.
func (c *Cluster) StopNode(ctx context.Context, id NodeID) error {
	c.mu.Lock()
	nd, ok := c.nodes[id]
	if !ok {
		c.mu.Unlock()
		return fmtErr(ErrInvalidConfig, "node %s unknown", id)
	}
	delete(c.nodes, id)
	c.mu.Unlock()
	c.transport.Unregister(id)
	if err := nd.Close(); err != nil {
		return err
	}
	c.logf("node %s stopped (state retained on disk)", id)
	return nil
}

// RestartNode re-creates a stopped node and replays its durable log to the
// last committed position.
func (c *Cluster) RestartNode(ctx context.Context, id NodeID) (*Node, error) {
	return c.AddNode(ctx, id, c.opts.DataRoot)
}
