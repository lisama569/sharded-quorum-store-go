package store

import (
	"context"
	"fmt"
	"sync"
)

// NodeConfig 是节点配置。
type NodeConfig struct {
	ID      string
	DataDir string // 持久化目录；为空则仅内存
}

// shardReplica 是节点上一个分片副本的运行时状态。
type shardReplica struct {
	log     *shardLog
	fence   uint64 // 当前分片代际；旧代际的写入必须拒绝
	serving bool
	frozen  bool              // 迁移切换窗口：拒绝写入（读仍可服务），避免新旧分片同时接受写
	state   map[string][]byte // 回放日志得到的键值状态
}

// Node 是一个独立存储节点，持有多个分片副本。
type Node struct {
	mu      sync.RWMutex
	cfg     NodeConfig
	store   *PersistentStore
	shards  map[string]*shardReplica
	dedup   map[string]dedupRecord
	started bool
}

type dedupRecord struct {
	resp WriteResp
}

func NewNode(cfg NodeConfig) *Node {
	return &Node{cfg: cfg, shards: map[string]*shardReplica{}, dedup: map[string]dedupRecord{}}
}

// Start 加载磁盘状态并使节点可服务。
// 日志不连续、位置回退或校验不一致时返回 CORRUPT 并拒绝启动（文件保留以便诊断）。
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return nil
	}
	n.store = newPersistentStore(n.cfg.DataDir)
	logs, metas, err := n.store.loadAll()
	if err != nil {
		return err
	}
	for shard, entries := range logs {
		lg, err := shardLogFromEntries(shard, entries)
		if err != nil {
			return err
		}
		r := &shardReplica{log: lg, state: map[string][]byte{}, serving: true}
		for _, e := range entries {
			applyEntry(r.state, e)
			if e.Fence > r.fence {
				r.fence = e.Fence
			}
			if e.RequestID != "" {
				n.dedup[e.RequestID] = dedupRecord{resp: WriteResp{
					Pos:      LogPosition{ShardID: shard, LastSeq: e.Seq, LastHash: append([]byte(nil), e.Hash...)},
					Executed: true,
				}}
			}
		}
		if m, ok := metas[shard]; ok {
			// 元数据 fence 不得低于日志中观察到的 fence，否则视为状态回退。
			if m.Fence < r.fence {
				return opError(ResultCorrupt, "node.start",
					fmt.Sprintf("node %s shard %s: persisted fence %d below log fence %d (regression refused)",
						n.cfg.ID, shard, m.Fence, r.fence))
			}
			r.fence = m.Fence
			r.serving = m.Serving
			r.frozen = m.Frozen
		}
		n.shards[shard] = r
	}
	n.started = true
	return nil
}

// Shutdown 刷盘并停止服务。
func (n *Node) Shutdown(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.store != nil {
		n.store.close()
	}
	n.started = false
	return nil
}

// WriteReq / WriteResp 是副本写入 RPC 的载荷。
type WriteReq struct {
	Entry LogEntry
}

type WriteResp struct {
	Pos       LogPosition
	Duplicate bool // 命中幂等去重，请求未重复生效
	Executed  bool // 本次真正执行
}

// HandleWrite 处理客户端写：fencing 校验 + 幂等去重 + WAL 追加。
func (n *Node) HandleWrite(req WriteReq) (WriteResp, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		return WriteResp{}, opError(ResultUnavailable, "node.write", "node not started")
	}
	e := req.Entry
	r, ok := n.shards[e.ShardID]
	if !ok {
		return WriteResp{}, opError(ResultUnavailable, "node.write",
			fmt.Sprintf("node %s is not a replica of shard %s", n.cfg.ID, e.ShardID))
	}
	if !r.serving {
		return WriteResp{}, opError(ResultRejected, "node.write",
			fmt.Sprintf("node %s shard %s: replica is not serving (old generation)", n.cfg.ID, e.ShardID))
	}
	if r.frozen {
		return WriteResp{}, opError(ResultUnavailable, "node.write",
			fmt.Sprintf("node %s shard %s: writes frozen during migration cutover", n.cfg.ID, e.ShardID))
	}
	if e.Fence < r.fence {
		return WriteResp{}, opError(ResultConflict, "node.write",
			fmt.Sprintf("node %s shard %s: stale fence %d (current %d), write from superseded configuration",
				n.cfg.ID, e.ShardID, e.Fence, r.fence))
	}
	if e.RequestID != "" {
		if rec, ok := n.dedup[e.RequestID]; ok {
			rec.resp.Duplicate = true
			rec.resp.Executed = false
			return rec.resp, nil
		}
	}
	pos, err := r.log.append(&e)
	if err != nil {
		return WriteResp{}, err
	}
	if err := n.store.appendEntry(e.ShardID, e); err != nil {
		r.log.undoLast()
		return WriteResp{}, err
	}
	applyEntry(r.state, e)
	if e.Fence > r.fence {
		r.fence = e.Fence
		if err := n.store.setMeta(e.ShardID, shardMeta{Fence: r.fence, Serving: r.serving}); err != nil {
			return WriteResp{}, err
		}
	}
	resp := WriteResp{Pos: pos, Executed: true}
	if e.RequestID != "" {
		n.dedup[e.RequestID] = dedupRecord{resp: resp}
	}
	return resp, nil
}

// HandleRead 在本地副本读取。
func (n *Node) HandleRead(shard, key string) (value []byte, found bool, pos LogPosition, err error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	r, ok := n.shards[shard]
	if !ok {
		return nil, false, LogPosition{}, opError(ResultUnavailable, "node.read",
			fmt.Sprintf("node %s is not a replica of shard %s", n.cfg.ID, shard))
	}
	if !r.serving {
		return nil, false, LogPosition{}, opError(ResultRejected, "node.read",
			fmt.Sprintf("node %s shard %s: replica is not serving (old generation)", n.cfg.ID, shard))
	}
	v, ok := r.state[key]
	if ok {
		value = append([]byte(nil), v...)
	}
	return value, ok, r.log.position(), nil
}

// FoundEntry 是按 requestID 查询的结果。
type FoundEntry struct {
	Entry LogEntry
	Pos   LogPosition
}

// HandleFindRequest 按幂等键查询本副本是否已执行该请求，返回对应日志条目。
func (n *Node) HandleFindRequest(requestID string) (FoundEntry, bool, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.started {
		return FoundEntry{}, false, opError(ResultUnavailable, "node.find", "node not started")
	}
	rec, ok := n.dedup[requestID]
	if !ok {
		return FoundEntry{}, false, nil
	}
	shard := rec.resp.Pos.ShardID
	r, ok := n.shards[shard]
	if !ok {
		return FoundEntry{}, false, nil
	}
	e, ok := r.log.entryAt(rec.resp.Pos.LastSeq)
	if !ok {
		return FoundEntry{}, false, nil
	}
	return FoundEntry{Entry: e, Pos: rec.resp.Pos}, true, nil
}

// ShardStatus 是副本诊断信息。
type ShardStatus struct {
	Fence   uint64
	Serving bool
	Pos     LogPosition
}

// HandleStatus 返回某分片副本的 fence、serving 与同步位置。
func (n *Node) HandleStatus(shard string) (ShardStatus, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	r, ok := n.shards[shard]
	if !ok {
		return ShardStatus{}, opError(ResultUnavailable, "node.status",
			fmt.Sprintf("node %s has no replica of shard %s", n.cfg.ID, shard))
	}
	return ShardStatus{Fence: r.fence, Serving: r.serving, Pos: r.log.position()}, nil
}

// HandlePosition 返回某分片的本地同步位置。
func (n *Node) HandlePosition(shard string) (LogPosition, error) {
	st, err := n.HandleStatus(shard)
	if err != nil {
		return LogPosition{}, err
	}
	return st.Pos, nil
}

// HandleEntryAt 返回指定序号的日志条目（用于构造截断/追赶的哈希锚点）。
func (n *Node) HandleEntryAt(shard string, seq uint64) (LogEntry, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	r, ok := n.shards[shard]
	if !ok {
		return LogEntry{}, opError(ResultUnavailable, "node.entry-at",
			fmt.Sprintf("node %s has no replica of shard %s", n.cfg.ID, shard))
	}
	e, ok := r.log.entryAt(seq)
	if !ok {
		return LogEntry{}, opError(ResultUnavailable, "node.entry-at",
			fmt.Sprintf("node %s shard %s: no entry at seq %d", n.cfg.ID, shard, seq))
	}
	return e, nil
}

// HandleFetch 返回 fromSeq 之后的日志用于副本追赶。
func (n *Node) HandleFetch(shard string, fromSeq uint64) ([]LogEntry, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	r, ok := n.shards[shard]
	if !ok {
		return nil, opError(ResultUnavailable, "node.fetch",
			fmt.Sprintf("node %s has no replica of shard %s", n.cfg.ID, shard))
	}
	return r.log.entriesAfter(fromSeq), nil
}

// InstallReq 是迁移追赶请求：要求 entries 紧接 from 之后连续到达。
type InstallReq struct {
	ShardID  string
	FromPos  LogPosition
	Entries  []LogEntry
	NewFence uint64 // 追赶完成后该副本将进入的代际
}

// HandleInstallEntries 在非 serving 的新副本上安装/追赶日志。
// 必须与本地位置连续且哈希链一致，否则明确拒绝（落后副本不得被当作已同步）。
func (n *Node) HandleInstallEntries(req InstallReq) (LogPosition, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.started {
		return LogPosition{}, opError(ResultUnavailable, "node.install", "node not started")
	}
	r := n.shards[req.ShardID]
	if r == nil {
		r = &shardReplica{log: newShardLog(req.ShardID), state: map[string][]byte{}, fence: 0, serving: false}
		n.shards[req.ShardID] = r
	}
	cur := r.log.position()
	if cur.LastSeq != req.FromPos.LastSeq ||
		!bytesEqualHash(cur.LastHash, req.FromPos.LastHash) {
		return LogPosition{}, opError(ResultConflict, "node.install",
			fmt.Sprintf("node %s shard %s: install base %s diverges from local %s",
				n.cfg.ID, req.ShardID, req.FromPos, cur))
	}
	for _, e := range req.Entries {
		ec := e
		if _, err := r.log.append(&ec); err != nil {
			return LogPosition{}, err
		}
		if err := n.store.appendEntry(req.ShardID, ec); err != nil {
			r.log.undoLast()
			return LogPosition{}, err
		}
		applyEntry(r.state, ec)
		if ec.RequestID != "" {
			// 去重记录随日志一起迁移：切换后重试旧 requestID 不会二次生效。
			n.dedup[ec.RequestID] = dedupRecord{resp: WriteResp{
				Pos:      LogPosition{ShardID: req.ShardID, LastSeq: ec.Seq, LastHash: append([]byte(nil), ec.Hash...)},
				Executed: true,
			}}
		}
	}
	if req.NewFence > r.fence {
		r.fence = req.NewFence
	}
	if err := n.store.setMeta(req.ShardID, shardMeta{Fence: r.fence, Serving: r.serving}); err != nil {
		return LogPosition{}, err
	}
	return r.log.position(), nil
}

// SetServing 提升/降级一个副本并推进 fence（fence 不得回退）。
// serving=true 同时解除迁移冻结。
func (n *Node) SetServing(shard string, fence uint64, serving bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, ok := n.shards[shard]
	if !ok {
		return opError(ResultUnavailable, "node.serving",
			fmt.Sprintf("node %s has no replica of shard %s", n.cfg.ID, shard))
	}
	if fence < r.fence {
		return opError(ResultConflict, "node.serving",
			fmt.Sprintf("node %s shard %s: fence regression %d -> %d refused", n.cfg.ID, shard, r.fence, fence))
	}
	r.fence = fence
	r.serving = serving
	if serving {
		r.frozen = false
	}
	return n.store.setMeta(shard, shardMeta{Fence: r.fence, Serving: r.serving, Frozen: r.frozen})
}

// SetFrozen 进入/退出迁移冻结：frozen=true 时写入被拒（UNAVAILABLE），读与日志抓取仍可用。
func (n *Node) SetFrozen(shard string, fence uint64, frozen bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, ok := n.shards[shard]
	if !ok {
		return opError(ResultUnavailable, "node.frozen",
			fmt.Sprintf("node %s has no replica of shard %s", n.cfg.ID, shard))
	}
	if fence < r.fence {
		return opError(ResultConflict, "node.frozen",
			fmt.Sprintf("node %s shard %s: fence regression %d -> %d refused", n.cfg.ID, shard, r.fence, fence))
	}
	r.fence = fence
	r.frozen = frozen
	return n.store.setMeta(shard, shardMeta{Fence: r.fence, Serving: r.serving, Frozen: r.frozen})
}

// EnsureReplica 确保节点持有指定分片的 serving 副本（初始配置下发使用）。
func (n *Node) EnsureReplica(shard string, fence uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if r, ok := n.shards[shard]; ok {
		if fence > r.fence {
			r.fence = fence
			return n.store.setMeta(shard, shardMeta{Fence: fence, Serving: r.serving, Frozen: r.frozen})
		}
		return nil
	}
	r := &shardReplica{log: newShardLog(shard), state: map[string][]byte{}, fence: fence, serving: true}
	n.shards[shard] = r
	return n.store.setMeta(shard, shardMeta{Fence: fence, Serving: true})
}

// InstallShard 以指定 fence 接入一个空分片副本（迁移准备阶段）。
func (n *Node) InstallShard(shardID string, fence uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.shards[shardID]; ok {
		return nil
	}
	r := &shardReplica{log: newShardLog(shardID), state: map[string][]byte{}, fence: fence, serving: false}
	n.shards[shardID] = r
	return n.store.setMeta(shardID, shardMeta{Fence: fence, Serving: false})
}

// DropShard 让旧代际副本停止生效并删除其数据。
// fence 不得低于副本已知代际（防止更旧的迁移消息误删新数据）；相等时也允许，以保证清理幂等。
func (n *Node) DropShard(shardID string, fence uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, ok := n.shards[shardID]
	if !ok {
		return nil
	}
	if fence < r.fence {
		return opError(ResultConflict, "node.drop",
			fmt.Sprintf("node %s shard %s: drop fence %d below current %d refused", n.cfg.ID, shardID, fence, r.fence))
	}
	if err := n.store.removeShard(shardID); err != nil {
		return err
	}
	delete(n.shards, shardID)
	return nil
}

// TruncateReq 要求副本把其日志截断到 prefix 所描述的已提交前缀。
type TruncateReq struct {
	ShardID   string
	PrefixPos LogPosition // 多数派确认的提交点；其后的本副本内容视为未提交分叉
}

// HandleTruncate 在副本的提交点之后丢弃未提交的分叉尾部。
// 必须严格匹配 prefix 的 (seq,hash)：任何不一致都拒绝，绝不静默截断。
// 被截断条目的 requestID 去重记录一并删除，使这些未提交请求可以在链尾重新生效一次。
func (n *Node) HandleTruncate(req TruncateReq) (LogPosition, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, ok := n.shards[req.ShardID]
	if !ok {
		return LogPosition{}, opError(ResultUnavailable, "node.truncate",
			fmt.Sprintf("node %s has no replica of shard %s", n.cfg.ID, req.ShardID))
	}
	cur := r.log.position()
	if req.PrefixPos.LastSeq > cur.LastSeq {
		return LogPosition{}, opError(ResultConflict, "node.truncate",
			fmt.Sprintf("node %s shard %s: truncate prefix %s ahead of local %s", n.cfg.ID, req.ShardID, req.PrefixPos, cur))
	}
	if req.PrefixPos.LastSeq == cur.LastSeq {
		if !bytesEqualHash(req.PrefixPos.LastHash, cur.LastHash) {
			return LogPosition{}, opError(ResultCorrupt, "node.truncate",
				fmt.Sprintf("node %s shard %s: committed-prefix hash mismatch at seq %d", n.cfg.ID, req.ShardID, cur.LastSeq))
		}
		return cur, nil
	}
	anchorEntry, ok2 := r.log.entryAt(req.PrefixPos.LastSeq)
	if !ok2 || !bytesEqualHash(anchorEntry.Hash, req.PrefixPos.LastHash) {
		return LogPosition{}, opError(ResultCorrupt, "node.truncate",
			fmt.Sprintf("node %s shard %s: local hash at seq %d diverges from quorum prefix", n.cfg.ID, req.ShardID, req.PrefixPos.LastSeq))
	}
	tail := r.log.entriesAfter(req.PrefixPos.LastSeq)
	if n := r.log.truncateFrom(req.PrefixPos.LastSeq + 1); n != len(tail) {
		return LogPosition{}, opError(ResultCorrupt, "node.truncate", "internal truncation mismatch")
	}
	// 重写状态与去重表：重放保留前缀，移除尾部请求的去重记录。
	for _, e := range tail {
		if e.RequestID != "" {
			delete(n.dedup, e.RequestID)
		}
	}
	state := map[string][]byte{}
	var maxFence uint64
	for _, e := range r.log.entries {
		applyEntry(state, e)
		if e.Fence > maxFence {
			maxFence = e.Fence
		}
	}
	r.state = state
	if r.fence < maxFence {
		r.fence = maxFence
	}
	keep := append([]LogEntry(nil), r.log.entries...)
	if err := n.store.rewriteWAL(req.ShardID, keep); err != nil {
		return LogPosition{}, err
	}
	if err := n.store.setMeta(req.ShardID, shardMeta{Fence: r.fence, Serving: r.serving, Frozen: r.frozen}); err != nil {
		return LogPosition{}, err
	}
	return r.log.position(), nil
}

// AbortMigration 在迁移中止时把副本回滚到旧 fence 并解除冻结。
// 仅用于控制面确认“新路由未提交”的场景；正常路径绝不允许 fence 回退。
func (n *Node) AbortMigration(shard string, oldFence uint64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	r, ok := n.shards[shard]
	if !ok {
		return nil
	}
	r.fence = oldFence
	r.frozen = false
	return n.store.setMeta(shard, shardMeta{Fence: r.fence, Serving: r.serving, Frozen: false})
}

// ID 返回节点 ID。
func (n *Node) ID() string { return n.cfg.ID }

func applyEntry(state map[string][]byte, e LogEntry) {
	switch e.Op {
	case "set":
		state[e.Key] = append([]byte(nil), e.Value...)
	case "delete":
		delete(state, e.Key)
	}
}

func bytesEqualHash(a, b []byte) bool {
	if len(a) != len(b) {
		return len(a) == 0 && len(b) == 0
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
