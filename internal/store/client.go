package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Consistency 是读写一致性级别。
type Consistency int

const (
	// ONE：任一副本应答即成功（读可能读到旧值）。
	ONE Consistency = iota
	// QUORUM：多数派应答。
	QUORUM
	// ALL：全部成员应答。
	ALL
)

func (c Consistency) String() string {
	switch c {
	case ONE:
		return "ONE"
	case QUORUM:
		return "QUORUM"
	default:
		return "ALL"
	}
}

// ClientOption 配置客户端行为。
type ClientOption func(*Client)

func WithTimeout(d time.Duration) ClientOption {
	return func(cl *Client) { cl.timeout = d }
}

func WithLogger(f func(string, ...any)) ClientOption {
	return func(cl *Client) { cl.logger = f }
}

// WithIdentity 让客户端以某个节点视角接入模拟网络，
// 从而可以被 SetPartition(id, peer, true) 分区规则覆盖。
func WithIdentity(id string) ClientOption {
	return func(cl *Client) { cl.identity = id }
}

// Client 在模拟网络上执行仲裁读写。
type Client struct {
	cluster  *Cluster
	net      *Network
	timeout  time.Duration
	logger   func(string, ...any)
	identity string // 模拟网络中的客户端视角，缺省为 "client"
}

func NewClient(c *Cluster, net *Network, opts ...ClientOption) *Client {
	cl := &Client{cluster: c, net: net, timeout: 2 * time.Second, logger: func(string, ...any) {}, identity: "client"}
	for _, o := range opts {
		o(cl)
	}
	return cl
}

// writeAck 是单个副本对写请求的应答。
type writeAck struct {
	member string
	resp   WriteResp
	err    error
}

// quorumSize 返回多数派大小。
func quorumSize(n int) int { return n/2 + 1 }

func (cl *Client) need(cons Consistency, n int) int {
	switch cons {
	case ONE:
		if n > 0 {
			return 1
		}
		return 0
	case QUORUM:
		return quorumSize(n)
	default:
		return n
	}
}

func (cl *Client) logf(format string, args ...any) { cl.logger(fmt.Sprintf(format, args...)) }

// phaseTimeout 是单个 RPC 阶段（位置收集 / 扇出）的预算。
// 即便客户端未配置总超时，也必须有有界 RPC 超时，否则离线节点会让调用永久挂起。
func (cl *Client) phaseTimeout() time.Duration {
	const (
		floor   = 80 * time.Millisecond
		ceiling = 300 * time.Millisecond
	)
	var d time.Duration
	if cl.timeout <= 0 {
		d = floor
	} else {
		d = cl.timeout / 2
		if d < floor {
			d = floor
		}
	}
	if d > ceiling {
		d = ceiling
	}
	return d
}

// Put 写入键值；requestID 非空时做幂等去重，重复提交不会重复生效。
func (cl *Client) Put(ctx context.Context, key string, value []byte, cons Consistency, requestID string) error {
	return cl.write(ctx, "set", key, value, cons, requestID)
}

// Delete 删除键。
func (cl *Client) Delete(ctx context.Context, key string, cons Consistency, requestID string) error {
	return cl.write(ctx, "delete", key, nil, cons, requestID)
}

// probeResult 是单个副本对“位置 + 是否已见本请求”的探测结果。
type probeResult struct {
	member string
	pos    LogPosition
	found  *FoundEntry
	chain  map[uint64][]byte // 该副本链上 seq -> hash（用于逐槽多数派投票）
	err    error
}

// probeShard 并发探测各副本：当前位置以及是否已执行 requestID。
func (cl *Client) probeShard(ctx context.Context, self, shardID, requestID string, members []string) []probeResult {
	res := make([]probeResult, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Add(1)
		go func(i int, m string) {
			defer wg.Done()
			var pos LogPosition
			var found *FoundEntry
			var chainMap map[uint64][]byte
			err := cl.net.Call(ctx, self, m, func(n *Node) error {
				p, e := n.HandlePosition(shardID)
				if e != nil {
					return e
				}
				pos = p
				if fe, ok, e := n.HandleFindRequest(requestID); e != nil {
					return e
				} else if ok {
					found = &fe
				}
				entries, e := n.HandleFetch(shardID, 0)
				if e != nil {
					return e
				}
				chainMap = make(map[uint64][]byte, len(entries))
				for _, en := range entries {
					chainMap[en.Seq] = en.Hash
				}
				return nil
			})
			res[i] = probeResult{member: m, pos: pos, found: found, chain: chainMap, err: err}
		}(i, m)
	}
	wg.Wait()
	return res
}

// write 在分片定序锁内执行“探测 → 定序 → 仲裁扇出 → 补复制”。
// 同一分片的所有写操作全局串行定序，因此不会产生同槽位分叉；重试复用同一
// requestID，已执行的副本返回 Duplicate，保证一个请求至多生效一次。
func (cl *Client) write(ctx context.Context, op, key string, value []byte, cons Consistency, requestID string) error {
	if requestID == "" {
		requestID = newRequestID()
	}
	self := cl.identity
	const maxAttempts = 30

	// 路由解析（在锁外读取一次即可；成员变更由每次 attempt 内部刷新）。
	spec0, ok := cl.cluster.ShardForKey(key)
	if !ok {
		return opError(ResultUnavailable, "client.write", fmt.Sprintf("no shard owns key %q", key))
	}
	cl.net.ShardLock(spec0.ID).Lock()
	defer cl.net.ShardLock(spec0.ID).Unlock()

	var spec ShardSpec
	var members []string
	var need int
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var ok bool
		spec, ok = cl.cluster.ShardForKey(key)
		if !ok {
			return opError(ResultUnavailable, "client.write", fmt.Sprintf("no shard owns key %q", key))
		}
		members = append([]string(nil), spec.Members...)
		need = cl.need(cons, len(members))
		if need == 0 {
			return opError(ResultUnavailable, "client.write", fmt.Sprintf("shard %s has no members", spec.ID))
		}

		pctx, pcancel := context.WithTimeout(ctx, cl.phaseTimeout())
		probes := cl.probeShard(pctx, self, spec.ID, requestID, members)
		pcancel()

		var good []probeResult
		var timeouts int
		for _, pr := range probes {
			switch {
			case pr.err == nil:
				good = append(good, pr)
			case CodeOf(pr.err) == ResultTimeout:
				timeouts++
			}
		}
		if len(good) < need {
			if attempt < maxAttempts && (timeouts > 0 || len(good) == 0) {
				continue
			}
			if timeouts > 0 {
				return opError(ResultTimeout, "client.write",
					fmt.Sprintf("shard %s: only %d/%d replicas reachable before timeout", spec.ID, len(good), need))
			}
			return opError(ResultUnavailable, "client.write",
				fmt.Sprintf("shard %s: only %d/%d replicas available", spec.ID, len(good), need))
		}

		// 定序基线：在所有可达副本中选最高链尾（定序锁保证写入串行，链只会向前）。
		var base LogPosition
		for _, pr := range good {
			if pr.pos.LastSeq > base.LastSeq {
				base = pr.pos
			}
		}
		base.ShardID = spec.ID

		// 若本 requestID 已在某个副本执行（重试/重复提交），沿用原条目；否则分配新 seq。
		var entry LogEntry
		followed := false
		for _, pr := range good {
			if pr.found != nil {
				entry = pr.found.Entry
				followed = true
				break
			}
		}
		if !followed {
			entry = LogEntry{
				ShardID:   spec.ID,
				Seq:       base.LastSeq + 1,
				PrevHash:  append([]byte(nil), base.LastHash...),
				Op:        op,
				Key:       key,
				Value:     append([]byte(nil), value...),
				RequestID: requestID,
				Fence:     spec.Fence,
			}
		}

		wctx, wcancel := context.WithTimeout(ctx, cl.phaseTimeout())
		acks := make([]writeAck, len(members))
		var wg sync.WaitGroup
		for i, m := range members {
			wg.Add(1)
			go func(i int, m string) {
				defer wg.Done()
				var r WriteResp
				err := cl.net.Call(wctx, self, m, func(n *Node) error {
					var e error
					r, e = n.HandleWrite(WriteReq{Entry: entry})
					return e
				})
				acks[i] = writeAck{member: m, resp: r, err: err}
			}(i, m)
		}
		wg.Wait()
		wcancel()

		okCount, dupCount := 0, 0
		var conflictN, timeoutN, unavailN, rejectN int
		for _, a := range acks {
			if a.err == nil {
				okCount++
				if a.resp.Duplicate {
					dupCount++
				}
				continue
			}
			switch CodeOf(a.err) {
			case ResultConflict:
				conflictN++
			case ResultTimeout:
				timeoutN++
			case ResultUnavailable:
				unavailN++
			default:
				rejectN++
			}
		}
		cl.logf("[write %s] shard=%s key=%s req=%s fence=%d attempt=%d ack=%d/%d (exec=%d dup=%d conflict=%d timeout=%d unavail=%d reject=%d) base=%s",
			op, spec.ID, key, requestID, spec.Fence, attempt, okCount, need, okCount-dupCount, dupCount, conflictN, timeoutN, unavailN, rejectN, base)

		// 缺口/落后副本：先用已确认本条目的副本把它们连续补齐，再统计。
		if okCount < need && okCount > 0 {
			okCount = cl.healPartialWrite(ctx, spec, self, members, entry, acks, need)
		}
		if okCount >= need {
			cl.syncFollowers(ctx, spec, self, entry, acks)
			return nil
		}
		// 定序锁下不应出现同槽位冲突；若有（迁移 fence 推进等），刷新路由后重试。
		if attempt < maxAttempts && (conflictN > 0 || timeoutN > 0 || unavailN > 0) {
			select {
			case <-ctx.Done():
				return opError(ResultTimeout, "client.write", ctx.Err().Error())
			case <-time.After(time.Duration(attempt) * 5 * time.Millisecond):
			}
			continue
		}
		switch {
		case conflictN > 0:
			return opError(ResultConflict, "client.write", fmt.Sprintf("shard %s: write conflict after %d attempts", spec.ID, maxAttempts))
		case unavailN > 0:
			return opError(ResultUnavailable, "client.write", fmt.Sprintf("shard %s: %d replica(s) unavailable; acks=%d need=%d", spec.ID, unavailN, okCount, need))
		case rejectN > 0:
			return opError(ResultRejected, "client.write", fmt.Sprintf("shard %s: writes rejected (stale generation); acks=%d need=%d", spec.ID, okCount, need))
		default:
			return opError(ResultTimeout, "client.write", fmt.Sprintf("shard %s: quorum not reached acks=%d need=%d", spec.ID, okCount, need))
		}
	}
	return opError(ResultTimeout, "client.write", "retries exhausted")
}

// healPartialWrite 在“部分副本已含本请求但未达仲裁”时把其余副本补齐到同一条日志。
func (cl *Client) healPartialWrite(ctx context.Context, spec ShardSpec, self string, members []string, entry LogEntry, acks []writeAck, need int) int {
	var wg sync.WaitGroup
	for i := range acks {
		if acks[i].err == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hctx, cancel := context.WithTimeout(ctx, cl.phaseTimeout())
			defer cancel()
			if r, ok := cl.bringToEntry(hctx, spec, self, members[i], entry, acks); ok {
				acks[i] = r
			}
		}(i)
	}
	wg.Wait()
	ok := countOK(acks)
	cl.logf("[write-heal] shard=%s req=%s recovered entry seq=%d to %d/%d replicas",
		spec.ID, entry.RequestID, entry.Seq, ok, need)
	return ok
}

// syncFollowers 在提交成功后把 entry 补复制给所有未确认的健康副本。
// 返回新确认的副本数；ack 数组会被就地更新。
func (cl *Client) syncFollowers(ctx context.Context, spec ShardSpec, self string, entry LogEntry, acks []writeAck) int {
	members := make([]string, len(acks))
	for i, a := range acks {
		members[i] = a.member
	}
	before := countOK(acks)
	var wg sync.WaitGroup
	for i := range acks {
		if acks[i].err == nil {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			hctx, cancel := context.WithTimeout(ctx, cl.phaseTimeout())
			defer cancel()
			if r, ok := cl.bringToEntry(hctx, spec, self, members[i], entry, acks); ok {
				acks[i] = r
			}
		}(i)
	}
	wg.Wait()
	after := countOK(acks)
	if added := after - before; added > 0 {
		cl.logf("[sync-followers] shard=%s req=%s seq=%d post-commit replication confirmed on %d more replica(s)",
			spec.ID, entry.RequestID, entry.Seq, added)
	}
	return after - before
}

// bringToEntry 让单个目标副本收敛到包含 entry 的位置。
// 只做“连续追加”与“幂等重放”，绝不回退、绝不用更短的源链重置目标。
// 若目标在追加起点处分叉，返回 false，交由提交点截断协议在下一轮处理。
func (cl *Client) bringToEntry(ctx context.Context, spec ShardSpec, self, target string, entry LogEntry, acks []writeAck) (writeAck, bool) {
	source := ""
	for _, a := range acks {
		if a.err == nil && a.resp.Pos.LastSeq == entry.Seq {
			source = a.member
			break
		}
	}
	if source == "" {
		return writeAck{}, false
	}

	var local LogPosition
	if err := cl.net.Call(ctx, self, target, func(n *Node) error {
		st, e := n.HandleStatus(spec.ID)
		if e != nil {
			return e
		}
		local = st.Pos
		return nil
	}); err != nil {
		return writeAck{}, false
	}
	// 已含该条目：直接幂等重放（返回 Duplicate）。
	if local.LastSeq >= entry.Seq {
		var r WriteResp
		err := cl.net.Call(ctx, self, target, func(n *Node) error {
			var e error
			r, e = n.HandleWrite(WriteReq{Entry: entry})
			return e
		})
		if err == nil {
			return writeAck{member: target, resp: r}, true
		}
		return writeAck{}, false
	}

	var entries []LogEntry
	if err := cl.net.Call(ctx, self, source, func(n *Node) error {
		var e error
		entries, e = n.HandleFetch(spec.ID, local.LastSeq)
		return e
	}); err != nil {
		return writeAck{}, false
	}
	var batch []LogEntry
	for _, e := range entries {
		if e.Seq > entry.Seq {
			break
		}
		batch = append(batch, e)
	}
	if len(batch) == 0 || batch[len(batch)-1].Seq != entry.Seq {
		return writeAck{}, false
	}
	installErr := cl.net.Call(ctx, self, target, func(n *Node) error {
		_, e := n.HandleInstallEntries(InstallReq{
			ShardID:  spec.ID,
			FromPos:  LogPosition{ShardID: spec.ID, LastSeq: local.LastSeq, LastHash: append([]byte(nil), local.LastHash...)},
			Entries:  batch,
			NewFence: spec.Fence,
		})
		return e
	})
	if installErr != nil {
		// CORRUPT/CONFLICT 表示目标链在追赶点已分叉：不回退、不重置，留给截断协议处理。
		return writeAck{}, false
	}
	var r WriteResp
	err := cl.net.Call(ctx, self, target, func(n *Node) error {
		var e error
		r, e = n.HandleWrite(WriteReq{Entry: entry})
		return e
	})
	if err != nil {
		return writeAck{}, false
	}
	return writeAck{member: target, resp: r}, true
}

func ackAtSeq(acks []writeAck, seq uint64) bool {
	for _, a := range acks {
		if a.err == nil && a.resp.Pos.LastSeq == seq {
			return true
		}
	}
	return false
}

func countOK(acks []writeAck) int {
	n := 0
	for _, a := range acks {
		if a.err == nil {
			n++
		}
	}
	return n
}

// GetResult 是读取结果。
type GetResult struct {
	Value     []byte
	Found     bool
	Fence     uint64
	Responses int // 实际收集到的应答数（仲裁日志）
	Positions []LogPosition
}

type readResult struct {
	member string
	value  []byte
	found  bool
	pos    LogPosition
	err    error
}

// Get 读取键值。落后副本会被标注；同 seq 不同哈希（分叉）时按链式哈希多数派裁决。
func (cl *Client) Get(ctx context.Context, key string, cons Consistency) (GetResult, error) {
	spec, ok := cl.cluster.ShardForKey(key)
	if !ok {
		return GetResult{}, opError(ResultUnavailable, "client.get", fmt.Sprintf("no shard owns key %q", key))
	}
	members := append([]string(nil), spec.Members...)
	need := cl.need(cons, len(members))
	if need == 0 {
		return GetResult{}, opError(ResultUnavailable, "client.get", fmt.Sprintf("shard %s has no members", spec.ID))
	}
	cctx, cancel := context.WithTimeout(ctx, cl.phaseTimeout())
	defer cancel()

	results := make([]readResult, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		wg.Add(1)
		go func(i int, m string) {
			defer wg.Done()
			var v []byte
			var found bool
			var p LogPosition
			err := cl.net.Call(cctx, cl.identity, m, func(n *Node) error {
				var e error
				v, found, p, e = n.HandleRead(spec.ID, key)
				return e
			})
			results[i] = readResult{member: m, value: v, found: found, pos: p, err: err}
		}(i, m)
	}
	wg.Wait()

	var good []readResult
	var timeouts int
	for _, r := range results {
		if r.err == nil {
			good = append(good, r)
		} else if CodeOf(r.err) == ResultTimeout {
			timeouts++
		}
	}
	if len(good) < need {
		if cctx.Err() != nil || timeouts > 0 {
			return GetResult{}, opError(ResultTimeout, "client.get",
				fmt.Sprintf("shard %s: only %d/%d replicas responded before timeout", spec.ID, len(good), need))
		}
		return GetResult{}, opError(ResultUnavailable, "client.get",
			fmt.Sprintf("shard %s: only %d/%d replicas available", spec.ID, len(good), need))
	}

	// 选择最高 seq；同 seq 多哈希时通过多数派哈希裁决。
	sort.SliceStable(good, func(i, j int) bool { return good[i].pos.LastSeq > good[j].pos.LastSeq })
	top := good[0].pos.LastSeq
	leaders := []readResult{}
	for _, r := range good {
		if r.pos.LastSeq == top {
			leaders = append(leaders, r)
		}
	}
	hashVotes := map[string]int{}
	for _, r := range leaders {
		hashVotes[string(r.pos.LastHash)]++
	}
	bestHash, bestVotes := "", -1
	for h, v := range hashVotes {
		if v > bestVotes {
			bestHash, bestVotes = h, v
		}
	}
	chosen := leaders[0]
	for _, r := range leaders {
		if string(r.pos.LastHash) == bestHash {
			chosen = r
			break
		}
	}

	// 收集全部应答位置用于诊断日志：落后副本不得被当作已同步。
	positions := make([]LogPosition, 0, len(good))
	lagging := 0
	for _, r := range good {
		positions = append(positions, r.pos)
		if r.pos.LastSeq < top {
			lagging++
		}
	}
	cl.logf("[read] shard=%s key=%s cons=%s quorum=%d/%d top=%s winners=%d lagging=%d timeout=%d found=%t",
		spec.ID, key, cons, len(good), need, chosen.pos, bestVotes, lagging, timeouts, chosen.found)

	out := GetResult{
		Value: append([]byte(nil), chosen.value...), Found: chosen.found,
		Fence: spec.Fence, Responses: len(good), Positions: positions,
	}
	if len(hashVotes) > 1 {
		cl.logf("[read] shard=%s key=%s DIVERGENCE at seq=%d: %d competing chains, resolved by hash majority",
			spec.ID, key, top, len(hashVotes))
	}

	// 读修复：落后副本必须从最高位置连续补齐，追赶失败不会影响本次读结果，只记录诊断。
	if lagging > 0 {
		cl.repairLagging(cctx, spec, chosen.member, good, top)
	}
	return out, nil
}

// repairLagging 以 leader 为源，把落后副本追赶至最高位置；位置不连续时源端拒绝安装。
func (cl *Client) repairLagging(ctx context.Context, spec ShardSpec, leader string, all []readResult, top uint64) {
	var wg sync.WaitGroup
	for _, r := range all {
		if r.pos.LastSeq >= top {
			continue
		}
		wg.Add(1)
		go func(r readResult) {
			defer wg.Done()
			var entries []LogEntry
			if err := cl.net.Call(ctx, cl.identity, leader, func(n *Node) error {
				var e error
				entries, e = n.HandleFetch(spec.ID, r.pos.LastSeq)
				return e
			}); err != nil {
				cl.logf("[read-repair] shard=%s replica=%s fetch from %s failed: %v", spec.ID, r.member, leader, err)
				return
			}
			err := cl.net.Call(ctx, cl.identity, r.member, func(n *Node) error {
				_, e := n.HandleInstallEntries(InstallReq{
					ShardID:  spec.ID,
					FromPos:  LogPosition{ShardID: spec.ID, LastSeq: r.pos.LastSeq, LastHash: append([]byte(nil), r.pos.LastHash...)},
					Entries:  entries,
					NewFence: spec.Fence,
				})
				return e
			})
			if err != nil {
				cl.logf("[read-repair] shard=%s replica=%s lagging at %s NOT yet synced: %v",
					spec.ID, r.member, r.pos, err)
			} else {
				cl.logf("[read-repair] shard=%s replica=%s caught up %s -> seq=%d", spec.ID, r.member, r.pos, top)
			}
		}(r)
	}
	wg.Wait()
}

// requestID 生成。
func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func newRequestID() string {
	return fmt.Sprintf("req-%d-%d", time.Now().UnixNano(), idCounter())
}

var reqCounter struct {
	sync.Mutex
	n uint64
}

func idCounter() uint64 {
	reqCounter.Lock()
	defer reqCounter.Unlock()
	reqCounter.n++
	return reqCounter.n
}
