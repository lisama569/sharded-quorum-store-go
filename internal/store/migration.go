package store

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"
)

// MigrationPhase 是分片迁移状态机阶段。
type MigrationPhase int

const (
	MigrateNone      MigrationPhase = iota
	MigratePreparing                // 新副本安装分片并追赶
	MigrateFrozen                   // 冻结旧属主写入
	MigrateSwitching                // 提升 fence 并切换路由
	MigrateCleanup                  // 旧副本降级丢弃
	MigrateDone
)

func (p MigrationPhase) String() string {
	names := []string{"NONE", "PREPARING", "FROZEN", "SWITCHING", "CLEANUP", "DONE"}
	if int(p) < len(names) {
		return names[p]
	}
	return "UNKNOWN"
}

// Migration 描述一次分片迁移。
type Migration struct {
	ShardID   string
	From      []string
	To        []string
	NewFence  uint64
	Phase     MigrationPhase
	StartedAt time.Time
}

// Migrator 协调迁移状态机，并对同一分片的迁移串行化。
type Migrator struct {
	cluster    *Cluster
	net        *Network
	mu         sync.Mutex
	locks      map[string]*sync.Mutex
	current    map[string]*Migration
	logger     func(string, ...any)
	rpcTimeout time.Duration // 单次迁移 RPC 的截止，离线节点快速失败而不是永久挂起
}

func NewMigrator(c *Cluster, net *Network, logger func(string, ...any)) *Migrator {
	if logger == nil {
		logger = func(string, ...any) {}
	}
	return &Migrator{cluster: c, net: net, locks: map[string]*sync.Mutex{}, current: map[string]*Migration{}, logger: logger, rpcTimeout: 300 * time.Millisecond}
}

func (m *Migrator) logf(format string, args ...any) { m.logger(fmt.Sprintf(format, args...)) }

// call 执行一次迁移 RPC，并始终施加有界截止，保证离线/分区节点快速返回 TIMEOUT。
func (m *Migrator) call(parent context.Context, to string, fn func(n *Node) error) error {
	d := m.rpcTimeout
	if d <= 0 {
		d = 300 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(parent, d)
	defer cancel()
	return m.net.Call(ctx, "migrator", to, fn)
}

func (m *Migrator) shardLock(shard string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.locks[shard]
	if l == nil {
		l = &sync.Mutex{}
		m.locks[shard] = l
	}
	return l
}

// Status 返回迁移阶段（供日志/测试诊断）。
func (m *Migrator) Status(shardID string) (MigrationPhase, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mg, ok := m.current[shardID]
	if !ok {
		return MigrateNone, false
	}
	return mg.Phase, true
}

func (m *Migrator) setPhase(mg *Migration, p MigrationPhase) {
	m.mu.Lock()
	mg.Phase = p
	m.current[mg.ShardID] = mg
	m.mu.Unlock()
	m.logf("[migration] shard=%s phase=%s fence=%d from=%v to=%v", mg.ShardID, p, mg.NewFence, mg.From, mg.To)
}

// MigrateShard 将分片成员切换为 to。同一键在迁移过程中不会被新旧分片同时接受写入。
// 前置条件无法满足（分片不存在、目标成员未知、旧仲裁不可达）时返回 UNAVAILABLE/TIMEOUT，不做部分提交。
func (m *Migrator) MigrateShard(ctx context.Context, shardID string, to []string) error {
	lock := m.shardLock(shardID)
	lock.Lock()
	defer lock.Unlock()

	spec, ok := m.cluster.Shard(shardID)
	if !ok {
		return opError(ResultUnavailable, "migration.start", fmt.Sprintf("unknown shard %q", shardID))
	}

	// 与该分片的所有客户端写共享同一把定序锁：迁移状态机执行期间，
	// 任何在途写都已完成、新的写会排队，从而“同一键不会被新旧分片同时接受写”。
	seqLock := m.net.ShardLock(shardID)
	seqLock.Lock()
	defer seqLock.Unlock()

	toSet := map[string]bool{}
	for _, id := range to {
		if !m.cluster.HasMember(id) {
			return opError(ResultUnavailable, "migration.start", fmt.Sprintf("target member %q not in cluster", id))
		}
		if toSet[id] {
			return opError(ResultRejected, "migration.start", fmt.Sprintf("duplicate target member %q", id))
		}
		toSet[id] = true
	}
	from := append([]string(nil), spec.Members...)
	newFence := spec.Fence + 1
	mg := &Migration{ShardID: shardID, From: from, To: append([]string(nil), to...), NewFence: newFence, StartedAt: time.Now()}

	// PREPARING：新副本安装（serving=false，不接受客户端写），并从旧属主追赶。
	m.setPhase(mg, MigratePreparing)
	var freshNodes []string // 仅属于新集合、不在旧集合中的节点
	for _, id := range to {
		if !contains(from, id) {
			freshNodes = append(freshNodes, id)
		}
	}
	for _, id := range freshNodes {
		err := m.call(ctx, id, func(n *Node) error { return n.InstallShard(shardID, newFence) })
		if err != nil {
			m.abort(mg)
			return err
		}
	}
	if err := m.catchup(ctx, shardID, from, to, newFence, false); err != nil {
		m.abort(mg)
		return err
	}

	// FROZEN：提升旧属主 fence 并冻结写入；此窗口内客户端写得到可区分的 UNAVAILABLE。
	m.setPhase(mg, MigrateFrozen)
	frozen := 0
	var firstErr error
	for _, id := range from {
		err := m.call(ctx, id, func(n *Node) error {
			return n.SetFrozen(shardID, newFence, true)
		})
		if err == nil {
			frozen++
		} else if firstErr == nil {
			firstErr = err
		}
	}
	// 必须能冻结旧仲裁，否则无法保证旧代际停止生效 -> 放弃迁移。
	if frozen < quorumSize(len(from)) {
		m.abort(mg)
		code := ResultUnavailable
		if firstErr != nil {
			code = CodeOf(firstErr)
		}
		return opError(code, "migration.freeze",
			fmt.Sprintf("shard %s: froze only %d/%d old replicas; migration aborted", shardID, frozen, quorumSize(len(from))))
	}

	// 最终追赶：冻结点之后不应再有写入；若出现新日志则再同步一轮并严格校验位置。
	if err := m.catchup(ctx, shardID, from, to, newFence, true); err != nil {
		m.abort(mg)
		return err
	}

	// SWITCHING：提交新路由（fence 严格递增），随后提升新副本。
	m.setPhase(mg, MigrateSwitching)
	newSpec := ShardSpec{ID: spec.ID, StartKey: spec.StartKey, EndKey: spec.EndKey, Members: append([]string(nil), to...), Fence: newFence}
	if err := m.cluster.UpdateShard(newSpec); err != nil {
		m.abort(mg)
		return err
	}
	for _, id := range to {
		err := m.call(ctx, id, func(n *Node) error {
			return n.SetServing(shardID, newFence, true)
		})
		if err != nil {
			m.logf("[migration] shard=%s WARN promote target %s failed: %v (will retry/catch-up later)", shardID, id, err)
		}
	}
	promoted, _ := m.confirmPromoted(ctx, shardID, to, newFence)
	if promoted < quorumSize(len(to)) {
		m.logf("[migration] shard=%s WARN only %d/%d new replicas serving right now", shardID, promoted, quorumSize(len(to)))
	}

	// CLEANUP：旧属主（不在新集合中）丢弃分片；fence 更高，旧代际永远无法再生效。
	m.setPhase(mg, MigrateCleanup)
	for _, id := range from {
		if toSet[id] {
			continue
		}
		err := m.call(ctx, id, func(n *Node) error {
			return n.DropShard(shardID, newFence)
		})
		if err != nil {
			m.logf("[migration] shard=%s WARN cleanup old replica %s failed: %v (its stale fence still rejects writes)", shardID, id, err)
		}
	}

	m.setPhase(mg, MigrateDone)
	m.logf("[migration] shard=%s COMPLETE fence=%d members=%v", shardID, newFence, to)
	return nil
}

func (m *Migrator) abort(mg *Migration) {
	// 路由未提交：尽力解冻旧属主并把副本 fence 回滚到旧值，恢复服务。
	oldFence := mg.NewFence - 1
	for _, id := range mg.From {
		err := m.call(context.Background(), id, func(n *Node) error {
			return n.AbortMigration(mg.ShardID, oldFence)
		})
		if err != nil {
			m.logf("[migration] shard=%s WARN rollback freeze on %s failed: %v", mg.ShardID, id, err)
		}
	}
	m.mu.Lock()
	delete(m.current, mg.ShardID)
	m.mu.Unlock()
	m.logf("[migration] shard=%s ABORTED at phase=%s (no routing change committed)", mg.ShardID, mg.Phase)
}

// confirmPromoted 统计已以 newFence 提升为 serving 的新副本数。
func (m *Migrator) confirmPromoted(ctx context.Context, shard string, to []string, fence uint64) (int, error) {
	count := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range to {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := m.call(ctx, id, func(n *Node) error {
				st, e := n.HandleStatus(shard)
				if e != nil {
					return e
				}
				if st.Serving && st.Fence == fence {
					mu.Lock()
					count++
					mu.Unlock()
				}
				return nil
			})
			if err != nil {
				m.logf("[migration] shard=%s status check %s failed: %v", shard, id, err)
			}
		}(id)
	}
	wg.Wait()
	return count, nil
}

// catchup 让 to 中落后的副本从旧仲裁的最新位置追赶。final=true 时要求收敛到完全一致。
func (m *Migrator) catchup(ctx context.Context, shard string, from, to []string, fence uint64, final bool) error {
	const maxRounds = 20
	for round := 1; round <= maxRounds; round++ {
		// 选取旧集合中可达的最高位置作为追赶源。
		srcPos, srcID, err := m.latestSource(ctx, shard, from)
		if err != nil {
			return err
		}
		m.logf("[migration] shard=%s catchup round=%d source=%s pos=%s final=%t", shard, round, srcID, srcPos, final)

		allCaught := true
		for _, id := range to {
			var local LogPosition
			err := m.call(ctx, id, func(n *Node) error {
				st, e := n.HandleStatus(shard)
				if e != nil {
					return e
				}
				local = st.Pos
				return nil
			})
			if err != nil {
				return err
			}
			if local.LastSeq >= srcPos.LastSeq && bytes.Equal(local.LastHash, srcPos.LastHash) {
				continue
			}
			allCaught = false

			var entries []LogEntry
			err = m.call(ctx, srcID, func(n *Node) error {
				var e error
				entries, e = n.HandleFetch(shard, local.LastSeq)
				return e
			})
			if err != nil {
				return err
			}
			// 源在并发写期间前进：只安装与源当前位置连续一致的前缀，剩余部分下一轮追赶。
			var batch []LogEntry
			for _, e := range entries {
				if e.Seq > srcPos.LastSeq {
					break
				}
				batch = append(batch, e)
			}
			err = m.call(ctx, id, func(n *Node) error {
				_, e := n.HandleInstallEntries(InstallReq{
					ShardID: shard, FromPos: LogPosition{ShardID: shard, LastSeq: local.LastSeq, LastHash: append([]byte(nil), local.LastHash...)},
					Entries: batch, NewFence: fence,
				})
				return e
			})
			if err != nil {
				return err
			}
		}
		if allCaught {
			m.logf("[migration] shard=%s catchup converged at %s", shard, srcPos)
			return nil
		}
		if final && round == maxRounds {
			return opError(ResultTimeout, "migration.catchup",
				fmt.Sprintf("shard %s: targets failed to converge to %s within %d rounds", shard, srcPos, maxRounds))
		}
	}
	return opError(ResultTimeout, "migration.catchup", "catchup rounds exhausted")
}

// latestSource 返回旧集合中可达的最高同步位置；要求至少一个旧仲裁级别的源。
func (m *Migrator) latestSource(ctx context.Context, shard string, from []string) (LogPosition, string, error) {
	var (
		best     LogPosition
		bestID   string
		okCount  int
		mu       sync.Mutex
		timeoutN int
	)
	var wg sync.WaitGroup
	for _, id := range from {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := m.call(ctx, id, func(n *Node) error {
				st, e := n.HandleStatus(shard)
				if e != nil {
					return e
				}
				mu.Lock()
				okCount++
				if st.Pos.LastSeq > best.LastSeq {
					best = st.Pos
					bestID = id
				}
				mu.Unlock()
				return nil
			})
			if err != nil {
				mu.Lock()
				if CodeOf(err) == ResultTimeout {
					timeoutN++
				}
				mu.Unlock()
			}
		}(id)
	}
	wg.Wait()
	if okCount < quorumSize(len(from)) {
		if timeoutN > 0 {
			return LogPosition{}, "", opError(ResultTimeout, "migration.catchup",
				fmt.Sprintf("shard %s: only %d/%d old replicas reachable before timeout", shard, okCount, quorumSize(len(from))))
		}
		return LogPosition{}, "", opError(ResultUnavailable, "migration.catchup",
			fmt.Sprintf("shard %s: only %d/%d old replicas available", shard, okCount, quorumSize(len(from))))
	}
	return best, bestID, nil
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
