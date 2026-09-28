package store

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Network 是可注入故障的节点间模拟传输层。
type Network struct {
	mu         sync.Mutex
	nodes      map[string]*Node
	offline    map[string]bool
	partitions map[string]map[string]bool // a -> set(b)：a 无法访问 b
	baseDelay  time.Duration
	shardLocks map[string]*sync.Mutex
	shardLockG sync.Mutex
}

func NewNetwork() *Network {
	return &Network{nodes: map[string]*Node{}, offline: map[string]bool{}, partitions: map[string]map[string]bool{}, shardLocks: map[string]*sync.Mutex{}}
}

// ShardLock 返回该分片的共享写定序锁。所有客户端对同一分片的写操作在该锁内
// 完成“探测 -> 定序 -> 仲裁扇出”，等价于单领导定序，从根本上消除同槽位分叉。
// 定序器是进程内共享控制面而非某个节点：节点宕机只影响仲裁票数，不影响定序。
func (net *Network) ShardLock(shard string) *sync.Mutex {
	net.shardLockG.Lock()
	defer net.shardLockG.Unlock()
	l := net.shardLocks[shard]
	if l == nil {
		l = &sync.Mutex{}
		net.shardLocks[shard] = l
	}
	return l
}

// Register 注册节点；重复注册会被拒绝，避免节点身份混淆。
func (net *Network) Register(n *Node) error {
	net.mu.Lock()
	defer net.mu.Unlock()
	if _, exists := net.nodes[n.ID()]; exists {
		return opError(ResultRejected, "net.register", fmt.Sprintf("duplicate node id %q", n.ID()))
	}
	net.nodes[n.ID()] = n
	return nil
}

// SetOffline 模拟节点宕机/重启（离线期间所有 RPC 超时）。
func (net *Network) SetOffline(id string, offline bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.offline[id] = offline
}

// SetPartition 注入或解除 a、b 之间的双向网络分区。
func (net *Network) SetPartition(a, b string, cut bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.setLinkLocked(a, b, cut)
	net.setLinkLocked(b, a, cut)
}

func (net *Network) setLinkLocked(a, b string, cut bool) {
	set := net.partitions[a]
	if set == nil {
		set = map[string]bool{}
		net.partitions[a] = set
	}
	set[b] = cut
}

// HealAll 清除全部离线与分区故障。
func (net *Network) HealAll() {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.offline = map[string]bool{}
	net.partitions = map[string]map[string]bool{}
}

func (net *Network) reachable(from, to string) bool {
	net.mu.Lock()
	defer net.mu.Unlock()
	// 只检查目标是否宕机，以及 from->to 的单向链路；
	// 发送方可能是客户端/迁移控制面等非注册身份，不应被当作离线节点。
	if net.offline[to] {
		return false
	}
	return !net.partitions[from][to]
}

// Call 在模拟传输上调用节点方法。
// 节点未注册 -> UNAVAILABLE；离线/分区 -> 等到 ctx 截止后 TIMEOUT；
// 正常情况下施加 baseDelay 以模拟传播延迟。
func (net *Network) Call(ctx context.Context, from, to string, fn func(n *Node) error) error {
	net.mu.Lock()
	node, ok := net.nodes[to]
	delay := net.baseDelay
	net.mu.Unlock()
	if !ok {
		return opError(ResultUnavailable, "net.call", fmt.Sprintf("unknown node %q", to))
	}
	if !net.reachable(from, to) {
		// 真实分区下请求会挂起直到超时，而不是得到错误应答。
		<-ctx.Done()
		return opError(ResultTimeout, "net.call",
			fmt.Sprintf("call %s -> %s unreachable (offline/partition)", from, to))
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return opError(ResultTimeout, "net.call", ctx.Err().Error())
		}
	}
	if err := ctx.Err(); err != nil {
		return opError(ResultTimeout, "net.call", err.Error())
	}
	return fn(node)
}

// ApplyShards 依据路由表快照确保各节点持有应属分片的 serving 副本。
// 仅创建缺失的副本；迁移期间的 fence 提升由 Migrator 显式完成。
func (net *Network) ApplyShards(shards []ShardSpec) error {
	net.mu.Lock()
	nodes := make(map[string]*Node, len(net.nodes))
	for id, n := range net.nodes {
		nodes[id] = n
	}
	net.mu.Unlock()
	for _, spec := range shards {
		for _, id := range spec.Members {
			n := nodes[id]
			if n == nil {
				continue
			}
			if err := n.EnsureReplica(spec.ID, spec.Fence); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetBaseDelay 配置每次调用的模拟传播延迟。
func (net *Network) SetBaseDelay(d time.Duration) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.baseDelay = d
}
