package store

import (
	"fmt"
	"sort"
	"sync"
)

// ShardSpec 描述一个分片的键范围、成员与代际。
type ShardSpec struct {
	ID       string
	StartKey string // 含
	EndKey   string // 不含；空字符串表示键空间最大端
	Members  []string
	Fence    uint64 // 每次成员/归属变更递增（fencing token）
	Version  uint64 // 路由表版本
}

func (s ShardSpec) ownsKey(k string) bool {
	if k < s.StartKey {
		return false
	}
	if s.EndKey != "" && k >= s.EndKey {
		return false
	}
	return true
}

// Cluster 维护成员列表与范围分片路由表。
type Cluster struct {
	mu        sync.RWMutex
	members   map[string]bool
	shards    []ShardSpec
	version   uint64
	onChanged func(version uint64, shards []ShardSpec)
}

func NewCluster() *Cluster {
	return &Cluster{members: map[string]bool{}}
}

// OnChange 注册路由表变更回调（由控制面注入，用于把分片分配下发到节点）。
func (c *Cluster) OnChange(f func(version uint64, shards []ShardSpec)) {
	c.mu.Lock()
	c.onChanged = f
	c.mu.Unlock()
}

func (c *Cluster) fireChangedLocked() {
	if c.onChanged == nil {
		return
	}
	out := make([]ShardSpec, len(c.shards))
	copy(out, c.shards)
	c.onChanged(c.version, out)
}

func (c *Cluster) AddMember(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.members[id] {
		return opError(ResultConflict, "cluster.add-member", fmt.Sprintf("member %q already exists", id))
	}
	c.members[id] = true
	c.version++
	return nil
}

func (c *Cluster) RemoveMember(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.members[id] {
		return opError(ResultUnavailable, "cluster.remove-member", fmt.Sprintf("unknown member %q", id))
	}
	for _, s := range c.shards {
		for _, m := range s.Members {
			if m == id {
				return opError(ResultConflict, "cluster.remove-member",
					fmt.Sprintf("member %q still hosts shard %s; migrate it off first", id, s.ID))
			}
		}
	}
	delete(c.members, id)
	c.version++
	return nil
}

func (c *Cluster) HasMember(id string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.members[id]
}

func (c *Cluster) Members() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]string, 0, len(c.members))
	for id := range c.members {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// UpdateShard 新增或更新分片定义；Fence 必须严格递增，防止旧配置生效。
func (c *Cluster) UpdateShard(spec ShardSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range spec.Members {
		if !c.members[m] {
			return opError(ResultUnavailable, "cluster.update-shard",
				fmt.Sprintf("shard %s references unknown member %q", spec.ID, m))
		}
	}
	for i := range c.shards {
		if c.shards[i].ID == spec.ID {
			if spec.Fence <= c.shards[i].Fence {
				return opError(ResultConflict, "cluster.update-shard",
					fmt.Sprintf("shard %s: fence regression %d -> %d refused", spec.ID, c.shards[i].Fence, spec.Fence))
			}
			spec.Version = c.version + 1
			c.shards[i] = spec
			c.version++
			c.fireChangedLocked()
			return nil
		}
	}
	spec.Version = c.version + 1
	c.shards = append(c.shards, spec)
	c.version++
	c.fireChangedLocked()
	return nil
}

// ShardForKey 按范围定位分片。
func (c *Cluster) ShardForKey(key string) (ShardSpec, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.shards {
		if s.ownsKey(key) {
			return s, true
		}
	}
	return ShardSpec{}, false
}

// Shard 返回指定分片定义。
func (c *Cluster) Shard(id string) (ShardSpec, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, s := range c.shards {
		if s.ID == id {
			return s, true
		}
	}
	return ShardSpec{}, false
}

// Snapshot 返回当前路由表快照（版本号升序拷贝）。
func (c *Cluster) Snapshot() (version uint64, shards []ShardSpec) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]ShardSpec, len(c.shards))
	copy(out, c.shards)
	sort.Slice(out, func(i, j int) bool { return out[i].StartKey < out[j].StartKey })
	return c.version, out
}
