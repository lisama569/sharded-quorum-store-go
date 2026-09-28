package store

import (
	"context"
	"testing"
)

type testCluster struct {
	t      *testing.T
	net    *Network
	clu    *Cluster
	dir    string
	nodes  map[string]*Node
	client *Client
}

func newTestCluster(t *testing.T, ids []string, persistent bool) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, net: NewNetwork(), clu: NewCluster(), nodes: map[string]*Node{}}
	tc.dir = t.TempDir()
	tc.clu.OnChange(func(_ uint64, shards []ShardSpec) {
		if err := tc.net.ApplyShards(shards); err != nil {
			t.Errorf("apply shards: %v", err)
		}
	})
	for _, id := range ids {
		dir := ""
		if persistent {
			dir = tc.dir + "/" + id
		}
		n := NewNode(NodeConfig{ID: id, DataDir: dir})
		if err := n.Start(); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		if err := tc.net.Register(n); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		if err := tc.clu.AddMember(id); err != nil {
			t.Fatalf("add member %s: %v", id, err)
		}
		tc.nodes[id] = n
	}
	tc.client = NewClient(tc.clu, tc.net, WithLogger(t.Logf), WithTimeout(0))
	return tc
}

func (tc *testCluster) addShard(id, start, end string, members []string, fence uint64) {
	tc.t.Helper()
	if err := tc.clu.UpdateShard(ShardSpec{ID: id, StartKey: start, EndKey: end, Members: members, Fence: fence}); err != nil {
		tc.t.Fatalf("add shard %s: %v", id, err)
	}
}

func (tc *testCluster) shutdown(id string) {
	tc.t.Helper()
	if err := tc.nodes[id].Shutdown(context.Background()); err != nil {
		tc.t.Fatalf("shutdown %s: %v", id, err)
	}
}

// restart 用同一数据目录重启节点，模拟崩溃后恢复。
func (tc *testCluster) restart(id string) {
	tc.t.Helper()
	n := NewNode(NodeConfig{ID: id, DataDir: tc.nodes[id].cfg.DataDir})
	if err := n.Start(); err != nil {
		tc.t.Fatalf("restart %s: %v", id, err)
	}
	tc.nodes[id] = n
	// 用新实例替换网络注册表中的节点。
	tc.net.mu.Lock()
	tc.net.nodes[id] = n
	tc.net.mu.Unlock()
}

func (tc *testCluster) put(key, val, req string) {
	tc.t.Helper()
	if err := tc.client.Put(context.Background(), key, []byte(val), QUORUM, req); err != nil {
		tc.t.Fatalf("put %s: %v", key, err)
	}
}

func (tc *testCluster) get(key string) GetResult {
	tc.t.Helper()
	g, err := tc.client.Get(context.Background(), key, QUORUM)
	if err != nil {
		tc.t.Fatalf("get %s: %v", key, err)
	}
	return g
}

func (tc *testCluster) shardEntryCount(shard string) map[string]int {
	out := map[string]int{}
	for id, n := range tc.nodes {
		n.mu.RLock()
		if r, ok := n.shards[shard]; ok {
			out[id] = len(r.log.entries)
		}
		n.mu.RUnlock()
	}
	return out
}
