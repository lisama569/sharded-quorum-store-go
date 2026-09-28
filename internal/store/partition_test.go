package store

import (
	"context"
	"testing"
)

// TestNetworkPartition 五节点三分片：分区后少数派写失败、多数派写成功，恢复后数据不丢。
func TestNetworkPartition(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3", "n4", "n5"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)

	ctx := context.Background()
	tc.put("k1", "v1", "p1")

	// n3 被与 n1/n2 隔离：以 n3 视角发起的 QUORUM 写只有自己一票。
	minority := NewClient(tc.clu, tc.net, WithLogger(t.Logf), WithTimeout(0), WithIdentity("n3"))
	tc.net.SetPartition("n3", "n1", true)
	tc.net.SetPartition("n3", "n2", true)

	err := minority.Put(ctx, "k2", []byte("v2"), QUORUM, "p2")
	if CodeOf(err) != ResultTimeout {
		t.Fatalf("minority write code=%s want TIMEOUT: %v", CodeOf(err), err)
	}
	t.Logf("minority-side quorum write correctly failed: code=%s", CodeOf(err))

	// 多数派（n1/n2）视角写成功。
	majority := NewClient(tc.clu, tc.net, WithLogger(t.Logf), WithTimeout(0), WithIdentity("n1"))
	if err := majority.Put(ctx, "k3", []byte("v3"), QUORUM, "p3"); err != nil {
		t.Fatalf("majority write: %v", err)
	}

	// 分区期间以 ONE 从 n3 读 k3 必须读不到（少数派没有该写），证明没有静默分歧。
	g, err := minority.Get(ctx, "k3", ONE)
	if err != nil {
		t.Fatalf("one-read on minority: %v", err)
	}
	if g.Found {
		t.Fatal("minority must not observe a write committed only by majority during partition")
	}
	t.Logf("minority ONE-read of majority-only key found=%t (no split-branching)", g.Found)

	// 恢复分区后，n3 通过读修复追上 k3；数据无丢失。
	tc.net.HealAll()
	g = tc.get("k3")
	if !g.Found || string(g.Value) != "v3" {
		t.Fatalf("k3 after heal found=%v value=%q", g.Found, g.Value)
	}
	st, _ := tc.nodes["n3"].HandleStatus("s1")
	if st.Pos.LastSeq != 2 {
		t.Fatalf("n3 after heal pos=%s want s1@2", st.Pos)
	}
	t.Logf("partition healed; n3 converged to %s, k3=%q", st.Pos, g.Value)
}

// TestOfflineFailover 节点离线期间仲裁写持续可用，恢复后读修复。
func TestOfflineFailover(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("a", "1", "f1")

	tc.net.SetOffline("n1", true)
	for i := 0; i < 5; i++ {
		k := string(rune('b' + i))
		if err := tc.client.Put(context.Background(), k, []byte("v"), QUORUM, "f-"+k); err != nil {
			t.Fatalf("write during n1 offline %s: %v", k, err)
		}
	}
	t.Log("quorum writes continued with n1 offline (failover via remaining replicas)")
	tc.net.SetOffline("n1", false)
	g := tc.get("f")
	if string(g.Value) != "v" {
		t.Fatalf("f=%q", g.Value)
	}
	st, _ := tc.nodes["n1"].HandleStatus("s1")
	if st.Pos.LastSeq != 6 {
		t.Fatalf("n1 after recovery pos=%s want s1@6", st.Pos)
	}
	t.Logf("n1 recovered and caught up to %s", st.Pos)
}
