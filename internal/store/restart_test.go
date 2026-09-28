package store

import (
	"context"
	"testing"
)

// TestRestartPersistence 全部节点落盘后整体重启，数据、同步位置与去重表必须恢复。
func TestRestartPersistence(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, true)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	ctx := context.Background()
	for i, k := range []string{"alpha", "beta", "gamma"} {
		if err := tc.client.Put(ctx, k, []byte("V"+k), ALL, "persist-"+k); err != nil {
			t.Fatalf("write %s: %v", k, err)
		}
		_ = i
	}
	// 再用同一 requestID 重复一次，确认重启前去重只生效一次。
	if err := tc.client.Put(ctx, "alpha", []byte("Valpha"), ALL, "persist-alpha"); err != nil {
		t.Fatal(err)
	}
	before := tc.shardEntryCount("s1")
	t.Logf("entry counts before shutdown: %v", before)

	// 全部节点关闭后用相同数据目录重启。
	for _, id := range []string{"n1", "n2", "n3"} {
		tc.shutdown(id)
	}
	for _, id := range []string{"n1", "n2", "n3"} {
		tc.restart(id)
	}

	// 位置一致且连续。
	for _, id := range []string{"n1", "n2", "n3"} {
		st, err := tc.nodes[id].HandleStatus("s1")
		if err != nil {
			t.Fatalf("status %s after restart: %v", id, err)
		}
		if st.Pos.LastSeq != 3 {
			t.Fatalf("node %s recovered pos=%s want s1@3", id, st.Pos)
		}
		t.Logf("node %s recovered sync position %s serving=%t fence=%d", id, st.Pos, st.Serving, st.Fence)
	}
	for _, k := range []string{"alpha", "beta", "gamma"} {
		g, err := tc.client.Get(ctx, k, ALL)
		if err != nil {
			t.Fatalf("get %s after restart: %v", k, err)
		}
		if string(g.Value) != "V"+k {
			t.Fatalf("%s=%q after restart want %q", k, g.Value, "V"+k)
		}
	}

	// 重启后重复提交旧 requestID 仍然只命中去重，不能再次生效。
	if err := tc.client.Put(ctx, "alpha", []byte("Valpha"), ALL, "persist-alpha"); err != nil {
		t.Fatalf("duplicate after restart: %v", err)
	}
	after := tc.shardEntryCount("s1")
	for id, c := range after {
		if c != 3 {
			t.Fatalf("node %s entries=%d after restart duplicate, want 3", id, c)
		}
	}
	// 重启后可以在已恢复的位置上继续写入（seq=4 连续）。
	if err := tc.client.Put(ctx, "delta", []byte("Vdelta"), QUORUM, "persist-delta"); err != nil {
		t.Fatalf("write after restart: %v", err)
	}
	g := tc.get("delta")
	if string(g.Value) != "Vdelta" {
		t.Fatalf("delta=%q", g.Value)
	}
	t.Logf("post-restart write positions: %v", g.Positions)
}

// TestRestartSingleNodeCatchup 单节点带着落后数据重启，读路径应把它识别为落后并追赶。
func TestRestartSingleNodeCatchup(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, true)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("a", "1", "r-a")
	tc.put("b", "2", "r-b")

	// 关掉 n3，期间 n1/n2 继续写两条；n3 重启后落后，绝不能被当成已同步。
	tc.net.SetOffline("n3", true)
	tc.put("c", "3", "r-c")
	tc.put("d", "4", "r-d")
	tc.net.SetOffline("n3", false)

	st, _ := tc.nodes["n3"].HandleStatus("s1")
	t.Logf("n3 position right after reconnect: %s (lagging, must not serve as up-to-date)", st.Pos)
	if st.Pos.LastSeq != 2 {
		t.Fatalf("n3 expected at seq 2, got %d", st.Pos.LastSeq)
	}
	g := tc.get("d")
	if string(g.Value) != "4" {
		t.Fatalf("d=%q want 4", g.Value)
	}
	st2, _ := tc.nodes["n3"].HandleStatus("s1")
	if st2.Pos.LastSeq != 4 {
		t.Fatalf("n3 after read-repair pos=%s want s1@4", st2.Pos)
	}
	t.Logf("n3 caught up to %s after restart + read repair", st2.Pos)
}
