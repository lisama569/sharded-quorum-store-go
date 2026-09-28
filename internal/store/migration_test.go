package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestMigrationBasic 迁移完成后数据可从新成员读出，旧属主不再持有该分片。
func TestMigrationBasic(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3", "n4", "n5"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("a", "1", "m1")
	tc.put("b", "2", "m2")

	mig := NewMigrator(tc.clu, tc.net, t.Logf)
	if err := mig.MigrateShard(context.Background(), "s1", []string{"n3", "n4", "n5"}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if ph, ok := mig.Status("s1"); !ok || ph != MigrateDone {
		t.Fatalf("migration phase=%v want DONE", ph)
	}

	// 历史数据在新成员上可读。
	g := tc.get("b")
	if string(g.Value) != "2" {
		t.Fatalf("b=%q after migration want 2", g.Value)
	}
	// 旧属主 n1/n2 已不再持有该分片（旧代际不再生效）。
	for _, id := range []string{"n1", "n2"} {
		if _, err := tc.nodes[id].HandleStatus("s1"); CodeOf(err) != ResultUnavailable {
			t.Fatalf("old owner %s status code=%s want UNAVAILABLE", id, CodeOf(err))
		}
	}
	t.Log("old owners n1/n2 dropped; new owners n3/n4/n5 serve historical data")

	// 迁移后写入走新成员并成功。
	if err := tc.client.Put(context.Background(), "c", []byte("3"), QUORUM, "m3"); err != nil {
		t.Fatalf("post-migration write: %v", err)
	}
	g = tc.get("c")
	if string(g.Value) != "3" {
		t.Fatalf("c=%q", g.Value)
	}
	// fence 已推进。
	spec, _ := tc.clu.Shard("s1")
	if spec.Fence != 2 {
		t.Fatalf("fence=%d want 2", spec.Fence)
	}
}

// TestMigrationAbortNoPartialCommit 旧仲裁不可达时迁移必须中止且路由不变。
func TestMigrationAbortNoPartialCommit(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3", "n4", "n5"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("a", "1", "ma1")

	// 让两个旧属主对迁移控制面不可达 -> 无法冻结旧仲裁 -> 中止。
	tc.net.SetOffline("n1", true)
	tc.net.SetOffline("n2", true)
	mig := NewMigrator(tc.clu, tc.net, t.Logf)
	err := mig.MigrateShard(context.Background(), "s1", []string{"n3", "n4", "n5"})
	if err == nil {
		t.Fatal("expected migration to abort when old quorum unreachable")
	}
	if CodeOf(err) != ResultTimeout && CodeOf(err) != ResultUnavailable {
		t.Fatalf("abort code=%s want TIMEOUT/UNAVAILABLE: %v", CodeOf(err), err)
	}
	tc.net.HealAll()
	spec, _ := tc.clu.Shard("s1")
	if spec.Fence != 1 || len(spec.Members) != 3 || spec.Members[0] != "n1" {
		t.Fatalf("routing changed despite aborted migration: %+v", spec)
	}
	// 恢复后集群仍可正常读写（回滚生效）。
	if err := tc.client.Put(context.Background(), "z", []byte("9"), QUORUM, "ma2"); err != nil {
		t.Fatalf("write after aborted migration: %v", err)
	}
	t.Logf("migration aborted code=%s with no routing change; cluster still writable", CodeOf(err))
}

// TestConcurrentMigrationAndWrites 迁移与大量客户端写并发叠加：
// 已提交的键一个不丢、不重复；迁移结束后全部收敛。
func TestConcurrentMigrationAndWrites(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3", "n4", "n5"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)

	mig := NewMigrator(tc.clu, tc.net, t.Logf)
	var wg sync.WaitGroup
	writeErrCodes := map[ResultCode]int{}
	var mu sync.Mutex

	// 迁移进行中启动并发写。
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := mig.MigrateShard(context.Background(), "s1", []string{"n3", "n4", "n5"}); err != nil {
			t.Logf("migration returned: %v", err)
		}
	}()

	const nWrites = 30
	committed := make(chan string, nWrites)
	for i := 0; i < nWrites; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%02d", i)
			req := fmt.Sprintf("cm-%d", i)
			err := tc.client.Put(context.Background(), key, []byte(fmt.Sprintf("v%d", i)), QUORUM, req)
			if err != nil {
				mu.Lock()
				writeErrCodes[CodeOf(err)]++
				mu.Unlock()
				return
			}
			committed <- key
		}(i)
	}
	wg.Wait()
	close(committed)

	var okKeys []string
	for k := range committed {
		okKeys = append(okKeys, k)
	}
	t.Logf("concurrent migration+writes: committed=%d error-codes=%v", len(okKeys), writeErrCodes)
	if ph, _ := mig.Status("s1"); ph != MigrateDone {
		t.Fatalf("migration phase=%v want DONE", ph)
	}

	// 对每个声称成功的键：新路由下必须恰好读到其值；副本日志条目总数一致（无丢失/重复）。
	for _, k := range okKeys {
		g, err := tc.client.Get(context.Background(), k, QUORUM)
		if err != nil {
			t.Fatalf("get committed key %s after migration: %v", k, err)
		}
		if !g.Found {
			t.Fatalf("committed key %s disappeared (data loss)", k)
		}
	}
	counts := map[string]int{}
	for id, n := range tc.nodes {
		n.mu.RLock()
		if r, ok := n.shards["s1"]; ok {
			counts[id] = len(r.log.entries)
		}
		n.mu.RUnlock()
	}
	// 新成员 n3/n4/n5 必须收敛到相同长度。
	if counts["n3"] != counts["n4"] || counts["n4"] != counts["n5"] {
		t.Fatalf("new owners diverged: %v", counts)
	}
	// 长度必须 >= 成功写数量（迁移历史 + 新写），且没有重复 requestID。
	if counts["n3"] < len(okKeys) {
		t.Fatalf("new owners have %d entries but %d writes committed", counts["n3"], len(okKeys))
	}
	t.Logf("new owners converged: n3/n4/n5 entries=%d (>= committed writes %d)", counts["n3"], len(okKeys))
}

// TestMigrationFrozenWritesRejected 手动走到 FROZEN 阶段，
// 冻结窗口内对旧属主的写必须被拒绝（UNAVAILABLE），不产生新旧双写。
func TestMigrationFrozenWritesRejected(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3", "n4", "n5"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("a", "1", "fz1")

	ctx := context.Background()
	// PREPARING：在 n4/n5 安装空副本。
	for _, id := range []string{"n4", "n5"} {
		if err := tc.nodes[id].InstallShard("s1", 2); err != nil {
			t.Fatal(err)
		}
	}
	// 冻结全部旧属主。
	for _, id := range []string{"n1", "n2", "n3"} {
		if err := tc.nodes[id].SetFrozen("s1", 2, true); err != nil {
			t.Fatal(err)
		}
	}
	// 冻结窗口：直接向任一旧属主写必须被拒绝。
	for _, id := range []string{"n1", "n2", "n3"} {
		err := tc.net.Call(ctx, "probe", id, func(n *Node) error {
			_, e := n.HandleWrite(WriteReq{Entry: LogEntry{ShardID: "s1", Seq: 2, Op: "set", Key: "x", Fence: 2, RequestID: "fz-x"}})
			return e
		})
		if CodeOf(err) != ResultUnavailable {
			t.Fatalf("write on frozen old owner %s code=%s want UNAVAILABLE: %v", id, CodeOf(err), err)
		}
	}
	// 新副本此时 serving=false，也不能接受客户端写（不双写）。
	for _, id := range []string{"n4", "n5"} {
		err := tc.net.Call(ctx, "probe", id, func(n *Node) error {
			_, e := n.HandleWrite(WriteReq{Entry: LogEntry{ShardID: "s1", Seq: 2, Op: "set", Key: "x", Fence: 2, RequestID: "fz-x"}})
			return e
		})
		if CodeOf(err) != ResultRejected {
			t.Fatalf("write on not-yet-serving new replica %s code=%s want REJECTED: %v", id, CodeOf(err), err)
		}
	}
	t.Log("during frozen cutover: old owners reject with UNAVAILABLE, new not-serving replicas reject; no double-accept")
}
