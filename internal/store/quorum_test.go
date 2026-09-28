package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestIdempotentDuplicateRequest(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)

	ctx := context.Background()
	// 同一 requestID 重复提交 5 次，只应生效一次。
	for i := 0; i < 5; i++ {
		if err := tc.client.Put(ctx, "k", []byte("v"), ALL, "dup-1"); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	for id, n := range tc.nodes {
		n.mu.RLock()
		cnt := len(n.shards["s1"].log.entries)
		n.mu.RUnlock()
		if cnt != 1 {
			t.Fatalf("node %s has %d entries, want exactly 1 (duplicate must not re-execute)", id, cnt)
		}
	}
	g := tc.get("k")
	if string(g.Value) != "v" {
		t.Fatalf("value=%q want v", g.Value)
	}
	t.Logf("sync positions after duplicate requests: %v", g.Positions)
}

func TestDistinctFailureCodes(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	ctx := context.Background()

	// 无分片拥有该键 -> UNAVAILABLE（路由层）
	err := tc.client.Put(ctx, "zzz", nil, QUORUM, "r0")
	_ = err // "zzz" 仍落在全范围分片中；改为显式校验未知键场景：
	tc2clu := NewCluster()
	c2 := NewClient(tc2clu, tc.net, WithLogger(t.Logf), WithTimeout(0))
	err = c2.Put(ctx, "zzz", []byte("x"), QUORUM, "r0b")
	if got := CodeOf(err); got != ResultUnavailable {
		t.Fatalf("no-shard write code=%s want UNAVAILABLE", got)
	}
	t.Logf("no-shard result code=%s as expected", CodeOf(err))

	// 多数派离线 -> TIMEOUT（离线在传输层表现为超时）
	tc.net.SetOffline("n2", true)
	tc.net.SetOffline("n3", true)
	err = tc.client.Put(ctx, "k1", []byte("v"), QUORUM, "r1")
	if got := CodeOf(err); got != ResultTimeout {
		t.Fatalf("minority write code=%s want TIMEOUT", got)
	}
	t.Logf("partitioned/minority quorum write code=%s as expected", CodeOf(err))

	// ONE 级别在单节点存活时仍可成功（可区分的降级行为）
	if err := tc.client.Put(ctx, "k1", []byte("v"), ONE, "r2"); err != nil {
		t.Fatalf("ONE write with single live node: %v", err)
	}
	tc.net.HealAll()
	g := tc.get("k1") // 读修复让 n2/n3 追赶
	if string(g.Value) != "v" {
		t.Fatalf("after heal k1=%q want v", g.Value)
	}
}

func TestConcurrentWritesNoLossNoDuplicate(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	ctx := context.Background()

	const writers = 20
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%02d", i)
			err := tc.client.Put(ctx, key, []byte(fmt.Sprintf("val-%d", i)), QUORUM, fmt.Sprintf("w-%d", i))
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent write: %v", err)
		}
	}

	// 触发读修复使三副本收敛，然后校验日志连续、键齐全、无重复 seq。
	for i := 0; i < writers; i++ {
		tc.get(fmt.Sprintf("key-%02d", i))
	}
	counts := tc.shardEntryCount("s1")
	t.Logf("entry counts per replica after convergence: %v", counts)
	for id, c := range counts {
		if c != writers {
			t.Fatalf("node %s has %d entries, want %d (data loss or duplicate)", id, c, writers)
		}
	}
	// 三副本位置必须一致
	g := tc.get("key-00")
	first := g.Positions[0]
	for _, p := range g.Positions[1:] {
		if p.LastSeq != first.LastSeq || string(p.LastHash) != string(first.LastHash) {
			t.Fatalf("replicas diverged: %v", g.Positions)
		}
	}
	t.Logf("all replicas converged at %s", first)
}
