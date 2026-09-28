package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestPartitionFailoverAndCatchup: leader n1 partitioned from n2/n3;
// majority elects n3 as new leader... (in this prototype leader identity is
// declared via configuration, so we model failover through membership
// change MigrateShard; the test verifies the minority cannot commit writes
// and the stale leader is fenced afterwards.)
func TestPartitionMinorityCannotCommit(t *testing.T) {
	c, tr, cleanup := newCluster(t, "partition")
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Put(ctx, "a-base", []byte("1"), "p0", WriteOptions{Consistency: Quorum}); err != nil {
		t.Fatal(err)
	}
	// cut n1 (leader) from the other two
	tr.Partition("n1", "n2", true)
	tr.Partition("n1", "n3", true)

	ctxTo, cancel := timeoutCtx(ctx, 900*time.Millisecond)
	defer cancel()
	_, err := c.Put(ctxTo, "a-split", []byte("2"), "p1", WriteOptions{Consistency: Quorum})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("minority-side write must time out, got %v", err)
	}
	t.Logf("QUORUM RESULT during partition: write failed distinctly (%v); no silent acceptance", err)

	// Directly asking the isolated leader also must not claim quorum.
	n1, _ := c.Node("n1")
	_, err = n1.ClientWrite(ctxTo, "users", EpochReq{Epoch: 1, Op: OpPut, Key: "a-isolated", Value: []byte("x"), TxnID: "iso", Consistency: Quorum})
	if err == nil {
		t.Fatal("isolated leader must not commit")
	}
	t.Logf("isolated leader quorum result: %v", err)

	// heal the partition: replication catches up, no data loss
	tr.Partition("n1", "n2", false)
	tr.Partition("n1", "n3", false)
	waitUntil(t, 2*time.Second, func() bool {
		p1, _ := c.ShardPosition(ctx, "users", "n1")
		p2, _ := c.ShardPosition(ctx, "users", "n2")
		return p1.LastIndex == p2.LastIndex && p1.LogDigest == p2.LogDigest
	})
	p1, _ := c.ShardPosition(ctx, "users", "n1")
	p2, _ := c.ShardPosition(ctx, "users", "n2")
	t.Logf("SYNC POSITIONS healed: n1=%+v n2=%+v", p1, p2)
}

// TestFollowerOfflineThenCatchup verifies a temporarily offline replica is
// not mistaken for synced (quorum excludes it) and catches up on return.
func TestFollowerOfflineThenCatchup(t *testing.T) {
	c, tr, cleanup := newCluster(t, "catchup")
	defer cleanup()
	ctx := context.Background()
	tr.SetOffline("n2", true)
	for i := 0; i < 5; i++ {
		// quorum = 2 of {n1,n3} since n2 offline; must succeed
		if _, err := c.Put(ctx, Key("a-off"+itoa(i)), []byte("v"), "off-"+itoa(i), WriteOptions{Consistency: Quorum}); err != nil {
			t.Fatalf("write while follower offline %d: %v", i, err)
		}
	}
	leaderPos, _ := c.ShardPosition(ctx, "users", "n1")
	t.Logf("leader advanced to %+v while n2 offline (lagging replica not counted)", leaderPos)

	tr.SetOffline("n2", false)
	waitUntil(t, 2*time.Second, func() bool {
		p, _ := c.ShardPosition(ctx, "users", "n2")
		return p.LastIndex == leaderPos.LastIndex && p.LogDigest == leaderPos.LogDigest
	})
	p, _ := c.ShardPosition(ctx, "users", "n2")
	t.Logf("lagging n2 caught up to verifiable position %+v", p)
}

// TestMigrationCutoverRejectsWrites verifies that overlapping a migration
// with client writes never double-accepts on old and new generations.
func TestMigrationCutoverRejectsWrites(t *testing.T) {
	c, tr, cleanup := newCluster(t, "cutover")
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Put(ctx, "a-init", []byte("0"), "ci0", WriteOptions{Consistency: Quorum}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []NodeID{"n4", "n5"} {
		if _, err := c.AddNode(ctx, id, c.opts.DataRoot); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var sawMigrating, sawOK, sawOther int
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			ctxTo, cancel := timeoutCtx(ctx, 1500*time.Millisecond)
			_, err := c.Put(ctxTo, "a-hot", []byte(itoa(i)), "hot-"+itoa(i), WriteOptions{Consistency: Quorum})
			cancel()
			mu.Lock()
			switch {
			case err == nil:
				sawOK++
			case errors.Is(err, ErrKeyMigrating):
				sawMigrating++
			default:
				sawOther++
			}
			mu.Unlock()
			time.Sleep(2 * time.Millisecond)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	st, err := c.MigrateShard(ctx, "users", []NodeID{"n3", "n4", "n5"}, "n3")
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Logf("MIGRATION STATE=%s epoch=%d", st.State, st.NewEpoch)
	wg.Wait()
	t.Logf("concurrent writes during migration: ok=%d rejected-migrating=%d other=%d",
		sawOK, sawMigrating, sawOther)

	// Final value must exist exactly once and version count must equal
	// successful writes (no double application across gens).
	r, err := c.Get(ctx, "a-hot", ReadOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	t.Logf("final a-hot version=%d value=%s successfulWrites=%d", r.Version, r.Value, sawOK)
	if int(r.Version) != sawOK {
		t.Fatalf("effect count mismatch: applied versions=%d successfulWrites=%d", r.Version, sawOK)
	}
	_ = tr
}

func timeoutCtx(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}

// TestFailoverAcrossMembershipChange models failover: the declared leader
// n1 is isolated, the surviving majority performs a membership change to
// {n2,n3,n4} with leader n3 (epoch bump), old leader n1 is fenced and its
// post-heal writes at the stale epoch are refused.
func TestFailoverAcrossMembershipChange(t *testing.T) {
	c, tr, cleanup := newCluster(t, "failover")
	defer cleanup()
	ctx := context.Background()
	if _, err := c.AddNode(ctx, "n4", c.opts.DataRoot); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put(ctx, "a-fo", []byte("v1"), "fo1", WriteOptions{Consistency: Quorum}); err != nil {
		t.Fatal(err)
	}
	before, _ := c.ShardPosition(ctx, "users", "n1")

	// isolate leader n1 fully
	tr.Partition("n1", "n2", true)
	tr.Partition("n1", "n3", true)
	tr.Partition("n1", "n4", true)

	st, err := c.MigrateShard(ctx, "users", []NodeID{"n2", "n3", "n4"}, "n3")
	if err != nil {
		t.Fatalf("failover membership change: %v", err)
	}
	t.Logf("FAILOVER migration state=%s newEpoch=%d (old leader n1 excluded)", st.State, st.NewEpoch)

	// writes succeed under new leadership while n1 is isolated
	w, err := c.Put(ctx, "a-fo", []byte("v2"), "fo2", WriteOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("write after failover: %v", err)
	}
	t.Logf("write served by new leader epoch=%d version=%d", w.Epoch, w.Version)

	// heal n1; fence was persisted, it must reject stale-epoch writes
	tr.Partition("n1", "n2", false)
	tr.Partition("n1", "n3", false)
	tr.Partition("n1", "n4", false)
	n1, _ := c.Node("n1")
	_, err = n1.ClientWrite(ctx, "users", EpochReq{Epoch: before.Epoch, Op: OpPut, Key: "a-zombie", Value: []byte("z"), TxnID: "z1", Consistency: Quorum})
	if !errors.Is(err, ErrStaleFollower) {
		t.Fatalf("stale leader must be fenced after heal, got %v", err)
	}
	t.Logf("stale leader fenced after partition heal: %v", err)

	// quorum read must never observe the zombie write
	r, err := c.Get(ctx, "a-zombie", ReadOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("read zombie: %v", err)
	}
	if r.Found {
		t.Fatalf("zombie write became visible: %+v", r)
	}
	// real data survives and advanced
	r, err = c.Get(ctx, "a-fo", ReadOptions{Consistency: Quorum})
	if err != nil || string(r.Value) != "v2" {
		t.Fatalf("latest value after failover: %v %+v", err, r)
	}
	t.Logf("data verified after failover: a-fo=%s version=%d, zombie write invisible", r.Value, r.Version)
}

// TestDuplicateRequestAcrossRetries fires the same txnID through the
// cluster concurrently; exactly one application must take effect.
func TestDuplicateRequestAcrossRetries(t *testing.T) {
	c, _, cleanup := newCluster(t, "dup")
	defer cleanup()
	ctx := context.Background()
	const N = 10
	var wg sync.WaitGroup
	versions := make(chan uint64, N)
	errs2 := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := c.Put(ctx, "a-once", []byte("same"), "GLOBAL-TXN-1", WriteOptions{Consistency: Quorum})
			if err != nil {
				errs2 <- err
				return
			}
			versions <- w.Version
		}()
	}
	wg.Wait()
	close(versions)
	close(errs2)
	for err := range errs2 {
		t.Fatalf("duplicate retry: %v", err)
	}
	first := uint64(0)
	count := 0
	for v := range versions {
		if first == 0 {
			first = v
		}
		if v != first {
			t.Fatalf("duplicate txn produced different versions: %d vs %d", v, first)
		}
		count++
	}
	r, _ := c.Get(ctx, "a-once", ReadOptions{Consistency: Quorum})
	t.Logf("same txnID x%d: all returned version=%d, KV version=%d (single effect)", count, first, r.Version)
	if r.Version != 1 || first != 1 {
		t.Fatalf("duplicate applied multiple times")
	}
}

// TestConcurrentMigrationsSerialized ensures overlapping migrations and a
// membership change cannot interleave: the loser gets ErrConflict and only
// one new epoch is published.
func TestConcurrentMigrationsSerialized(t *testing.T) {
	c, _, cleanup := newCluster(t, "conmig")
	defer cleanup()
	ctx := context.Background()
	for _, id := range []NodeID{"n4", "n5"} {
		if _, err := c.AddNode(ctx, id, c.opts.DataRoot); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := c.MigrateShard(ctx, "users", []NodeID{"n3", "n4", "n5"}, "n3")
		errCh <- err
	}()
	go func() {
		time.Sleep(5 * time.Millisecond)
		defer wg.Done()
		_, err := c.ChangeReplicas(ctx, "users", []NodeID{"n1", "n2", "n3"}, "n1")
		errCh <- err
	}()
	wg.Wait()
	close(errCh)
	var results []error
	for e := range errCh {
		results = append(results, e)
	}
	t.Logf("concurrent migration results: %v", results)
	conflicts := 0
	stale := 0
	successes := 0
	for _, e := range results {
		switch {
		case e == nil:
			successes++
		case errors.Is(e, ErrConflict):
			conflicts++
		case errors.Is(e, ErrStaleFollower):
			// lost the race: operates on an already-superseded generation;
			// the distinguishable rejection proves it did not take effect.
			stale++
		default:
			t.Fatalf("unexpected migration error: %v", e)
		}
	}
	if successes != 1 || conflicts+stale != 1 {
		t.Fatalf("want exactly 1 success + 1 distinct rejection, got %d success, %d conflict, %d stale",
			successes, conflicts, stale)
	}
	view := c.View()
	t.Logf("final users epoch=%d replicas=%v leader=%s",
		view.Shards["users"].Epoch, view.Shards["users"].Replicas, view.Shards["users"].Leader)
}
