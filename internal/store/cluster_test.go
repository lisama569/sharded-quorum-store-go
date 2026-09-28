package store

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newCluster(t *testing.T, name string) (*Cluster, *Transport, func()) {
	t.Helper()
	logger := log.New(os.Stdout, "    ", log.Ltime|log.Lmicroseconds)
	tr := NewTransport()
	dir := t.TempDir()
	c := NewCluster(tr, ClusterOptions{
		CutoverWindow: 60 * time.Millisecond,
		RPCDeadline:   400 * time.Millisecond,
		DataRoot:      dir,
		Logger:        logger,
	})
	ctx := context.Background()
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		if _, err := c.AddNode(ctx, id, dir); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	if _, err := c.CreateShard(ctx, "users", "", Key("m"), []NodeID{"n1", "n2", "n3"}, "n1"); err != nil {
		t.Fatalf("create shard: %v", err)
	}
	if _, err := c.CreateShard(ctx, "orders", Key("m"), "", []NodeID{"n1", "n2", "n3"}, "n1"); err != nil {
		t.Fatalf("create shard: %v", err)
	}
	return c, tr, func() { _ = c.Close() }
}

func TestClusterReadWriteQuorum(t *testing.T) {
	c, _, cleanup := newCluster(t, "basic")
	defer cleanup()
	ctx := context.Background()

	wres, err := c.Put(ctx, "alice", []byte("v1"), "tx-a", WriteOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	t.Logf("write result: version=%d applied=%v pos=%+v", wres.Version, wres.Applied, wres.Position)

	// duplicate request: same txnID -> same version, no second effect
	w2, err := c.Put(ctx, "alice", []byte("v1"), "tx-a", WriteOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("dup put: %v", err)
	}
	if !w2.Duplicate || w2.Version != 1 {
		t.Fatalf("duplicate mishandled: %+v", w2)
	}
	t.Logf("duplicate request: duplicate=%v version=%d (no double effect)", w2.Duplicate, w2.Version)

	r, err := c.Get(ctx, "alice", ReadOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !r.Found || string(r.Value) != "v1" || r.Version != 1 {
		t.Fatalf("get mismatch: %+v", r)
	}
	t.Logf("quorum read: value=%s version=%d pos=%+v", r.Value, r.Version, r.Position)

	// second key belongs to other shard
	if _, err := c.Put(ctx, "zed", []byte("o1"), "tx-b", WriteOptions{Consistency: Quorum}); err != nil {
		t.Fatalf("put orders: %v", err)
	}
	r2, err := c.Get(ctx, "zed", ReadOptions{Consistency: Quorum})
	if err != nil || string(r2.Value) != "o1" {
		t.Fatalf("get orders: %v %+v", err, r2)
	}
}

func TestClusterQuorumUnavailable(t *testing.T) {
	c, tr, cleanup := newCluster(t, "unavail")
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Put(ctx, "alice", []byte("v1"), "tx-1", WriteOptions{Consistency: Quorum}); err != nil {
		t.Fatalf("seed put: %v", err)
	}
	// take down 2 of 3 -> quorum impossible; partition leader too.
	tr.SetOffline("n1", true)
	tr.SetOffline("n2", true)

	ctx2, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancel()
	_, err := c.Put(ctx2, "alice", []byte("v2"), "tx-2", WriteOptions{Consistency: Quorum})
	if !errors.Is(err, ErrTimeout) && !errors.Is(err, ErrUnavailable) {
		t.Fatalf("want timeout/unavailable, got %v", err)
	}
	t.Logf("with 2/3 down write correctly failed (distinguishable): %v", err)
	// Quorum read needs responses from 2 nodes; only n3 alive -> timeout.
	_, err = c.Get(ctx2, "alice", ReadOptions{Consistency: Quorum})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("quorum read want timeout, got %v", err)
	}
	t.Logf("quorum read with insufficient nodes correctly timed out: %v", err)
}

func TestClusterMigration(t *testing.T) {
	c, tr, cleanup := newCluster(t, "migrate")
	defer cleanup()
	ctx := context.Background()
	// keys strictly before "m" belong to the users shard
	for i := 0; i < 10; i++ {
		key := Key("a-user" + itoa(i))
		if _, err := c.Put(ctx, key, []byte("v"+itoa(i)), "seed-"+itoa(i), WriteOptions{Consistency: Quorum}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	before, _ := c.ShardPosition(ctx, "users", "n1")
	t.Logf("pre-migration position on n1: %+v", before)

	// add two new nodes n4,n5 then migrate users shard to {n3,n4,n5}
	for _, id := range []NodeID{"n4", "n5"} {
		if _, err := c.AddNode(ctx, id, c.opts.DataRoot); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	_ = tr
	st, err := c.MigrateShard(ctx, "users", []NodeID{"n3", "n4", "n5"}, "n3")
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Logf("migration state=%s fromEpoch=%d newEpoch=%d synced=%+v", st.State, st.From.Epoch, st.NewEpoch, st.Synced)

	// new replicas must contain the identical historical prefix and be at
	// least one entry ahead (new-epoch marker).
	for _, r := range []NodeID{"n3", "n4", "n5"} {
		pos, perr := c.ShardPosition(ctx, "users", r)
		if perr != nil {
			t.Fatalf("pos %s: %v", r, perr)
		}
		nnode, _ := c.Node(r)
		digest, plen := nnode.prefixDigest("users", before.LastIndex)
		if plen != before.LastIndex || digest != before.LogDigest || pos.LastIndex < before.LastIndex+1 {
			t.Fatalf("replica %s prefix mismatch: digest=%s/%d pos=%+v source=%s/%d",
				r, digest, plen, pos, before.LogDigest, before.LastIndex)
		}
		t.Logf("post-migration replica=%s epoch=%d last=%d commit=%d prefixDigest=%s",
			r, pos.Epoch, pos.LastIndex, pos.CommitIdx, digest)
	}

	// write under new epoch works
	w, err := c.Put(ctx, "a-user_new", []byte("post"), "post-1", WriteOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("post migrate write: %v", err)
	}
	t.Logf("post-migration write version=%d epoch=%d", w.Version, w.Epoch)
	if w.Epoch != 2 {
		t.Fatalf("epoch=%d want 2", w.Epoch)
	}

	// old-only replica n1 must be fenced: a direct write routed to it at
	// the old epoch must be rejected with ErrStaleFollower.
	n1, _ := c.Node("n1")
	_, err = n1.ClientWrite(ctx, "users", EpochReq{Epoch: 1, Op: OpPut, Key: "a-ghost", Value: []byte("x"), TxnID: "ghost", Consistency: Quorum})
	if !errors.Is(err, ErrStaleFollower) {
		t.Fatalf("old replica write: want ErrStaleFollower, got %v", err)
	}
	t.Logf("old replica n1 fenced: %v", err)

	// data survived migration and is verifiable
	r, err := c.Get(ctx, "a-user4", ReadOptions{Consistency: Quorum})
	if err != nil || string(r.Value) != "v4" {
		t.Fatalf("data after migration: %v %+v", err, r)
	}
}

func TestClusterConcurrentWritesNoLostUpdate(t *testing.T) {
	c, _, cleanup := newCluster(t, "concurrent")
	defer cleanup()
	ctx := context.Background()
	const N = 20
	var wg sync.WaitGroup
	errs := make(chan error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Put(ctx, "counter", []byte(itoa(i)), "tx-c-"+itoa(i), WriteOptions{Consistency: Quorum})
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
	r, err := c.Get(ctx, "counter", ReadOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	n1, _ := c.Node("n1")
	kv, _ := n1.ReadOnlyKV("users")
	t.Logf("counter final version=%d (expect %d) value=%s", r.Version, N, r.Value)
	if r.Version != N {
		t.Fatalf("lost update: version=%d want %d kv=%v", r.Version, N, kv)
	}
}

func itoa(i int) string {
	return strconv.Itoa(i)
}
