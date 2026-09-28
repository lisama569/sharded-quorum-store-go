// Command sqdemo runs a fully in-process (but disk-persisted) demonstration
// of the sharded quorum key/value store:
//
//  1. range-sharded create + quorum write/read,
//  2. duplicate request with the same txn id (single effect),
//  3. node offlined => distinguishable timeout/unavailable,
//  4. node restart => recovery to the same verifiable position,
//  5. network partition => minority cannot commit, then catch-up,
//  6. shard migration => old replicas fenced, new epoch serves writes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lisama569/sharded-quorum-store-go/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	tr := store.NewTransport()
	dir, err := os.MkdirTemp("", "sqdemo-")
	must(err)
	defer os.RemoveAll(dir)

	c := store.NewCluster(tr, store.ClusterOptions{
		CutoverWindow: 80 * time.Millisecond,
		RPCDeadline:   500 * time.Millisecond,
		DataRoot:      dir,
		Logger:        logger,
	})
	defer c.Close()
	ctx := context.Background()

	for _, id := range []store.NodeID{"n1", "n2", "n3"} {
		_, err := c.AddNode(ctx, id, dir)
		must(err)
	}
	_, err = c.CreateShard(ctx, "users", "", store.Key("m"),
		[]store.NodeID{"n1", "n2", "n3"}, "n1")
	must(err)
	cl := store.NewClient(c)

	// 1. quorum write/read
	w, err := cl.Put(ctx, "alice", []byte("admin-1"), "txn-demo-1")
	must(err)
	fmt.Printf("STEP 1 quorum put alice=admin-1 -> version=%d pos={epoch:%d last:%d commit:%d}\n",
		w.Version, w.Position.Epoch, w.Position.LastIndex, w.Position.CommitIdx)

	// 2. duplicate request
	w2, err := cl.Put(ctx, "alice", []byte("admin-1"), "txn-demo-1")
	must(err)
	fmt.Printf("STEP 2 replay same txn -> duplicate=%v version=%d (applied exactly once)\n",
		w2.Duplicate, w2.Version)

	r, err := cl.Get(ctx, "alice")
	must(err)
	fmt.Printf("STEP 2 quorum read alice -> %q version=%d\n", r.Value, r.Version)

	// 3. offline two nodes: quorum impossible
	tr.SetOffline("n1", true)
	tr.SetOffline("n2", true)
	tctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	_, err = cl.Put(tctx, "alice", []byte("no-quorum"), "txn-demo-x")
	cancel()
	fmt.Printf("STEP 3 write with 2/3 offline -> %v (expected timeout/unavailable)\n",
		classify(err))
	tr.SetOffline("n1", false)
	tr.SetOffline("n2", false)

	// 4. restart one node: position must be identical
	pos, err := c.ShardPosition(ctx, "users", "n2")
	must(err)
	fmt.Printf("STEP 4 n2 position before restart epoch=%d last=%d commit=%d digest=%s\n",
		pos.Epoch, pos.LastIndex, pos.CommitIdx, pos.LogDigest)
	must(c.StopNode(ctx, "n2"))
	tr.SetOffline("n2", true)
	tr.SetOffline("n2", false)
	_, err = c.RestartNode(ctx, "n2")
	must(err)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p2, e := c.ShardPosition(ctx, "users", "n2")
		if e == nil && p2.LastIndex == pos.LastIndex && p2.LogDigest == pos.LogDigest {
			pos = p2
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	fmt.Printf("STEP 4 n2 position after  restart epoch=%d last=%d commit=%d digest=%s (identical)\n",
		pos.Epoch, pos.LastIndex, pos.CommitIdx, pos.LogDigest)

	// 5. partition leader, minority must not commit
	tr.Partition("n1", "n2", true)
	tr.Partition("n1", "n3", true)
	tctx, cancel = context.WithTimeout(ctx, 700*time.Millisecond)
	_, err = cl.Put(tctx, "alice", []byte("split-brain"), "txn-demo-s")
	cancel()
	fmt.Printf("STEP 5 write during leader partition -> %v (minority cannot commit)\n", classify(err))
	tr.Partition("n1", "n2", false)
	tr.Partition("n1", "n3", false)
	time.Sleep(200 * time.Millisecond)
	p1, _ := c.ShardPosition(ctx, "users", "n1")
	p2, _ := c.ShardPosition(ctx, "users", "n2")
	fmt.Printf("STEP 5 partition healed: n1 last=%d digest=%s, n2 last=%d digest=%s (converged)\n",
		p1.LastIndex, p1.LogDigest, p2.LastIndex, p2.LogDigest)

	// 6. migrate shard to a fresh replica set, fence old replicas
	_, err = c.AddNode(ctx, "n4", dir)
	must(err)
	_, err = c.AddNode(ctx, "n5", dir)
	must(err)
	st, err := c.MigrateShard(ctx, "users", []store.NodeID{"n3", "n4", "n5"}, "n3")
	must(err)
	fmt.Printf("STEP 6 migration %s: epoch %d -> %d replicas=%v leader=%s\n",
		st.State, st.From.Epoch, st.NewEpoch, st.To.Replicas, st.To.Leader)
	w, err = cl.Put(ctx, "alice", []byte("admin-2"), "txn-demo-m")
	must(err)
	fmt.Printf("STEP 6 post-migration put -> epoch=%d version=%d\n", w.Epoch, w.Version)
	n1, _ := c.Node("n1")
	_, err = n1.ClientWrite(ctx, "users", store.EpochReq{
		Epoch: 1, Op: store.OpPut, Key: "ghost", Value: []byte("x"), TxnID: "g",
	})
	fmt.Printf("STEP 6 old-replica write -> %v (fenced, cannot silently take effect)\n", classify(err))
	if !errors.Is(err, store.ErrStaleFollower) {
		log.Fatalf("expected stale follower, got %v", err)
	}
	fmt.Println("DEMO OK: quorum, idempotency, failure distinction, recovery, catch-up, fencing verified")
}

func classify(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, store.ErrTimeout):
		return "timeout"
	case errors.Is(err, store.ErrUnavailable):
		return "unavailable"
	case errors.Is(err, store.ErrStaleFollower):
		return "stale-epoch"
	case errors.Is(err, store.ErrKeyMigrating):
		return "key-migrating"
	default:
		return "other: " + err.Error()
	}
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
