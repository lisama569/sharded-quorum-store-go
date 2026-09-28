package store

import (
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func newTestNodes(t *testing.T, n int, logger *log.Logger) (*Transport, []*Node) {
	t.Helper()
	tr := NewTransport()
	nodes := make([]*Node, 0, n)
	dir := t.TempDir()
	for i := 0; i < n; i++ {
		id := NodeID("n" + string(rune('1'+i)))
		nd := NewNode(NodeOptions{
			ID:                id,
			DataDir:           dir,
			Transport:         tr,
			ElectionTimeout:   300 * time.Millisecond,
			HeartbeatInterval: 40 * time.Millisecond,
			Logger:            logger,
		})
		if err := nd.Start(context.Background()); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		nodes = append(nodes, nd)
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			_ = nd.Close()
		}
	})
	return tr, nodes
}

func TestNodeQuorumWriteSmoke(t *testing.T) {
	logger := log.New(os.Stdout, "    ", log.Ltime|log.Lmicroseconds)
	tr, nodes := newTestNodes(t, 3, logger)
	ids := []NodeID{"n1", "n2", "n3"}
	cfg := ShardConfig{Shard: "s1", Epoch: 1, Start: "", End: "", Replicas: ids, Leader: "n1"}
	for _, nd := range nodes {
		if err := nd.InstallShard(context.Background(), cfg, nil, 0); err != nil {
			t.Fatalf("install on %s: %v", nd.ID(), err)
		}
	}
	// n1 is configured leader; give the actor loops time to replicate.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	leader := nodes[0]
	res, err := leader.ClientWrite(ctx, "s1", EpochReq{
		Epoch: 1, Op: OpPut, Key: "color", Value: []byte("blue"), TxnID: "tx-1", Consistency: Quorum,
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Logf("QUORUM write ok: applied=%v version=%d pos=%+v", res.Applied, res.Version, res.Position)

	waitUntil(t, time.Second, func() bool {
		for _, nd := range nodes {
			pos, _ := nd.Position(ctx, "s1")
			if pos.LastIndex < 1 || pos.CommitIdx < 1 {
				return false
			}
		}
		return true
	})
	for i, nd := range nodes {
		pos, _ := nd.Position(ctx, "s1")
		kv, _ := nd.ReadOnlyKV("s1")
		t.Logf("node %s position epoch=%d last=%d commit=%d kv=%v", nd.ID(), pos.Epoch, pos.LastIndex, pos.CommitIdx, kv["color"])
		if got := kv[Key("color")].Version; got != 1 {
			t.Fatalf("node index %d version=%d want 1", i, got)
		}
	}
	// duplicate write must not apply twice
	res2, err := leader.ClientWrite(ctx, "s1", EpochReq{
		Epoch: 1, Op: OpPut, Key: "color", Value: []byte("blue"), TxnID: "tx-1", Consistency: Quorum,
	})
	if err != nil {
		t.Fatalf("dup write: %v", err)
	}
	if !res2.Duplicate || res2.Version != 1 {
		t.Fatalf("dup not recognized: %+v", res2)
	}
	_ = tr
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", d)
}
