package store

import (
	"context"
	"errors"
	"testing"
)

// TestPositionRegressionRejected verifies that a node refuses to install a
// shard at an older epoch than the one it holds.
func TestPositionRegressionRejected(t *testing.T) {
	c, _, cleanup := newCluster(t, "regress")
	defer cleanup()
	ctx := context.Background()
	for _, id := range []NodeID{"n4", "n5"} {
		if _, err := c.AddNode(ctx, id, c.opts.DataRoot); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.MigrateShard(ctx, "users", []NodeID{"n3", "n4", "n5"}, "n3"); err != nil {
		t.Fatal(err)
	}
	// attempt reinstall of epoch 1 config on an epoch-2 node
	n5, _ := c.Node("n5")
	old := ShardConfig{Shard: "users", Epoch: 1, Replicas: []NodeID{"n1", "n2", "n3"}, Leader: "n1"}
	err := n5.InstallShard(ctx, old, nil, 0)
	if !errors.Is(err, ErrPositionRegression) {
		t.Fatalf("install older epoch: want ErrPositionRegression, got %v", err)
	}
	t.Logf("SYNC POSITION regression refused explicitly: %v", err)
}

// TestLogGapOnCatchupRejected asks a follower to serve a FetchLog whose
// predecessor index does not exist: it must refuse with ErrLogGap rather
// than stream a misleading suffix.
func TestLogGapOnCatchupRejected(t *testing.T) {
	c, tr, cleanup := newCluster(t, "gap")
	defer cleanup()
	ctx := context.Background()
	if _, err := c.Put(ctx, "a-gap", []byte("v"), "g1", WriteOptions{Consistency: Quorum}); err != nil {
		t.Fatal(err)
	}
	// ask n2 for entries starting after an impossible predecessor index
	resp, err := tr.Send(ctx, Message{
		Kind: MsgFetchLog, To: "n2", From: "client", Shard: "users",
		Epoch: 0, FromIndex: 999,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatal("expected fetchlog rejection for missing predecessor")
	}
	if err := codeErr(resp.ErrCode, resp.RejectReason); !errors.Is(err, ErrLogGap) {
		t.Fatalf("want ErrLogGap, got %v (%s)", err, resp.RejectReason)
	}
	t.Logf("non-contiguous catch-up rejected: %v detail=%q", err, resp.RejectReason)
	_ = c
}

// TestReplicaLogCorruptionDetectedOnReplay closes a follower, corrupts a
// non-tail WAL byte, and verifies the node refuses to restart; the cluster
// surface reports the node as unavailable rather than silently diverging.
func TestReplicaLogCorruptionDetectedOnReplay(t *testing.T) {
	c, _, cleanup := newCluster(t, "diskcorrupt")
	defer cleanup()
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := c.Put(ctx, Key("a-d"+itoa(i)), []byte("v"), "d-"+itoa(i), WriteOptions{Consistency: Quorum}); err != nil {
			t.Fatal(err)
		}
	}
	dir := shardDir(c.opts.DataRoot, "n3", "users")
	if err := c.StopNode(ctx, "n3"); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(dir, "users")
	if err != nil {
		t.Fatal(err)
	}
	if err := CorruptWALMiddle(w, 2); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = c.RestartNode(ctx, "n3")
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("corrupt follower restart: want ErrChecksumMismatch, got %v", err)
	}
	t.Logf("corrupt replica rejected on replay with diagnostics: %v", err)
	// cluster still serves quorum (2/3 healthy) and does not treat n3 as synced
	r, err := c.Get(ctx, "a-d2", ReadOptions{Consistency: Quorum})
	if err != nil || !r.Found {
		t.Fatalf("healthy majority read after corrupt replica: %v %+v", err, r)
	}
	t.Logf("majority still serves data while corrupt node is excluded: version=%d", r.Version)
}
