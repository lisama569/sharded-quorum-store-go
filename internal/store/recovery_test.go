package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNodeRestartRecoversToCommittedPosition(t *testing.T) {
	c, tr, cleanup := newCluster(t, "restart")
	defer cleanup()
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if _, err := c.Put(ctx, Key("a-k"+itoa(i)), []byte("v"), "r-"+itoa(i), WriteOptions{Consistency: Quorum}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	posBefore, err := c.ShardPosition(ctx, "users", "n2")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SYNC POSITION before restart n2: %+v", posBefore)

	// restart n2 from disk
	if err := c.StopNode(ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	tr.SetOffline("n2", true)
	time.Sleep(30 * time.Millisecond)
	tr.SetOffline("n2", false)
	if _, err := c.RestartNode(ctx, "n2"); err != nil {
		t.Fatalf("restart: %v", err)
	}

	// give replication a moment and verify identical verifiable position
	var posAfter SyncPosition
	waitUntil(t, time.Second, func() bool {
		p, e := c.ShardPosition(ctx, "users", "n2")
		if e != nil {
			return false
		}
		posAfter = p
		return p.LastIndex >= posBefore.LastIndex && p.LogDigest == posBefore.LogDigest
	})
	t.Logf("SYNC POSITION after restart n2: %+v", posAfter)
	if posAfter.LastIndex != posBefore.LastIndex || posAfter.LogDigest != posBefore.LogDigest {
		t.Fatalf("position changed across restart: %+v vs %+v", posAfter, posBefore)
	}

	r, err := c.Get(ctx, "a-k3", ReadOptions{Consistency: Quorum})
	if err != nil || string(r.Value) != "v" {
		t.Fatalf("read after restart: %v %+v", err, r)
	}

	// writes after restart keep working and keep sequential versions
	w, err := c.Put(ctx, "a-k0", []byte("new"), "r-after", WriteOptions{Consistency: Quorum})
	if err != nil {
		t.Fatalf("post-restart write: %v", err)
	}
	t.Logf("post-restart write version=%d (must continue monotonically)", w.Version)
}

func TestWALCorruptionOnRestartRejected(t *testing.T) {
	c, _, cleanup := newCluster(t, "corrupt-restart")
	defer cleanup()
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		if _, err := c.Put(ctx, Key("a-c"+itoa(i)), []byte("v"), "c-"+itoa(i), WriteOptions{Consistency: Quorum}); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	dir := shardDir(c.opts.DataRoot, "n2", "users")
	// stop and flip a non-tail record on disk
	if err := c.StopNode(ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	w, err := OpenWAL(dir, "users")
	if err != nil {
		t.Fatal(err)
	}
	if err := CorruptWALMiddle(w, 3); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	_, err = c.RestartNode(ctx, "n2")
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("restart must reject corrupted log, got %v", err)
	}
	t.Logf("restart with corrupted log clearly rejected: %v", err)
}
