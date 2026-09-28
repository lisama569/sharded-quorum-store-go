package store

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, "s1")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	entries := []Entry{
		{Epoch: 1, Op: OpPut, Key: "a", Value: []byte("1"), TxnID: "t1"},
		{Epoch: 1, Op: OpPut, Key: "b", Value: []byte("22"), TxnID: "t2"},
		{Epoch: 1, Op: OpDelete, Key: "a", TxnID: "t3"},
	}
	for i, e := range entries {
		got, err := w.Append(e)
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if got.Index != uint64(i+1) {
			t.Fatalf("index = %d want %d", got.Index, i+1)
		}
	}
	pos := w.LastIndex()
	digest := w.LogDigest()
	chain := w.HashChain()
	t.Logf("WAL position after append: lastIndex=%d digest=%s chain=%x", pos, digest, chain[:8])
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: positions must be byte-identical.
	w2, err := OpenWAL(dir, "s1")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if w2.LastIndex() != pos {
		t.Fatalf("reopened lastIndex=%d want %d", w2.LastIndex(), pos)
	}
	if w2.LogDigest() != digest {
		t.Fatalf("digest changed after reopen: %s vs %s", w2.LogDigest(), digest)
	}
	if string(w2.HashChain()) != string(chain) {
		t.Fatalf("hash chain changed after reopen")
	}
	e2, ok := w2.Entry(2)
	if !ok || e2.Key != "b" || string(e2.Value) != "22" {
		t.Fatalf("entry2 mismatch: %+v ok=%v", e2, ok)
	}
	t.Logf("WAL recovered to identical position lastIndex=%d", w2.LastIndex())
}

func TestWALTornTailRecovered(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir, "s1")
	for i := 0; i < 3; i++ {
		_, _ = w.Append(Entry{Epoch: 1, Op: OpPut, Key: Key("k" + string(rune('a'+i))), Value: []byte("v"), TxnID: "x"})
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen, then corrupt the tail while closed.
	w, err := OpenWAL(dir, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if err := CorruptWALTail(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2, err := OpenWAL(dir, "s1")
	if err != nil {
		t.Fatalf("torn tail should be auto-truncated, got %v", err)
	}
	defer w2.Close()
	if w2.LastIndex() != 2 {
		t.Fatalf("torn tail recovery: lastIndex=%d want 2", w2.LastIndex())
	}
	t.Logf("torn-tail truncated cleanly: lastIndex=%d", w2.LastIndex())
}

func TestWALMiddleCorruptionRejected(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir, "s1")
	for i := 0; i < 4; i++ {
		_, _ = w.Append(Entry{Epoch: 1, Op: OpPut, Key: Key("k" + string(rune('a'+i))), Value: []byte("v"), TxnID: "x"})
	}
	_ = w.Close()

	w, err := OpenWAL(dir, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if err := CorruptWALMiddle(w, 2); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	_, err = OpenWAL(dir, "s1")
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("want ErrChecksumMismatch, got %v", err)
	}
	t.Logf("middle corruption rejected with distinguishable error: %v", err)
	matches, _ := filepath.Glob(filepath.Join(dir, "*.corrupt-*"))
	if len(matches) != 1 {
		t.Fatalf("quarantine file missing, got %v", matches)
	}
	t.Logf("corrupt wal quarantined for diagnosis: %s", filepath.Base(matches[0]))
}

func TestWALTruncateSuffixRebuildsChain(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir, "s1")
	for i := 0; i < 4; i++ {
		_, _ = w.Append(Entry{Epoch: 1, Op: OpNoop})
	}
	wantChainAt2 := w.HashChain()
	// chain at 4 differs
	if err := w.TruncateSuffix(2); err != nil {
		t.Fatal(err)
	}
	if w.LastIndex() != 2 {
		t.Fatalf("lastIndex=%d want 2", w.LastIndex())
	}
	// recompute expected chain from fresh re-marshal: append-only comparison.
	w2, _ := OpenWAL(t.TempDir(), "s1")
	_, _ = w2.Append(Entry{Epoch: 1, Op: OpNoop})
	_, _ = w2.Append(Entry{Epoch: 1, Op: OpNoop})
	if string(w.HashChain()) != string(w2.HashChain()) {
		t.Fatal("hash chain after truncate differs from fresh 2-entry chain")
	}
	_ = wantChainAt2
}
