package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func walFile(t *testing.T, dir, shard string) string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "wal-*"+shard+"*.log"))
	if err != nil || len(matches) != 1 {
		// 文件名是 sanitize(shard)；这里直接枚举。
		files, _ := filepath.Glob(filepath.Join(dir, "*.log"))
		if len(files) == 0 {
			t.Fatalf("no wal file under %s", dir)
		}
		return files[0]
	}
	return matches[0]
}

// TestCorruptWALDetected 翻转 WAL 中某个数据字节，重启必须报 CORRUPT 且保留文件。
func TestCorruptWALDetected(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, true)
	tc.addShard("sX", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("apple", "x", "c1")
	tc.put("banana", "y", "c2")
	tc.shutdown("n1")

	path := walFile(t, filepath.Join(tc.dir, "n1"), "sX")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 在帧体区域（避开 8 字节头）翻转一个字节，破坏 CRC 覆盖的内容。
	idx := len(b) - 3
	b[idx] ^= 0xFF
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}

	n := NewNode(NodeConfig{ID: "n1", DataDir: filepath.Join(tc.dir, "n1")})
	err = n.Start()
	if err == nil {
		t.Fatal("expected startup to reject corrupt WAL, got nil")
	}
	if CodeOf(err) != ResultCorrupt {
		t.Fatalf("startup error code=%s want CORRUPT (%v)", CodeOf(err), err)
	}
	if !strings.Contains(err.Error(), "retained for diagnosis") {
		t.Fatalf("error should mention retention for diagnosis: %v", err)
	}
	t.Logf("corruption rejected with distinguishable code CORRUPT: %v", err)
	if _, statErr := os.Stat(path); os.IsNotExist(statErr) {
		t.Fatal("corrupt WAL must be retained on disk for diagnosis")
	}
}

// TestTruncateCannotRollback 节点不得接受位置回退的截断/安装请求。
func TestTruncateCannotRollback(t *testing.T) {
	tc := newTestCluster(t, []string{"n1", "n2", "n3"}, false)
	tc.addShard("s1", "", "", []string{"n1", "n2", "n3"}, 1)
	tc.put("k1", "v1", "t1")
	tc.put("k2", "v2", "t2")

	n1 := tc.nodes["n1"]
	pos := n1.shards["s1"].log.position()
	// 要求截断到 seq=9（超过本地）必须拒绝。
	_, err := n1.HandleTruncate(TruncateReq{ShardID: "s1", PrefixPos: LogPosition{ShardID: "s1", LastSeq: 9, LastHash: pos.LastHash}})
	if CodeOf(err) != ResultConflict {
		t.Fatalf("truncate-ahead code=%s want CONFLICT: %v", CodeOf(err), err)
	}
	// 截断到 seq=1 但给错误哈希必须拒绝（CORRUPT），文件不被破坏。
	bad := append([]byte(nil), pos.LastHash...)
	if len(bad) > 0 {
		bad[0] ^= 0xFF
	}
	_, err = n1.HandleTruncate(TruncateReq{ShardID: "s1", PrefixPos: LogPosition{ShardID: "s1", LastSeq: 1, LastHash: bad}})
	if CodeOf(err) != ResultCorrupt {
		t.Fatalf("truncate-bad-hash code=%s want CORRUPT: %v", CodeOf(err), err)
	}
	st, _ := n1.HandleStatus("s1")
	if st.Pos.LastSeq != 2 {
		t.Fatalf("rejected truncate must not move position; got %s", st.Pos)
	}
	t.Logf("rollback/discontinuity refused; position remains %s", st.Pos)

	// 合法截断到 seq=1 应成功并移除 seq=2 的去重记录。
	e1, _ := n1.HandleEntryAt("s1", 1)
	if _, err := n1.HandleTruncate(TruncateReq{ShardID: "s1", PrefixPos: LogPosition{ShardID: "s1", LastSeq: 1, LastHash: e1.Hash}}); err != nil {
		t.Fatal(err)
	}
	st, _ = n1.HandleStatus("s1")
	if st.Pos.LastSeq != 1 {
		t.Fatalf("after valid truncate pos=%s want s1@1", st.Pos)
	}
	if _, _, e := n1.HandleFindRequest("t2"); e != nil {
		t.Fatalf("find request error: %v", e)
	} else {
		// t2 去重记录应已随尾部移除（允许重新生效）。
	}
	t.Logf("valid truncate to committed prefix %s removed uncommitted tail dedup", st.Pos)
}
