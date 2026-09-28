package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// LogEntry 是某个分片上的一条日志条目。
type LogEntry struct {
	ShardID   string
	Seq       uint64 // 该分片内单调递增、从 1 开始且连续
	PrevHash  []byte
	Hash      []byte // 覆盖本条内容与 PrevHash 的校验值
	Op        string // set / delete
	Key       string
	Value     []byte
	RequestID string // 幂等去重键
	Fence     uint64 // 分片代际（迁移 fencing token）
}

// LogPosition 描述一个副本在某分片上的同步位置。
type LogPosition struct {
	ShardID  string
	LastSeq  uint64
	LastHash []byte
}

func (p LogPosition) String() string {
	return fmt.Sprintf("%s@%d", p.ShardID, p.LastSeq)
}

// shardLog 是单个分片副本的追加式内存日志；持久化由 PersistentStore 负责。
type shardLog struct {
	mu      sync.Mutex
	shardID string
	entries []LogEntry
}

func newShardLog(shardID string) *shardLog {
	return &shardLog{shardID: shardID}
}

func shardLogFromEntries(shardID string, in []LogEntry) (*shardLog, error) {
	l := &shardLog{shardID: shardID}
	var prev []byte
	for i, e := range in {
		if e.ShardID != shardID {
			return nil, opError(ResultCorrupt, "log.recover",
				fmt.Sprintf("shard %s: entry seq %d belongs to shard %q", shardID, e.Seq, e.ShardID))
		}
		wantSeq := uint64(i + 1)
		if e.Seq != wantSeq {
			return nil, opError(ResultCorrupt, "log.recover",
				fmt.Sprintf("shard %s: non-contiguous sequence at seq %d (expected %d)", shardID, e.Seq, wantSeq))
		}
		if !bytes.Equal(e.PrevHash, prev) {
			return nil, opError(ResultCorrupt, "log.recover",
				fmt.Sprintf("shard %s: prev-hash mismatch at seq %d (log discontinuous)", shardID, e.Seq))
		}
		got := hashEntry(e, e.PrevHash)
		if !bytes.Equal(got, e.Hash) {
			return nil, opError(ResultCorrupt, "log.recover",
				fmt.Sprintf("shard %s: checksum mismatch at seq %d", shardID, e.Seq))
		}
		prev = e.Hash
		l.entries = append(l.entries, e)
	}
	return l, nil
}

// append 校验并追加一条日志，并就地写回 e.Hash。
// 序号回退/跳跃、代际回退、prevHash 不符、校验不一致都会被明确拒绝。
func (l *shardLog) append(e *LogEntry) (LogPosition, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if e.ShardID != l.shardID {
		return LogPosition{}, opError(ResultRejected, "log.append",
			fmt.Sprintf("entry for shard %q sent to log %q", e.ShardID, l.shardID))
	}
	last := len(l.entries)
	wantSeq := uint64(last + 1)
	var prev []byte
	if last > 0 {
		prev = l.entries[last-1].Hash
		if e.Fence < l.entries[last-1].Fence {
			return LogPosition{}, opError(ResultRejected, "log.append",
				fmt.Sprintf("shard %s: fence regression %d -> %d at seq %d",
					l.shardID, l.entries[last-1].Fence, e.Fence, e.Seq))
		}
	}
	if e.Seq < wantSeq {
		return LogPosition{}, opError(ResultConflict, "log.append",
			fmt.Sprintf("shard %s: position regression seq %d (already at %d)", l.shardID, e.Seq, wantSeq-1))
	}
	if e.Seq > wantSeq {
		return LogPosition{}, opError(ResultUnavailable, "log.append",
			fmt.Sprintf("shard %s: gap before seq %d (expected %d)", l.shardID, e.Seq, wantSeq))
	}
	if !bytes.Equal(e.PrevHash, prev) {
		return LogPosition{}, opError(ResultCorrupt, "log.append",
			fmt.Sprintf("shard %s: prev-hash mismatch at seq %d (log discontinuous)", l.shardID, e.Seq))
	}
	e.Hash = hashEntry(*e, e.PrevHash)
	l.entries = append(l.entries, *e)
	return LogPosition{ShardID: l.shardID, LastSeq: e.Seq, LastHash: e.Hash}, nil
}

// truncateFrom 删除序号 >= fromSeq 的全部尾部条目，返回删除条数。
// 仅在确认这些条目属于竞争失败的分叉尾部时，由控制路径（客户端仲裁修复）调用。
func (l *shardLog) truncateFrom(fromSeq uint64) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if fromSeq == 0 || fromSeq > uint64(len(l.entries)) {
		return 0
	}
	removed := len(l.entries) - int(fromSeq) + 1
	l.entries = l.entries[:fromSeq-1]
	return removed
}

// rewriteWAL 用当前内存条目重写 WAL（先写临时文件再原子替换）。
func (ps *PersistentStore) rewriteWAL(shard string, entries []LogEntry) error {
	if !ps.enabled() {
		return nil
	}
	ps.mu.Lock()
	if f, ok := ps.files[shard]; ok {
		_ = f.Close()
		delete(ps.files, shard)
	}
	path := ps.walPath(shard)
	tmp := path + ".rewrite"
	ps.mu.Unlock()
	f, err := openWALFile(tmp)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := writeWALFrame(f, e); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return opError(ResultRejected, "wal.rewrite.fsync", err.Error())
	}
	if err := f.Close(); err != nil {
		return opError(ResultRejected, "wal.rewrite.close", err.Error())
	}
	return renameWAL(tmp, path)
}

// undoLast 回滚内存中的最后一条（持久化失败时使用）。
func (l *shardLog) undoLast() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := len(l.entries); n > 0 {
		l.entries = l.entries[:n-1]
	}
}

func (l *shardLog) entriesAfter(seq uint64) []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq >= uint64(len(l.entries)) {
		return nil
	}
	out := make([]LogEntry, len(l.entries)-int(seq))
	copy(out, l.entries[seq:])
	return out
}

func (l *shardLog) entryAt(seq uint64) (LogEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq == 0 || int(seq) > len(l.entries) {
		return LogEntry{}, false
	}
	return l.entries[seq-1], true
}

func (l *shardLog) position() LogPosition {
	l.mu.Lock()
	defer l.mu.Unlock()
	p := LogPosition{ShardID: l.shardID}
	if n := len(l.entries); n > 0 {
		p.LastSeq = l.entries[n-1].Seq
		p.LastHash = append([]byte(nil), l.entries[n-1].Hash...)
	}
	return p
}

// hashEntry 计算条目内容与前驱哈希的链式 SHA-256。
func hashEntry(e LogEntry, prev []byte) []byte {
	h := newLogHasher()
	h.writeString(e.ShardID)
	h.writeUvarint(e.Seq)
	h.writeBytes(prev)
	h.writeString(e.Op)
	h.writeString(e.Key)
	h.writeBytes(e.Value)
	h.writeString(e.RequestID)
	h.writeUvarint(e.Fence)
	return h.sum()
}

// ---------------------------------------------------------------------------
// 持久化：每个分片一个 WAL 帧文件，帧格式 [CRC32(4)][Len(4)][gob LogEntry]；
// 另存 meta.gob 记录每个分片的 fence 与 serving 状态。
// ---------------------------------------------------------------------------

type shardMeta struct {
	Fence   uint64
	Serving bool
	Frozen  bool
}

type PersistentStore struct {
	dir   string
	mu    sync.Mutex
	files map[string]*os.File
	meta  map[string]shardMeta
}

func newPersistentStore(dir string) *PersistentStore {
	return &PersistentStore{dir: dir, files: map[string]*os.File{}, meta: map[string]shardMeta{}}
}

func (ps *PersistentStore) enabled() bool { return ps.dir != "" }

func (ps *PersistentStore) metaPath() string { return filepath.Join(ps.dir, "meta.gob") }
func (ps *PersistentStore) walPath(shard string) string {
	return filepath.Join(ps.dir, "wal-"+sanitize(shard)+".log")
}

func sanitize(s string) string {
	r := strings.NewReplacer("/", "_", string(os.PathSeparator), "_", "..", "__")
	return r.Replace(s)
}

// loadAll 读取全部 WAL 与元数据；CRC/解码失败立即返回 CORRUPT 并保留现场文件。
func (ps *PersistentStore) loadAll() (map[string][]LogEntry, map[string]shardMeta, error) {
	logs := map[string][]LogEntry{}
	if !ps.enabled() {
		return logs, ps.meta, nil
	}
	if err := os.MkdirAll(ps.dir, 0o755); err != nil {
		return nil, nil, opError(ResultRejected, "store.open", err.Error())
	}

	// 元数据可选（首次启动可能不存在）。
	if b, err := os.ReadFile(ps.metaPath()); err == nil {
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&ps.meta); err != nil {
			ps.meta = map[string]shardMeta{}
		}
	} else if !os.IsNotExist(err) {
		return nil, nil, opError(ResultRejected, "store.meta", err.Error())
	}

	files, err := filepath.Glob(filepath.Join(ps.dir, "wal-*.log"))
	if err != nil {
		return nil, nil, opError(ResultRejected, "store.glob", err.Error())
	}
	sort.Strings(files)
	for _, f := range files {
		nameKey := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "wal-"), ".log")
		entries, err := readWAL(f)
		if err != nil {
			return nil, nil, err // 文件保留在磁盘上以便诊断
		}
		shard := nameKey
		if len(entries) > 0 {
			shard = entries[0].ShardID
		}
		logs[shard] = entries
	}
	return logs, ps.meta, nil
}

func readWAL(path string) ([]LogEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, opError(ResultRejected, "wal.open", err.Error())
	}
	defer f.Close()

	var entries []LogEntry
	var offset int64
	for {
		header := make([]byte, 8)
		n, err := io.ReadFull(f, header)
		if err == io.EOF && n == 0 {
			break
		}
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return nil, opError(ResultCorrupt, "wal.read",
				fmt.Sprintf("%s: truncated frame header at offset %d (file not modified, retained for diagnosis)", path, offset))
		}
		if err != nil {
			return nil, opError(ResultRejected, "wal.read", err.Error())
		}
		crc := binary.BigEndian.Uint32(header[:4])
		length := binary.BigEndian.Uint32(header[4:8])
		if length > 64*1024*1024 {
			return nil, opError(ResultCorrupt, "wal.read",
				fmt.Sprintf("%s: implausible frame length %d at offset %d", path, length, offset))
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil, opError(ResultCorrupt, "wal.read",
				fmt.Sprintf("%s: truncated frame body at offset %d", path, offset))
		}
		if crc32.ChecksumIEEE(payload) != crc {
			return nil, opError(ResultCorrupt, "wal.read",
				fmt.Sprintf("%s: crc mismatch at offset %d (checksum inconsistent, wal retained for diagnosis)", path, offset))
		}
		var e LogEntry
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&e); err != nil {
			return nil, opError(ResultCorrupt, "wal.read",
				fmt.Sprintf("%s: undecodable frame at offset %d: %v", path, offset, err))
		}
		entries = append(entries, e)
		offset += 8 + int64(length)
	}
	return entries, nil
}

func (ps *PersistentStore) openWAL(shard string) (*os.File, error) {
	if !ps.enabled() {
		return nil, nil
	}
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if f, ok := ps.files[shard]; ok {
		return f, nil
	}
	f, err := os.OpenFile(ps.walPath(shard), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, opError(ResultRejected, "wal.open", err.Error())
	}
	ps.files[shard] = f
	return f, nil
}

// appendEntry 持久化一条日志（崩溃后可由 CRC + 哈希链发现不一致）。
func (ps *PersistentStore) appendEntry(shard string, e LogEntry) error {
	f, err := ps.openWAL(shard)
	if err != nil || f == nil {
		return err
	}
	return writeWALFrame(f, e)
}

func openWALFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, opError(ResultRejected, "wal.open", err.Error())
	}
	return f, nil
}

func writeWALFrame(f *os.File, e LogEntry) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(e); err != nil {
		return opError(ResultRejected, "wal.encode", err.Error())
	}
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[:4], crc32.ChecksumIEEE(buf.Bytes()))
	binary.BigEndian.PutUint32(header[4:8], uint32(buf.Len()))
	if _, err := f.Write(header); err != nil {
		return opError(ResultRejected, "wal.write", err.Error())
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		return opError(ResultRejected, "wal.write", err.Error())
	}
	if err := f.Sync(); err != nil {
		return opError(ResultRejected, "wal.fsync", err.Error())
	}
	return nil
}

func renameWAL(tmp, path string) error {
	if err := os.Rename(tmp, path); err != nil {
		return opError(ResultRejected, "wal.rewrite.rename", err.Error())
	}
	return nil
}

func (ps *PersistentStore) saveMetaLocked() error {
	if !ps.enabled() {
		return nil
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(ps.meta); err != nil {
		return opError(ResultRejected, "meta.encode", err.Error())
	}
	tmp := ps.metaPath() + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return opError(ResultRejected, "meta.write", err.Error())
	}
	if err := os.Rename(tmp, ps.metaPath()); err != nil {
		return opError(ResultRejected, "meta.rename", err.Error())
	}
	return nil
}

func (ps *PersistentStore) setMeta(shard string, m shardMeta) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.meta[shard] = m
	return ps.saveMetaLocked()
}

func (ps *PersistentStore) removeShard(shard string) error {
	ps.mu.Lock()
	if f, ok := ps.files[shard]; ok {
		_ = f.Close()
		delete(ps.files, shard)
	}
	delete(ps.meta, shard)
	err := ps.saveMetaLocked()
	ps.mu.Unlock()
	if err != nil {
		return err
	}
	if ps.enabled() {
		if err := os.Remove(ps.walPath(shard)); err != nil && !os.IsNotExist(err) {
			return opError(ResultRejected, "wal.remove", err.Error())
		}
	}
	return nil
}

func (ps *PersistentStore) close() {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	for id, f := range ps.files {
		_ = f.Close()
		delete(ps.files, id)
	}
}

// logHasher 封装日志链式哈希（SHA-256）。
type logHasher struct {
	h hasher
}

func newLogHasher() *logHasher { return &logHasher{h: newSHA256()} }
func (lh *logHasher) writeString(s string) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], uint64(len(s)))
	lh.h.Write(b[:n])
	lh.h.Write([]byte(s))
}
func (lh *logHasher) writeBytes(b []byte) {
	var n [binary.MaxVarintLen64]byte
	k := binary.PutUvarint(n[:], uint64(len(b)))
	lh.h.Write(n[:k])
	lh.h.Write(b)
}
func (lh *logHasher) writeUvarint(v uint64) {
	var b [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(b[:], v)
	lh.h.Write(b[:n])
}
func (lh *logHasher) sum() []byte { return lh.h.Sum(nil) }
