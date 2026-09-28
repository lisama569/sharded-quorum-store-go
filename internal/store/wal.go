package store

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"time"
)

// WAL is a durable, append-only record log for a single shard on a node.
//
// Record layout (little endian):
//
//	uint64 index
//	uint64 epoch
//	uint64 version
//	uint8  op
//	uint16 keyLen   | key bytes
//	uint32 valueLen | value bytes
//	uint16 txnLen   | txn bytes
//	uint32 crc32(IEEE, over every preceding field of the record)
//
// Opening behaviour:
//   - a short/crc-bad *tail* record (the last bytes in the file) is treated
//     as a torn write, the file is truncated at that boundary and recovery
//     continues;
//   - a bad record anywhere earlier is hard corruption: the bad file is
//     copied aside as <name>.corrupt-<unixnano> for diagnosis and
//     ErrChecksumMismatch (or ErrLogGap on non-contiguous indexes) is
//     returned with the offending index.
type WAL struct {
	file    *os.File
	path    string
	entries []Entry
	offsets []int64 // offsets[i] = start of record entries[i]; len = n+1 (end)
	// hashChain last element = sha256 over the whole sequence of records.
	chain []byte
}

const (
	walSuffix    = ".wal"
	walFixedHead = 8 + 8 + 8 + 1 + 2 + 4 + 2 // 33 bytes, before payloads
	walCRCLen    = 4
)

var crcTable = crc32.MakeTable(crc32.IEEE)

// OpenWAL opens (or creates) the per-shard WAL inside dir and recovers it.
func OpenWAL(dir string, shard ShardID) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal mkdir: %w", err)
	}
	path := filepath.Join(dir, "shard-"+string(shard)+walSuffix)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}
	w := &WAL{file: f, path: path, chain: make([]byte, 32)}
	if err := w.recoverFromDisk(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

func marshalEntry(e Entry) []byte {
	kl, vl, tl := len(e.Key), len(e.Value), len(e.TxnID)
	buf := make([]byte, walFixedHead+kl+vl+tl+walCRCLen)
	binary.LittleEndian.PutUint64(buf[0:8], e.Index)
	binary.LittleEndian.PutUint64(buf[8:16], e.Epoch)
	binary.LittleEndian.PutUint64(buf[16:24], e.Version)
	buf[24] = byte(e.Op)
	binary.LittleEndian.PutUint16(buf[25:27], uint16(kl))
	copy(buf[27:27+kl], []byte(e.Key))
	p := 27 + kl
	binary.LittleEndian.PutUint32(buf[p:p+4], uint32(vl))
	copy(buf[p+4:p+4+vl], e.Value)
	p += 4 + vl
	binary.LittleEndian.PutUint16(buf[p:p+2], uint16(tl))
	copy(buf[p+2:p+2+tl], []byte(e.TxnID))
	p += 2 + tl
	csum := crc32.Checksum(buf[:p], crcTable)
	binary.LittleEndian.PutUint32(buf[p:p+4], csum)
	return buf
}

func unmarshalRecord(buf []byte) (Entry, int, error) {
	if len(buf) < walFixedHead+walCRCLen {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}
	kl := int(binary.LittleEndian.Uint16(buf[25:27]))
	p := 27 + kl
	if len(buf) < p+4 {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}
	vl := int(binary.LittleEndian.Uint32(buf[p : p+4]))
	p += 4
	if len(buf) < p+vl+2 {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}
	val := make([]byte, vl)
	copy(val, buf[p:p+vl])
	p += vl
	tl := int(binary.LittleEndian.Uint16(buf[p : p+2]))
	p += 2
	if len(buf) < p+tl+4 {
		return Entry{}, 0, io.ErrUnexpectedEOF
	}
	txn := make([]byte, tl)
	copy(txn, buf[p:p+tl])
	p += tl
	wantCrc := binary.LittleEndian.Uint32(buf[p : p+4])
	body := buf[:p]
	if crc32.Checksum(body, crcTable) != wantCrc {
		return Entry{}, 0, ErrChecksumMismatch
	}
	return Entry{
		Index:   binary.LittleEndian.Uint64(buf[0:8]),
		Epoch:   binary.LittleEndian.Uint64(buf[8:16]),
		Version: binary.LittleEndian.Uint64(buf[16:24]),
		Op:      OpKind(buf[24]),
		Key:     Key(buf[27 : 27+kl]),
		Value:   val,
		TxnID:   string(txn),
	}, p + 4, nil
}

func (w *WAL) recoverFromDisk() error {
	data, err := io.ReadAll(w.file)
	if err != nil {
		return fmt.Errorf("wal read %s: %w", w.path, err)
	}
	w.entries = w.entries[:0]
	w.offsets = w.offsets[:0]
	w.chain = make([]byte, 32)
	off := 0
	w.offsets = append(w.offsets, 0)
	for off < len(data) {
		e, n, perr := unmarshalRecord(data[off:])
		if perr != nil {
			atTail := true
			// A fixed-head-sized short read or a bad crc: it is only a
			// torn write if this garbage runs to EOF.
			if errors.Is(perr, ErrChecksumMismatch) {
				// even a crc-bad record could be length-consistent;
				// verify it truly ends at EOF using declared lengths.
				decl, ok := declaredLen(data[off:])
				atTail = ok && off+decl == len(data)
			}
			if atTail {
				// Torn write: drop the tail and persist the truncation.
				if terr := w.file.Truncate(int64(off)); terr != nil {
					return fmt.Errorf("wal tail truncate: %w", terr)
				}
				if serr := w.file.Sync(); serr != nil {
					return serr
				}
				break
			}
			return w.quarantine(off, perr)
		}
		// Index sequence must be contiguous starting at 1.
		if e.Index != uint64(len(w.entries)+1) {
			return w.quarantine(off, fmt.Errorf("%w: expected index %d, record has %d",
				ErrLogGap, len(w.entries)+1, e.Index))
		}
		w.entries = append(w.entries, e)
		off += n
		w.offsets = append(w.offsets, int64(off))
		// chain over record bytes without trailing crc
		w.chain = chainHash(w.chain, data[off-n:off-walCRCLen])
	}
	_, serr := w.file.Seek(int64(off), io.SeekStart)
	return serr
}

// declaredLen computes total record size from the length fields when the
// record is fully present.
func declaredLen(buf []byte) (int, bool) {
	if len(buf) < walFixedHead+walCRCLen {
		return 0, false
	}
	kl := int(binary.LittleEndian.Uint16(buf[25:27]))
	p := 27 + kl + 4
	vl := int(binary.LittleEndian.Uint32(buf[27+kl : 27+kl+4]))
	p += vl + 2
	tl := int(binary.LittleEndian.Uint16(buf[p-2 : p]))
	p += tl + 4
	if p > len(buf) {
		return 0, false
	}
	return p, true
}

func (w *WAL) quarantine(off int, cause error) error {
	// Copy the corrupted file aside for post-mortem inspection.
	badPath := fmt.Sprintf("%s.corrupt-%d", w.path, time.Now().UnixNano())
	src, err := os.Open(w.path)
	if err == nil {
		if dst, derr := os.Create(badPath); derr == nil {
			_, _ = io.Copy(dst, src)
			_ = dst.Close()
		}
		_ = src.Close()
	}
	idx := uint64(len(w.entries) + 1)
	return fmt.Errorf("%w: shard wal %s bad record at byte %d (expected index %d), quarantined to %s: %v",
		ErrChecksumMismatch, w.path, off, idx, filepath.Base(badPath), cause)
}

func chainHash(prev []byte, body []byte) []byte {
	h := newSHA256()
	h.Write(prev)
	h.Write(body)
	return h.Sum(nil)
}

// Append durably appends e, assigning Index = LastIndex()+1. The record is
// fsynced before returning so callers may treat success as durable.
func (w *WAL) Append(e Entry) (Entry, error) {
	e.Index = uint64(len(w.entries) + 1)
	rec := marshalEntry(e)
	if _, err := w.file.Write(rec); err != nil {
		return Entry{}, fmt.Errorf("wal write: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return Entry{}, fmt.Errorf("wal sync: %w", err)
	}
	w.entries = append(w.entries, e)
	w.offsets = append(w.offsets, int64(len(rec))+w.offsets[len(w.offsets)-1])
	w.chain = chainHash(w.chain, rec[:len(rec)-walCRCLen])
	return e, nil
}

// Entries returns a copy of all persisted entries.
func (w *WAL) Entries() []Entry {
	out := make([]Entry, len(w.entries))
	copy(out, w.entries)
	return out
}

// Entry returns the entry at 1-based index.
func (w *WAL) Entry(index uint64) (Entry, bool) {
	if index == 0 || index > uint64(len(w.entries)) {
		return Entry{}, false
	}
	return w.entries[index-1], true
}

// LastIndex returns the highest persisted index.
func (w *WAL) LastIndex() uint64 { return uint64(len(w.entries)) }

// HashChain returns the chained hash over records 1..LastIndex.
func (w *WAL) HashChain() []byte {
	out := make([]byte, len(w.chain))
	copy(out, w.chain)
	return out
}

// LogDigest returns an independent digest over the full record stream.
func (w *WAL) LogDigest() string {
	h := newSHA256()
	for i := range w.entries {
		h.Write(marshalEntry(w.entries[i]))
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// Sync forces buffered writes to stable storage.
func (w *WAL) Sync() error { return w.file.Sync() }

// Close releases the WAL file.
func (w *WAL) Close() error { return w.file.Close() }

// TruncateSuffix removes every entry with index > keep and fsyncs.
func (w *WAL) TruncateSuffix(keep uint64) error {
	if keep > uint64(len(w.entries)) {
		return fmt.Errorf("%w: truncate keep=%d > len=%d", ErrInvalidConfig, keep, len(w.entries))
	}
	if uint64(len(w.entries)) == keep {
		return nil
	}
	end := w.offsets[keep]
	if err := w.file.Truncate(end); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	if _, err := w.file.Seek(end, io.SeekStart); err != nil {
		return err
	}
	w.entries = w.entries[:keep]
	w.offsets = w.offsets[:keep+1]
	w.chain = make([]byte, 32)
	// rebuild chain
	if f, err := os.Open(w.path); err == nil {
		data, _ := io.ReadAll(f)
		_ = f.Close()
		off := 0
		for off < len(data) {
			_, n, err := unmarshalRecord(data[off:])
			if err != nil {
				break
			}
			w.chain = chainHash(w.chain, data[off:off+n-walCRCLen])
			off += n
		}
	}
	return nil
}

// CorruptWALTail simulates a torn write by chopping bytes off the tail.
// Use on a WAL whose Node has been Closed.
func CorruptWALTail(w *WAL) error {
	f, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.Size() <= 4 {
		return errors.New("wal too small to corrupt tail")
	}
	return f.Truncate(st.Size() - 3)
}

// CorruptWALMiddle flips a byte inside the record at atIndex.
// Use on a WAL whose Node has been Closed.
func CorruptWALMiddle(w *WAL, atIndex uint64) error {
	if atIndex == 0 || atIndex > uint64(len(w.entries)) || atIndex == uint64(len(w.entries)) {
		return fmt.Errorf("%w: can only corrupt non-tail index, got %d of %d",
			ErrInvalidConfig, atIndex, len(w.entries))
	}
	f, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	off := w.offsets[atIndex-1] + 2 // flip inside the index/epoch fields
	if _, err := f.WriteAt([]byte{0xFF}, off); err != nil {
		return err
	}
	return f.Sync()
}

// PrefixDigest returns the independent record digest over entries 1..n and
// the effective n (clamped to the log length).
func (w *WAL) PrefixDigest(n uint64) (string, uint64) {
	if n > uint64(len(w.entries)) {
		n = uint64(len(w.entries))
	}
	h := newSHA256()
	for i := uint64(0); i < n; i++ {
		h.Write(marshalEntry(w.entries[i]))
	}
	return hex.EncodeToString(h.Sum(nil)[:8]), n
}
