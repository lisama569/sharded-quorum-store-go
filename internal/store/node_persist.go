package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Per-shard on-disk layout under <dataDir>/<nodeID>/shard-<id>/:
//
//	shard.wal    replicated log (crc + hash chain)
//	meta.json    installed ShardConfig + fence epoch
//	commit.bin   uint64 persisted commit index
//
// commit index is persisted separately from the log so a restart can
// distinguish durable-but-uncommitted tail entries (dropped) from
// committed prefix (re-applied into the in-memory KV).

func shardDir(dataDir string, id NodeID, shard ShardID) string {
	return filepath.Join(dataDir, string(id), "shard-"+string(shard))
}

type persistedMeta struct {
	Config     ShardConfig `json:"config"`
	FenceEpoch uint64      `json:"fence_epoch"`
}

func writeCommitFile(path string, idx uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], idx)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b[:], 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return nil
}

func readCommitFile(path string) (uint64, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("%w: commit file len=%d", ErrChecksumMismatch, len(b))
	}
	return binary.LittleEndian.Uint64(b), nil
}

func writeMetaFile(path string, m persistedMeta) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func readMetaFile(path string) (persistedMeta, error) {
	var m persistedMeta
	b, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return m, fmt.Errorf("%w: meta.json: %v", ErrChecksumMismatch, err)
	}
	return m, nil
}

// loadShardsFromDisk discovers persisted shards and rebuilds state.
// Must be called once from Start before the actor loop runs.
func (n *Node) loadShardsFromDisk() error {
	base := filepath.Join(n.opts.DataDir, string(n.opts.ID))
	ents, err := os.ReadDir(base)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range ents {
		if !e.IsDir() || len(e.Name()) < 6 || e.Name()[:6] != "shard-" {
			continue
		}
		sid := ShardID(e.Name()[6:])
		dir := filepath.Join(base, e.Name())
		meta, err := readMetaFile(filepath.Join(dir, "meta.json"))
		if err != nil {
			return fmt.Errorf("node %s shard %s: %w", n.opts.ID, sid, err)
		}
		wal, err := OpenWAL(dir, sid)
		if err != nil {
			return err
		}
		commitIdx, err := readCommitFile(filepath.Join(dir, "commit.bin"))
		if err != nil {
			return err
		}
		// Uncommitted tail from a crashed lease is discarded explicitly
		// (kept in WAL until first new AppendEntries replaces it; here we
		// truncate on load for a clean, verifiable boundary).
		if wal.LastIndex() > commitIdx {
			dropped := wal.LastIndex() - commitIdx
			if err := wal.TruncateSuffix(commitIdx); err != nil {
				return err
			}
			n.logf("shard %s restart: dropped %d uncommitted tail entries, recovered commit=%d",
				sid, dropped, commitIdx)
		}
		if commitIdx > wal.LastIndex() {
			return fmt.Errorf("%w: node %s shard %s commit=%d > logLen=%d",
				ErrChecksumMismatch, n.opts.ID, sid, commitIdx, wal.LastIndex())
		}
		s := &shardState{
			cfg:         meta.Config,
			wal:         wal,
			kv:          map[Key]kvRec{},
			commitIdx:   commitIdx,
			fenceEpoch:  meta.FenceEpoch,
			installed:   true,
			role:        roleFollower,
			seenTxn:     map[string]writeCache{},
			nextVersion: map[Key]uint64{},
			ackIndex:    map[NodeID]uint64{},
		}
		if commitIdx >= 1 && s.cfg.Leader == n.opts.ID {
			s.leader = n.opts.ID
		}
		if s.cfg.Leader == n.opts.ID && meta.Config.Epoch > meta.FenceEpoch {
			s.role = roleLeader
			s.leader = n.opts.ID
			s.term = 1
			s.ackIndex = map[NodeID]uint64{n.opts.ID: commitIdx}
		}
		n.applyShardThrough(s, commitIdx)
		n.shards[sid] = s
		n.logf("shard %s loaded: epoch=%d commit=%d kv=%d chain=%x",
			sid, s.cfg.Epoch, commitIdx, len(s.kv), wal.HashChain()[:8])
	}
	return nil
}

func (n *Node) logf(format string, args ...any) {
	if n.opts.Logger != nil {
		n.opts.Logger.Printf("[node %s] %s", n.opts.ID, fmt.Sprintf(format, args...))
	}
}
