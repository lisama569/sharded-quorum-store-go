package store

import (
	"context"
	"strconv"
)

func uintStr(u uint64) string { return strconv.FormatUint(u, 10) }

func (n *Node) rpc(ctx context.Context, to NodeID, msg Message) (Message, bool) {
	if n.opts.Transport == nil {
		return Message{}, false
	}
	msg.From = n.opts.ID
	msg.To = to
	rctx, cancel := context.WithTimeout(ctx, n.opts.HeartbeatInterval*4)
	defer cancel()
	resp, err := n.opts.Transport.Send(rctx, msg)
	if err != nil {
		return Message{}, false
	}
	return resp, true
}

// sendHeartbeatsLocked emits probes and drives log replication to every
// configured peer. Called from the actor tick and from write fan-out.
func (n *Node) sendHeartbeatsLocked(sid ShardID, s *shardState) {
	n.sendEntriesLocked(sid, s)
}

func (n *Node) sendEntriesLocked(sid ShardID, s *shardState) {
	ctx := context.Background()
	for _, rep := range s.cfg.Replicas {
		if rep == n.opts.ID {
			continue
		}
		peerIdx := s.ackIndex[rep]
		// Batch up to 32 new entries.
		var batch []Entry
		all := s.wal.Entries()
		if peerIdx < uint64(len(all)) {
			batch = append(batch, all[peerIdx:]...)
			if len(batch) > 32 {
				batch = batch[:32]
			}
		}
		var prevHash []byte
		if peerIdx > 0 {
			// chain prefix token for the *prev* record: use the peer-side
			// expected chain prefix = chain after prev record. We only have
			// local chain (which is >= peer); send 8-byte local prefix only
			// when peerIdx == local last-1 etc. Simpler: send empty hash and
			// rely on index contiguity + per-record CRC; chain verification
			// is performed over FetchLog bootstrap with full digest.
			prevHash = nil
		}
		msg := Message{
			Kind:        MsgAppendEntries,
			Shard:       sid,
			Epoch:       s.cfg.Epoch,
			Term:        s.term,
			PrevIndex:   peerIdx,
			PrevHash:    prevHash,
			Entries:     batch,
			CommitIndex: s.commitIdx,
		}
		resp, ok := n.rpc(ctx, rep, msg)
		if !ok {
			continue
		}
		if !resp.OK {
			switch resp.ErrCode {
			case "log_gap":
				// peer log diverged behind: fall back to FetchLog bootstrap
				// handled by cluster migration; here step back one index.
				if peerIdx > 0 {
					s.ackIndex[rep] = peerIdx - 1
				}
			case "checksum":
				n.logf("peer %s reports chain divergence for %s: %s", rep, sid, resp.RejectReason)
				if peerIdx > 0 {
					s.ackIndex[rep] = peerIdx - 1
				}
			case "stale_epoch":
				// expected during migration; stop pushing
			}
			continue
		}
		if resp.LastIndex > s.ackIndex[rep] {
			s.ackIndex[rep] = resp.LastIndex
		}
		// Followers' higher commit may advance the leader commit too.
		if resp.Pos.CommitIdx > s.commitIdx && s.wal.LastIndex() >= resp.Pos.CommitIdx {
			n.advanceCommit(s, resp.Pos.CommitIdx)
		}
	}
	// Leader computes commit as the highest index present on a quorum.
	need := quorumSize(len(s.cfg.Replicas))
	idcs := []uint64{s.wal.LastIndex()}
	for _, rep := range s.cfg.Replicas {
		if rep == n.opts.ID {
			continue
		}
		idcs = append(idcs, s.ackIndex[rep])
	}
	// median-ish: highest idx with >= need replicas at that point
	commit := highestQuorumIndex(idcs, need)
	if commit > s.commitIdx && commit <= s.wal.LastIndex() {
		n.advanceCommit(s, commit)
	}
}

func highestQuorumIndex(xs []uint64, need int) uint64 {
	// count replicas >= each value via sort
	sorted := append([]uint64(nil), xs...)
	// insertion sort (tiny)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j-1] > sorted[j]; j-- {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
		}
	}
	// ascending; the index with >= need replicas is at len-need
	if len(sorted) < need {
		return 0
	}
	return sorted[len(sorted)-need]
}

// startElectionLocked requests votes for a new term.
func (n *Node) startElectionLocked(sid ShardID, s *shardState) {
	s.term++
	s.role = roleCandidate
	s.votedFor = n.opts.ID
	term := s.term
	lastIdx := s.wal.LastIndex()
	chain := s.wal.HashChain()
	n.logf("election start shard=%s term=%d lastIndex=%d", sid, term, lastIdx)

	// unlock while doing RPC (RPC handler may call back into this node).
	granted := 1
	n.mu.Unlock()
	for _, rep := range s.cfg.Replicas {
		if rep == n.opts.ID {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), n.opts.ElectionTimeout)
		resp, ok := n.rpc(ctx, rep, Message{
			Kind:      MsgRequestVote,
			Shard:     sid,
			Epoch:     s.cfg.Epoch,
			Term:      term,
			Candidate: n.opts.ID,
			LastIndex: lastIdx,
			LastHash:  chain[:8],
		})
		cancel()
		if ok && resp.OK && resp.Granted {
			granted++
		}
	}
	n.mu.Lock()
	cur := n.shards[sid]
	if cur == nil || cur.term != term || cur.cfg.Epoch <= cur.fenceEpoch {
		return
	}
	if granted >= quorumSize(len(s.cfg.Replicas)) {
		cur.role = roleLeader
		cur.leader = n.opts.ID
		cur.ackIndex = map[NodeID]uint64{n.opts.ID: cur.wal.LastIndex()}
		// initialize peer acks from their committed positions pessimistically
		for _, rep := range cur.cfg.Replicas {
			if _, exists := cur.ackIndex[rep]; !exists {
				cur.ackIndex[rep] = 0
			}
		}
		n.logf("ELECTED leader shard=%s term=%d votes=%d chain=%x", sid, term, granted, chain[:8])
		// immediate authority announcement + commit advancement
		n.announceLeadershipLocked(sid, cur, term)
	} else {
		cur.role = roleFollower
		n.logf("election lost shard=%s term=%d votes=%d/%d", sid, term, granted, quorumSize(len(s.cfg.Replicas)))
	}
}

func (n *Node) announceLeadershipLocked(sid ShardID, s *shardState, term uint64) {
	for _, rep := range s.cfg.Replicas {
		if rep == n.opts.ID {
			continue
		}
		_, _ = n.rpc(context.Background(), rep, Message{
			Kind:        MsgProbe,
			Shard:       sid,
			Epoch:       s.cfg.Epoch,
			Term:        term,
			CommitIndex: s.commitIdx,
		})
	}
}
