package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"time"
)

// HandleMessage dispatches one RPC envelope; implements Handler.
func (n *Node) HandleMessage(ctx context.Context, msg Message) Message {
	// Client-facing calls manage their own locking (and fan out RPCs).
	switch msg.Kind {
	case MsgClientWrite:
		r := EpochReq{Epoch: msg.Epoch, Op: msg.Op, Key: msg.Key, Value: msg.Value, TxnID: msg.TxnID, Consistency: msg.Consistency}
		res, err := n.ClientWrite(ctx, msg.Shard, r)
		resp := Message{Kind: MsgClientWrite, From: n.opts.ID, To: msg.From}
		if err != nil {
			resp.OK = false
			resp.ErrCode = errCode(err)
			resp.RejectReason = err.Error()
			return resp
		}
		resp.OK = true
		resp.Version = res.Version
		resp.Duplicate = res.Duplicate
		resp.Pos = res.Position
		return resp
	case MsgClientRead:
		res, err := n.ClientRead(ctx, msg.Shard, msg.Epoch, msg.Key, msg.Consistency)
		resp := Message{Kind: MsgClientRead, From: n.opts.ID, To: msg.From}
		if err != nil {
			resp.OK = false
			resp.ErrCode = errCode(err)
			resp.RejectReason = err.Error()
			return resp
		}
		resp.OK = true
		resp.Value = res.Value
		resp.Found = res.Found
		resp.Version = res.Version
		resp.Pos = res.Position
		return resp
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	switch msg.Kind {
	case MsgProbe:
		return n.onProbeLocked(msg)
	case MsgAppendEntries:
		return n.onAppendEntriesLocked(msg)
	case MsgRequestVote:
		return n.onRequestVoteLocked(msg)
	case MsgFetchLog:
		return n.onFetchLogLocked(msg)
	case MsgAdminInstallShard:
		err := n.installLocked(ctx, msg.Config, msg.Snapshot, msg.SnapIndex)
		return fillErr(Message{Kind: MsgAdminInstallShard, From: n.opts.ID, To: msg.From}, err)
	case MsgAdminFence:
		err := n.fenceLocked(msg.Shard, msg.Epoch)
		return fillErr(Message{Kind: MsgAdminFence, From: n.opts.ID, To: msg.From}, err)
	default:
		return Message{OK: false, ErrCode: "internal", RejectReason: "unknown message kind"}
	}
}

func fillErr(m Message, err error) Message {
	if err != nil {
		m.OK = false
		m.ErrCode = errCode(err)
		m.RejectReason = err.Error()
		return m
	}
	m.OK = true
	return m
}

func (n *Node) fenceLocked(shard ShardID, epoch uint64) error {
	s := n.shards[shard]
	if s == nil {
		return nil
	}
	if epoch <= s.fenceEpoch {
		return nil
	}
	s.fenceEpoch = epoch
	if s.cfg.Epoch <= epoch {
		s.role = roleFollower
		s.leader = ""
		if s.wal.LastIndex() > s.commitIdx {
			if err := s.wal.TruncateSuffix(s.commitIdx); err != nil {
				return err
			}
		}
	}
	if err := n.persistMetaLocked(s); err != nil {
		return err
	}
	n.logf("FENCED shard %s epoch<=%d via RPC", shard, epoch)
	return nil
}

// onProbeLocked handles a heartbeat / leadership announcement.
func (n *Node) onProbeLocked(m Message) Message {
	s := n.shards[m.Shard]
	resp := Message{Kind: MsgProbe, From: n.opts.ID, To: m.From}
	if s == nil {
		resp.OK = false
		resp.ErrCode = errCode(ErrUnavailable)
		return resp
	}
	resp.Pos = n.positionLocked(s)
	if s.cfg.Epoch <= s.fenceEpoch {
		// Fenced replicas never accept new authority claims at <= fence.
		if m.Epoch <= s.fenceEpoch {
			resp.OK = false
			resp.ErrCode = errCode(ErrStaleFollower)
			return resp
		}
	}
	if m.Epoch < s.cfg.Epoch {
		resp.OK = false
		resp.ErrCode = errCode(ErrStaleFollower)
		return resp
	}
	if m.Epoch > s.cfg.Epoch {
		// Higher-epoch control traffic arrives via InstallShard first;
		// a probe alone cannot rewrite membership.
		resp.OK = false
		resp.ErrCode = errCode(ErrStaleFollower)
		return resp
	}
	// Same epoch: recognize leader and adopt term.
	if m.Term >= s.term {
		s.term = m.Term
		s.leader = m.From
		if s.role == roleCandidate {
			s.role = roleFollower
		}
		s.lastContact = time.Now()
	}
	if m.CommitIndex > s.commitIdx {
		n.advanceCommit(s, m.CommitIndex)
	}
	resp.OK = true
	return resp
}

// onAppendEntriesLocked validates and appends a replication batch.
func (n *Node) onAppendEntriesLocked(m Message) Message {
	s := n.shards[m.Shard]
	resp := Message{Kind: MsgAppendEntriesAck, From: n.opts.ID, To: m.From}
	if s == nil {
		resp.OK = false
		resp.ErrCode = errCode(ErrUnavailable)
		return resp
	}
	resp.Pos = n.positionLocked(s)
	if m.Epoch != s.cfg.Epoch {
		resp.OK = false
		resp.ErrCode = errCode(ErrStaleFollower)
		resp.RejectReason = "epoch mismatch"
		return resp
	}
	if s.cfg.Epoch <= s.fenceEpoch {
		resp.OK = false
		resp.ErrCode = errCode(ErrStaleFollower)
		return resp
	}
	if m.Term < s.term {
		resp.OK = false
		resp.ErrCode = errCode(ErrStaleFollower)
		resp.RejectReason = "stale term"
		return resp
	}
	s.term = m.Term
	s.leader = m.From
	s.lastContact = time.Now()
	if s.role == roleCandidate {
		s.role = roleFollower
	}

	// Contiguity + chain check at prevIndex.
	if m.PrevIndex > 0 {
		pe, ok := s.wal.Entry(m.PrevIndex)
		if !ok {
			resp.OK = false
			resp.ErrCode = errCode(ErrLogGap)
			resp.LastIndex = s.wal.LastIndex()
			resp.RejectReason = "missing prev index"
			return resp
		}
		// hash chain check
		chain := s.wal.HashChain()
		// prev hash is chain after appending prev record; compare digest token
		if len(m.PrevHash) > 0 && !bytes.HasPrefix(chain, m.PrevHash) {
			// Hash chain divergence: follower must re-bootstrap.
			resp.OK = false
			resp.ErrCode = errCode(ErrChecksumMismatch)
			resp.LastIndex = s.wal.LastIndex()
			resp.RejectReason = "hash chain diverged at prevIndex=" +
				uintStr(m.PrevIndex) + " have=" + hex.EncodeToString(chain[:8])
			n.logf("APPEND REJECT shard %s: hash chain mismatch at %d have=%x want-prefix=%x",
				m.Shard, m.PrevIndex, chain[:8], m.PrevHash)
			return resp
		}
		_ = pe
	}

	// Append (or reject conflicting tail) with index continuity.
	matched := m.PrevIndex
	for _, e := range m.Entries {
		next := matched + 1
		if existing, ok := s.wal.Entry(next); ok && existing.TxnID != e.TxnID && existing.Epoch == e.Epoch {
			// conflicting tail from an old leader: truncate before replacing
			if err := s.wal.TruncateSuffix(matched); err != nil {
				resp.OK = false
				resp.ErrCode = errCode(ErrChecksumMismatch)
				resp.RejectReason = err.Error()
				return resp
			}
			// KV will be rebuilt by applying through commit after re-append.
		}
		if e.Index != next {
			// leader uses continuous indexes; defensive copy sets index.
			e.Index = next
		}
		if have, ok := s.wal.Entry(next); ok && have.TxnID == e.TxnID && have.Epoch == e.Epoch {
			matched = next
			continue
		}
		saved, err := s.wal.Append(e)
		if err != nil {
			resp.OK = false
			resp.ErrCode = errCode(ErrChecksumMismatch)
			resp.RejectReason = err.Error()
			return resp
		}
		matched = saved.Index
	}
	resp.LastIndex = matched
	if m.CommitIndex > s.commitIdx {
		n.advanceCommit(s, m.CommitIndex)
	}
	resp.Pos = n.positionLocked(s)
	resp.OK = true
	return resp
}

// onRequestVoteLocked grants a vote when the candidate is at least as
// up-to-date as local log and no vote is already cast for someone else.
func (n *Node) onRequestVoteLocked(m Message) Message {
	s := n.shards[m.Shard]
	resp := Message{Kind: MsgRequestVoteAck, From: n.opts.ID, To: m.From}
	if s == nil {
		resp.OK = false
		resp.ErrCode = errCode(ErrUnavailable)
		return resp
	}
	resp.Pos = n.positionLocked(s)
	if m.Epoch != s.cfg.Epoch || s.cfg.Epoch <= s.fenceEpoch {
		resp.Granted = false
		resp.OK = true
		resp.RejectReason = "epoch/fence"
		return resp
	}
	if m.Term < s.term {
		resp.Granted = false
		resp.OK = true
		resp.RejectReason = "stale term"
		return resp
	}
	// up-to-date check by (lastIndex, chain): candidate must contain
	// everything the voter has committed.
	if m.LastIndex < s.commitIdx {
		resp.Granted = false
		resp.OK = true
		resp.RejectReason = "candidate behind committed log"
		return resp
	}
	if s.votedFor != "" && s.votedFor != m.Candidate {
		resp.Granted = false
		resp.OK = true
		resp.RejectReason = "voted for " + string(s.votedFor)
		return resp
	}
	s.term = m.Term
	s.votedFor = m.Candidate
	s.lastContact = time.Now()
	resp.Granted = true
	resp.OK = true
	n.logf("VOTE granted shard %s term=%d candidate=%s", m.Shard, m.Term, m.Candidate)
	return resp
}

// onFetchLogLocked serves catch-up / bootstrap streaming.
func (n *Node) onFetchLogLocked(m Message) Message {
	s := n.shards[m.Shard]
	resp := Message{Kind: MsgFetchLogAck, From: n.opts.ID, To: m.From}
	if s == nil {
		resp.OK = false
		resp.ErrCode = errCode(ErrUnavailable)
		return resp
	}
	resp.Pos = n.positionLocked(s)
	// Cluster-internal migration bootstrap: the coordinator fetches the
	// source's log while the source is still at the old epoch and the
	// request carries Epoch=0 as a "control stream" marker.
	if m.Epoch != 0 && m.Epoch != s.cfg.Epoch {
		resp.OK = false
		resp.ErrCode = errCode(ErrStaleFollower)
		return resp
	}
	from := m.FromIndex + 1
	if from == 0 {
		from = 1
	}
	if from > 1 {
		// Verify predecessor exists; a request for a missing predecessor
		// (gap) is rejected rather than silently returning wrong suffix.
		if _, ok := s.wal.Entry(from - 1); !ok {
			resp.OK = false
			resp.ErrCode = errCode(ErrLogGap)
			resp.RejectReason = "predecessor missing during catchup"
			return resp
		}
	}
	all := s.wal.Entries()
	if from <= uint64(len(all)) {
		batch := append([]Entry(nil), all[from-1:]...)
		if len(batch) > 64 {
			batch = batch[:64]
		}
		resp.Entries = batch
	}
	resp.LastIndex = s.wal.LastIndex()
	resp.CommitIndex = s.commitIdx
	resp.OK = true
	return resp
}
