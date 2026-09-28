package store

import (
	"context"
	"time"
)

// MigrateShard moves a shard to a new replica set at a new epoch.
//
// Protocol (serialized cluster-wide by migrationMu):
//
//  1. PREPARE: install cfg(epoch+1) on the new replica set (new nodes get
//     empty logs; continuing nodes retain their logs and are installed in
//     place). New replicas fetch the old leader's full log and must report
//     positions contiguous and hash-digest consistent at the old epoch.
//  2. CUTOVER: block client writes (ErrKeyMigrating), fence the old
//     generation, publish new metadata, wait CutoverWindow, unblock.
//  3. COMMIT: new leader serves writes. Old replicas that did not continue
//     are fenced and can never accept writes again (persisted fence).
//
// On any failed validation the migration is aborted with status recorded;
// metadata stays at the old epoch.
func (c *Cluster) MigrateShard(ctx context.Context, shard ShardID, newReplicas []NodeID, newLeader NodeID) (MigrationStatus, error) {
	c.migrationMu.Lock()
	defer c.migrationMu.Unlock()

	c.mu.Lock()
	old, ok := c.shards[shard]
	if !ok {
		c.mu.Unlock()
		return MigrationStatus{}, fmtErr(ErrInvalidConfig, "unknown shard %s", shard)
	}
	if c.cutover[shard] {
		c.mu.Unlock()
		return MigrationStatus{}, fmtErr(ErrConflict, "shard %s already in cutover", shard)
	}
	if _, migrating := c.migrations[shard]; migrating {
		c.mu.Unlock()
		return MigrationStatus{}, fmtErr(ErrConflict, "migration already in progress")
	}
	for _, r := range newReplicas {
		if _, exists := c.nodes[r]; !exists {
			c.mu.Unlock()
			return MigrationStatus{}, fmtErr(ErrInvalidConfig, "target replica %s not in cluster", r)
		}
	}
	if !containsID(newReplicas, newLeader) {
		c.mu.Unlock()
		return MigrationStatus{}, fmtErr(ErrInvalidConfig, "new leader %s not in new replica set", newLeader)
	}
	if len(newReplicas) == 0 {
		c.mu.Unlock()
		return MigrationStatus{}, fmtErr(ErrInvalidConfig, "empty new replica set")
	}
	newCfg := ShardConfig{
		Shard:    shard,
		Epoch:    old.Epoch + 1,
		Start:    old.Start,
		End:      old.End,
		Replicas: append([]NodeID(nil), newReplicas...),
		Leader:   newLeader,
	}
	status := &MigrationStatus{
		Shard: shard, From: old, To: newCfg,
		State: MigrationPrepare, NewEpoch: newCfg.Epoch,
		Synced: map[NodeID]SyncPosition{},
	}
	c.migrations[shard] = status
	oldLeaderNode := c.nodes[old.Leader]
	nodeSet := c.nodes
	c.mu.Unlock()

	abort := func(cause error) (MigrationStatus, error) {
		c.mu.Lock()
		status.State = MigrationAborted
		c.cutover[shard] = false
		st := *status
		delete(c.migrations, shard)
		c.mu.Unlock()
		c.logf("MIGRATION ABORT shard=%s fromEpoch=%d: %v", shard, old.Epoch, cause)
		return st, cause
	}

	// ---- Block writes before any data movement ---------------------------
	// The same key must never be accepted by old AND new generation during
	// migration. Blocking at prepare start makes the source position
	// stable for byte-identical bootstrap; clients get ErrKeyMigrating.
	c.mu.Lock()
	c.cutover[shard] = true
	status.State = MigrationPrepare
	c.mu.Unlock()
	c.logf("MIGRATION BEGIN shard=%s epoch=%d->%d: writes blocked (ErrKeyMigrating)",
		shard, old.Epoch, newCfg.Epoch)

	// ---- Phase 1: install new generation on target replicas --------------
	for _, r := range newCfg.Replicas {
		err := c.sendAdmin(ctx, nodeSet[r], Message{
			Kind:   MsgAdminInstallShard,
			Shard:  shard,
			Epoch:  newCfg.Epoch,
			Config: newCfg,
		})
		if err != nil {
			return abort(err)
		}
	}

	// ---- Phase 1b: stream full old log from old leader to new replicas ---
	oldLeaderPos, err := c.probePosition(ctx, old.Leader, shard, old.Epoch)
	if err != nil {
		return abort(err)
	}
	c.logf("MIGRATION PREPARE shard=%s oldLeader=%s sourcePos=%+v", shard, old.Leader, oldLeaderPos)
	for _, r := range newCfg.Replicas {
		// continuing replica: nothing to fetch if its digest matches
		pos, perr := c.probePosition(ctx, r, shard, newCfg.Epoch)
		if perr == nil && pos.LogDigest == oldLeaderPos.LogDigest && pos.LastIndex == oldLeaderPos.LastIndex {
			status.Synced[r] = pos
			c.logf("MIGRATION replica=%s already synced pos=%+v", r, pos)
			continue
		}
		if perr := c.bootstrapReplica(ctx, shard, oldLeaderNode, r, newCfg, oldLeaderPos); perr != nil {
			return abort(perr)
		}
		pos, perr = c.probePosition(ctx, r, shard, newCfg.Epoch)
		if perr != nil {
			return abort(perr)
		}
		// Verifiable sync position: historical prefix must be byte-equal
		// to the source's log; the new-epoch marker extends it by one.
		if pos.LastIndex != oldLeaderPos.LastIndex+1 || pos.CommitIdx < oldLeaderPos.CommitIdx {
			return abort(fmtErr(ErrChecksumMismatch,
				"bootstrap mismatch replica=%s pos=%+v source=%+v", r, pos, oldLeaderPos))
		}
		// Compare the prefix digest directly from the replica's WAL.
		nnode := nodeSet[r]
		prefixDigest, plen := nnode.prefixDigest(shard, oldLeaderPos.LastIndex)
		if plen != oldLeaderPos.LastIndex || prefixDigest != oldLeaderPos.LogDigest {
			return abort(fmtErr(ErrChecksumMismatch,
				"prefix digest mismatch replica=%s digest=%s/%d source=%s/%d",
				r, prefixDigest, plen, oldLeaderPos.LogDigest, oldLeaderPos.LastIndex))
		}
		status.Synced[r] = pos
		c.logf("MIGRATION replica=%s caught up: prefix=%d digest=%s markerIdx=%d",
			r, plen, prefixDigest, pos.LastIndex)
	}

	// ---- Phase 2: cutover --------------------------------------------------
	// Writes have been blocked since prepare start. Fence the old
	// generation atomically, publish new metadata and hold the declared
	// window before releasing clients onto the new leader.
	c.logf("MIGRATION CUTOVER shard=%s epoch=%d->%d fencing old replicas", shard, old.Epoch, newCfg.Epoch)

	// fence every old replica (including continuing ones, at the old epoch)
	for _, r := range old.Replicas {
		if err := c.sendAdmin(ctx, nodeSet[r], Message{
			Kind: MsgAdminFence, Shard: shard, Epoch: old.Epoch,
		}); err != nil {
			return abort(err)
		}
	}
	c.mu.Lock()
	status.State = MigrationCutover
	c.mu.Unlock()
	// Wait for the declared cutover window: writes during this window get
	// ErrKeyMigrating instead of silently accepting on either generation.
	select {
	case <-time.After(c.opts.CutoverWindow):
	case <-ctx.Done():
		return abort(fmtErr(ErrTimeout, "cutover: %v", ctx.Err()))
	}

	c.mu.Lock()
	c.shards[shard] = newCfg
	c.view.Shards[shard] = newCfg
	c.cutover[shard] = false
	status.State = MigrationCommitted
	st := *status
	delete(c.migrations, shard)
	c.mu.Unlock()
	c.logf("MIGRATION COMMITTED shard=%s newEpoch=%d replicas=%v leader=%s",
		shard, newCfg.Epoch, newCfg.Replicas, newCfg.Leader)
	return st, nil
}

// probePosition asks one node for its position, validating epoch.
func (c *Cluster) probePosition(ctx context.Context, nid NodeID, shard ShardID, epoch uint64) (SyncPosition, error) {
	rctx, cancel := context.WithTimeout(ctx, c.opts.RPCDeadline)
	defer cancel()
	resp, err := c.transport.Send(rctx, Message{
		Kind: MsgProbe, Shard: shard, Epoch: epoch, From: "coordinator", To: nid,
	})
	if err != nil {
		return SyncPosition{}, err
	}
	if !resp.OK {
		return SyncPosition{}, codeErr(resp.ErrCode, resp.RejectReason)
	}
	return resp.Pos, nil
}

// bootstrapReplica streams the source leader's full log into target through
// MsgFetchLog batches and appends each batch contiguously via
// AppendEntries; every batch boundary is verified.
func (c *Cluster) bootstrapReplica(ctx context.Context, shard ShardID, srcNode *Node, target NodeID, cfg ShardConfig, sourcePos SyncPosition) error {
	var from uint64
	rounds := 0
	for {
		rounds++
		rctx, cancel := context.WithTimeout(ctx, c.opts.RPCDeadline)
		resp, err := c.transport.Send(rctx, Message{
			Kind: MsgFetchLog, Shard: shard, Epoch: 0,
			FromIndex: from, From: "coordinator", To: srcNode.ID(),
		})
		cancel()
		if err != nil {
			return err
		}
		if !resp.OK {
			return codeErr(resp.ErrCode, resp.RejectReason)
		}
		if len(resp.Entries) == 0 {
			break
		}
		// Historical entries are copied byte-for-byte (same epoch, indexes
		// and versions): this is what makes source/target digests equal.
		batch := make([]Entry, len(resp.Entries))
		copy(batch, resp.Entries)
		arctx, acancel := context.WithTimeout(ctx, c.opts.RPCDeadline)
		aresp, aerr := c.transport.Send(arctx, Message{
			Kind:        MsgAppendEntries,
			Shard:       shard,
			Epoch:       cfg.Epoch,
			Term:        1,
			PrevIndex:   from,
			Entries:     batch,
			CommitIndex: 0,
			From:        "coordinator",
			To:          target,
		})
		acancel()
		if aerr != nil {
			return aerr
		}
		if !aresp.OK {
			return codeErr(aresp.ErrCode, aresp.RejectReason)
		}
		from = aresp.LastIndex
		if from >= sourcePos.LastIndex {
			break
		}
		if rounds > 10000 {
			return fmtErr(ErrLogGap, "bootstrap did not converge")
		}
	}
	// Append a new-epoch Noop marker after the copied historical prefix.
	// It makes the new generation's intent durable and advances commit; the
	// historical digest remains comparable to the source.
	nctx, ncancel := context.WithTimeout(ctx, c.opts.RPCDeadline)
	nresp, nerr := c.transport.Send(nctx, Message{
		Kind: MsgAppendEntries, Shard: shard, Epoch: cfg.Epoch, Term: 1,
		PrevIndex:   sourcePos.LastIndex,
		Entries:     []Entry{{Epoch: cfg.Epoch, Op: OpNoop, TxnID: "epoch-" + u64str(cfg.Epoch)}},
		CommitIndex: sourcePos.LastIndex,
		From:        "coordinator", To: target,
	})
	ncancel()
	if nerr != nil {
		return nerr
	}
	if !nresp.OK {
		return codeErr(nresp.ErrCode, nresp.RejectReason)
	}

	// Persist the streamed prefix as committed via a leader probe so a
	// crash right after migration still treats it as committed.
	pctx, pcancel := context.WithTimeout(ctx, c.opts.RPCDeadline)
	defer pcancel()
	presp, perr := c.transport.Send(pctx, Message{
		Kind: MsgProbe, Shard: shard, Epoch: cfg.Epoch, Term: 1,
		CommitIndex: sourcePos.LastIndex, From: cfg.Leader, To: target,
	})
	if perr != nil {
		return perr
	}
	if !presp.OK {
		return codeErr(presp.ErrCode, presp.RejectReason)
	}
	return nil
}

// MigrationStatus returns the current diagnostic migration view.
func (c *Cluster) MigrationStatus(shard ShardID) (MigrationStatus, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.migrations[shard]
	if !ok {
		return MigrationStatus{}, false
	}
	cp := *s
	return cp, true
}

// ShardPosition polls a replica's sync position.
func (c *Cluster) ShardPosition(ctx context.Context, shard ShardID, replica NodeID) (SyncPosition, error) {
	c.mu.Lock()
	cfg, ok := c.shards[shard]
	c.mu.Unlock()
	if !ok {
		return SyncPosition{}, fmtErr(ErrInvalidConfig, "unknown shard %s", shard)
	}
	return c.probePosition(ctx, replica, shard, cfg.Epoch)
}

// ChangeReplicas is membership change: same key range, new replica set,
// new epoch. It shares the migration state machine, so membership changes
// are mutually exclusive with migrations and obey the same fencing rules.
func (c *Cluster) ChangeReplicas(ctx context.Context, shard ShardID, replicas []NodeID, leader NodeID) (ShardConfig, error) {
	st, err := c.MigrateShard(ctx, shard, replicas, leader)
	if err != nil {
		return ShardConfig{}, err
	}
	return st.To, nil
}
