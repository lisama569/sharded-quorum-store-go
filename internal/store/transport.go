package store

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MessageKind enumerates every inter-node / client-node RPC.
type MessageKind int

const (
	MsgProbe MessageKind = iota // position + liveness heartbeat
	MsgClientWrite
	MsgClientRead
	MsgAppendEntries
	MsgAppendEntriesAck
	MsgRequestVote
	MsgRequestVoteAck
	MsgFetchLog // follower pulls entries / new replica bootstrap
	MsgFetchLogAck
	MsgAdminInstallShard // push ShardConfig to a replica
	MsgAdminFence        // fence shard at epoch
)

// Message is the single RPC envelope used on the simulated network.
type Message struct {
	Kind MessageKind
	From NodeID
	To   NodeID

	Shard ShardID
	Epoch uint64
	Term  uint64

	Key         Key
	Value       Value
	Op          OpKind
	TxnID       string
	Consistency Consistency

	// replication
	PrevIndex   uint64
	PrevHash    []byte
	Entries     []Entry
	CommitIndex uint64

	// voting
	Candidate NodeID
	LastIndex uint64
	LastHash  []byte

	// catchup / install
	FromIndex uint64
	Config    ShardConfig
	Snapshot  map[Key]kvRec // bootstrap KV image for migration
	SnapIndex uint64

	// response payload
	OK           bool
	ErrCode      string
	Pos          SyncPosition
	Version      uint64
	Found        bool
	Duplicate    bool
	Granted      bool
	RejectReason string
}

type kvRec struct {
	Value   Value
	Version uint64
}

// Handler processes one delivered message. Implementations must be safe for
// concurrent use; Node serializes calls through its actor goroutine.
type Handler interface {
	HandleMessage(ctx context.Context, msg Message) Message
}

// Transport simulates an unreliable network between in-process nodes.
// It supports node offlining (delivery refused), bidirectional link cuts
// (network partitions) and optional per-link latency.
type Transport struct {
	mu      sync.Mutex
	nodes   map[NodeID]Handler
	offline map[NodeID]bool
	// blocked[a][b] == true means a<->b traffic is cut both directions.
	blocked  map[NodeID]map[NodeID]bool
	latency  map[NodeID]map[NodeID]time.Duration
	dropOnce map[string]int
}

// NewTransport creates an empty in-memory transport.
func NewTransport() *Transport {
	return &Transport{
		nodes:    map[NodeID]Handler{},
		offline:  map[NodeID]bool{},
		blocked:  map[NodeID]map[NodeID]bool{},
		latency:  map[NodeID]map[NodeID]time.Duration{},
		dropOnce: map[string]int{},
	}
}

// Register attaches a node's message handler.
func (t *Transport) Register(id NodeID, h Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = h
}

// Unregister removes a node.
func (t *Transport) Unregister(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, id)
}

// SetOffline marks a node reachable/unreachable (simulates stop/restart).
func (t *Transport) SetOffline(id NodeID, offline bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.offline[id] = offline
}

// IsOffline reports the node's simulated reachability.
func (t *Transport) IsOffline(id NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.offline[id]
}

// Partition cuts/restores the bidirectional link between a and b.
func (t *Transport) Partition(a, b NodeID, cut bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.blocked[a] == nil {
		t.blocked[a] = map[NodeID]bool{}
	}
	t.blocked[a][b] = cut
}

// IsPartitioned reports whether a link is currently cut.
func (t *Transport) IsPartitioned(a, b NodeID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.blocked[a][b] || t.blocked[b][a]
}

// SetLatency injects fixed one-way latency on a link (both directions).
func (t *Transport) SetLatency(a, b NodeID, d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.latency[a] == nil {
		t.latency[a] = map[NodeID]time.Duration{}
	}
	t.latency[a][b] = d
}

// Send delivers msg synchronously to the destination handler. It models
// node-offline delivery refusal, link partitions and injected latency.
func (t *Transport) Send(ctx context.Context, msg Message) (Message, error) {
	t.mu.Lock()
	dst, ok := t.nodes[msg.To]
	if !ok || t.offline[msg.To] {
		t.mu.Unlock()
		return Message{}, ErrUnavailable
	}
	if t.offline[msg.From] {
		t.mu.Unlock()
		return Message{}, ErrUnavailable
	}
	if t.blocked[msg.From][msg.To] || t.blocked[msg.To][msg.From] {
		t.mu.Unlock()
		return Message{}, fmtErr(ErrTimeout, "link %s->%s partitioned", msg.From, msg.To)
	}
	d := t.latency[msg.From][msg.To]
	if d == 0 {
		d = t.latency[msg.To][msg.From]
	}
	t.mu.Unlock()

	if d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return Message{}, ErrTimeout
		}
	}
	done := make(chan Message, 1)
	go func() { done <- dst.HandleMessage(ctx, msg) }()
	select {
	case resp := <-done:
		return resp, nil
	case <-ctx.Done():
		return Message{}, ErrTimeout
	}
}

// errCode mapping: sentinel error -> stable code across the fake network.
func errCode(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrTimeout):
		return "timeout"
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrKeyMigrating):
		return "key_migrating"
	case errors.Is(err, ErrStaleFollower):
		return "stale_epoch"
	case errors.Is(err, ErrNoLeader):
		return "no_leader"
	case errors.Is(err, ErrChecksumMismatch):
		return "checksum"
	case errors.Is(err, ErrLogGap):
		return "log_gap"
	case errors.Is(err, ErrPositionRegression):
		return "position_regression"
	case errors.Is(err, ErrInvalidConfig):
		return "invalid_config"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	default:
		return "internal"
	}
}

func codeErr(code, detail string) error {
	switch code {
	case "":
		return nil
	case "timeout":
		return ErrTimeout
	case "unavailable":
		return ErrUnavailable
	case "conflict":
		return ErrConflict
	case "key_migrating":
		return ErrKeyMigrating
	case "stale_epoch":
		return ErrStaleFollower
	case "no_leader":
		return ErrNoLeader
	case "checksum":
		return ErrChecksumMismatch
	case "log_gap":
		return ErrLogGap
	case "position_regression":
		return ErrPositionRegression
	case "invalid_config":
		return ErrInvalidConfig
	case "not_found":
		return ErrNotFound
	default:
		return errors.New(code + ": " + detail)
	}
}
