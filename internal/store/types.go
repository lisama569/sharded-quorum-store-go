package store

// ShardID identifies a shard.
type ShardID string

// NodeID identifies a node.
type NodeID string

// Key / Value types.
type (
	Key   string
	Value []byte
)

// OpKind enumerates replicated write operations.
type OpKind int

const (
	OpPut OpKind = iota
	OpDelete
	// OpNoop is appended on epoch change/fence; it does not mutate data.
	OpNoop
)

// Entry is one record of a shard's replicated log.
type Entry struct {
	Index   uint64
	Epoch   uint64 // configuration generation at append time
	Op      OpKind
	Key     Key
	Value   Value
	TxnID   string // idempotency key of the client request
	Version uint64 // version after applying (0 until applied)
}

// SyncPosition identifies a verifiable log position on a replica.
type SyncPosition struct {
	Epoch     uint64
	LastIndex uint64
	CommitIdx uint64
	LastHash  string // hash chain over entries 1..LastIndex
	LogDigest string // content digest used for corruption detection
}

// AtOrAfter reports whether p includes every entry up to target.
func (p SyncPosition) AtOrAfter(target SyncPosition) bool {
	return p.Epoch >= target.Epoch && p.LastIndex >= target.LastIndex
}

// ShardConfig is the membership/range configuration for one generation.
type ShardConfig struct {
	Shard    ShardID
	Epoch    uint64
	Start    Key // inclusive; empty = -inf
	End      Key // exclusive; empty = +inf
	Replicas []NodeID
	// Leader is the coordinator of writes for this generation.
	Leader NodeID
}

// Contains reports whether k falls in the shard's key range.
func (c ShardConfig) Contains(k Key) bool {
	if len(c.Start) > 0 && k < c.Start {
		return false
	}
	if len(c.End) > 0 && k >= c.End {
		return false
	}
	return true
}

// ClusterView is the current configuration metadata handed to clients.
type ClusterView struct {
	Shards map[ShardID]ShardConfig
	// EpochsPerShard lets clients detect stale cached routing cheaply.
}

// WriteOptions carries consistency and timeout configuration.
type WriteOptions struct {
	Consistency Consistency
	// Timeout bounds the whole operation; zero = no client-side timeout.
	// Callers may also pass a deadline via context.
}

// ReadOptions carries read consistency configuration.
type ReadOptions struct {
	Consistency Consistency
}

// WriteResult is the distinguishable outcome of a write.
type WriteResult struct {
	Applied bool
	Version uint64
	// Duplicate is true when the same TxnID had been applied before; the call
	// has no effect and the previous result is returned.
	Duplicate bool
	Epoch     uint64
	Position  SyncPosition
}

// ReadResult is the outcome of a read.
type ReadResult struct {
	Found   bool
	Value   Value
	Version uint64
	Epoch   uint64
	// Position is the replica position that served the value (diagnostics).
	Position SyncPosition
}

// MigrationState is a snapshot of one shard-migration state machine.
type MigrationState int

const (
	MigrationIdle MigrationState = iota
	MigrationPrepare
	MigrationCutover
	MigrationCommitted
	MigrationAborted
)

func (m MigrationState) String() string {
	switch m {
	case MigrationIdle:
		return "idle"
	case MigrationPrepare:
		return "prepare"
	case MigrationCutover:
		return "cutover"
	case MigrationCommitted:
		return "committed"
	case MigrationAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

// MigrationStatus is a diagnostic view of a migration.
type MigrationStatus struct {
	Shard    ShardID
	From     ShardConfig
	To       ShardConfig
	State    MigrationState
	Synced   map[NodeID]SyncPosition
	NewEpoch uint64
}

// Result is the common result envelope (kept for documentation/future use).
type Result struct {
	OK      bool
	Message string
}
