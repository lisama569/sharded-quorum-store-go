package store

import (
	"errors"
	"fmt"
)

// ErrNotImplemented is returned by skeleton stubs until implemented.
var ErrNotImplemented = errors.New("not implemented")

// Distinguishable sentinel errors returned by client operations.
var (
	// ErrTimeout means the operation exceeded its deadline / quorum unmet in time.
	ErrTimeout = errors.New("operation timeout")
	// ErrUnavailable means no leader / not enough live nodes for the required quorum.
	ErrUnavailable = errors.New("unavailable")
	// ErrConflict means a conditional write (CAS/version) conflict or an operation
	// was rejected because the shard is mid-cutover.
	ErrConflict = errors.New("conflict")
	// ErrKeyMigrating means the key cannot be accepted during migration cutover.
	ErrKeyMigrating = errors.New("key is migrating")
	// ErrStaleFollower means a node in an old generation/epoch tried to accept a write.
	ErrStaleFollower = errors.New("stale epoch: replica no longer owns this shard")
	// ErrNoLeader means leader election failed or no leader known.
	ErrNoLeader = errors.New("no leader")
	// ErrChecksumMismatch means a log record failed CRC validation on load/stream.
	ErrChecksumMismatch = errors.New("log checksum mismatch")
	// ErrLogGap means a follower's log is behind without a contiguous bridge.
	ErrLogGap = errors.New("log gap: follower log not contiguous")
	// ErrPositionRegression means a requested sync position is behind the node's position.
	ErrPositionRegression = errors.New("sync position regression rejected")
	// ErrInvalidConfig means malformed shard range / replica configuration.
	ErrInvalidConfig = errors.New("invalid configuration")
	// ErrNotFound means the key does not exist.
	ErrNotFound = errors.New("key not found")
)

// Consistency level requested by a client operation.
type Consistency int

const (
	// Quorum requires a quorum of replicas for the operation.
	Quorum Consistency = iota
	// One requires a single live replica.
	One
	// RequireAll requires every configured replica to acknowledge.
	RequireAll
)

func (c Consistency) String() string {
	switch c {
	case Quorum:
		return "quorum"
	case One:
		return "one"
	case RequireAll:
		return "all"
	default:
		return "unknown"
	}
}

// quorumSize returns R/2+1 for R replicas.
func quorumSize(replicas int) int { return replicas/2 + 1 }

// fmtErr wraps a sentinel error with diagnostic detail.
func fmtErr(sentinel error, format string, args ...any) error {
	return fmt.Errorf("%w: %s", sentinel, fmt.Sprintf(format, args...))
}
