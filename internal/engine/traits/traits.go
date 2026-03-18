// Package traits defines the storage engine interface abstractions for gookvs.
// These interfaces mirror TiKV's engine_traits crate, adapted to Go idioms.
package traits

import "errors"

var (
	// ErrNotFound is returned when a key is not found.
	ErrNotFound = errors.New("engine: key not found")
	// ErrCFNotFound is returned when a column family does not exist.
	ErrCFNotFound = errors.New("engine: column family not found")
)

// KvEngine is the primary interface for a key-value storage engine.
// It supports multiple column families, snapshots, and atomic write batches.
type KvEngine interface {
	// Get retrieves the value for a key in the given column family.
	// Returns ErrNotFound if the key does not exist.
	Get(cf string, key []byte) ([]byte, error)

	// GetMsg retrieves and unmarshals a protobuf message.
	// Returns ErrNotFound if the key does not exist.
	GetMsg(cf string, key []byte, msg interface{ Unmarshal([]byte) error }) error

	// Put stores a key-value pair in the given column family.
	Put(cf string, key, value []byte) error

	// PutMsg marshals and stores a protobuf message.
	PutMsg(cf string, key []byte, msg interface{ Marshal() ([]byte, error) }) error

	// Delete removes a key from the given column family.
	Delete(cf string, key []byte) error

	// DeleteRange removes all keys in [startKey, endKey) from the given column family.
	DeleteRange(cf string, startKey, endKey []byte) error

	// NewSnapshot creates a consistent read-only snapshot of the engine.
	NewSnapshot() Snapshot

	// NewWriteBatch creates a new atomic write batch.
	NewWriteBatch() WriteBatch

	// NewIterator creates a new iterator for the given column family.
	NewIterator(cf string, opts IterOptions) Iterator

	// SyncWAL flushes the write-ahead log to disk.
	SyncWAL() error

	// GetProperty returns an engine property value.
	GetProperty(cf string, name string) (string, error)

	// Close shuts down the engine, releasing all resources.
	Close() error
}

// Snapshot is a consistent read-only view of the engine at a point in time.
type Snapshot interface {
	// Get retrieves the value for a key in the given column family.
	Get(cf string, key []byte) ([]byte, error)

	// GetMsg retrieves and unmarshals a protobuf message.
	GetMsg(cf string, key []byte, msg interface{ Unmarshal([]byte) error }) error

	// NewIterator creates an iterator over the snapshot.
	NewIterator(cf string, opts IterOptions) Iterator

	// Close releases the snapshot.
	Close()
}

// WriteBatch accumulates mutations for atomic application.
type WriteBatch interface {
	// Put stores a key-value pair.
	Put(cf string, key, value []byte) error

	// PutMsg marshals and stores a protobuf message.
	PutMsg(cf string, key []byte, msg interface{ Marshal() ([]byte, error) }) error

	// Delete removes a key.
	Delete(cf string, key []byte) error

	// DeleteRange removes all keys in [startKey, endKey).
	DeleteRange(cf string, startKey, endKey []byte) error

	// Count returns the number of operations in the batch.
	Count() int

	// DataSize returns the total size of data in the batch.
	DataSize() int

	// Clear resets the batch to empty.
	Clear()

	// SetSavePoint marks the current position for potential rollback.
	SetSavePoint()

	// RollbackToSavePoint rolls back to the last save point.
	RollbackToSavePoint() error

	// PopSavePoint removes the most recent save point without rolling back.
	PopSavePoint() error

	// Commit atomically applies all mutations in the batch.
	Commit() error
}

// Iterator enables sequential access to keys within a column family.
type Iterator interface {
	// SeekToFirst positions the iterator at the first key.
	SeekToFirst()

	// SeekToLast positions the iterator at the last key.
	SeekToLast()

	// Seek positions the iterator at the first key >= target.
	Seek(target []byte)

	// SeekForPrev positions the iterator at the last key <= target.
	SeekForPrev(target []byte)

	// Next advances the iterator to the next key.
	Next()

	// Prev moves the iterator to the previous key.
	Prev()

	// Valid returns true if the iterator is positioned at a valid entry.
	Valid() bool

	// Key returns the current key. Only valid when Valid() is true.
	Key() []byte

	// Value returns the current value. Only valid when Valid() is true.
	Value() []byte

	// Error returns any accumulated error.
	Error() error

	// Close releases the iterator.
	Close()
}

// IterOptions configures iterator behavior.
type IterOptions struct {
	// LowerBound is the inclusive lower bound for iteration.
	// The iterator will not return keys before this bound.
	LowerBound []byte

	// UpperBound is the exclusive upper bound for iteration.
	// The iterator will not return keys at or after this bound.
	UpperBound []byte

	// FillCache controls whether data read by this iterator populates the block cache.
	FillCache bool

	// PrefixSameAsStart configures prefix-based iteration.
	PrefixSameAsStart bool
}
