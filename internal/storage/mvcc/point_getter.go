package mvcc

import (
	"errors"

	"github.com/ryogrid/gookv/pkg/txntypes"
)

// IsolationLevel defines the transaction isolation level.
type IsolationLevel int

const (
	// IsolationLevelSI is Snapshot Isolation (default).
	IsolationLevelSI IsolationLevel = iota
	// IsolationLevelRC is Read Committed.
	IsolationLevelRC
)

var (
	// ErrKeyIsLocked is returned when a key is locked by another transaction.
	ErrKeyIsLocked = errors.New("mvcc: key is locked")
)

// LockError is a structured error that carries the conflicting lock details.
// It wraps ErrKeyIsLocked so errors.Is(err, ErrKeyIsLocked) still works.
type LockError struct {
	Key  Key
	Lock *txntypes.Lock
}

func (e *LockError) Error() string {
	return ErrKeyIsLocked.Error()
}

func (e *LockError) Is(target error) bool {
	return target == ErrKeyIsLocked
}

// LockInfo contains information about a conflicting lock.
type LockInfo struct {
	Key      Key
	Lock     *txntypes.Lock
	LockTTL  uint64
	StartTS  txntypes.TimeStamp
	Primary  []byte
	LockType txntypes.LockType
}

// PointGetter performs optimized single-key MVCC reads.
type PointGetter struct {
	reader         *MvccReader
	ts             txntypes.TimeStamp
	isolationLevel IsolationLevel
	bypassLocks    map[txntypes.TimeStamp]bool // Lock timestamps to bypass.
}

// NewPointGetter creates a PointGetter for reading at the given timestamp.
func NewPointGetter(reader *MvccReader, ts txntypes.TimeStamp, level IsolationLevel) *PointGetter {
	return &PointGetter{
		reader:         reader,
		ts:             ts,
		isolationLevel: level,
		bypassLocks:    make(map[txntypes.TimeStamp]bool),
	}
}

// SetBypassLocks configures lock timestamps that should be bypassed (e.g., own locks).
func (pg *PointGetter) SetBypassLocks(locks map[txntypes.TimeStamp]bool) {
	pg.bypassLocks = locks
}

// Get reads the value for key at the configured timestamp.
// Returns (nil, nil) if the key does not exist at that timestamp.
func (pg *PointGetter) Get(key Key) ([]byte, error) {
	// 1. Check for blocking locks (SI mode only).
	if pg.isolationLevel == IsolationLevelSI {
		lock, err := pg.reader.LoadLock(key)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.StartTS <= pg.ts {
			if !pg.bypassLocks[lock.StartTS] {
				return nil, &LockError{Key: key, Lock: lock}
			}
		}
	}

	// 2. Find the visible write record.
	write, _, err := pg.reader.GetWrite(key, pg.ts)
	if err != nil {
		return nil, err
	}
	if write == nil {
		return nil, nil // Key does not exist at this timestamp.
	}

	// 3. Read the value.
	if write.ShortValue != nil {
		return write.ShortValue, nil
	}

	// Value is stored in CF_DEFAULT.
	value, err := pg.reader.GetValue(key, write.StartTS)
	if err != nil {
		return nil, err
	}
	return value, nil
}
