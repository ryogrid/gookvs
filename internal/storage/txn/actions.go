// Package txn implements the transaction processing layer including Percolator
// protocol actions (prewrite, commit, rollback) and the TxnScheduler.
package txn

import (
	"errors"

	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

var (
	// ErrWriteConflict is returned when a prewrite encounters a newer committed write.
	ErrWriteConflict = errors.New("txn: write conflict")
	// ErrKeyIsLocked is returned when a key is locked by another transaction.
	ErrKeyIsLocked = errors.New("txn: key is locked")
	// ErrTxnLockNotFound is returned during commit when the expected lock is missing.
	ErrTxnLockNotFound = errors.New("txn: lock not found")
	// ErrAlreadyCommitted is returned when trying to rollback an already committed txn.
	ErrAlreadyCommitted = errors.New("txn: already committed")
)

// PrewriteProps holds parameters for a prewrite action.
type PrewriteProps struct {
	StartTS    txntypes.TimeStamp
	Primary    []byte
	LockTTL    uint64
	IsPrimary  bool
}

// MutationOp represents the type of mutation.
type MutationOp int

const (
	MutationOpPut    MutationOp = iota
	MutationOpDelete
	MutationOpLock
)

// Mutation represents a single key mutation in a transaction.
type Mutation struct {
	Op    MutationOp
	Key   mvcc.Key
	Value []byte // Nil for Delete and Lock.
}

// Prewrite performs the first phase of 2PC for a single key.
// It checks for conflicts, then writes the lock (and value if needed).
func Prewrite(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, props PrewriteProps, mutation Mutation) error {
	key := mutation.Key

	// 1. Check for existing lock on this key.
	existingLock, err := reader.LoadLock(key)
	if err != nil {
		return err
	}
	if existingLock != nil {
		if existingLock.StartTS == props.StartTS {
			// Already prewrote by us (idempotent).
			return nil
		}
		return ErrKeyIsLocked
	}

	// 2. Check for write conflicts (newer writes since start_ts).
	write, commitTS, err := reader.SeekWrite(key, txntypes.TSMax)
	if err != nil {
		return err
	}
	if write != nil && commitTS > props.StartTS {
		if write.WriteType != txntypes.WriteTypeRollback && write.WriteType != txntypes.WriteTypeLock {
			return ErrWriteConflict
		}
	}

	// 3. Write the lock.
	lockType := mutationOpToLockType(mutation.Op)
	lock := &txntypes.Lock{
		LockType: lockType,
		Primary:  props.Primary,
		StartTS:  props.StartTS,
		TTL:      props.LockTTL,
	}

	// Inline short values.
	if mutation.Value != nil && len(mutation.Value) <= txntypes.ShortValueMaxLen {
		lock.ShortValue = mutation.Value
	}

	txn.PutLock(key, lock)

	// Write large value to CF_DEFAULT if needed.
	if mutation.Op == MutationOpPut && mutation.Value != nil && len(mutation.Value) > txntypes.ShortValueMaxLen {
		txn.PutValue(key, props.StartTS, mutation.Value)
	}

	return nil
}

// Commit performs the second phase of 2PC for a single key.
// It removes the lock and writes the commit record.
func Commit(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, key mvcc.Key, startTS, commitTS txntypes.TimeStamp) error {
	// 1. Load and validate lock.
	lock, err := reader.LoadLock(key)
	if err != nil {
		return err
	}
	if lock == nil || lock.StartTS != startTS {
		return ErrTxnLockNotFound
	}

	// 2. Check min_commit_ts constraint.
	if commitTS < lock.MinCommitTS {
		return errors.New("txn: commit_ts less than min_commit_ts")
	}

	// 3. Handle pessimistic lock that wasn't prewrote.
	if lock.LockType == txntypes.LockTypePessimistic {
		txn.UnlockKey(key, true)
		return nil
	}

	// 4. Remove lock.
	txn.UnlockKey(key, false)

	// 5. Write commit record.
	writeType := lockTypeToWriteType(lock.LockType)
	write := &txntypes.Write{
		WriteType:  writeType,
		StartTS:    lock.StartTS,
		ShortValue: lock.ShortValue,
	}
	txn.PutWrite(key, commitTS, write)

	return nil
}

// Rollback rolls back a transaction's lock on a key and writes a rollback record.
func Rollback(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, key mvcc.Key, startTS txntypes.TimeStamp) error {
	// 1. Check if already committed.
	existingWrite, _, err := reader.GetTxnCommitRecord(key, startTS)
	if err != nil {
		return err
	}
	if existingWrite != nil {
		if existingWrite.WriteType != txntypes.WriteTypeRollback {
			return ErrAlreadyCommitted
		}
		// Already rolled back (idempotent).
		return nil
	}

	// 2. Remove lock if present.
	lock, err := reader.LoadLock(key)
	if err != nil {
		return err
	}
	if lock != nil && lock.StartTS == startTS {
		isPessimistic := lock.LockType == txntypes.LockTypePessimistic
		txn.UnlockKey(key, isPessimistic)

		// Delete value from CF_DEFAULT if it was written.
		if lock.ShortValue == nil && (lock.LockType == txntypes.LockTypePut) {
			txn.DeleteValue(key, startTS)
		}
	}

	// 3. Write rollback record.
	rollbackWrite := &txntypes.Write{
		WriteType: txntypes.WriteTypeRollback,
		StartTS:   startTS,
	}
	txn.PutWrite(key, startTS, rollbackWrite)

	return nil
}

// CheckTxnStatus checks the status of a transaction by examining its primary key.
// Returns the lock (if still locked) or the commit/rollback timestamp.
type TxnStatus struct {
	IsLocked    bool
	Lock        *txntypes.Lock
	CommitTS    txntypes.TimeStamp // Non-zero if committed.
	IsRolledBack bool
}

// CheckTxnStatus determines the status of a transaction.
func CheckTxnStatus(reader *mvcc.MvccReader, primaryKey mvcc.Key, startTS txntypes.TimeStamp) (*TxnStatus, error) {
	// Check for lock.
	lock, err := reader.LoadLock(primaryKey)
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.StartTS == startTS {
		return &TxnStatus{IsLocked: true, Lock: lock}, nil
	}

	// Check for commit or rollback record.
	write, commitTS, err := reader.GetTxnCommitRecord(primaryKey, startTS)
	if err != nil {
		return nil, err
	}
	if write != nil {
		if write.WriteType == txntypes.WriteTypeRollback {
			return &TxnStatus{IsRolledBack: true}, nil
		}
		return &TxnStatus{CommitTS: commitTS}, nil
	}

	// Transaction was not found (lock expired and cleaned up, or never existed).
	return &TxnStatus{}, nil
}

// TxnHeartBeat updates the TTL of an existing lock on the primary key.
// Returns the actual TTL set on the lock after the update.
func TxnHeartBeat(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, primaryKey mvcc.Key, startTS txntypes.TimeStamp, adviseTTL uint64) (uint64, error) {
	lock, err := reader.LoadLock(primaryKey)
	if err != nil {
		return 0, err
	}
	if lock == nil || lock.StartTS != startTS {
		return 0, ErrTxnLockNotFound
	}

	if adviseTTL > lock.TTL {
		lock.TTL = adviseTTL
		txn.PutLock(primaryKey, lock)
	}

	return lock.TTL, nil
}

// ResolveLock resolves a single key's lock for a given transaction.
// If commitTS > 0, the lock is committed; if commitTS == 0, the lock is rolled back.
// If no lock exists for this key/startTS, the operation is silently skipped.
func ResolveLock(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, key mvcc.Key, startTS, commitTS txntypes.TimeStamp) error {
	lock, err := reader.LoadLock(key)
	if err != nil {
		return err
	}
	if lock == nil || lock.StartTS != startTS {
		return nil // No lock to resolve; silently skip.
	}

	if commitTS > 0 {
		// Commit the lock.
		return Commit(txn, reader, key, startTS, commitTS)
	}
	// Rollback the lock.
	return Rollback(txn, reader, key, startTS)
}

func mutationOpToLockType(op MutationOp) txntypes.LockType {
	switch op {
	case MutationOpPut:
		return txntypes.LockTypePut
	case MutationOpDelete:
		return txntypes.LockTypeDelete
	case MutationOpLock:
		return txntypes.LockTypeLock
	default:
		return txntypes.LockTypePut
	}
}

func lockTypeToWriteType(lt txntypes.LockType) txntypes.WriteType {
	switch lt {
	case txntypes.LockTypePut:
		return txntypes.WriteTypePut
	case txntypes.LockTypeDelete:
		return txntypes.WriteTypeDelete
	case txntypes.LockTypeLock:
		return txntypes.WriteTypeLock
	default:
		return txntypes.WriteTypePut
	}
}
