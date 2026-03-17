// pessimistic.go implements pessimistic transaction support for gookvs.
//
// Pessimistic locking allows clients to acquire locks before prewrite,
// preventing write conflicts during interactive transactions.
// The flow is: AcquirePessimisticLock -> Prewrite (upgrade) -> Commit
//
// Key semantics:
// - Pessimistic locks use LockTypePessimistic
// - Pessimistic locks are invisible to readers (readers skip them)
// - During prewrite, a pessimistic lock is upgraded to a normal lock
// - Pessimistic rollback removes only pessimistic locks
package txn

import (
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// PessimisticLockProps holds parameters for pessimistic lock acquisition.
type PessimisticLockProps struct {
	StartTS     txntypes.TimeStamp
	ForUpdateTS txntypes.TimeStamp
	Primary     []byte
	LockTTL     uint64
	WaitTimeout uint64 // Milliseconds to wait for existing lock (0 = no wait)
}

// AcquirePessimisticLock acquires a pessimistic lock on a key.
// This does not write any data, only a lock in CF_LOCK.
// The lock is invisible to other readers.
func AcquirePessimisticLock(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, props PessimisticLockProps, key mvcc.Key) error {
	// 1. Check for existing lock.
	existingLock, err := reader.LoadLock(key)
	if err != nil {
		return err
	}
	if existingLock != nil {
		if existingLock.StartTS == props.StartTS {
			// Lock belongs to us.
			if existingLock.LockType == txntypes.LockTypePessimistic {
				// Already acquired pessimistic lock (idempotent).
				// Update ForUpdateTS if needed.
				if props.ForUpdateTS > existingLock.ForUpdateTS {
					existingLock.ForUpdateTS = props.ForUpdateTS
					txn.PutLock(key, existingLock)
				}
				return nil
			}
			// Already has a normal lock (prewrite already happened).
			return nil
		}
		// Lock belongs to another transaction.
		return ErrKeyIsLocked
	}

	// 2. Check for write conflicts.
	// A pessimistic lock conflicts with writes after ForUpdateTS.
	write, commitTS, err := reader.SeekWrite(key, txntypes.TSMax)
	if err != nil {
		return err
	}
	if write != nil && commitTS > props.ForUpdateTS {
		if write.WriteType != txntypes.WriteTypeRollback && write.WriteType != txntypes.WriteTypeLock {
			return ErrWriteConflict
		}
	}

	// 3. Write pessimistic lock.
	lock := &txntypes.Lock{
		LockType:    txntypes.LockTypePessimistic,
		Primary:     props.Primary,
		StartTS:     props.StartTS,
		TTL:         props.LockTTL,
		ForUpdateTS: props.ForUpdateTS,
	}
	txn.PutLock(key, lock)

	return nil
}

// PessimisticPrewriteProps extends PrewriteProps for pessimistic prewrite.
type PessimisticPrewriteProps struct {
	PrewriteProps
	ForUpdateTS    txntypes.TimeStamp
	IsPessimistic  bool
}

// PrewritePessimistic performs prewrite that upgrades a pessimistic lock to
// a normal lock, or creates a new lock if none exists.
// This is used during the transition from pessimistic lock -> prewrite.
func PrewritePessimistic(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, props PessimisticPrewriteProps, mutation Mutation) error {
	key := mutation.Key

	// 1. Check for existing lock.
	existingLock, err := reader.LoadLock(key)
	if err != nil {
		return err
	}

	if existingLock != nil {
		if existingLock.StartTS != props.StartTS {
			return ErrKeyIsLocked
		}

		if existingLock.LockType != txntypes.LockTypePessimistic {
			// Already has a normal lock (idempotent prewrite).
			return nil
		}

		// Upgrade pessimistic lock to normal lock.
		// Remove old pessimistic lock first.
		txn.UnlockKey(key, true) // pessimistic = true
	} else if props.IsPessimistic {
		// Expected pessimistic lock but not found.
		// This can happen if the lock expired and was cleaned up.
		return ErrTxnLockNotFound
	} else {
		// Non-pessimistic prewrite on new key - check write conflicts.
		write, commitTS, err := reader.SeekWrite(key, txntypes.TSMax)
		if err != nil {
			return err
		}
		if write != nil && commitTS > props.StartTS {
			if write.WriteType != txntypes.WriteTypeRollback && write.WriteType != txntypes.WriteTypeLock {
				return ErrWriteConflict
			}
		}
	}

	// 2. Write the normal lock.
	lockType := mutationOpToLockType(mutation.Op)
	lock := &txntypes.Lock{
		LockType:    lockType,
		Primary:     props.Primary,
		StartTS:     props.StartTS,
		TTL:         props.LockTTL,
		ForUpdateTS: props.ForUpdateTS,
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

// PessimisticRollback removes pessimistic locks for the given keys.
// Unlike normal rollback, this does NOT write a rollback record -
// it only removes the pessimistic lock.
func PessimisticRollback(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, keys []mvcc.Key, startTS, forUpdateTS txntypes.TimeStamp) []error {
	errs := make([]error, len(keys))

	for i, key := range keys {
		lock, err := reader.LoadLock(key)
		if err != nil {
			errs[i] = err
			continue
		}

		if lock == nil || lock.StartTS != startTS {
			// Lock not found or belongs to another transaction.
			// This is OK - the lock may have already been cleaned up.
			continue
		}

		if lock.LockType != txntypes.LockTypePessimistic {
			// Not a pessimistic lock - don't touch it.
			// The caller should use normal rollback for this.
			continue
		}

		// Only rollback if ForUpdateTS matches (or is 0, meaning any).
		if forUpdateTS != 0 && lock.ForUpdateTS != forUpdateTS {
			continue
		}

		txn.UnlockKey(key, true) // pessimistic = true
	}

	return errs
}
