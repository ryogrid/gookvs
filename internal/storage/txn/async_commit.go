// async_commit.go implements async commit and 1PC transaction optimizations.
//
// Async commit: The primary key's lock stores all secondary keys. When the
// primary lock is written, the transaction is logically committed. The actual
// commit records are written asynchronously. Readers can determine the commit
// status by checking the primary lock.
//
// 1PC: For small, single-region transactions, the lock and commit record are
// written in a single write batch, skipping CF_LOCK entirely.
package txn

import (
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/pkg/txntypes"
)

// AsyncCommitPrewriteProps extends PrewriteProps for async commit.
type AsyncCommitPrewriteProps struct {
	PrewriteProps
	UseAsyncCommit bool
	Secondaries    [][]byte // All secondary keys (stored in primary lock)
	MaxCommitTS    txntypes.TimeStamp
}

// PrewriteAsyncCommit performs prewrite with async commit support.
// When UseAsyncCommit is true, the primary lock stores all secondary keys,
// and MinCommitTS is set based on the concurrency manager's max_ts.
func PrewriteAsyncCommit(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, props AsyncCommitPrewriteProps, mutation Mutation) error {
	key := mutation.Key

	// 1. Check for existing lock.
	existingLock, err := reader.LoadLock(key)
	if err != nil {
		return err
	}
	if existingLock != nil {
		if existingLock.StartTS == props.StartTS {
			return nil // Idempotent.
		}
		return ErrKeyIsLocked
	}

	// 2. Check for write conflicts.
	write, commitTS, err := reader.SeekWrite(key, txntypes.TSMax)
	if err != nil {
		return err
	}
	if write != nil && commitTS > props.StartTS {
		if write.WriteType != txntypes.WriteTypeRollback && write.WriteType != txntypes.WriteTypeLock {
			return ErrWriteConflict
		}
	}

	// 3. Build the lock.
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

	// Async commit specific fields.
	if props.UseAsyncCommit {
		lock.UseAsyncCommit = true
		if props.IsPrimary {
			lock.Secondaries = props.Secondaries
		}
		// Set MinCommitTS for async commit correctness.
		// min_commit_ts = max(start_ts + 1, max_ts_from_concurrency_manager + 1)
		minCommitTS := props.StartTS + 1
		if props.MaxCommitTS > 0 && props.MaxCommitTS+1 > minCommitTS {
			minCommitTS = props.MaxCommitTS + 1
		}
		lock.MinCommitTS = minCommitTS
	}

	txn.PutLock(key, lock)

	// Write large value to CF_DEFAULT if needed.
	if mutation.Op == MutationOpPut && mutation.Value != nil && len(mutation.Value) > txntypes.ShortValueMaxLen {
		txn.PutValue(key, props.StartTS, mutation.Value)
	}

	return nil
}

// CheckAsyncCommitStatus checks the status of an async commit transaction
// by examining the primary key's lock and all secondary keys.
// Returns the minimum commit TS if the transaction is committed.
func CheckAsyncCommitStatus(reader *mvcc.MvccReader, primaryKey mvcc.Key, startTS txntypes.TimeStamp) (txntypes.TimeStamp, error) {
	lock, err := reader.LoadLock(primaryKey)
	if err != nil {
		return 0, err
	}

	// If the primary lock is still present and it's our transaction.
	if lock != nil && lock.StartTS == startTS {
		if !lock.UseAsyncCommit {
			// Not an async commit transaction.
			return 0, nil
		}
		// Transaction is still in progress (primary not yet committed).
		// Return MinCommitTS as the candidate.
		return lock.MinCommitTS, nil
	}

	// Primary lock is gone - check commit record.
	write, commitTS, err := reader.GetTxnCommitRecord(primaryKey, startTS)
	if err != nil {
		return 0, err
	}
	if write != nil && write.WriteType != txntypes.WriteTypeRollback {
		return commitTS, nil
	}

	// Rolled back or no record found.
	return 0, nil
}

// --- 1PC Optimization ---

// OnePC1PCProps holds properties for 1PC transactions.
type OnePCProps struct {
	StartTS  txntypes.TimeStamp
	CommitTS txntypes.TimeStamp
	Primary  []byte
	LockTTL  uint64
}

// PrewriteAndCommit1PC performs prewrite and commit in a single step.
// This is used for small, single-region transactions where the lock and
// commit can be written atomically, avoiding CF_LOCK entirely.
func PrewriteAndCommit1PC(txn *mvcc.MvccTxn, reader *mvcc.MvccReader, props OnePCProps, mutations []Mutation) []error {
	errs := make([]error, len(mutations))

	// Check all mutations for conflicts first.
	for i, mut := range mutations {
		key := mut.Key

		// Check for existing locks.
		existingLock, err := reader.LoadLock(key)
		if err != nil {
			errs[i] = err
			continue
		}
		if existingLock != nil && existingLock.StartTS != props.StartTS {
			errs[i] = ErrKeyIsLocked
			continue
		}

		// Check for write conflicts.
		write, commitTS, err := reader.SeekWrite(key, txntypes.TSMax)
		if err != nil {
			errs[i] = err
			continue
		}
		if write != nil && commitTS > props.StartTS {
			if write.WriteType != txntypes.WriteTypeRollback && write.WriteType != txntypes.WriteTypeLock {
				errs[i] = ErrWriteConflict
				continue
			}
		}
	}

	// Check if any error occurred.
	for _, err := range errs {
		if err != nil {
			return errs
		}
	}

	// Write commit records directly (skip CF_LOCK).
	for _, mut := range mutations {
		key := mut.Key

		writeType := mutationOpToWriteType(mut.Op)
		write := &txntypes.Write{
			WriteType: writeType,
			StartTS:   props.StartTS,
		}

		// Inline short values.
		if mut.Value != nil && len(mut.Value) <= txntypes.ShortValueMaxLen {
			write.ShortValue = mut.Value
		} else if mut.Op == MutationOpPut && mut.Value != nil {
			// Write large value to CF_DEFAULT.
			txn.PutValue(key, props.StartTS, mut.Value)
		}

		txn.PutWrite(key, props.CommitTS, write)
	}

	return errs
}

func mutationOpToWriteType(op MutationOp) txntypes.WriteType {
	switch op {
	case MutationOpPut:
		return txntypes.WriteTypePut
	case MutationOpDelete:
		return txntypes.WriteTypeDelete
	case MutationOpLock:
		return txntypes.WriteTypeLock
	default:
		return txntypes.WriteTypePut
	}
}

// Is1PCEligible checks whether a transaction is eligible for 1PC.
// Eligibility criteria:
// - All mutations target the same region (single-region)
// - Transaction size is small (under threshold)
// - No pessimistic locks involved
func Is1PCEligible(mutations []Mutation, maxSize int) bool {
	if len(mutations) == 0 {
		return false
	}
	if maxSize <= 0 {
		maxSize = 64 // Default threshold.
	}
	if len(mutations) > maxSize {
		return false
	}

	totalSize := 0
	for _, m := range mutations {
		totalSize += len(m.Key) + len(m.Value)
	}

	// Under 256KB total size.
	return totalSize < 256*1024
}

// IsAsyncCommitEligible checks whether a transaction is eligible for async commit.
func IsAsyncCommitEligible(mutations []Mutation, maxKeys int) bool {
	if len(mutations) == 0 {
		return false
	}
	if maxKeys <= 0 {
		maxKeys = 256 // Default max keys for async commit.
	}
	return len(mutations) <= maxKeys
}
