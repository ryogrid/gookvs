// Package server implements the gRPC server for gookv, providing
// TiKV-compatible KV service RPCs.
package server

import (
	"bytes"
	"errors"
	"sync"

	"github.com/ryogrid/gookv/internal/engine/traits"
	"github.com/ryogrid/gookv/internal/storage/mvcc"
	"github.com/ryogrid/gookv/internal/storage/txn"
	"github.com/ryogrid/gookv/internal/storage/txn/concurrency"
	"github.com/ryogrid/gookv/internal/storage/txn/latch"
	"github.com/ryogrid/gookv/pkg/cfnames"
	"github.com/ryogrid/gookv/pkg/txntypes"
)

// Storage provides the transaction-aware storage interface used by gRPC service handlers.
// It bridges the engine layer, MVCC layer, and transaction layer together.
type Storage struct {
	engine      traits.KvEngine
	latches     *latch.Latches
	concMgr     *concurrency.Manager
	mu          sync.Mutex
	nextCmdID   uint64
}

// NewStorage creates a new Storage backed by the given engine.
func NewStorage(engine traits.KvEngine) *Storage {
	return &Storage{
		engine:    engine,
		latches:   latch.New(2048),
		concMgr:   concurrency.New(),
		nextCmdID: 1,
	}
}

func (s *Storage) allocCmdID() uint64 {
	s.mu.Lock()
	id := s.nextCmdID
	s.nextCmdID++
	s.mu.Unlock()
	return id
}

// LatchGuard holds a latch that must be released after the caller finishes
// proposing modifications to Raft. This ensures the latch is held across
// the Raft proposal, preventing concurrent transactions from reading stale
// snapshots before the modifications are applied.
type LatchGuard struct {
	lock  *latch.Lock
	cmdID uint64
}

// ReleaseLatch releases a LatchGuard obtained from a *Modifies method.
// Safe to call with nil (no-op).
func (s *Storage) ReleaseLatch(g *LatchGuard) {
	if g != nil {
		s.latches.Release(g.lock, g.cmdID)
	}
}

// ApplyModifies writes the accumulated MVCC modifications to the engine atomically.
// Exported so coordinator can call it from applyFunc.
func (s *Storage) ApplyModifies(modifies []mvcc.Modify) error {
	if len(modifies) == 0 {
		return nil
	}
	wb := s.engine.NewWriteBatch()
	for _, m := range modifies {
		switch m.Type {
		case mvcc.ModifyTypePut:
			if err := wb.Put(m.CF, m.Key, m.Value); err != nil {
				return err
			}
		case mvcc.ModifyTypeDelete:
			if err := wb.Delete(m.CF, m.Key); err != nil {
				return err
			}
		case mvcc.ModifyTypeDeleteRange:
			if err := wb.DeleteRange(m.CF, m.Key, m.EndKey); err != nil {
				return err
			}
		}
	}
	return wb.Commit()
}

// Get performs a transactional point read at the given timestamp.
func (s *Storage) Get(key []byte, version txntypes.TimeStamp) ([]byte, error) {
	return s.GetWithIsolation(key, version, mvcc.IsolationLevelSI)
}

// GetWithIsolation performs a point read at the given timestamp with the specified isolation level.
func (s *Storage) GetWithIsolation(key []byte, version txntypes.TimeStamp, level mvcc.IsolationLevel) ([]byte, error) {
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	pg := mvcc.NewPointGetter(reader, version, level)
	return pg.Get(key)
}

// Scan performs a transactional range scan at the given timestamp.
func (s *Storage) Scan(startKey, endKey []byte, limit uint32, version txntypes.TimeStamp, keyOnly bool) ([]*KvPairResult, error) {
	snap := s.engine.NewSnapshot()
	defer snap.Close()

	cfg := mvcc.ScannerConfig{
		Snapshot:       snap,
		ReadTS:         version,
		IsolationLevel: mvcc.IsolationLevelSI,
		KeyOnly:        keyOnly,
		LowerBound:     startKey,
	}
	if len(endKey) > 0 {
		cfg.UpperBound = endKey
	}

	scanner := mvcc.NewScanner(cfg)
	defer scanner.Close()

	var results []*KvPairResult
	for limit == 0 || uint32(len(results)) < limit {
		key, value, err := scanner.Next()
		if err != nil {
			if errors.Is(err, mvcc.ErrKeyIsLocked) {
				return nil, err
			}
			return nil, err
		}
		if key == nil {
			break
		}
		pair := &KvPairResult{Key: key}
		if !keyOnly {
			pair.Value = value
		}
		results = append(results, pair)
	}

	return results, nil
}

// KvPairResult holds a key-value pair from a scan operation.
type KvPairResult struct {
	Key   []byte
	Value []byte
	Err   error
}

// BatchGet performs transactional multi-key reads at the given timestamp.
func (s *Storage) BatchGet(keys [][]byte, version txntypes.TimeStamp) ([]*KvPairResult, error) {
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	var results []*KvPairResult
	for _, key := range keys {
		pg := mvcc.NewPointGetter(reader, version, mvcc.IsolationLevelSI)
		value, err := pg.Get(key)
		if err != nil {
			results = append(results, &KvPairResult{Key: key, Err: err})
			continue
		}
		if value != nil {
			results = append(results, &KvPairResult{Key: key, Value: value})
		}
	}
	return results, nil
}

// PrewriteModifies performs the first phase of 2PC and returns the MVCC modifications
// without applying them to the engine. Used in cluster mode to propose via Raft.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) PrewriteModifies(mutations []txn.Mutation, primary []byte, startTS txntypes.TimeStamp, lockTTL uint64) ([]mvcc.Modify, []error, *LatchGuard) {
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}

	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	errs := make([]error, len(mutations))
	props := txn.PrewriteProps{
		StartTS:   startTS,
		Primary:   primary,
		LockTTL:   lockTTL,
		IsPrimary: false,
	}

	for i, mut := range mutations {
		if bytes.Equal(mut.Key, primary) {
			props.IsPrimary = true
		} else {
			props.IsPrimary = false
		}
		errs[i] = txn.Prewrite(mvccTxn, reader, props, mut)
	}

	for _, err := range errs {
		if err != nil {
			return nil, errs, guard
		}
	}

	return mvccTxn.Modifies, errs, guard
}

// CommitModifies performs the second phase of 2PC and returns the MVCC modifications
// without applying them to the engine. Used in cluster mode to propose via Raft.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) CommitModifies(keys [][]byte, startTS, commitTS txntypes.TimeStamp) ([]mvcc.Modify, error, *LatchGuard) {
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	for _, key := range keys {
		if err := txn.Commit(mvccTxn, reader, key, startTS, commitTS); err != nil {
			return nil, err, guard
		}
	}

	return mvccTxn.Modifies, nil, guard
}

// Prewrite performs the first phase of 2PC for multiple mutations.
func (s *Storage) Prewrite(mutations []txn.Mutation, primary []byte, startTS txntypes.TimeStamp, lockTTL uint64) []error {
	// Collect keys for latching.
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}

	// Acquire latches.
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
		// Spin until acquired. In production, use a wait channel.
	}
	defer s.latches.Release(lock, cmdID)

	// Take snapshot and create reader/writer.
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	errs := make([]error, len(mutations))
	props := txn.PrewriteProps{
		StartTS:   startTS,
		Primary:   primary,
		LockTTL:   lockTTL,
		IsPrimary: false,
	}

	for i, mut := range mutations {
		if bytes.Equal(mut.Key, primary) {
			props.IsPrimary = true
		} else {
			props.IsPrimary = false
		}
		errs[i] = txn.Prewrite(mvccTxn, reader, props, mut)
	}

	// Check if any error occurred.
	hasError := false
	for _, err := range errs {
		if err != nil {
			hasError = true
			break
		}
	}

	// Only apply modifications if all prewrites succeeded.
	if !hasError {
		if err := s.ApplyModifies(mvccTxn.Modifies); err != nil {
			// If apply fails, return the error for all mutations.
			for i := range errs {
				errs[i] = err
			}
		}
	}

	return errs
}

// Commit performs the second phase of 2PC for multiple keys.
func (s *Storage) Commit(keys [][]byte, startTS, commitTS txntypes.TimeStamp) error {
	// Acquire latches.
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	for _, key := range keys {
		if err := txn.Commit(mvccTxn, reader, key, startTS, commitTS); err != nil {
			return err
		}
	}

	return s.ApplyModifies(mvccTxn.Modifies)
}

// BatchRollback rolls back a transaction's locks on the given keys.
func (s *Storage) BatchRollback(keys [][]byte, startTS txntypes.TimeStamp) error {
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	for _, key := range keys {
		if err := txn.Rollback(mvccTxn, reader, key, startTS); err != nil {
			return err
		}
	}

	return s.ApplyModifies(mvccTxn.Modifies)
}

// BatchRollbackModifies rolls back a transaction's locks and returns MVCC modifications
// without applying them to the engine. Used in cluster mode to propose via Raft.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) BatchRollbackModifies(keys [][]byte, startTS txntypes.TimeStamp) ([]mvcc.Modify, error, *LatchGuard) {
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	for _, key := range keys {
		if err := txn.Rollback(mvccTxn, reader, key, startTS); err != nil {
			return nil, err, guard
		}
	}

	return mvccTxn.Modifies, nil, guard
}

// Cleanup cleans up a transaction lock on a key (same as rollback for a single key).
func (s *Storage) Cleanup(key []byte, startTS txntypes.TimeStamp) (txntypes.TimeStamp, error) {
	keys := [][]byte{key}
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	// Check if this key already has a commit/rollback record.
	status, err := txn.CheckTxnStatus(reader, key, startTS)
	if err != nil {
		return 0, err
	}
	if status.CommitTS != 0 {
		return status.CommitTS, nil
	}
	if status.IsRolledBack {
		return 0, nil
	}

	// Key has a lock. Check the PRIMARY key's status to determine commit/rollback.
	keyLock, err := reader.LoadLock(key)
	if err != nil {
		return 0, err
	}
	if keyLock == nil || keyLock.StartTS != startTS {
		// No lock to clean up.
		return 0, nil
	}

	primaryKey := keyLock.Primary
	mvccTxn := mvcc.NewMvccTxn(startTS)

	// Check primary key status.
	primaryStatus, err := txn.CheckTxnStatus(reader, primaryKey, startTS)
	if err != nil {
		// If primary check fails, default to rollback.
		if rbErr := txn.Rollback(mvccTxn, reader, key, startTS); rbErr != nil {
			return 0, rbErr
		}
		return 0, s.ApplyModifies(mvccTxn.Modifies)
	}

	if primaryStatus.CommitTS != 0 {
		// Primary was committed — commit this secondary key too.
		if err := txn.ResolveLock(mvccTxn, reader, key, startTS, primaryStatus.CommitTS); err != nil {
			return 0, err
		}
		if err := s.ApplyModifies(mvccTxn.Modifies); err != nil {
			return 0, err
		}
		return primaryStatus.CommitTS, nil
	}

	// Primary was rolled back or not found — rollback this key.
	if err := txn.Rollback(mvccTxn, reader, key, startTS); err != nil {
		return 0, err
	}
	return 0, s.ApplyModifies(mvccTxn.Modifies)
}

// CleanupModifies is like Cleanup but returns modifications instead of applying.
// Used in cluster mode to propose via Raft.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) CleanupModifies(key []byte, startTS txntypes.TimeStamp) (txntypes.TimeStamp, []mvcc.Modify, error, *LatchGuard) {
	keys := [][]byte{key}
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	// Check if a lock still exists on this key.
	keyLock, err := reader.LoadLock(key)
	if err != nil {
		return 0, nil, err, guard
	}
	if keyLock == nil || keyLock.StartTS != startTS {
		// No lock to clean — check if already resolved.
		status, err := txn.CheckTxnStatus(reader, key, startTS)
		if err != nil {
			return 0, nil, err, guard
		}
		return status.CommitTS, nil, nil, guard
	}

	// Lock exists. Check primary key's status to determine commit vs rollback.
	primaryKey := keyLock.Primary
	mvccTxn := mvcc.NewMvccTxn(startTS)

	primaryStatus, err := txn.CheckTxnStatus(reader, primaryKey, startTS)
	if err != nil {
		// Primary check failed — remove the lock and write a rollback record
		// to prevent a late-arriving commit from succeeding.
		isPessimistic := keyLock.LockType == txntypes.LockTypePessimistic
		mvccTxn.UnlockKey(key, isPessimistic)
		if keyLock.ShortValue == nil && keyLock.LockType == txntypes.LockTypePut {
			mvccTxn.DeleteValue(key, startTS)
		}
		rollbackWrite := &txntypes.Write{
			WriteType: txntypes.WriteTypeRollback,
			StartTS:   startTS,
		}
		mvccTxn.PutWrite(key, startTS, rollbackWrite)
		return 0, mvccTxn.Modifies, nil, guard
	}

	if primaryStatus.CommitTS != 0 {
		// Primary was committed — commit this secondary.
		if err := txn.ResolveLock(mvccTxn, reader, key, startTS, primaryStatus.CommitTS); err != nil {
			return 0, nil, err, guard
		}
		return primaryStatus.CommitTS, mvccTxn.Modifies, nil, guard
	}

	// Primary was rolled back, not found, or still locked — rollback this key.
	// Directly remove the lock and write rollback record.
	isPessimistic := keyLock.LockType == txntypes.LockTypePessimistic
	mvccTxn.UnlockKey(key, isPessimistic)
	if keyLock.ShortValue == nil && keyLock.LockType == txntypes.LockTypePut {
		mvccTxn.DeleteValue(key, startTS)
	}
	rollbackWrite := &txntypes.Write{
		WriteType: txntypes.WriteTypeRollback,
		StartTS:   startTS,
	}
	mvccTxn.PutWrite(key, startTS, rollbackWrite)
	return 0, mvccTxn.Modifies, nil, guard
}

// CheckTxnStatus checks the status of a transaction (read-only, no cleanup).
func (s *Storage) CheckTxnStatus(primaryKey []byte, startTS txntypes.TimeStamp) (*txn.TxnStatus, error) {
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	return txn.CheckTxnStatus(reader, primaryKey, startTS)
}

// CheckTxnStatusWithCleanup checks status and handles expired lock cleanup / RollbackIfNotExist.
// Returns (status, modifies, error, guard). modifies is non-nil when writes occurred (expired lock cleanup
// or RollbackIfNotExist wrote a rollback record).
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) CheckTxnStatusWithCleanup(primaryKey []byte, startTS, callerStartTS txntypes.TimeStamp, rollbackIfNotExist bool) (*txn.TxnStatus, []mvcc.Modify, error, *LatchGuard) {
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock([][]byte{primaryKey})
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)
	status, err := txn.CheckTxnStatusWithCleanup(mvccTxn, reader, primaryKey, startTS, callerStartTS, rollbackIfNotExist)
	if err != nil {
		return nil, nil, err, guard
	}

	return status, mvccTxn.Modifies, nil, guard
}

// PessimisticLock acquires pessimistic locks on the given keys (standalone mode).
func (s *Storage) PessimisticLock(keys [][]byte, primary []byte, startTS, forUpdateTS txntypes.TimeStamp, lockTTL uint64) []error {
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	props := txn.PessimisticLockProps{
		StartTS:     startTS,
		ForUpdateTS: forUpdateTS,
		Primary:     primary,
		LockTTL:     lockTTL,
	}
	mvccTxn := mvcc.NewMvccTxn(startTS)
	errs := make([]error, len(keys))
	for i, key := range keys {
		errs[i] = txn.AcquirePessimisticLock(mvccTxn, reader, props, key)
	}

	hasError := false
	for _, err := range errs {
		if err != nil {
			hasError = true
			break
		}
	}
	if !hasError {
		if err := s.ApplyModifies(mvccTxn.Modifies); err != nil {
			for i := range errs {
				errs[i] = err
			}
		}
	}
	return errs
}

// PessimisticLockModifies returns modifies for cluster mode.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) PessimisticLockModifies(keys [][]byte, primary []byte, startTS, forUpdateTS txntypes.TimeStamp, lockTTL uint64) ([]mvcc.Modify, []error, *LatchGuard) {
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	props := txn.PessimisticLockProps{
		StartTS:     startTS,
		ForUpdateTS: forUpdateTS,
		Primary:     primary,
		LockTTL:     lockTTL,
	}
	mvccTxn := mvcc.NewMvccTxn(startTS)
	errs := make([]error, len(keys))
	for i, key := range keys {
		errs[i] = txn.AcquirePessimisticLock(mvccTxn, reader, props, key)
	}

	for _, err := range errs {
		if err != nil {
			return nil, errs, guard
		}
	}
	return mvccTxn.Modifies, errs, guard
}

// PessimisticRollbackKeys removes pessimistic locks (standalone mode).
func (s *Storage) PessimisticRollbackKeys(keys [][]byte, startTS, forUpdateTS txntypes.TimeStamp) []error {
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)
	mvccKeys := make([]mvcc.Key, len(keys))
	for i, k := range keys {
		mvccKeys[i] = k
	}
	errs := txn.PessimisticRollback(mvccTxn, reader, mvccKeys, startTS, forUpdateTS)

	hasError := false
	for _, err := range errs {
		if err != nil {
			hasError = true
			break
		}
	}
	if !hasError && len(mvccTxn.Modifies) > 0 {
		if err := s.ApplyModifies(mvccTxn.Modifies); err != nil {
			for i := range errs {
				errs[i] = err
			}
		}
	}
	return errs
}

// TxnHeartBeat extends the TTL of a transaction's primary lock.
func (s *Storage) TxnHeartBeat(primaryKey []byte, startTS txntypes.TimeStamp, adviseLockTTL uint64) (uint64, error) {
	keys := [][]byte{primaryKey}
	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)
	ttl, err := txn.TxnHeartBeat(mvccTxn, reader, primaryKey, startTS, adviseLockTTL)
	if err != nil {
		return 0, err
	}

	if len(mvccTxn.Modifies) > 0 {
		if err := s.ApplyModifies(mvccTxn.Modifies); err != nil {
			return 0, err
		}
	}
	return ttl, nil
}

// ResolveLock resolves all locks for a transaction (commit or rollback).
func (s *Storage) ResolveLock(startTS, commitTS txntypes.TimeStamp, keys [][]byte) error {
	if len(keys) == 0 {
		// Scan for all locks matching startTS.
		keys = s.scanLocksForTxn(startTS)
		if len(keys) == 0 {
			return nil
		}
	}

	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)
	for _, key := range keys {
		if err := txn.ResolveLock(mvccTxn, reader, key, startTS, commitTS); err != nil {
			return err
		}
	}

	return s.ApplyModifies(mvccTxn.Modifies)
}

// ResolveLockModifies returns modifies for cluster mode.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) ResolveLockModifies(startTS, commitTS txntypes.TimeStamp, keys [][]byte) ([]mvcc.Modify, error, *LatchGuard) {
	if len(keys) == 0 {
		keys = s.scanLocksForTxn(startTS)
		if len(keys) == 0 {
			return nil, nil, nil
		}
	}

	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)
	for _, key := range keys {
		if err := txn.ResolveLock(mvccTxn, reader, key, startTS, commitTS); err != nil {
			return nil, err, guard
		}
	}

	return mvccTxn.Modifies, nil, guard
}

// scanLocksForTxn scans CF_LOCK for all keys locked by the given transaction.
func (s *Storage) scanLocksForTxn(startTS txntypes.TimeStamp) [][]byte {
	snap := s.engine.NewSnapshot()
	defer snap.Close()

	iter := snap.NewIterator(cfnames.CFLock, traits.IterOptions{})
	defer iter.Close()

	var keys [][]byte
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		lockData := iter.Value()
		lock, err := txntypes.UnmarshalLock(lockData)
		if err != nil {
			continue
		}
		if lock.StartTS == startTS {
			userKey, err := mvcc.DecodeLockKey(iter.Key())
			if err != nil {
				continue
			}
			keys = append(keys, userKey)
		}
	}
	return keys
}

// PrewriteAsyncCommit performs the first phase of async commit 2PC for multiple mutations.
// Returns per-mutation errors and the minCommitTS.
func (s *Storage) PrewriteAsyncCommit(mutations []txn.Mutation, primary []byte, startTS txntypes.TimeStamp, lockTTL uint64, secondaries [][]byte, maxCommitTS txntypes.TimeStamp) ([]error, txntypes.TimeStamp) {
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}

	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	errs := make([]error, len(mutations))
	var minCommitTS txntypes.TimeStamp

	for i, mut := range mutations {
		props := txn.AsyncCommitPrewriteProps{
			PrewriteProps: txn.PrewriteProps{
				StartTS:   startTS,
				Primary:   primary,
				LockTTL:   lockTTL,
				IsPrimary: bytes.Equal(mut.Key, primary),
			},
			UseAsyncCommit: true,
			Secondaries:    secondaries,
			MaxCommitTS:    maxCommitTS,
		}
		errs[i] = txn.PrewriteAsyncCommit(mvccTxn, reader, props, mut)
	}

	hasError := false
	for _, err := range errs {
		if err != nil {
			hasError = true
			break
		}
	}

	if !hasError {
		if err := s.ApplyModifies(mvccTxn.Modifies); err != nil {
			for i := range errs {
				errs[i] = err
			}
			return errs, 0
		}
		// Compute minCommitTS = max(startTS+1, maxCommitTS+1)
		minCommitTS = startTS + 1
		if maxCommitTS > 0 && maxCommitTS+1 > minCommitTS {
			minCommitTS = maxCommitTS + 1
		}
	}

	return errs, minCommitTS
}

// PrewriteAsyncCommitModifies performs async commit prewrite and returns MVCC modifications
// without applying them to the engine. Used in cluster mode to propose via Raft.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) PrewriteAsyncCommitModifies(mutations []txn.Mutation, primary []byte, startTS txntypes.TimeStamp, lockTTL uint64, secondaries [][]byte, maxCommitTS txntypes.TimeStamp) ([]mvcc.Modify, []error, txntypes.TimeStamp, *LatchGuard) {
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}

	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	errs := make([]error, len(mutations))
	var minCommitTS txntypes.TimeStamp

	for i, mut := range mutations {
		props := txn.AsyncCommitPrewriteProps{
			PrewriteProps: txn.PrewriteProps{
				StartTS:   startTS,
				Primary:   primary,
				LockTTL:   lockTTL,
				IsPrimary: bytes.Equal(mut.Key, primary),
			},
			UseAsyncCommit: true,
			Secondaries:    secondaries,
			MaxCommitTS:    maxCommitTS,
		}
		errs[i] = txn.PrewriteAsyncCommit(mvccTxn, reader, props, mut)
	}

	for _, err := range errs {
		if err != nil {
			return nil, errs, 0, guard
		}
	}

	// Compute minCommitTS = max(startTS+1, maxCommitTS+1)
	minCommitTS = startTS + 1
	if maxCommitTS > 0 && maxCommitTS+1 > minCommitTS {
		minCommitTS = maxCommitTS + 1
	}

	return mvccTxn.Modifies, errs, minCommitTS, guard
}

// Prewrite1PC performs prewrite and commit in a single step for 1PC transactions.
// Returns per-mutation errors and the commit timestamp used.
func (s *Storage) Prewrite1PC(mutations []txn.Mutation, primary []byte, startTS txntypes.TimeStamp, commitTS txntypes.TimeStamp, lockTTL uint64) ([]error, txntypes.TimeStamp) {
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}

	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	defer s.latches.Release(lock, cmdID)

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	props := txn.OnePCProps{
		StartTS:  startTS,
		CommitTS: commitTS,
		Primary:  primary,
		LockTTL:  lockTTL,
	}
	errs := txn.PrewriteAndCommit1PC(mvccTxn, reader, props, mutations)

	hasError := false
	for _, err := range errs {
		if err != nil {
			hasError = true
			break
		}
	}

	if !hasError {
		if err := s.ApplyModifies(mvccTxn.Modifies); err != nil {
			for i := range errs {
				errs[i] = err
			}
			return errs, 0
		}
		return errs, commitTS
	}

	return errs, 0
}

// Prewrite1PCModifies performs 1PC prewrite+commit and returns MVCC modifications
// without applying them to the engine. Used in cluster mode to propose via Raft.
// The caller MUST call ReleaseLatch(guard) after the Raft proposal completes.
func (s *Storage) Prewrite1PCModifies(mutations []txn.Mutation, primary []byte, startTS txntypes.TimeStamp, commitTS txntypes.TimeStamp, lockTTL uint64) ([]mvcc.Modify, []error, txntypes.TimeStamp, *LatchGuard) {
	keys := make([][]byte, len(mutations))
	for i, m := range mutations {
		keys[i] = m.Key
	}

	cmdID := s.allocCmdID()
	lock := s.latches.GenLock(keys)
	for !s.latches.Acquire(lock, cmdID) {
	}
	guard := &LatchGuard{lock: lock, cmdID: cmdID}

	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	mvccTxn := mvcc.NewMvccTxn(startTS)

	props := txn.OnePCProps{
		StartTS:  startTS,
		CommitTS: commitTS,
		Primary:  primary,
		LockTTL:  lockTTL,
	}
	errs := txn.PrewriteAndCommit1PC(mvccTxn, reader, props, mutations)

	for _, err := range errs {
		if err != nil {
			return nil, errs, 0, guard
		}
	}

	return mvccTxn.Modifies, errs, commitTS, guard
}

// CheckSecondaryLocks checks secondary keys for an async commit transaction.
// Returns the lock info for each key that still has a lock belonging to the transaction,
// the commit timestamp if any lock has been committed, and any error.
func (s *Storage) CheckSecondaryLocks(keys [][]byte, startTS txntypes.TimeStamp) ([]*SecondaryLockStatus, txntypes.TimeStamp, error) {
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	var results []*SecondaryLockStatus
	var commitTS txntypes.TimeStamp

	for _, key := range keys {
		lock, err := reader.LoadLock(key)
		if err != nil {
			return nil, 0, err
		}

		if lock != nil && lock.StartTS == startTS {
			// Lock is still present and belongs to our transaction.
			results = append(results, &SecondaryLockStatus{
				Key:  key,
				Lock: lock,
			})
			continue
		}

		// Lock is gone — check if the transaction was committed on this key.
		write, wCommitTS, err := reader.GetTxnCommitRecord(key, startTS)
		if err != nil {
			return nil, 0, err
		}
		if write != nil && write.WriteType != txntypes.WriteTypeRollback {
			commitTS = wCommitTS
		}
		// If rolled back or no record, we just don't add a lock entry.
		results = append(results, &SecondaryLockStatus{
			Key:  key,
			Lock: nil,
		})
	}

	return results, commitTS, nil
}

// SecondaryLockStatus holds the lock status for a secondary key.
type SecondaryLockStatus struct {
	Key  []byte
	Lock *txntypes.Lock
}

// ScanLock scans CF_LOCK for locks with StartTS <= maxVersion in [startKey, endKey).
func (s *Storage) ScanLock(maxVersion txntypes.TimeStamp, startKey, endKey []byte, limit uint32) ([]*ScanLockResult, error) {
	snap := s.engine.NewSnapshot()
	defer snap.Close()

	iterOpts := traits.IterOptions{}
	if len(startKey) > 0 {
		iterOpts.LowerBound = mvcc.EncodeLockKey(startKey)
	}
	if len(endKey) > 0 {
		iterOpts.UpperBound = mvcc.EncodeLockKey(endKey)
	}

	iter := snap.NewIterator(cfnames.CFLock, iterOpts)
	defer iter.Close()

	var results []*ScanLockResult
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if limit > 0 && uint32(len(results)) >= limit {
			break
		}

		lockData := iter.Value()
		lock, err := txntypes.UnmarshalLock(lockData)
		if err != nil {
			continue
		}

		if lock.StartTS <= maxVersion {
			userKey, err := mvcc.DecodeLockKey(iter.Key())
			if err != nil {
				continue
			}
			results = append(results, &ScanLockResult{
				Key:  userKey,
				Lock: lock,
			})
		}
	}

	return results, nil
}

// ScanLockResult holds a key and its lock from a ScanLock operation.
type ScanLockResult struct {
	Key  []byte
	Lock *txntypes.Lock
}

// Engine returns the underlying engine for direct access (used in tests).
func (s *Storage) Engine() traits.KvEngine {
	return s.engine
}

// Close closes the storage.
func (s *Storage) Close() error {
	return s.engine.Close()
}
