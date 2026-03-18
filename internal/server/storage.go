// Package server implements the gRPC server for gookvs, providing
// TiKV-compatible KV service RPCs.
package server

import (
	"bytes"
	"errors"
	"sync"

	"github.com/ryogrid/gookvs/internal/engine/traits"
	"github.com/ryogrid/gookvs/internal/storage/mvcc"
	"github.com/ryogrid/gookvs/internal/storage/txn"
	"github.com/ryogrid/gookvs/internal/storage/txn/concurrency"
	"github.com/ryogrid/gookvs/internal/storage/txn/latch"
	"github.com/ryogrid/gookvs/pkg/cfnames"
	"github.com/ryogrid/gookvs/pkg/txntypes"
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
		}
	}
	return wb.Commit()
}

// Get performs a transactional point read at the given timestamp.
func (s *Storage) Get(key []byte, version txntypes.TimeStamp) ([]byte, error) {
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	pg := mvcc.NewPointGetter(reader, version, mvcc.IsolationLevelSI)
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
func (s *Storage) PrewriteModifies(mutations []txn.Mutation, primary []byte, startTS txntypes.TimeStamp, lockTTL uint64) ([]mvcc.Modify, []error) {
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
			return nil, errs
		}
	}

	return mvccTxn.Modifies, errs
}

// CommitModifies performs the second phase of 2PC and returns the MVCC modifications
// without applying them to the engine. Used in cluster mode to propose via Raft.
func (s *Storage) CommitModifies(keys [][]byte, startTS, commitTS txntypes.TimeStamp) ([]mvcc.Modify, error) {
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
			return nil, err
		}
	}

	return mvccTxn.Modifies, nil
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

	// Check if already committed.
	status, err := txn.CheckTxnStatus(reader, key, startTS)
	if err != nil {
		return 0, err
	}
	if status.CommitTS != 0 {
		return status.CommitTS, nil
	}

	mvccTxn := mvcc.NewMvccTxn(startTS)
	if err := txn.Rollback(mvccTxn, reader, key, startTS); err != nil {
		return 0, err
	}
	return 0, s.ApplyModifies(mvccTxn.Modifies)
}

// CheckTxnStatus checks the status of a transaction.
func (s *Storage) CheckTxnStatus(primaryKey []byte, startTS txntypes.TimeStamp) (*txn.TxnStatus, error) {
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	return txn.CheckTxnStatus(reader, primaryKey, startTS)
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
func (s *Storage) PessimisticLockModifies(keys [][]byte, primary []byte, startTS, forUpdateTS txntypes.TimeStamp, lockTTL uint64) ([]mvcc.Modify, []error) {
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

	for _, err := range errs {
		if err != nil {
			return nil, errs
		}
	}
	return mvccTxn.Modifies, errs
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
func (s *Storage) ResolveLockModifies(startTS, commitTS txntypes.TimeStamp, keys [][]byte) ([]mvcc.Modify, error) {
	if len(keys) == 0 {
		keys = s.scanLocksForTxn(startTS)
		if len(keys) == 0 {
			return nil, nil
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
			return nil, err
		}
	}

	return mvccTxn.Modifies, nil
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

// Engine returns the underlying engine for direct access (used in tests).
func (s *Storage) Engine() traits.KvEngine {
	return s.engine
}

// Close closes the storage.
func (s *Storage) Close() error {
	return s.engine.Close()
}
