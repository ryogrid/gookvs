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

// applyModifies writes the accumulated MVCC modifications to the engine atomically.
func (s *Storage) applyModifies(modifies []mvcc.Modify) error {
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
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	var results []*KvPairResult

	// Use the MVCC key encoding to iterate through CF_WRITE.
	iter := snap.NewIterator(cfnames.CFWrite, traits.IterOptions{})
	defer iter.Close()

	// Seek to the start position.
	seekKey := mvcc.EncodeKey(startKey, version)
	iter.Seek(seekKey)

	visited := make(map[string]bool)

	for iter.Valid() && (limit == 0 || uint32(len(results)) < limit) {
		encodedKey := iter.Key()
		userKey, commitTS, err := mvcc.DecodeKey(encodedKey)
		if err != nil {
			return nil, err
		}

		// Check bounds.
		if len(endKey) > 0 && bytes.Compare(userKey, endKey) >= 0 {
			break
		}

		// Skip keys we've already processed.
		keyStr := string(userKey)
		if visited[keyStr] {
			iter.Next()
			continue
		}
		visited[keyStr] = true

		// Skip if this write's commitTS is after our read version.
		if commitTS > version {
			iter.Next()
			continue
		}

		// Use PointGetter for correct MVCC read (handles locks, rollbacks, etc.)
		pg := mvcc.NewPointGetter(reader, version, mvcc.IsolationLevelSI)
		value, err := pg.Get(userKey)
		if err != nil {
			if errors.Is(err, mvcc.ErrKeyIsLocked) {
				return nil, err
			}
			// Skip keys with errors.
			iter.Next()
			continue
		}
		if value != nil {
			pair := &KvPairResult{Key: userKey}
			if !keyOnly {
				pair.Value = value
			}
			results = append(results, pair)
		}

		// Skip to the next user key.
		nextKey := mvcc.EncodeKey(userKey, 0) // ts=0 is the smallest for this user key
		iter.Seek(nextKey)
		// Actually, since ts is descending, ts=0 would be the largest encoded.
		// We need to seek past all entries for this user key.
		// Use a key just after this user key.
		nextUserKey := append([]byte{}, userKey...)
		nextUserKey = append(nextUserKey, 0)
		seekNext := mvcc.EncodeKey(nextUserKey, version)
		iter.Seek(seekNext)
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
		if err := s.applyModifies(mvccTxn.Modifies); err != nil {
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

	return s.applyModifies(mvccTxn.Modifies)
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

	return s.applyModifies(mvccTxn.Modifies)
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
	return 0, s.applyModifies(mvccTxn.Modifies)
}

// CheckTxnStatus checks the status of a transaction.
func (s *Storage) CheckTxnStatus(primaryKey []byte, startTS txntypes.TimeStamp) (*txn.TxnStatus, error) {
	snap := s.engine.NewSnapshot()
	reader := mvcc.NewMvccReader(snap)
	defer reader.Close()

	return txn.CheckTxnStatus(reader, primaryKey, startTS)
}

// Engine returns the underlying engine for direct access (used in tests).
func (s *Storage) Engine() traits.KvEngine {
	return s.engine
}

// Close closes the storage.
func (s *Storage) Close() error {
	return s.engine.Close()
}
