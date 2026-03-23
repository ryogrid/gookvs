package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookv/pkg/txntypes"
)

// Transaction state errors.
var (
	ErrTxnCommitted   = errors.New("txn: already committed")
	ErrTxnRolledBack  = errors.New("txn: already rolled back")
	ErrWriteConflict  = errors.New("txn: write conflict")
	ErrTxnLockNotFound = errors.New("txn: lock not found")
	ErrDeadlock       = errors.New("txn: deadlock")
)

// mutationEntry represents a buffered write operation.
type mutationEntry struct {
	op    kvrpcpb.Op
	value []byte
}

// TxnHandle represents an active transaction with a buffered mutation set.
type TxnHandle struct {
	mu         sync.Mutex
	client     *TxnKVClient
	startTS    txntypes.TimeStamp
	opts       TxnOptions
	membuf     map[string]mutationEntry
	lockKeys   [][]byte // pessimistic lock keys
	committed  bool
	rolledBack bool
}

// newTxnHandle creates a new TxnHandle.
func newTxnHandle(client *TxnKVClient, startTS txntypes.TimeStamp, opts TxnOptions) *TxnHandle {
	return &TxnHandle{
		client:  client,
		startTS: startTS,
		opts:    opts,
		membuf:  make(map[string]mutationEntry),
	}
}

// StartTS returns the transaction's start timestamp.
func (t *TxnHandle) StartTS() txntypes.TimeStamp {
	return t.startTS
}

// Get reads the value for key. It first checks the local mutation buffer,
// then falls back to a KvGet RPC. If the key is locked by another transaction,
// it resolves the lock and retries.
func (t *TxnHandle) Get(ctx context.Context, key []byte) ([]byte, error) {
	t.mu.Lock()
	if t.committed {
		t.mu.Unlock()
		return nil, ErrTxnCommitted
	}
	if t.rolledBack {
		t.mu.Unlock()
		return nil, ErrTxnRolledBack
	}

	// Check local buffer first.
	if entry, ok := t.membuf[string(key)]; ok {
		t.mu.Unlock()
		if entry.op == kvrpcpb.Op_Del {
			return nil, nil
		}
		return entry.value, nil
	}
	t.mu.Unlock()

	// RPC with lock resolution retry.
	const maxRetries = 20
	var lastLockInfo *kvrpcpb.LockInfo
	for i := 0; i < maxRetries; i++ {
		value, lockInfo, err := t.kvGet(ctx, key)
		if err != nil {
			return nil, err
		}
		if lockInfo == nil {
			return value, nil
		}
		lastLockInfo = lockInfo
		// Resolve the lock and retry.
		if err := t.client.resolver.ResolveLocks(ctx, []*kvrpcpb.LockInfo{lockInfo}); err != nil {
			return nil, fmt.Errorf("resolve lock: %w", err)
		}
		// Brief pause to allow Raft proposals from lock resolution to be applied.
		time.Sleep(200 * time.Millisecond)
	}
	if lastLockInfo != nil {
		return nil, fmt.Errorf("get key %x: lock resolution retries exhausted (lockVersion=%d, primary=%x)",
			key, lastLockInfo.GetLockVersion(), lastLockInfo.GetPrimaryLock())
	}
	return nil, fmt.Errorf("get key %x: lock resolution retries exhausted", key)
}

// kvGet performs a single KvGet RPC. Returns (value, lockInfo, err).
// If a lock is encountered, lockInfo is non-nil and value is nil.
func (t *TxnHandle) kvGet(ctx context.Context, key []byte) ([]byte, *kvrpcpb.LockInfo, error) {
	var value []byte
	var notFound bool
	var lockInfo *kvrpcpb.LockInfo

	err := t.client.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		resp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
			Context: buildContext(info),
			Key:     key,
			Version: uint64(t.startTS),
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != nil {
			if resp.GetError().GetLocked() != nil {
				lockInfo = resp.GetError().GetLocked()
				return nil, nil
			}
			return nil, fmt.Errorf("kv get error: %s", resp.GetError().String())
		}
		notFound = resp.GetNotFound()
		value = resp.GetValue()
		return nil, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if lockInfo != nil {
		return nil, lockInfo, nil
	}
	if notFound {
		return nil, nil, nil
	}
	return value, nil, nil
}

// BatchGet reads multiple keys. It checks the local buffer first, then issues
// KvBatchGet RPCs for remaining keys grouped by region.
func (t *TxnHandle) BatchGet(ctx context.Context, keys [][]byte) ([]KvPair, error) {
	t.mu.Lock()
	if t.committed {
		t.mu.Unlock()
		return nil, ErrTxnCommitted
	}
	if t.rolledBack {
		t.mu.Unlock()
		return nil, ErrTxnRolledBack
	}

	var results []KvPair
	var remoteKeys [][]byte
	for _, key := range keys {
		if entry, ok := t.membuf[string(key)]; ok {
			if entry.op != kvrpcpb.Op_Del {
				results = append(results, KvPair{Key: key, Value: entry.value})
			}
		} else {
			remoteKeys = append(remoteKeys, key)
		}
	}
	t.mu.Unlock()

	if len(remoteKeys) == 0 {
		return results, nil
	}

	const maxRetries = 3
	for i := 0; i < maxRetries; i++ {
		pairs, lockInfo, err := t.kvBatchGet(ctx, remoteKeys)
		if err != nil {
			return nil, err
		}
		if lockInfo == nil {
			results = append(results, pairs...)
			return results, nil
		}
		if err := t.client.resolver.ResolveLocks(ctx, []*kvrpcpb.LockInfo{lockInfo}); err != nil {
			return nil, fmt.Errorf("resolve lock: %w", err)
		}
	}
	return nil, fmt.Errorf("batch get: lock resolution retries exhausted")
}

// kvBatchGet performs a KvBatchGet RPC.
func (t *TxnHandle) kvBatchGet(ctx context.Context, keys [][]byte) ([]KvPair, *kvrpcpb.LockInfo, error) {
	// Use the first key for region routing; the server handles multi-key reads.
	var pairs []KvPair
	var lockInfo *kvrpcpb.LockInfo

	err := t.client.sender.SendToRegion(ctx, keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		resp, err := client.KvBatchGet(ctx, &kvrpcpb.BatchGetRequest{
			Context: buildContext(info),
			Keys:    keys,
			Version: uint64(t.startTS),
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != nil {
			if resp.GetError().GetLocked() != nil {
				lockInfo = resp.GetError().GetLocked()
				return nil, nil
			}
			return nil, fmt.Errorf("batch get error: %s", resp.GetError().String())
		}
		for _, p := range resp.GetPairs() {
			pairs = append(pairs, KvPair{Key: p.GetKey(), Value: p.GetValue()})
		}
		return nil, nil
	})
	if err != nil {
		return nil, nil, err
	}
	return pairs, lockInfo, nil
}

// Set buffers a put mutation. In pessimistic mode, it also acquires a
// pessimistic lock.
func (t *TxnHandle) Set(ctx context.Context, key, value []byte) error {
	t.mu.Lock()
	if t.committed {
		t.mu.Unlock()
		return ErrTxnCommitted
	}
	if t.rolledBack {
		t.mu.Unlock()
		return ErrTxnRolledBack
	}
	t.membuf[string(key)] = mutationEntry{op: kvrpcpb.Op_Put, value: value}
	isPessimistic := t.opts.Mode == TxnModePessimistic
	t.mu.Unlock()

	if isPessimistic {
		return t.acquirePessimisticLock(ctx, key)
	}
	return nil
}

// Delete buffers a delete mutation. In pessimistic mode, it also acquires a
// pessimistic lock.
func (t *TxnHandle) Delete(ctx context.Context, key []byte) error {
	t.mu.Lock()
	if t.committed {
		t.mu.Unlock()
		return ErrTxnCommitted
	}
	if t.rolledBack {
		t.mu.Unlock()
		return ErrTxnRolledBack
	}
	t.membuf[string(key)] = mutationEntry{op: kvrpcpb.Op_Del, value: nil}
	isPessimistic := t.opts.Mode == TxnModePessimistic
	t.mu.Unlock()

	if isPessimistic {
		return t.acquirePessimisticLock(ctx, key)
	}
	return nil
}

// acquirePessimisticLock acquires a pessimistic lock on the given key.
func (t *TxnHandle) acquirePessimisticLock(ctx context.Context, key []byte) error {
	t.mu.Lock()
	primary := t.primaryKey()
	t.mu.Unlock()

	return t.client.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		resp, err := client.KvPessimisticLock(ctx, &kvrpcpb.PessimisticLockRequest{
			Context:      buildContext(info),
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_PessimisticLock, Key: key}},
			PrimaryLock:  primary,
			StartVersion: uint64(t.startTS),
			ForUpdateTs:  uint64(t.startTS),
			LockTtl:      t.opts.LockTTL,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		for _, keyErr := range resp.GetErrors() {
			if keyErr.GetConflict() != nil {
				return nil, ErrWriteConflict
			}
			if keyErr.GetDeadlock() != nil {
				return nil, ErrDeadlock
			}
			return nil, fmt.Errorf("pessimistic lock error: %s", keyErr.String())
		}
		t.mu.Lock()
		t.lockKeys = append(t.lockKeys, key)
		t.mu.Unlock()
		return nil, nil
	})
}

// primaryKey returns the primary key for the transaction.
// Must be called with t.mu held.
func (t *TxnHandle) primaryKey() []byte {
	// Use the first key in the mutation buffer as primary.
	for k := range t.membuf {
		return []byte(k)
	}
	return nil
}

// Commit commits the transaction using the two-phase commit protocol.
func (t *TxnHandle) Commit(ctx context.Context) error {
	t.mu.Lock()
	if t.committed {
		t.mu.Unlock()
		return ErrTxnCommitted
	}
	if t.rolledBack {
		t.mu.Unlock()
		return ErrTxnRolledBack
	}
	if len(t.membuf) == 0 {
		t.committed = true
		t.mu.Unlock()
		return nil
	}

	committer := newTwoPhaseCommitter(t)
	t.mu.Unlock()

	err := committer.execute(ctx)
	if err != nil {
		return err
	}

	t.mu.Lock()
	t.committed = true
	t.mu.Unlock()
	return nil
}

// Rollback aborts the transaction by rolling back all prewritten/locked keys.
func (t *TxnHandle) Rollback(ctx context.Context) error {
	t.mu.Lock()
	if t.committed {
		t.mu.Unlock()
		return ErrTxnCommitted
	}
	if t.rolledBack {
		t.mu.Unlock()
		return nil // already rolled back
	}
	t.rolledBack = true

	// Collect all keys that might need rollback.
	var keys [][]byte
	for k := range t.membuf {
		keys = append(keys, []byte(k))
	}

	// In pessimistic mode, clean up both pessimistic locks and any prewrite
	// locks that may have been created by a partial Commit().
	if t.opts.Mode == TxnModePessimistic && len(t.lockKeys) > 0 {
		lockKeys := t.lockKeys
		t.mu.Unlock()
		// Rollback pessimistic locks first.
		if err := t.pessimisticRollback(ctx, lockKeys); err != nil {
			// Best-effort: continue to batch rollback even if pessimistic rollback fails.
			_ = err
		}
		// Also batch-rollback membuf keys to clean up any prewrite locks
		// left by a partial commit attempt.
		if len(keys) > 0 {
			return t.batchRollback(ctx, keys)
		}
		return nil
	}
	t.mu.Unlock()

	if len(keys) == 0 {
		return nil
	}

	return t.batchRollback(ctx, keys)
}

// batchRollback sends KvBatchRollback for the given keys.
func (t *TxnHandle) batchRollback(ctx context.Context, keys [][]byte) error {
	groups, err := t.client.cache.GroupKeysByRegion(ctx, keys)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if err := t.client.sender.SendToRegion(ctx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
			resp, err := client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
				Context:      buildContext(info),
				StartVersion: uint64(t.startTS),
				Keys:         group.Keys,
			})
			if err != nil {
				return nil, err
			}
			if resp.GetRegionError() != nil {
				return resp.GetRegionError(), nil
			}
			if resp.GetError() != nil {
				return nil, fmt.Errorf("batch rollback error: %s", resp.GetError().String())
			}
			return nil, nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// pessimisticRollback sends KVPessimisticRollback for the given keys.
func (t *TxnHandle) pessimisticRollback(ctx context.Context, keys [][]byte) error {
	groups, err := t.client.cache.GroupKeysByRegion(ctx, keys)
	if err != nil {
		return err
	}

	for _, group := range groups {
		if err := t.client.sender.SendToRegion(ctx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
			resp, err := client.KVPessimisticRollback(ctx, &kvrpcpb.PessimisticRollbackRequest{
				Context:      buildContext(info),
				StartVersion: uint64(t.startTS),
				ForUpdateTs:  uint64(t.startTS),
				Keys:         group.Keys,
			})
			if err != nil {
				return nil, err
			}
			if resp.GetRegionError() != nil {
				return resp.GetRegionError(), nil
			}
			for _, keyErr := range resp.GetErrors() {
				if keyErr != nil {
					return nil, fmt.Errorf("pessimistic rollback error: %s", keyErr.String())
				}
			}
			return nil, nil
		}); err != nil {
			return err
		}
	}
	return nil
}
