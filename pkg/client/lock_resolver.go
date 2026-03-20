package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/ryogrid/gookv/pkg/txntypes"
)

// LockResolver resolves locks left by crashed or slow transactions.
type LockResolver struct {
	sender   *RegionRequestSender
	cache    *RegionCache
	pdClient pdclient.Client

	mu        sync.Mutex
	resolving map[lockKey]chan struct{}
}

type lockKey struct {
	primary string
	startTS uint64
}

// NewLockResolver creates a new LockResolver.
func NewLockResolver(sender *RegionRequestSender, cache *RegionCache, pdClient pdclient.Client) *LockResolver {
	return &LockResolver{
		sender:    sender,
		cache:     cache,
		pdClient:  pdClient,
		resolving: make(map[lockKey]chan struct{}),
	}
}

// ResolveLocks resolves the given locks by checking their transaction status
// and either committing or rolling back each one.
func (lr *LockResolver) ResolveLocks(ctx context.Context, locks []*kvrpcpb.LockInfo) error {
	for _, lock := range locks {
		if err := lr.resolveSingleLock(ctx, lock); err != nil {
			return err
		}
	}
	return nil
}

func (lr *LockResolver) resolveSingleLock(ctx context.Context, lock *kvrpcpb.LockInfo) error {
	lk := lockKey{
		primary: string(lock.GetPrimaryLock()),
		startTS: lock.GetLockVersion(),
	}

	// Dedup: if another goroutine is already resolving this lock, wait.
	lr.mu.Lock()
	if ch, ok := lr.resolving[lk]; ok {
		lr.mu.Unlock()
		select {
		case <-ch:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	ch := make(chan struct{})
	lr.resolving[lk] = ch
	lr.mu.Unlock()

	defer func() {
		lr.mu.Lock()
		delete(lr.resolving, lk)
		lr.mu.Unlock()
		close(ch)
	}()

	// Check the transaction status via the primary key.
	statusResp, err := lr.checkTxnStatus(ctx, lock.GetPrimaryLock(), txntypes.TimeStamp(lock.GetLockVersion()))
	if err != nil {
		return fmt.Errorf("check txn status: %w", err)
	}

	commitTS := txntypes.TimeStamp(statusResp.GetCommitVersion())

	// Resolve the lock on this key.
	return lr.resolveLock(ctx, lock, commitTS)
}

// checkTxnStatus checks the status of the transaction that owns the lock.
func (lr *LockResolver) checkTxnStatus(ctx context.Context, primaryKey []byte, lockTS txntypes.TimeStamp) (*kvrpcpb.CheckTxnStatusResponse, error) {
	callerStartTS, err := lr.pdClient.GetTS(ctx)
	if err != nil {
		return nil, err
	}

	var result *kvrpcpb.CheckTxnStatusResponse
	err = lr.sender.SendToRegion(ctx, primaryKey, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		resp, err := client.KvCheckTxnStatus(ctx, &kvrpcpb.CheckTxnStatusRequest{
			Context:                buildContext(info),
			PrimaryKey:             primaryKey,
			LockTs:                 uint64(lockTS),
			CallerStartTs:          callerStartTS.ToUint64(),
			RollbackIfNotExist:     true,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != nil {
			return nil, fmt.Errorf("check txn status error: %s", resp.GetError().String())
		}
		result = resp
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// resolveLock resolves a single lock. If commitTS > 0, the lock is committed;
// if commitTS == 0, the lock is rolled back.
func (lr *LockResolver) resolveLock(ctx context.Context, lock *kvrpcpb.LockInfo, commitTS txntypes.TimeStamp) error {
	return lr.sender.SendToRegion(ctx, lock.GetKey(), func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		resp, err := client.KvResolveLock(ctx, &kvrpcpb.ResolveLockRequest{
			Context:       buildContext(info),
			StartVersion:  lock.GetLockVersion(),
			CommitVersion: uint64(commitTS),
			Keys:          [][]byte{lock.GetKey()},
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != nil {
			return nil, fmt.Errorf("resolve lock error: %s", resp.GetError().String())
		}
		return nil, nil
	})
}
