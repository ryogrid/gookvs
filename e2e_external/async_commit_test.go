package e2e_external_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookv/pkg/e2elib"
)

// TestAsyncCommit1PCPrewrite tests the 1PC (one-phase commit) optimisation path.
// A PrewriteRequest with TryOnePc=true should commit in a single round-trip,
// returning OnePcCommitTs > 0 and making the key immediately readable.
func TestAsyncCommit1PCPrewrite(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	key := []byte("1pc-key")
	value := []byte("1pc-value")
	startTS := uint64(100)

	// Send a PrewriteRequest with TryOnePc enabled.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: key, Value: value},
		},
		PrimaryLock:  key,
		StartVersion: startTS,
		LockTtl:      5000,
		TryOnePc:     true,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors(), "1PC prewrite should succeed without errors")
	assert.Greater(t, prewriteResp.GetOnePcCommitTs(), uint64(0),
		"OnePcCommitTs should be > 0 for successful 1PC")

	// The key should be immediately readable without a separate commit call.
	readTS := prewriteResp.GetOnePcCommitTs() + 1
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
		Key:     key,
		Version: readTS,
	})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound(), "key should be readable after 1PC")
	assert.Equal(t, value, getResp.GetValue())

	t.Log("1PC prewrite passed")
}

// TestAsyncCommitPrewrite tests the async commit prewrite path.
// PrewriteRequest with UseAsyncCommit=true and Secondaries should return
// MinCommitTs > 0 and leave a lock on the key (not yet committed).
func TestAsyncCommitPrewrite(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	primaryKey := []byte("async-primary")
	secondaryKey := []byte("async-secondary")
	startTS := uint64(200)

	// Prewrite with async commit enabled.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: primaryKey, Value: []byte("primary-val")},
		},
		PrimaryLock:    primaryKey,
		StartVersion:   startTS,
		LockTtl:        5000,
		UseAsyncCommit: true,
		Secondaries:    [][]byte{secondaryKey},
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors(), "async commit prewrite should succeed")
	assert.Greater(t, prewriteResp.GetMinCommitTs(), uint64(0),
		"MinCommitTs should be > 0 for async commit prewrite")

	// The key should NOT be readable yet (it has a lock, not committed).
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
		Key:     primaryKey,
		Version: startTS + 10,
	})
	require.NoError(t, err)
	// Either NotFound or an error about the lock is expected.
	if getResp.GetError() == nil {
		assert.True(t, getResp.GetNotFound() || len(getResp.GetValue()) == 0,
			"key should not be readable (still locked) before commit")
	}

	// Clean up: rollback the transaction.
	_, err = client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{primaryKey},
		StartVersion: startTS,
	})
	require.NoError(t, err)

	t.Log("Async commit prewrite passed")
}

// TestCheckSecondaryLocks tests the KvCheckSecondaryLocks RPC.
// It creates a lock via async commit prewrite, verifies that
// CheckSecondaryLocks returns the lock, then commits and verifies
// the commitTs is returned instead.
func TestCheckSecondaryLocks(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	primaryKey := []byte("csl-primary")
	secondaryKey := []byte("csl-secondary")
	startTS := uint64(300)

	// Create a lock on secondaryKey by prewriting it as part of an async commit txn.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: secondaryKey, Value: []byte("sec-val")},
		},
		PrimaryLock:    primaryKey,
		StartVersion:   startTS,
		LockTtl:        5000,
		UseAsyncCommit: true,
		Secondaries:    [][]byte{},
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors(), "prewrite secondary should succeed")

	// Check secondary locks: lock should be present.
	checkResp, err := client.KvCheckSecondaryLocks(ctx, &kvrpcpb.CheckSecondaryLocksRequest{
		Keys:         [][]byte{secondaryKey},
		StartVersion: startTS,
	})
	require.NoError(t, err)
	assert.Nil(t, checkResp.GetError(), "CheckSecondaryLocks should not return an error")
	assert.NotEmpty(t, checkResp.GetLocks(), "should find the lock on the secondary key")
	assert.Equal(t, uint64(0), checkResp.GetCommitTs(), "commitTs should be 0 while lock is held")

	// Now commit the transaction.
	commitTS := startTS + 10
	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{secondaryKey},
		StartVersion:  startTS,
		CommitVersion: commitTS,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError(), "commit should succeed")

	// Check secondary locks again: should now report the commit timestamp.
	checkResp2, err := client.KvCheckSecondaryLocks(ctx, &kvrpcpb.CheckSecondaryLocksRequest{
		Keys:         [][]byte{secondaryKey},
		StartVersion: startTS,
	})
	require.NoError(t, err)
	assert.Nil(t, checkResp2.GetError())
	// After commit, the lock is gone; commitTs should be reported.
	assert.Empty(t, checkResp2.GetLocks(), "lock should be cleared after commit")
	assert.Equal(t, commitTS, checkResp2.GetCommitTs(),
		"commitTs should match the committed version")

	t.Log("CheckSecondaryLocks passed")
}

// TestScanLock tests the KvScanLock RPC.
// It creates multiple locks with different startVersions and verifies
// that ScanLock returns only locks with startTS <= maxVersion.
// It also tests the limit parameter.
func TestScanLock(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	// Create locks at different start versions.
	type lockDef struct {
		key     []byte
		startTS uint64
	}
	locks := []lockDef{
		{key: []byte("sl-key-a"), startTS: 100},
		{key: []byte("sl-key-b"), startTS: 200},
		{key: []byte("sl-key-c"), startTS: 300},
		{key: []byte("sl-key-d"), startTS: 400},
		{key: []byte("sl-key-e"), startTS: 500},
	}

	for _, l := range locks {
		prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
			Mutations: []*kvrpcpb.Mutation{
				{Op: kvrpcpb.Op_Put, Key: l.key, Value: []byte(fmt.Sprintf("val-%d", l.startTS))},
			},
			PrimaryLock:  l.key,
			StartVersion: l.startTS,
			LockTtl:      10000,
		})
		require.NoError(t, err)
		assert.Empty(t, prewriteResp.GetErrors(), "prewrite for key %s should succeed", l.key)
	}

	// ScanLock with maxVersion=300: should return locks with startTS <= 300.
	scanResp, err := client.KvScanLock(ctx, &kvrpcpb.ScanLockRequest{
		MaxVersion: 300,
		StartKey:   []byte("sl-key-"),
		EndKey:     []byte("sl-key-~"),
		Limit:      100,
	})
	require.NoError(t, err)
	assert.Nil(t, scanResp.GetError(), "ScanLock should not return an error")
	assert.Len(t, scanResp.GetLocks(), 3,
		"should find 3 locks with startTS <= 300 (100, 200, 300)")

	// Verify lock versions are correct.
	for _, lock := range scanResp.GetLocks() {
		assert.LessOrEqual(t, lock.GetLockVersion(), uint64(300),
			"lock version should be <= maxVersion")
	}

	// ScanLock with limit=2: should return at most 2 locks.
	scanResp2, err := client.KvScanLock(ctx, &kvrpcpb.ScanLockRequest{
		MaxVersion: 500,
		StartKey:   []byte("sl-key-"),
		EndKey:     []byte("sl-key-~"),
		Limit:      2,
	})
	require.NoError(t, err)
	assert.Nil(t, scanResp2.GetError())
	assert.Len(t, scanResp2.GetLocks(), 2,
		"should return at most 2 locks when limit=2")

	// Clean up: rollback all locks.
	for _, l := range locks {
		_, err := client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
			Keys:         [][]byte{l.key},
			StartVersion: l.startTS,
		})
		require.NoError(t, err)
	}

	t.Log("ScanLock passed")
}
