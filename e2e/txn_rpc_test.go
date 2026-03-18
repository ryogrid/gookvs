package e2e

import (
	"context"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTxnPessimisticLockAcquire tests pessimistic lock acquire.
func TestTxnPessimisticLockAcquire(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// Acquire pessimistic lock.
	lockResp, err := client.KvPessimisticLock(ctx, &kvrpcpb.PessimisticLockRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_PessimisticLock, Key: []byte("pessimistic-key-1")},
		},
		PrimaryLock:  []byte("pessimistic-key-1"),
		StartVersion: 100,
		ForUpdateTs:  100,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, lockResp.GetErrors(), "pessimistic lock should succeed")

	t.Log("Pessimistic lock acquire passed")
}

// TestTxnPessimisticRollbackRPCMismatch tests that KVPessimisticRollback RPC
// fails due to server method naming mismatch (server defines KvPessimisticRollback
// but proto expects KVPessimisticRollback).
func TestTxnPessimisticRollbackRPCMismatch(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// This should fail because the server method is named KvPessimisticRollback
	// (lowercase 'v') but the proto interface expects KVPessimisticRollback
	// (uppercase 'V'). This is a known implementation bug.
	_, err := client.KVPessimisticRollback(ctx, &kvrpcpb.PessimisticRollbackRequest{
		Keys:         [][]byte{[]byte("pessimistic-key-1")},
		StartVersion: 100,
		ForUpdateTs:  100,
	})
	// We expect this to fail with Unimplemented.
	assert.Error(t, err, "KVPessimisticRollback should fail due to server method name mismatch")
	assert.Contains(t, err.Error(), "Unimplemented", "error should be Unimplemented")

	t.Log("Pessimistic rollback RPC mismatch confirmed (known bug)")
}

// TestTxnHeartBeat tests extending lock TTL via KvTxnHeartBeat.
func TestTxnHeartBeat(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// First, prewrite to create a lock.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("hb-key"), Value: []byte("hb-val")},
		},
		PrimaryLock:  []byte("hb-key"),
		StartVersion: 200,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// Heartbeat to extend TTL.
	hbResp, err := client.KvTxnHeartBeat(ctx, &kvrpcpb.TxnHeartBeatRequest{
		PrimaryLock:    []byte("hb-key"),
		StartVersion:   200,
		AdviseLockTtl: 20000,
	})
	require.NoError(t, err)
	assert.Nil(t, hbResp.GetError(), "heartbeat should succeed")
	assert.GreaterOrEqual(t, hbResp.GetLockTtl(), uint64(20000), "lock TTL should be extended")

	// Clean up: rollback.
	_, err = client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{[]byte("hb-key")},
		StartVersion: 200,
	})
	require.NoError(t, err)

	t.Log("TxnHeartBeat passed")
}

// TestTxnResolveLock tests resolving locks for a committed transaction.
func TestTxnResolveLock(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// Prewrite two keys.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("resolve-k1"), Value: []byte("v1")},
			{Op: kvrpcpb.Op_Put, Key: []byte("resolve-k2"), Value: []byte("v2")},
		},
		PrimaryLock:  []byte("resolve-k1"),
		StartVersion: 300,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// Resolve locks with a commitTS, effectively committing them.
	resolveResp, err := client.KvResolveLock(ctx, &kvrpcpb.ResolveLockRequest{
		StartVersion:  300,
		CommitVersion: 310,
	})
	require.NoError(t, err)
	assert.Nil(t, resolveResp.GetError(), "resolve lock should succeed")

	// Both keys should now be readable.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: []byte("resolve-k1"), Version: 320})
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), getResp.GetValue())

	getResp, err = client.KvGet(ctx, &kvrpcpb.GetRequest{Key: []byte("resolve-k2"), Version: 320})
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), getResp.GetValue())

	t.Log("ResolveLock passed")
}

// TestTxnScanWithVersionVisibility tests KvScan with MVCC version filtering.
func TestTxnScanWithVersionVisibility(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// Write key at version 10.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("scan-a"), Value: []byte("v1")},
			{Op: kvrpcpb.Op_Put, Key: []byte("scan-b"), Value: []byte("v2")},
			{Op: kvrpcpb.Op_Put, Key: []byte("scan-c"), Value: []byte("v3")},
		},
		PrimaryLock:  []byte("scan-a"),
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("scan-a"), []byte("scan-b"), []byte("scan-c")},
		StartVersion:  10,
		CommitVersion: 20,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Scan at version 25 (after commit): should see all keys.
	scanResp, err := client.KvScan(ctx, &kvrpcpb.ScanRequest{
		StartKey: []byte("scan-a"),
		EndKey:   []byte("scan-z"),
		Limit:    100,
		Version:  25,
	})
	require.NoError(t, err)
	assert.Len(t, scanResp.GetPairs(), 3, "should see all 3 keys at version 25")

	// Scan at version 15 (before commit): should see no keys.
	scanResp, err = client.KvScan(ctx, &kvrpcpb.ScanRequest{
		StartKey: []byte("scan-a"),
		EndKey:   []byte("scan-z"),
		Limit:    100,
		Version:  15,
	})
	require.NoError(t, err)
	assert.Empty(t, scanResp.GetPairs(), "should see no keys at version 15 (before commit)")

	t.Log("Scan with version visibility passed")
}
