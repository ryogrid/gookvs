package server

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookv/internal/engine/rocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// testSetup creates a test server and returns the client connection and cleanup function.
func testSetup(t *testing.T) (tikvpb.TikvClient, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "gookv-server-test-*")
	require.NoError(t, err)

	engine, err := rocks.Open(tmpDir)
	require.NoError(t, err)

	storage := NewStorage(engine)
	srv := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0", // Random port.
	}, storage)

	require.NoError(t, srv.Start())

	conn, err := grpc.NewClient(
		srv.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := tikvpb.NewTikvClient(conn)

	cleanup := func() {
		conn.Close()
		srv.Stop()
		storage.Close()
		os.RemoveAll(tmpDir)
	}

	return client, cleanup
}

func TestKvGetNotFound(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	resp, err := client.KvGet(context.Background(), &kvrpcpb.GetRequest{
		Key:     []byte("nonexistent"),
		Version: 100,
	})
	require.NoError(t, err)
	assert.True(t, resp.GetNotFound())
	assert.Nil(t, resp.GetError())
}

func TestKvPrewriteAndGet(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("key1")
	value := []byte("value1")
	startTS := uint64(10)
	commitTS := uint64(20)

	// Prewrite.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: key, Value: value},
		},
		PrimaryLock:  key,
		StartVersion: startTS,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// Read at version > startTS should see lock error (key is locked).
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
		Key:     key,
		Version: 15,
	})
	require.NoError(t, err)
	assert.NotNil(t, getResp.GetError())
	assert.NotNil(t, getResp.GetError().GetLocked())

	// Commit.
	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion:  startTS,
		Keys:          [][]byte{key},
		CommitVersion: commitTS,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Read at version >= commitTS should see the value.
	getResp, err = client.KvGet(ctx, &kvrpcpb.GetRequest{
		Key:     key,
		Version: commitTS,
	})
	require.NoError(t, err)
	assert.Nil(t, getResp.GetError())
	assert.False(t, getResp.GetNotFound())
	assert.Equal(t, value, getResp.GetValue())
}

func TestKvPrewriteAndCommitMultipleKeys(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	primary := []byte("pk")
	startTS := uint64(10)
	commitTS := uint64(20)

	// Prewrite two keys.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: primary, Value: []byte("pval")},
			{Op: kvrpcpb.Op_Put, Key: []byte("sk"), Value: []byte("sval")},
		},
		PrimaryLock:  primary,
		StartVersion: startTS,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// Commit both.
	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion:  startTS,
		Keys:          [][]byte{primary, []byte("sk")},
		CommitVersion: commitTS,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Read both keys.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: primary, Version: commitTS})
	require.NoError(t, err)
	assert.Equal(t, []byte("pval"), getResp.GetValue())

	getResp, err = client.KvGet(ctx, &kvrpcpb.GetRequest{Key: []byte("sk"), Version: commitTS})
	require.NoError(t, err)
	assert.Equal(t, []byte("sval"), getResp.GetValue())
}

func TestKvBatchGet(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Write 3 keys.
	for i := 0; i < 3; i++ {
		key := []byte(fmt.Sprintf("bkey%d", i))
		value := []byte(fmt.Sprintf("bval%d", i))
		startTS := uint64(10 + i*10)
		commitTS := uint64(15 + i*10)

		prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
			PrimaryLock:  key,
			StartVersion: startTS,
			LockTtl:      5000,
		})
		require.NoError(t, err)
		assert.Empty(t, prewriteResp.GetErrors())

		commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
			StartVersion:  startTS,
			Keys:          [][]byte{key},
			CommitVersion: commitTS,
		})
		require.NoError(t, err)
		assert.Nil(t, commitResp.GetError())
	}

	// BatchGet all 3 keys.
	batchResp, err := client.KvBatchGet(ctx, &kvrpcpb.BatchGetRequest{
		Keys:    [][]byte{[]byte("bkey0"), []byte("bkey1"), []byte("bkey2"), []byte("nonexistent")},
		Version: 100,
	})
	require.NoError(t, err)
	assert.Nil(t, batchResp.GetError())
	assert.Len(t, batchResp.GetPairs(), 3)
}

func TestKvBatchRollback(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("rkey")
	startTS := uint64(10)

	// Prewrite.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("rval")}},
		PrimaryLock:  key,
		StartVersion: startTS,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// Rollback.
	rollbackResp, err := client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
		StartVersion: startTS,
		Keys:         [][]byte{key},
	})
	require.NoError(t, err)
	assert.Nil(t, rollbackResp.GetError())

	// Key should not be found.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: 100})
	require.NoError(t, err)
	assert.True(t, getResp.GetNotFound())
}

func TestKvCleanup(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("ckey")
	startTS := uint64(10)

	// Prewrite.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("cval")}},
		PrimaryLock:  key,
		StartVersion: startTS,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// Cleanup (rollback).
	cleanupResp, err := client.KvCleanup(ctx, &kvrpcpb.CleanupRequest{
		Key:          key,
		StartVersion: startTS,
	})
	require.NoError(t, err)
	assert.Nil(t, cleanupResp.GetError())
	assert.Equal(t, uint64(0), cleanupResp.GetCommitVersion())

	// Key should not be found.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: 100})
	require.NoError(t, err)
	assert.True(t, getResp.GetNotFound())
}

func TestKvCheckTxnStatus(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("tskey")
	startTS := uint64(10)
	commitTS := uint64(20)

	// Case 1: Check status of a locked transaction.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("tsval")}},
		PrimaryLock:  key,
		StartVersion: startTS,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	statusResp, err := client.KvCheckTxnStatus(ctx, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey: key,
		LockTs:     startTS,
	})
	require.NoError(t, err)
	assert.Nil(t, statusResp.GetError())
	assert.True(t, statusResp.GetLockTtl() > 0) // Locked.
	assert.Equal(t, uint64(0), statusResp.GetCommitVersion())

	// Commit.
	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion:  startTS,
		Keys:          [][]byte{key},
		CommitVersion: commitTS,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Case 2: Check status of committed transaction.
	statusResp, err = client.KvCheckTxnStatus(ctx, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey: key,
		LockTs:     startTS,
	})
	require.NoError(t, err)
	assert.Nil(t, statusResp.GetError())
	assert.Equal(t, uint64(0), statusResp.GetLockTtl())
	assert.Equal(t, commitTS, statusResp.GetCommitVersion())
}

func TestKvPrewriteWriteConflict(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("cfkey")

	// First transaction writes and commits.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("v1")}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion: 10, Keys: [][]byte{key}, CommitVersion: 20,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Second transaction with start_ts=15 should see write conflict.
	prewriteResp, err = client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("v2")}},
		PrimaryLock:  key,
		StartVersion: 15,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, prewriteResp.GetErrors())
	assert.NotNil(t, prewriteResp.GetErrors()[0].GetConflict())
}

func TestKvDeleteMutation(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("dkey")

	// Write a value.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("dval")}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion: 10, Keys: [][]byte{key}, CommitVersion: 20,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Delete the key.
	prewriteResp, err = client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Del, Key: key}},
		PrimaryLock:  key,
		StartVersion: 30,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err = client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion: 30, Keys: [][]byte{key}, CommitVersion: 40,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Key should not be found at version 40.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: 40})
	require.NoError(t, err)
	assert.True(t, getResp.GetNotFound())

	// Key should still be visible at version 25.
	getResp, err = client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: 25})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound())
	assert.Equal(t, []byte("dval"), getResp.GetValue())
}

func TestKvMultipleVersions(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("mvkey")

	// Write version 1.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("v1")}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())
	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion: 10, Keys: [][]byte{key}, CommitVersion: 15,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Write version 2.
	prewriteResp, err = client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("v2")}},
		PrimaryLock:  key,
		StartVersion: 20,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())
	commitResp, err = client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion: 20, Keys: [][]byte{key}, CommitVersion: 25,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Read at version 18: should see v1.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: 18})
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), getResp.GetValue())

	// Read at version 30: should see v2.
	getResp, err = client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: 30})
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), getResp.GetValue())
}

func TestKvScan(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Write keys a, b, c.
	for i, k := range []string{"a", "b", "c"} {
		key := []byte(k)
		value := []byte(fmt.Sprintf("val_%s", k))
		startTS := uint64(10 + i*10)
		commitTS := startTS + 5

		prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
			PrimaryLock:  key,
			StartVersion: startTS,
			LockTtl:      5000,
		})
		require.NoError(t, err)
		assert.Empty(t, prewriteResp.GetErrors())

		commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
			StartVersion: startTS, Keys: [][]byte{key}, CommitVersion: commitTS,
		})
		require.NoError(t, err)
		assert.Nil(t, commitResp.GetError())
	}

	// Scan all keys.
	scanResp, err := client.KvScan(ctx, &kvrpcpb.ScanRequest{
		StartKey: []byte("a"),
		Version:  100,
		Limit:    10,
	})
	require.NoError(t, err)
	assert.Nil(t, scanResp.GetError())
	assert.GreaterOrEqual(t, len(scanResp.GetPairs()), 3)

	// Verify values.
	found := make(map[string]string)
	for _, p := range scanResp.GetPairs() {
		found[string(p.Key)] = string(p.Value)
	}
	assert.Equal(t, "val_a", found["a"])
	assert.Equal(t, "val_b", found["b"])
	assert.Equal(t, "val_c", found["c"])
}

func TestKvScanWithLimit(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Write 5 keys.
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("lkey%d", i))
		value := []byte(fmt.Sprintf("lval%d", i))
		startTS := uint64(10 + i*10)

		prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
			Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
			PrimaryLock:  key,
			StartVersion: startTS,
			LockTtl:      5000,
		})
		require.NoError(t, err)
		assert.Empty(t, prewriteResp.GetErrors())

		commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
			StartVersion: startTS, Keys: [][]byte{key}, CommitVersion: startTS + 5,
		})
		require.NoError(t, err)
		assert.Nil(t, commitResp.GetError())
	}

	// Scan with limit=2.
	scanResp, err := client.KvScan(ctx, &kvrpcpb.ScanRequest{
		StartKey: []byte("lkey"),
		Version:  100,
		Limit:    2,
	})
	require.NoError(t, err)
	assert.Nil(t, scanResp.GetError())
	assert.Len(t, scanResp.GetPairs(), 2)
}

func TestKvEndToEndTransactionFlow(t *testing.T) {
	// M5 milestone test: full transaction lifecycle via gRPC.
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// Transaction 1: Prewrite -> Commit.
	keys := [][]byte{[]byte("t1_k1"), []byte("t1_k2"), []byte("t1_k3")}
	primary := keys[0]
	startTS1 := uint64(100)
	commitTS1 := uint64(110)

	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: keys[0], Value: []byte("val1")},
			{Op: kvrpcpb.Op_Put, Key: keys[1], Value: []byte("val2")},
			{Op: kvrpcpb.Op_Put, Key: keys[2], Value: []byte("val3")},
		},
		PrimaryLock:  primary,
		StartVersion: startTS1,
		LockTtl:      10000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion:  startTS1,
		Keys:          keys,
		CommitVersion: commitTS1,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Verify all keys readable.
	for i, key := range keys {
		getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: commitTS1})
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("val%d", i+1)), getResp.GetValue())
	}

	// Transaction 2: Prewrite -> Rollback.
	key2 := []byte("t2_k1")
	startTS2 := uint64(200)

	prewriteResp, err = client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key2, Value: []byte("tmp")}},
		PrimaryLock:  key2,
		StartVersion: startTS2,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	rollbackResp, err := client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
		StartVersion: startTS2,
		Keys:         [][]byte{key2},
	})
	require.NoError(t, err)
	assert.Nil(t, rollbackResp.GetError())

	// Verify rolled-back key is not visible.
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key2, Version: 300})
	require.NoError(t, err)
	assert.True(t, getResp.GetNotFound())

	// BatchGet on transaction 1 keys.
	batchResp, err := client.KvBatchGet(ctx, &kvrpcpb.BatchGetRequest{
		Keys:    keys,
		Version: commitTS1,
	})
	require.NoError(t, err)
	assert.Len(t, batchResp.GetPairs(), 3)
}

func TestBatchCommands(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()

	// First, write some data via direct RPCs.
	key := []byte("batch_key")
	value := []byte("batch_value")

	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion: 10, Keys: [][]byte{key}, CommitVersion: 20,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	// Test BatchCommands stream.
	stream, err := client.BatchCommands(ctx)
	require.NoError(t, err)

	// Send a batch with a Get request.
	err = stream.Send(&tikvpb.BatchCommandsRequest{
		Requests: []*tikvpb.BatchCommandsRequest_Request{
			{
				Cmd: &tikvpb.BatchCommandsRequest_Request_Get{
					Get: &kvrpcpb.GetRequest{Key: key, Version: 30},
				},
			},
		},
		RequestIds: []uint64{1},
	})
	require.NoError(t, err)

	batchResp, err := stream.Recv()
	require.NoError(t, err)
	assert.Len(t, batchResp.GetResponses(), 1)
	assert.Equal(t, []uint64{1}, batchResp.GetRequestIds())

	getResp := batchResp.GetResponses()[0].GetGet()
	require.NotNil(t, getResp)
	assert.Equal(t, value, getResp.GetValue())

	stream.CloseSend()
}

func TestServerStartStop(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gookv-lifecycle-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	engine, err := rocks.Open(tmpDir)
	require.NoError(t, err)

	storage := NewStorage(engine)
	srv := NewServer(ServerConfig{
		ListenAddr: "127.0.0.1:0",
	}, storage)

	require.NoError(t, srv.Start())
	addr := srv.Addr()
	assert.NotEmpty(t, addr)

	// Verify server is reachable.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := tikvpb.NewTikvClient(conn)
	resp, err := client.KvGet(context.Background(), &kvrpcpb.GetRequest{
		Key: []byte("x"), Version: 1,
	})
	require.NoError(t, err)
	assert.True(t, resp.GetNotFound())

	conn.Close()
	srv.Stop()
	storage.Close()
}

func TestKvPrewriteIdempotent(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("idem_key")
	value := []byte("idem_val")

	// First prewrite.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// Second prewrite with same start_ts should be idempotent.
	prewriteResp, err = client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())
}

func TestKvRollbackIdempotent(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("ridem_key")

	// Prewrite.
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: []byte("v")}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	// First rollback.
	rollbackResp, err := client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
		StartVersion: 10, Keys: [][]byte{key},
	})
	require.NoError(t, err)
	assert.Nil(t, rollbackResp.GetError())

	// Second rollback should also succeed (idempotent).
	rollbackResp, err = client.KvBatchRollback(ctx, &kvrpcpb.BatchRollbackRequest{
		StartVersion: 10, Keys: [][]byte{key},
	})
	require.NoError(t, err)
	assert.Nil(t, rollbackResp.GetError())
}

func TestKvLargeValue(t *testing.T) {
	client, cleanup := testSetup(t)
	defer cleanup()

	ctx := context.Background()
	key := []byte("large_key")
	// Value larger than ShortValueMaxLen (64 bytes).
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(i % 256)
	}

	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations:    []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_Put, Key: key, Value: value}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      5000,
	})
	require.NoError(t, err)
	assert.Empty(t, prewriteResp.GetErrors())

	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		StartVersion: 10, Keys: [][]byte{key}, CommitVersion: 20,
	})
	require.NoError(t, err)
	assert.Nil(t, commitResp.GetError())

	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{Key: key, Version: 20})
	require.NoError(t, err)
	assert.Equal(t, value, getResp.GetValue())
}
