package e2e

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookvs/internal/engine/rocks"
	"github.com/ryogrid/gookvs/internal/server"
)

// startStandaloneServer creates a standalone gookvs server (no Raft) for raw KV testing.
func startStandaloneServer(t *testing.T) (string, *server.Server) {
	t.Helper()

	dir := t.TempDir()
	engine, err := rocks.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { engine.Close() })

	storage := server.NewStorage(engine)
	cfg := server.ServerConfig{ListenAddr: "127.0.0.1:0"}
	srv := server.NewServer(cfg, storage)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { srv.Stop() })

	return srv.Addr(), srv
}

// TestRawKVPutGetDelete tests basic raw KV operations via gRPC.
func TestRawKVPutGetDelete(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// RawPut
	putResp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
		Key:   []byte("raw-key-1"),
		Value: []byte("raw-value-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, putResp.GetError())

	// RawGet
	getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{
		Key: []byte("raw-key-1"),
	})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound())
	assert.Equal(t, []byte("raw-value-1"), getResp.GetValue())

	// RawDelete
	delResp, err := client.RawDelete(ctx, &kvrpcpb.RawDeleteRequest{
		Key: []byte("raw-key-1"),
	})
	require.NoError(t, err)
	assert.Empty(t, delResp.GetError())

	// RawGet after delete
	getResp, err = client.RawGet(ctx, &kvrpcpb.RawGetRequest{
		Key: []byte("raw-key-1"),
	})
	require.NoError(t, err)
	assert.True(t, getResp.GetNotFound(), "key should be deleted")

	t.Log("Raw KV put/get/delete passed")
}

// TestRawKVBatchOperations tests batch raw KV operations.
func TestRawKVBatchOperations(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// RawBatchPut
	pairs := make([]*kvrpcpb.KvPair, 10)
	for i := 0; i < 10; i++ {
		pairs[i] = &kvrpcpb.KvPair{
			Key:   []byte(fmt.Sprintf("batch-key-%02d", i)),
			Value: []byte(fmt.Sprintf("batch-value-%02d", i)),
		}
	}
	batchPutResp, err := client.RawBatchPut(ctx, &kvrpcpb.RawBatchPutRequest{
		Pairs: pairs,
	})
	require.NoError(t, err)
	assert.Empty(t, batchPutResp.GetError())

	// RawBatchGet
	keys := make([][]byte, 5)
	for i := 0; i < 5; i++ {
		keys[i] = []byte(fmt.Sprintf("batch-key-%02d", i*2))
	}
	batchGetResp, err := client.RawBatchGet(ctx, &kvrpcpb.RawBatchGetRequest{
		Keys: keys,
	})
	require.NoError(t, err)
	assert.Len(t, batchGetResp.GetPairs(), 5, "should get 5 pairs")

	// RawScan
	scanResp, err := client.RawScan(ctx, &kvrpcpb.RawScanRequest{
		StartKey: []byte("batch-key-00"),
		EndKey:   []byte("batch-key-99"),
		Limit:    100,
	})
	require.NoError(t, err)
	assert.Len(t, scanResp.GetKvs(), 10, "scan should return all 10 keys")

	// RawScan with limit
	scanResp, err = client.RawScan(ctx, &kvrpcpb.RawScanRequest{
		StartKey: []byte("batch-key-00"),
		EndKey:   []byte("batch-key-99"),
		Limit:    3,
	})
	require.NoError(t, err)
	assert.Len(t, scanResp.GetKvs(), 3, "scan with limit 3 should return 3 keys")

	t.Log("Raw KV batch operations passed")
}

// TestRawKVDeleteRange tests the RawDeleteRange operation.
func TestRawKVDeleteRange(t *testing.T) {
	addr, _ := startStandaloneServer(t)
	_, client := dialTikvClient(t, addr)
	ctx := context.Background()

	// Write some keys.
	for i := 0; i < 5; i++ {
		_, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
			Key:   []byte(fmt.Sprintf("dr-key-%02d", i)),
			Value: []byte(fmt.Sprintf("dr-val-%02d", i)),
		})
		require.NoError(t, err)
	}

	// DeleteRange: delete keys 01-03.
	drResp, err := client.RawDeleteRange(ctx, &kvrpcpb.RawDeleteRangeRequest{
		StartKey: []byte("dr-key-01"),
		EndKey:   []byte("dr-key-04"),
	})
	require.NoError(t, err)
	assert.Empty(t, drResp.GetError())

	// Verify: key-00 and key-04 still exist.
	getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: []byte("dr-key-00")})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound(), "key-00 should still exist")

	getResp, err = client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: []byte("dr-key-04")})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound(), "key-04 should still exist")

	// Verify: keys 01, 02, 03 deleted.
	for i := 1; i <= 3; i++ {
		getResp, err = client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: []byte(fmt.Sprintf("dr-key-%02d", i))})
		require.NoError(t, err)
		assert.True(t, getResp.GetNotFound(), "key-%02d should be deleted", i)
	}

	t.Log("Raw KV delete range passed")
}
