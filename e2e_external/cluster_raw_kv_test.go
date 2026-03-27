package e2e_external_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterRawKVOperations tests RawPut, RawGet, RawDelete via Raft consensus.
func TestClusterRawKVOperations(t *testing.T) {
	cluster := newClusterWithLeader(t)
	rawKV := cluster.RawKV()
	ctx := context.Background()

	// RawPut
	err := rawKV.Put(ctx, []byte("cluster-key-1"), []byte("cluster-val-1"))
	require.NoError(t, err)

	// RawGet
	val, notFound, err := rawKV.Get(ctx, []byte("cluster-key-1"))
	require.NoError(t, err)
	assert.False(t, notFound)
	assert.Equal(t, []byte("cluster-val-1"), val)

	// RawDelete
	err = rawKV.Delete(ctx, []byte("cluster-key-1"))
	require.NoError(t, err)

	// Verify deleted
	_, notFound, err = rawKV.Get(ctx, []byte("cluster-key-1"))
	require.NoError(t, err)
	assert.True(t, notFound, "key should be deleted")

	t.Log("Cluster RawKV operations passed")
}

// TestClusterRawKVBatchPutAndScan tests batch RawPut and RawScan via Raft consensus.
func TestClusterRawKVBatchPutAndScan(t *testing.T) {
	cluster := newClusterWithLeader(t)
	rawKV := cluster.RawKV()
	ctx := context.Background()

	// Batch put 20 keys.
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("batch-cluster-%02d", i))
		val := []byte(fmt.Sprintf("val-%02d", i))
		err := rawKV.Put(ctx, key, val)
		require.NoError(t, err)
	}

	// Scan all keys.
	pairs, err := rawKV.Scan(ctx, []byte("batch-cluster-00"), []byte("batch-cluster-99"), 100)
	require.NoError(t, err)
	assert.Equal(t, 20, len(pairs), "scan should return all 20 keys")

	// Verify order.
	for i := 1; i < len(pairs); i++ {
		assert.LessOrEqual(t, string(pairs[i-1].Key), string(pairs[i].Key), "keys should be sorted")
	}

	t.Log("Cluster RawKV batch put and scan passed")
}
