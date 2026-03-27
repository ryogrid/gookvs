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

// TestRawBatchScan tests the RawBatchScan RPC.
// It puts keys into different ranges and verifies that RawBatchScan
// returns results from each range independently.
func TestRawBatchScan(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	// Write keys in two separate ranges.
	// Range 1: "bs-a-00" .. "bs-a-04"
	// Range 2: "bs-b-00" .. "bs-b-02"
	for i := 0; i < 5; i++ {
		_, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
			Key:   []byte(fmt.Sprintf("bs-a-%02d", i)),
			Value: []byte(fmt.Sprintf("val-a-%02d", i)),
		})
		require.NoError(t, err)
	}
	for i := 0; i < 3; i++ {
		_, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
			Key:   []byte(fmt.Sprintf("bs-b-%02d", i)),
			Value: []byte(fmt.Sprintf("val-b-%02d", i)),
		})
		require.NoError(t, err)
	}

	// BatchScan both ranges.
	batchScanResp, err := client.RawBatchScan(ctx, &kvrpcpb.RawBatchScanRequest{
		Ranges: []*kvrpcpb.KeyRange{
			{StartKey: []byte("bs-a-00"), EndKey: []byte("bs-a-99")},
			{StartKey: []byte("bs-b-00"), EndKey: []byte("bs-b-99")},
		},
		EachLimit: 10,
	})
	require.NoError(t, err)
	// Expect 5 + 3 = 8 total pairs across both ranges.
	assert.Len(t, batchScanResp.GetKvs(), 8,
		"should get 8 KV pairs total across both ranges")

	// Verify that both range prefixes are represented.
	aCount, bCount := 0, 0
	for _, kv := range batchScanResp.GetKvs() {
		if len(kv.GetKey()) >= 4 && string(kv.GetKey()[:4]) == "bs-a" {
			aCount++
		}
		if len(kv.GetKey()) >= 4 && string(kv.GetKey()[:4]) == "bs-b" {
			bCount++
		}
	}
	assert.Equal(t, 5, aCount, "range A should have 5 keys")
	assert.Equal(t, 3, bCount, "range B should have 3 keys")

	// Test with EachLimit=2: should return at most 2 per range.
	batchScanResp2, err := client.RawBatchScan(ctx, &kvrpcpb.RawBatchScanRequest{
		Ranges: []*kvrpcpb.KeyRange{
			{StartKey: []byte("bs-a-00"), EndKey: []byte("bs-a-99")},
			{StartKey: []byte("bs-b-00"), EndKey: []byte("bs-b-99")},
		},
		EachLimit: 2,
	})
	require.NoError(t, err)
	assert.Len(t, batchScanResp2.GetKvs(), 4,
		"should get 2+2=4 KV pairs with EachLimit=2")

	t.Log("RawBatchScan passed")
}

// TestRawGetKeyTTL tests the RawGetKeyTTL RPC.
// Verifies TTL=0 for keys without TTL and not_found=true for missing keys.
func TestRawGetKeyTTL(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	key := []byte("ttl-test-key")
	value := []byte("ttl-test-value")

	// Put a key without TTL.
	putResp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
		Key:   key,
		Value: value,
	})
	require.NoError(t, err)
	assert.Empty(t, putResp.GetError())

	// GetKeyTTL: key exists, no TTL set => ttl=0, not_found=false.
	ttlResp, err := client.RawGetKeyTTL(ctx, &kvrpcpb.RawGetKeyTTLRequest{
		Key: key,
	})
	require.NoError(t, err)
	assert.Empty(t, ttlResp.GetError())
	assert.False(t, ttlResp.GetNotFound(), "key should be found")
	assert.Equal(t, uint64(0), ttlResp.GetTtl(), "TTL should be 0 for key without TTL")

	// GetKeyTTL for non-existent key: not_found=true.
	ttlResp2, err := client.RawGetKeyTTL(ctx, &kvrpcpb.RawGetKeyTTLRequest{
		Key: []byte("nonexistent-key"),
	})
	require.NoError(t, err)
	assert.Empty(t, ttlResp2.GetError())
	assert.True(t, ttlResp2.GetNotFound(), "nonexistent key should report not_found=true")

	t.Log("RawGetKeyTTL passed")
}

// TestRawCompareAndSwap tests the RawCompareAndSwap RPC.
// Covers: successful swap, failed swap (wrong previous value),
// create (prev_not_exist=true), and delete (delete=true).
func TestRawCompareAndSwap(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	key := []byte("cas-key")

	// Subtest 1: Create with prev_not_exist=true.
	t.Run("CreateWhenNotExist", func(t *testing.T) {
		casResp, err := client.RawCompareAndSwap(ctx, &kvrpcpb.RawCASRequest{
			Key:              key,
			Value:            []byte("initial"),
			PreviousNotExist: true,
		})
		require.NoError(t, err)
		assert.Empty(t, casResp.GetError())
		assert.True(t, casResp.GetSucceed(), "CAS create should succeed when key doesn't exist")
		assert.True(t, casResp.GetPreviousNotExist(), "previous should not exist")

		// Verify the key was written.
		getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: key})
		require.NoError(t, err)
		assert.Equal(t, []byte("initial"), getResp.GetValue())
	})

	// Subtest 2: Successful CAS with correct previous value.
	t.Run("SwapWithCorrectPreviousValue", func(t *testing.T) {
		casResp, err := client.RawCompareAndSwap(ctx, &kvrpcpb.RawCASRequest{
			Key:           key,
			Value:         []byte("updated"),
			PreviousValue: []byte("initial"),
		})
		require.NoError(t, err)
		assert.Empty(t, casResp.GetError())
		assert.True(t, casResp.GetSucceed(), "CAS should succeed with matching previous value")

		// Verify the key was updated.
		getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: key})
		require.NoError(t, err)
		assert.Equal(t, []byte("updated"), getResp.GetValue())
	})

	// Subtest 3: Failed CAS with wrong previous value.
	t.Run("FailWithWrongPreviousValue", func(t *testing.T) {
		casResp, err := client.RawCompareAndSwap(ctx, &kvrpcpb.RawCASRequest{
			Key:           key,
			Value:         []byte("should-not-write"),
			PreviousValue: []byte("wrong-value"),
		})
		require.NoError(t, err)
		assert.Empty(t, casResp.GetError())
		assert.False(t, casResp.GetSucceed(), "CAS should fail with wrong previous value")
		assert.Equal(t, []byte("updated"), casResp.GetPreviousValue(),
			"should return the current value")

		// Verify the key was NOT modified.
		getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: key})
		require.NoError(t, err)
		assert.Equal(t, []byte("updated"), getResp.GetValue())
	})

	// Subtest 4: Delete via CAS.
	t.Run("DeleteViaCAS", func(t *testing.T) {
		casResp, err := client.RawCompareAndSwap(ctx, &kvrpcpb.RawCASRequest{
			Key:           key,
			PreviousValue: []byte("updated"),
			Delete:        true,
		})
		require.NoError(t, err)
		assert.Empty(t, casResp.GetError())
		assert.True(t, casResp.GetSucceed(), "CAS delete should succeed with matching value")

		// Verify the key is deleted.
		getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: key})
		require.NoError(t, err)
		assert.True(t, getResp.GetNotFound(), "key should be deleted after CAS delete")
	})

	t.Log("RawCompareAndSwap passed")
}

// TestRawChecksum tests the RawChecksum RPC.
// It puts several keys, calls RawChecksum on the range, and verifies
// that totalKvs matches the expected count.
func TestRawChecksum(t *testing.T) {
	
	node := e2elib.NewStandaloneNode(t)
	
	

	client := e2elib.DialTikvClient(t, node.Addr())
	ctx := context.Background()

	// Write 5 keys in a known range.
	const keyCount = 5
	for i := 0; i < keyCount; i++ {
		_, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
			Key:   []byte(fmt.Sprintf("csum-%02d", i)),
			Value: []byte(fmt.Sprintf("csum-val-%02d", i)),
		})
		require.NoError(t, err)
	}

	// Call RawChecksum on the range.
	csumResp, err := client.RawChecksum(ctx, &kvrpcpb.RawChecksumRequest{
		Ranges: []*kvrpcpb.KeyRange{
			{StartKey: []byte("csum-00"), EndKey: []byte("csum-99")},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, csumResp.GetError())
	assert.Equal(t, uint64(keyCount), csumResp.GetTotalKvs(),
		"totalKvs should match the number of keys written")
	assert.Greater(t, csumResp.GetTotalBytes(), uint64(0),
		"totalBytes should be non-zero")
	assert.NotZero(t, csumResp.GetChecksum(), "checksum should be non-zero")

	// Write the same keys again and verify checksum is stable for same data.
	csumResp2, err := client.RawChecksum(ctx, &kvrpcpb.RawChecksumRequest{
		Ranges: []*kvrpcpb.KeyRange{
			{StartKey: []byte("csum-00"), EndKey: []byte("csum-99")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, csumResp.GetChecksum(), csumResp2.GetChecksum(),
		"checksum should be deterministic for unchanged data")
	assert.Equal(t, csumResp.GetTotalKvs(), csumResp2.GetTotalKvs())

	t.Log("RawChecksum passed")
}
