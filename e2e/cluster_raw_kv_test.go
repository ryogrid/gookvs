package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClusterRawKVOperations tests raw KV operations routed through Raft consensus.
func TestClusterRawKVOperations(t *testing.T) {
	tsc := newTestServerCluster(t)

	time.Sleep(500 * time.Millisecond)
	if tsc.findLeaderIdx() < 0 {
		peer := tsc.nodes[0].coord.GetPeer(1)
		require.NotNil(t, peer)
		require.NoError(t, peer.Campaign())
	}
	leaderIdx := tsc.waitForLeader(10 * time.Second)
	t.Logf("Leader: node %d", leaderIdx+1)

	_, client := tsc.dialNode(leaderIdx)
	ctx := context.Background()

	// RawPut via Raft.
	putResp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
		Key:   []byte("cluster-raw-key"),
		Value: []byte("cluster-raw-val"),
	})
	require.NoError(t, err)
	assert.Empty(t, putResp.GetError())

	// RawGet from leader.
	getResp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{
		Key: []byte("cluster-raw-key"),
	})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound())
	assert.Equal(t, []byte("cluster-raw-val"), getResp.GetValue())

	// Wait for replication and verify on a follower.
	time.Sleep(500 * time.Millisecond)
	followerIdx := -1
	for i := 0; i < serverClusterSize; i++ {
		if i != leaderIdx {
			followerIdx = i
			break
		}
	}
	require.NotEqual(t, -1, followerIdx)

	_, followerClient := tsc.dialNode(followerIdx)
	getResp, err = followerClient.RawGet(ctx, &kvrpcpb.RawGetRequest{
		Key: []byte("cluster-raw-key"),
	})
	require.NoError(t, err)
	assert.False(t, getResp.GetNotFound(), "replicated raw key should be found on follower")
	assert.Equal(t, []byte("cluster-raw-val"), getResp.GetValue())

	// RawDelete via Raft.
	delResp, err := client.RawDelete(ctx, &kvrpcpb.RawDeleteRequest{
		Key: []byte("cluster-raw-key"),
	})
	require.NoError(t, err)
	assert.Empty(t, delResp.GetError())

	// Verify deletion.
	getResp, err = client.RawGet(ctx, &kvrpcpb.RawGetRequest{
		Key: []byte("cluster-raw-key"),
	})
	require.NoError(t, err)
	assert.True(t, getResp.GetNotFound(), "key should be deleted")

	t.Log("Cluster raw KV operations passed")
}

// TestClusterRawKVBatchPutAndScan tests batch raw KV operations in cluster mode.
func TestClusterRawKVBatchPutAndScan(t *testing.T) {
	tsc := newTestServerCluster(t)

	time.Sleep(500 * time.Millisecond)
	if tsc.findLeaderIdx() < 0 {
		peer := tsc.nodes[0].coord.GetPeer(1)
		require.NotNil(t, peer)
		require.NoError(t, peer.Campaign())
	}
	leaderIdx := tsc.waitForLeader(10 * time.Second)

	_, client := tsc.dialNode(leaderIdx)
	ctx := context.Background()

	// Batch put 20 keys.
	pairs := make([]*kvrpcpb.KvPair, 20)
	for i := 0; i < 20; i++ {
		pairs[i] = &kvrpcpb.KvPair{
			Key:   []byte(fmt.Sprintf("cluster-batch-%03d", i)),
			Value: []byte(fmt.Sprintf("val-%03d", i)),
		}
	}
	batchPutResp, err := client.RawBatchPut(ctx, &kvrpcpb.RawBatchPutRequest{
		Pairs: pairs,
	})
	require.NoError(t, err)
	assert.Empty(t, batchPutResp.GetError())

	// Scan all keys.
	scanResp, err := client.RawScan(ctx, &kvrpcpb.RawScanRequest{
		StartKey: []byte("cluster-batch-000"),
		EndKey:   []byte("cluster-batch-999"),
		Limit:    100,
	})
	require.NoError(t, err)
	assert.Len(t, scanResp.GetKvs(), 20, "should scan all 20 keys")

	t.Log("Cluster raw KV batch put and scan passed")
}
