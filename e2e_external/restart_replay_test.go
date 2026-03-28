package e2e_external_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookv/pkg/e2elib"
)

// TestRestartDataSurvives writes data, restarts a follower node, and verifies
// all data is still readable. This confirms Raft log replay works on restart.
func TestRestartDataSurvives(t *testing.T) {
	cluster := newClusterWithLeader(t)
	rawKV := cluster.RawKV()
	ctx := context.Background()

	// Write 10 keys.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("restart-key-%02d", i))
		val := []byte(fmt.Sprintf("restart-val-%02d", i))
		err := rawKV.Put(ctx, key, val)
		require.NoError(t, err)
	}

	// Restart node 1 (a follower).
	require.NoError(t, cluster.RestartNode(1))
	require.NoError(t, cluster.Node(1).WaitForReady(30*time.Second))

	// Reset client to reconnect after restart.
	cluster.ResetClient()
	rawKV = cluster.RawKV()

	// Wait for cluster to stabilize.
	e2elib.WaitForCondition(t, 30*time.Second, "cluster operational after restart", func() bool {
		ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, _, err := rawKV.Get(ctx2, []byte("restart-key-00"))
		return err == nil
	})

	// Verify all 10 keys survived the restart.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("restart-key-%02d", i))
		val, notFound, err := rawKV.Get(ctx, key)
		require.NoError(t, err)
		assert.False(t, notFound, "key %s should exist after restart", key)
		assert.Equal(t, []byte(fmt.Sprintf("restart-val-%02d", i)), val)
	}

	t.Log("Restart data survives passed")
}

// TestRestartLeaderFailoverAndReplay stops the leader, writes new data on the
// new leader, restarts the old leader, and verifies all data (old + new) is
// readable. This confirms Raft log replay catches up a restarted node.
func TestRestartLeaderFailoverAndReplay(t *testing.T) {
	cluster := newClusterWithLeader(t)
	rawKV := cluster.RawKV()
	ctx := context.Background()

	// Write 10 keys on the original leader.
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("failover-key-%02d", i))
		val := []byte(fmt.Sprintf("failover-val-%02d", i))
		err := rawKV.Put(ctx, key, val)
		require.NoError(t, err)
	}

	// Find and stop the leader.
	pdClient := cluster.PD().Client()
	leaderStoreID := e2elib.WaitForRegionLeader(t, pdClient, []byte(""), 30*time.Second)
	leaderIdx := int(leaderStoreID) - 1
	require.NoError(t, cluster.StopNode(leaderIdx))

	// Wait for new leader and write 10 more keys.
	cluster.ResetClient()
	rawKV = cluster.RawKV()
	e2elib.WaitForCondition(t, 30*time.Second, "new leader elected", func() bool {
		ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return rawKV.Put(ctx2, []byte("failover-new-00"), []byte("new-val-00")) == nil
	})

	for i := 1; i < 10; i++ {
		key := []byte(fmt.Sprintf("failover-new-%02d", i))
		val := []byte(fmt.Sprintf("new-val-%02d", i))
		e2elib.WaitForCondition(t, 15*time.Second, fmt.Sprintf("put %s", key), func() bool {
			ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			return rawKV.Put(ctx2, key, val) == nil
		})
	}

	// Restart the old leader.
	require.NoError(t, cluster.RestartNode(leaderIdx))
	require.NoError(t, cluster.Node(leaderIdx).WaitForReady(30*time.Second))

	// Reset client and wait for cluster to stabilize.
	cluster.ResetClient()
	rawKV = cluster.RawKV()
	e2elib.WaitForCondition(t, 30*time.Second, "cluster stable after rejoin", func() bool {
		ctx2, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, _, err := rawKV.Get(ctx2, []byte("failover-key-00"))
		return err == nil
	})

	// Verify all 20 keys (10 old + 10 new).
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("failover-key-%02d", i))
		val, notFound, err := rawKV.Get(ctx, key)
		require.NoError(t, err, "old key %s", key)
		assert.False(t, notFound, "old key %s should exist", key)
		assert.Equal(t, []byte(fmt.Sprintf("failover-val-%02d", i)), val)
	}
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("failover-new-%02d", i))
		val, notFound, err := rawKV.Get(ctx, key)
		require.NoError(t, err, "new key %s", key)
		assert.False(t, notFound, "new key %s should exist after replay", key)
		assert.Equal(t, []byte(fmt.Sprintf("new-val-%02d", i)), val)
	}

	t.Log("Restart leader failover and replay passed")
}

// TestRestartAllNodesDataSurvives stops all 3 nodes, restarts them all,
// and verifies data integrity. This is the most demanding restart test,
// confirming the full cluster can recover from a cold start.
func TestRestartAllNodesDataSurvives(t *testing.T) {
	cluster := newClusterWithLeader(t)
	rawKV := cluster.RawKV()
	ctx := context.Background()

	// Write 20 keys.
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("allrestart-key-%02d", i))
		val := []byte(fmt.Sprintf("allrestart-val-%02d", i))
		err := rawKV.Put(ctx, key, val)
		require.NoError(t, err)
	}

	// Stop all 3 nodes.
	for i := 2; i >= 0; i-- {
		require.NoError(t, cluster.StopNode(i))
	}

	// Restart all 3 nodes.
	for i := 0; i < 3; i++ {
		require.NoError(t, cluster.RestartNode(i))
		require.NoError(t, cluster.Node(i).WaitForReady(30*time.Second))
	}

	// Reset client and wait for leader election.
	cluster.ResetClient()
	rawKV = cluster.RawKV()
	e2elib.WaitForCondition(t, 60*time.Second, "leader elected after full restart", func() bool {
		ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		_, _, err := rawKV.Get(ctx2, []byte("allrestart-key-00"))
		return err == nil
	})

	// Verify all 20 keys survived full cluster restart.
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("allrestart-key-%02d", i))
		val, notFound, err := rawKV.Get(ctx, key)
		require.NoError(t, err, "key %s", key)
		assert.False(t, notFound, "key %s should survive full restart", key)
		assert.Equal(t, []byte(fmt.Sprintf("allrestart-val-%02d", i)), val)
	}

	t.Log("Restart all nodes data survives passed")
}
