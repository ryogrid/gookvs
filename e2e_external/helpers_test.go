package e2e_external_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ryogrid/gookv/pkg/client"
	"github.com/ryogrid/gookv/pkg/e2elib"
)

// newClusterWithLeader creates a 3-node cluster and waits for Raft leader election.
func newClusterWithLeader(t *testing.T) *e2elib.GokvCluster {
	t.Helper()
	return newClusterWithLeaderN(t, 3)
}

// newClusterWithLeaderN creates an N-node cluster and waits for Raft leader election.
func newClusterWithLeaderN(t *testing.T, numNodes int) *e2elib.GokvCluster {
	t.Helper()
	e2elib.SkipIfNoBinary(t, "gookv-server", "gookv-pd")

	cluster := e2elib.NewGokvCluster(t, e2elib.GokvClusterConfig{NumNodes: numNodes})
	require.NoError(t, cluster.Start())
	t.Cleanup(func() { cluster.Stop() })

	// Wait for Raft leader election. Scale timeout with node count.
	electionTimeout := time.Duration(30+numNodes*10) * time.Second
	rawKV := cluster.RawKV()
	e2elib.WaitForCondition(t, electionTimeout, "cluster leader election", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return rawKV.Put(ctx, []byte("__health__"), []byte("ok")) == nil
	})

	return cluster
}

// newClientCluster creates a 3-node cluster and returns both the cluster and a RawKVClient.
// It waits for Raft leader election before returning.
func newClientCluster(t *testing.T) (*e2elib.GokvCluster, *client.RawKVClient) {
	t.Helper()
	cluster := newClusterWithLeader(t)
	return cluster, cluster.RawKV()
}
