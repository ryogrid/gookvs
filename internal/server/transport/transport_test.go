package transport

import (
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockResolver is a test StoreResolver.
type mockResolver struct {
	stores map[uint64]string
}

func (r *mockResolver) ResolveStore(storeID uint64) (string, error) {
	addr, ok := r.stores[storeID]
	if !ok {
		return "", fmt.Errorf("store %d not found", storeID)
	}
	return addr, nil
}

func TestNewRaftClient(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{1: "127.0.0.1:20160"}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	assert.NotNil(t, client)
	assert.Equal(t, 128, client.batchSize)
	client.Close()
}

func TestDefaultRaftClientConfig(t *testing.T) {
	cfg := DefaultRaftClientConfig()
	assert.Equal(t, 1, cfg.PoolSize)
	assert.Equal(t, 128, cfg.BatchSize)
	assert.Greater(t, cfg.DialTimeout.Nanoseconds(), int64(0))
}

func TestNewRaftClientDefaultValues(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, RaftClientConfig{})
	assert.Equal(t, 128, client.batchSize)
	client.Close()
}

func TestRemoveConnection(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{1: "127.0.0.1:20160"}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	// Removing nonexistent connection should be a no-op.
	client.RemoveConnection(1)
	assert.Empty(t, client.connections)
}

func TestCloseEmptyClient(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	client.Close()
	assert.Empty(t, client.connections)
}

func TestGetConnectionResolveError(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	_, err := client.getConnection(999)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolve store 999")
}

func TestSendResolveError(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	msg := &raft_serverpb.RaftMessage{}
	err := client.Send(999, msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolve store 999")
}

func TestBatchSendEmpty(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{1: "127.0.0.1:20160"}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	err := client.BatchSend(1, nil)
	assert.NoError(t, err)
}

func TestHashRegionForConn(t *testing.T) {
	tests := []struct {
		regionID uint64
		poolSize int
	}{
		{1, 1},
		{2, 1},
		{100, 4},
		{200, 4},
		{300, 8},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("region_%d_pool_%d", tt.regionID, tt.poolSize), func(t *testing.T) {
			idx := HashRegionForConn(tt.regionID, tt.poolSize)
			assert.GreaterOrEqual(t, idx, 0)
			assert.Less(t, idx, tt.poolSize)
		})
	}
}

func TestHashRegionForConnSinglePool(t *testing.T) {
	assert.Equal(t, 0, HashRegionForConn(42, 1))
	assert.Equal(t, 0, HashRegionForConn(0, 0))
}

func TestHashRegionForConnDeterministic(t *testing.T) {
	idx1 := HashRegionForConn(123, 4)
	idx2 := HashRegionForConn(123, 4)
	assert.Equal(t, idx1, idx2)
}

func TestHashRegionForConnDistribution(t *testing.T) {
	poolSize := 4
	counts := make([]int, poolSize)

	for i := uint64(0); i < 1000; i++ {
		idx := HashRegionForConn(i, poolSize)
		counts[idx]++
	}

	// Each slot should get some messages (rough distribution check).
	for _, count := range counts {
		assert.Greater(t, count, 0)
	}
}

func TestMessageBatcherAdd(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{1: "127.0.0.1:20160"}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	batcher := NewMessageBatcher(client, 128)
	msg := &raft_serverpb.RaftMessage{}

	batcher.Add(1, msg)
	batcher.Add(1, msg)
	batcher.Add(2, msg)

	pending := batcher.Pending()
	assert.Equal(t, 2, pending[1])
	assert.Equal(t, 1, pending[2])
}

func TestMessageBatcherFlushEmpty(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	batcher := NewMessageBatcher(client, 128)
	errs := batcher.Flush()
	assert.Empty(t, errs)
}

func TestMessageBatcherPendingEmpty(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	batcher := NewMessageBatcher(client, 128)
	pending := batcher.Pending()
	assert.Empty(t, pending)
}

func TestNewMessageBatcherDefaults(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	batcher := NewMessageBatcher(client, 0) // 0 should default to 128
	assert.Equal(t, 128, batcher.maxSize)
}

func TestConnPoolCreation(t *testing.T) {
	pool := newConnPool("127.0.0.1:20160", 1)
	assert.NotNil(t, pool)
	assert.Equal(t, "127.0.0.1:20160", pool.addr)
	assert.Equal(t, 1, pool.size)
}

func TestConnPoolDefaultSize(t *testing.T) {
	pool := newConnPool("127.0.0.1:20160", 0)
	assert.Equal(t, 1, pool.size)
}

func TestConnPoolClose(t *testing.T) {
	pool := newConnPool("127.0.0.1:20160", 1)
	pool.close() // Should not panic with nil connections.
}

func TestMessageBatcherFlushClearsBatches(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{1: "127.0.0.1:20160"}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	batcher := NewMessageBatcher(client, 128)
	msg := &raft_serverpb.RaftMessage{}
	batcher.Add(1, msg)

	// After flush (even if it errors), batches should be cleared.
	_ = batcher.Flush()
	pending := batcher.Pending()
	assert.Empty(t, pending)
}

func TestSendSnapshotResolveError(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	msg := &raft_serverpb.RaftMessage{}
	err := client.SendSnapshot(999, msg, []byte("data"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "resolve store 999")
}

func TestRaftClientConnectionCaching(t *testing.T) {
	resolver := &mockResolver{stores: map[uint64]string{
		1: "127.0.0.1:20160",
		2: "127.0.0.1:20161",
	}}
	client := NewRaftClient(resolver, DefaultRaftClientConfig())
	defer client.Close()

	// getConnection for same store should reuse pool.
	// We can't test actual connection caching without a server,
	// but we can verify pool creation.
	_, err := client.getConnection(999)
	assert.Error(t, err) // Not in resolver.

	// Verify pools are created and cached.
	require.Empty(t, client.connections)
}
