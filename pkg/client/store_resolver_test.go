package client

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolve_CacheMiss(t *testing.T) {
	mock := pdclient.NewMockClient(1)
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})

	r := NewPDStoreResolver(mock, 30*time.Second)
	addr, err := r.Resolve(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20160", addr)
}

func TestResolve_CacheHit(t *testing.T) {
	mock := pdclient.NewMockClient(1)
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})

	r := NewPDStoreResolver(mock, 30*time.Second)
	ctx := context.Background()

	// First call populates cache.
	addr1, err := r.Resolve(ctx, 1)
	require.NoError(t, err)

	// Change the store address in mock — cache should still return old value.
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:99999"})
	addr2, err := r.Resolve(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, addr1, addr2)
}

func TestResolve_CacheExpiry(t *testing.T) {
	mock := pdclient.NewMockClient(1)
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})

	r := NewPDStoreResolver(mock, 1*time.Second)
	ctx := context.Background()

	// Use controllable time.
	now := time.Now()
	r.nowFunc = func() time.Time { return now }

	_, err := r.Resolve(ctx, 1)
	require.NoError(t, err)

	// Advance time past TTL.
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20161"})
	now = now.Add(2 * time.Second)

	addr, err := r.Resolve(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20161", addr)
}

func TestInvalidateStore(t *testing.T) {
	mock := pdclient.NewMockClient(1)
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20160"})

	r := NewPDStoreResolver(mock, 30*time.Second)
	ctx := context.Background()

	_, err := r.Resolve(ctx, 1)
	require.NoError(t, err)

	r.InvalidateStore(1)

	// Update store address; after invalidation, should fetch new value.
	mock.SetStore(&metapb.Store{Id: 1, Address: "127.0.0.1:20161"})
	addr, err := r.Resolve(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:20161", addr)
}

func TestResolve_StoreNotFound(t *testing.T) {
	mock := pdclient.NewMockClient(1)
	// Don't set any store.

	r := NewPDStoreResolver(mock, 30*time.Second)
	_, err := r.Resolve(context.Background(), 99)
	assert.Error(t, err)
}
