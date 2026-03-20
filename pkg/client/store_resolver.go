package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ryogrid/gookv/pkg/pdclient"
)

type storeEntry struct {
	addr      string
	fetchedAt time.Time
}

// PDStoreResolver resolves store IDs to gRPC addresses via PD, with TTL caching.
type PDStoreResolver struct {
	mu       sync.RWMutex
	pdClient pdclient.Client
	stores   map[uint64]*storeEntry
	ttl      time.Duration
	nowFunc  func() time.Time // for testing
}

// NewPDStoreResolver creates a new resolver with the given PD client and cache TTL.
func NewPDStoreResolver(pdClient pdclient.Client, ttl time.Duration) *PDStoreResolver {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &PDStoreResolver{
		pdClient: pdClient,
		stores:   make(map[uint64]*storeEntry),
		ttl:      ttl,
		nowFunc:  time.Now,
	}
}

// Resolve returns the gRPC address for the given store ID.
// Returns from cache if fresh; queries PD if stale or missing.
func (r *PDStoreResolver) Resolve(ctx context.Context, storeID uint64) (string, error) {
	now := r.nowFunc()

	r.mu.RLock()
	if e, ok := r.stores[storeID]; ok && now.Sub(e.fetchedAt) < r.ttl {
		addr := e.addr
		r.mu.RUnlock()
		return addr, nil
	}
	r.mu.RUnlock()

	slog.Debug("resolver.GetStore", "store-id", storeID)
	store, err := r.pdClient.GetStore(ctx, storeID)
	if err != nil {
		return "", fmt.Errorf("resolve store %d: %w", storeID, err)
	}
	addr := store.GetAddress()
	if addr == "" {
		return "", fmt.Errorf("store %d has no address", storeID)
	}

	r.mu.Lock()
	r.stores[storeID] = &storeEntry{addr: addr, fetchedAt: now}
	r.mu.Unlock()

	return addr, nil
}

// InvalidateStore removes the cached address for a store.
func (r *PDStoreResolver) InvalidateStore(storeID uint64) {
	r.mu.Lock()
	delete(r.stores, storeID)
	r.mu.Unlock()
}
