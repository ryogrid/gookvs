package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ryogrid/gookv/internal/server/transport"
	"github.com/ryogrid/gookv/pkg/pdclient"
)

// Ensure PDStoreResolver implements transport.StoreResolver.
var _ transport.StoreResolver = (*PDStoreResolver)(nil)

type pdStoreEntry struct {
	addr      string
	fetchedAt time.Time
}

// PDStoreResolver resolves store IDs to network addresses via PD, with TTL caching.
// Unlike the client-side PDStoreResolver in pkg/client/, this implements
// transport.StoreResolver (no context parameter) for use with RaftClient.
type PDStoreResolver struct {
	mu       sync.RWMutex
	pdClient pdclient.Client
	stores   map[uint64]*pdStoreEntry
	ttl      time.Duration
	nowFunc  func() time.Time // for testing
}

// NewPDStoreResolver creates a resolver with the given PD client and cache TTL.
func NewPDStoreResolver(pdClient pdclient.Client, ttl time.Duration) *PDStoreResolver {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &PDStoreResolver{
		pdClient: pdClient,
		stores:   make(map[uint64]*pdStoreEntry),
		ttl:      ttl,
		nowFunc:  time.Now,
	}
}

// ResolveStore implements transport.StoreResolver.
// Returns the cached address if fresh; queries PD if stale or missing.
// Creates an internal context with 5-second timeout for PD queries.
func (r *PDStoreResolver) ResolveStore(storeID uint64) (string, error) {
	now := r.nowFunc()

	r.mu.RLock()
	if e, ok := r.stores[storeID]; ok && now.Sub(e.fetchedAt) < r.ttl {
		addr := e.addr
		r.mu.RUnlock()
		return addr, nil
	}
	r.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Debug("pd_resolver.GetStore", "store-id", storeID)
	store, err := r.pdClient.GetStore(ctx, storeID)
	if err != nil {
		return "", fmt.Errorf("resolve store %d via PD: %w", storeID, err)
	}
	addr := store.GetAddress()
	if addr == "" {
		return "", fmt.Errorf("store %d has no address", storeID)
	}

	r.mu.Lock()
	r.stores[storeID] = &pdStoreEntry{addr: addr, fetchedAt: now}
	r.mu.Unlock()

	return addr, nil
}

// InvalidateStore removes the cached address for a store.
func (r *PDStoreResolver) InvalidateStore(storeID uint64) {
	r.mu.Lock()
	delete(r.stores, storeID)
	r.mu.Unlock()
}
