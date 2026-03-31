package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// RegionRequestSender sends RPCs to the correct store for a given key,
// handling region errors and retries with cache invalidation.
type RegionRequestSender struct {
	cache      *RegionCache
	resolver   *PDStoreResolver
	maxRetries int
	dialTimeout time.Duration

	mu    sync.RWMutex
	conns map[string]*grpc.ClientConn
}

// NewRegionRequestSender creates a new sender.
func NewRegionRequestSender(cache *RegionCache, resolver *PDStoreResolver, maxRetries int, dialTimeout time.Duration) *RegionRequestSender {
	if maxRetries <= 0 {
		maxRetries = 10
	}
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	return &RegionRequestSender{
		cache:       cache,
		resolver:    resolver,
		maxRetries:  maxRetries,
		dialTimeout: dialTimeout,
		conns:       make(map[string]*grpc.ClientConn),
	}
}

// RPCFunc is the callback for making a gRPC call.
// It receives the TiKV client and region info (for populating request context).
// It should return the region_error from the response (if any) and any gRPC error.
type RPCFunc func(client tikvpb.TikvClient, info *RegionInfo) (regionErr *errorpb.Error, err error)

// SendToRegion locates the region for the given key, sends the RPC,
// and retries on retriable region errors.
// Retries up to maxRetries times with exponential backoff (100ms, 200ms, 400ms, ...),
// capped at 2s per sleep. Also respects the context deadline.
func (s *RegionRequestSender) SendToRegion(ctx context.Context, key []byte, rpcFn RPCFunc) error {
	var lastRegionErr string
	backoff := 100 * time.Millisecond
	const maxBackoff = 2 * time.Second

	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		// Respect context cancellation between retries.
		if ctx.Err() != nil {
			break
		}

		info, err := s.cache.LocateKey(ctx, key)
		if err != nil {
			return fmt.Errorf("locate key: %w", err)
		}

		conn, err := s.getOrDial(info.StoreAddr)
		if err != nil {
			s.cache.InvalidateRegion(info.Region.GetId())
			continue
		}

		slog.Debug("tikv.rpc", "region-id", info.Region.GetId(), "store", info.StoreAddr)
		client := tikvpb.NewTikvClient(conn)
		regionErr, err := rpcFn(client, info)

		if err != nil {
			// gRPC-level error: invalidate region cache and retry.
			// Do NOT close the connection — it may be shared by other goroutines,
			// and closing it causes cascading "connection is closing" errors.
			// gRPC handles reconnection automatically.
			s.cache.InvalidateRegion(info.Region.GetId())
			lastRegionErr = fmt.Sprintf("grpc: %v", err)
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		if regionErr == nil {
			return nil // success
		}

		lastRegionErr = regionErr.GetMessage()
		// Handle region errors with cache invalidation.
		if !s.handleRegionError(ctx, info, regionErr) {
			return fmt.Errorf("region error: %s", regionErr.GetMessage())
		}
		if nl := regionErr.GetNotLeader(); nl != nil {
			if nl.GetLeader() != nil {
				// Leader hint available — cache updated, minimal backoff.
				time.Sleep(50 * time.Millisecond)
				continue
			}
			// No leader hint — election may be in progress.
			// Use a fixed delay to allow leader election + PD heartbeat.
			time.Sleep(1 * time.Second)
			continue
		}
		// Exponential backoff before retry to allow region state to stabilize
		// (e.g., new region peers being created after a split, PD propagation).
		time.Sleep(backoff)
		backoff = min(backoff*2, maxBackoff)
	}
	return fmt.Errorf("max retries (%d) exhausted (last: %s, key=%x)", s.maxRetries, lastRegionErr, key)
}

// handleRegionError processes a region error, invalidates cache as needed,
// and returns true if the error is retriable.
func (s *RegionRequestSender) handleRegionError(ctx context.Context, info *RegionInfo, regionErr *errorpb.Error) bool {
	regionID := info.Region.GetId()

	if nl := regionErr.GetNotLeader(); nl != nil {
		if nl.GetLeader() != nil {
			addr, err := s.resolver.Resolve(ctx, nl.GetLeader().GetStoreId())
			if err == nil {
				s.cache.UpdateLeader(regionID, nl.GetLeader(), addr)
			} else {
				s.cache.InvalidateRegion(regionID)
			}
		} else {
			s.cache.InvalidateRegion(regionID)
		}
		return true
	}

	if enm := regionErr.GetEpochNotMatch(); enm != nil {
		s.cache.InvalidateRegion(regionID)
		// Use CurrentRegions from the server response to update cache directly,
		// bypassing PD which may not have propagated the split yet.
		for _, region := range enm.GetCurrentRegions() {
			s.cache.InsertFromEpochNotMatch(ctx, region, s.resolver)
		}
		return true
	}

	if regionErr.GetRegionNotFound() != nil ||
		regionErr.GetKeyNotInRegion() != nil {
		s.cache.InvalidateRegion(regionID)
		return true
	}

	if regionErr.GetStoreNotMatch() != nil {
		s.resolver.InvalidateStore(info.Leader.GetStoreId())
		s.cache.InvalidateRegion(regionID)
		return true
	}

	return false
}

// getOrDial returns a cached connection or dials a new one.
func (s *RegionRequestSender) getOrDial(addr string) (*grpc.ClientConn, error) {
	s.mu.RLock()
	if conn, ok := s.conns[addr]; ok {
		s.mu.RUnlock()
		return conn, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock.
	if conn, ok := s.conns[addr]; ok {
		return conn, nil
	}

	slog.Debug("tikv.dial", "addr", addr)
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                60 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: false,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	s.conns[addr] = conn
	return conn, nil
}

// closeConn closes and removes a cached connection.
func (s *RegionRequestSender) closeConn(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if conn, ok := s.conns[addr]; ok {
		conn.Close()
		delete(s.conns, addr)
	}
}

// Close closes all cached connections.
func (s *RegionRequestSender) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for addr, conn := range s.conns {
		conn.Close()
		delete(s.conns, addr)
	}
}
