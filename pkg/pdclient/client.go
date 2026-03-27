// Package pdclient provides a client for the Placement Driver (PD) service.
// PD manages cluster metadata, region-to-store mapping, and provides the
// Timestamp Oracle (TSO) for globally unique, monotonically increasing timestamps.
package pdclient

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TimeStamp represents a PD-allocated globally unique timestamp.
// It consists of a physical (milliseconds since epoch) and logical component.
type TimeStamp struct {
	Physical int64
	Logical  int64
}

// ToUint64 encodes the timestamp as a single uint64: physical<<18 | logical.
// This encoding is compatible with TiKV's timestamp format.
func (ts TimeStamp) ToUint64() uint64 {
	return uint64(ts.Physical)<<18 | uint64(ts.Logical)
}

// TimeStampFromUint64 decodes a uint64 timestamp.
func TimeStampFromUint64(v uint64) TimeStamp {
	return TimeStamp{
		Physical: int64(v >> 18),
		Logical:  int64(v & ((1 << 18) - 1)),
	}
}

// Client is the interface for interacting with PD.
type Client interface {
	// GetTS allocates a new globally unique timestamp from PD's TSO.
	GetTS(ctx context.Context) (TimeStamp, error)

	// GetRegion returns the region containing the given key and its leader.
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error)

	// GetRegionByID returns the region with the given ID and its leader.
	GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error)

	// GetStore returns store metadata by store ID.
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)

	// GetAllStores returns metadata for all stores in the cluster.
	GetAllStores(ctx context.Context) ([]*metapb.Store, error)

	// Bootstrap bootstraps the cluster with the given store and region.
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error)

	// IsBootstrapped returns whether the cluster has been bootstrapped.
	IsBootstrapped(ctx context.Context) (bool, error)

	// PutStore registers or updates a store in PD.
	PutStore(ctx context.Context, store *metapb.Store) error

	// ReportRegionHeartbeat sends region heartbeat to PD.
	// Returns the PD response which may contain scheduling commands.
	ReportRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error)

	// StoreHeartbeat sends a store-level heartbeat to PD.
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error

	// AskBatchSplit requests new region IDs for split operations.
	AskBatchSplit(ctx context.Context, region *metapb.Region, count uint32) (*pdpb.AskBatchSplitResponse, error)

	// ReportBatchSplit notifies PD of completed splits.
	ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error

	// GetGCSafePoint returns the cluster-wide GC safe point from PD.
	GetGCSafePoint(ctx context.Context) (uint64, error)

	// UpdateGCSafePoint advances the cluster-wide GC safe point on PD.
	// PD ensures the safe point only moves forward.
	UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error)

	// AllocID allocates a new unique ID from PD.
	AllocID(ctx context.Context) (uint64, error)

	// GetClusterID returns the cluster identifier.
	GetClusterID(ctx context.Context) uint64

	// Close shuts down the client.
	Close()
}

// Config holds PD client configuration.
type Config struct {
	Endpoints      []string      // PD server addresses (e.g., ["127.0.0.1:2379"])
	RetryInterval  time.Duration // retry interval for failed requests
	RetryMaxCount  int           // max retries (-1 = infinite)
	UpdateInterval time.Duration // interval for refreshing PD leader info
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Endpoints:      []string{"127.0.0.1:2379"},
		RetryInterval:  300 * time.Millisecond,
		RetryMaxCount:  10,
		UpdateInterval: 10 * time.Minute,
	}
}

// grpcClient is the real PD client that communicates over gRPC.
type grpcClient struct {
	cfg       Config
	clusterID uint64

	mu     sync.RWMutex
	conn   *grpc.ClientConn
	client pdpb.PDClient

	// TSO batching.
	tsoBatchSize int

	// Failover / retry fields.
	endpoints    []string      // all known PD addresses
	currentIdx   int           // current endpoint index
	reconnectMu  sync.Mutex    // serializes reconnection

	closed atomic.Bool
}

// NewClient creates a new PD client connected to the given endpoints.
func NewClient(ctx context.Context, cfg Config) (Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("pdclient: no endpoints configured")
	}

	// Apply defaults for retry config if not set.
	if cfg.RetryInterval <= 0 {
		cfg.RetryInterval = 500 * time.Millisecond
	}
	if cfg.RetryMaxCount <= 0 {
		cfg.RetryMaxCount = 3
	}
	// UpdateInterval=0 means disabled by default; no override needed.

	// Connect to the first available endpoint.
	var conn *grpc.ClientConn
	var connIdx int
	var err error
	for i, ep := range cfg.Endpoints {
		slog.Debug("pd.dial", "endpoint", ep)
		// TODO: grpc.DialContext is deprecated; migrate to grpc.NewClient
		// once a non-blocking connection establishment pattern is adopted.
		conn, err = grpc.DialContext(ctx, ep, //nolint:staticcheck
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(), //nolint:staticcheck
		)
		if err == nil {
			connIdx = i
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("pdclient: connect: %w", err)
	}

	c := &grpcClient{
		cfg:          cfg,
		conn:         conn,
		client:       pdpb.NewPDClient(conn),
		tsoBatchSize: 64,
		endpoints:    cfg.Endpoints,
		currentIdx:   connIdx,
	}

	// Discover cluster ID.
	slog.Debug("pd.GetMembers")
	resp, err := c.client.GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("pdclient: get members: %w", err)
	}
	if resp.GetHeader() != nil {
		c.clusterID = resp.GetHeader().GetClusterId()
	}

	return c, nil
}

func (c *grpcClient) header() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{ClusterId: c.clusterID}
}

// reconnect closes the existing connection and tries the next endpoint.
// It first tries to discover the leader via GetMembers, then falls back
// to round-robin over all endpoints. Must be called with reconnectMu held.
func (c *grpcClient) reconnect() error {
	// Try to discover leader via GetMembers on the current connection first.
	if leaderAddr := c.discoverLeader(); leaderAddr != "" {
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
		}
		c.mu.Unlock()

		slog.Debug("pd.reconnect.leader", "addr", leaderAddr)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		// TODO: grpc.DialContext is deprecated; migrate to grpc.NewClient
		// once a non-blocking connection establishment pattern is adopted.
		conn, err := grpc.DialContext(ctx, leaderAddr, //nolint:staticcheck
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(), //nolint:staticcheck
		)
		cancel()
		if err == nil {
			c.mu.Lock()
			c.conn = conn
			c.client = pdpb.NewPDClient(conn)
			c.mu.Unlock()
			return nil
		}
		slog.Debug("pd.reconnect.leader.failed", "addr", leaderAddr, "err", err)
	}

	// Fallback: round-robin over all endpoints.
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.mu.Unlock()

	n := len(c.endpoints)
	for i := 0; i < n; i++ {
		nextIdx := (c.currentIdx + 1 + i) % n
		ep := c.endpoints[nextIdx]

		slog.Debug("pd.reconnect", "endpoint", ep)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		// TODO: grpc.DialContext is deprecated; migrate to grpc.NewClient
		// once a non-blocking connection establishment pattern is adopted.
		conn, err := grpc.DialContext(ctx, ep, //nolint:staticcheck
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(), //nolint:staticcheck
		)
		cancel()
		if err != nil {
			continue
		}

		c.mu.Lock()
		c.conn = conn
		c.client = pdpb.NewPDClient(conn)
		c.currentIdx = nextIdx
		c.mu.Unlock()
		return nil
	}
	return fmt.Errorf("pdclient: reconnect failed: all %d endpoints exhausted", n)
}

// discoverLeader queries GetMembers on the current connection to find the leader's
// client address. Returns empty string if discovery fails.
func (c *grpcClient) discoverLeader() string {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return ""
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return ""
	}

	leader := resp.GetLeader()
	if leader == nil {
		return ""
	}

	urls := leader.GetClientUrls()
	if len(urls) == 0 {
		return ""
	}

	// Client URLs are in "http://host:port" format; strip the scheme.
	addr := urls[0]
	if strings.HasPrefix(addr, "http://") {
		addr = strings.TrimPrefix(addr, "http://")
	} else if strings.HasPrefix(addr, "https://") {
		addr = strings.TrimPrefix(addr, "https://")
	}
	return addr
}

// withRetry retries fn up to RetryMaxCount times with exponential backoff
// and endpoint reconnection between retries.
func (c *grpcClient) withRetry(fn func() error) error {
	err := fn()
	if err == nil {
		return nil
	}

	maxRetries := c.cfg.RetryMaxCount
	baseInterval := c.cfg.RetryInterval
	const maxBackoff = 5 * time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Exponential backoff: baseInterval * 2^attempt, capped at 5s.
		backoff := baseInterval * time.Duration(1<<uint(attempt))
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
		time.Sleep(backoff)

		c.reconnectMu.Lock()
		reconnErr := c.reconnect()
		c.reconnectMu.Unlock()
		if reconnErr != nil {
			continue
		}

		err = fn()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("pdclient: retries exhausted: %w", err)
}

func (c *grpcClient) GetTS(ctx context.Context) (TimeStamp, error) {
	var result TimeStamp
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.Tso")
		stream, err := client.Tso(ctx)
		if err != nil {
			return fmt.Errorf("pdclient: tso stream: %w", err)
		}

		req := &pdpb.TsoRequest{
			Header: c.header(),
			Count:  1,
		}
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("pdclient: tso send: %w", err)
		}

		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("pdclient: tso recv: %w", err)
		}

		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: tso error: %s", resp.GetHeader().GetError().GetMessage())
		}

		ts := resp.GetTimestamp()
		result = TimeStamp{
			Physical: int64(ts.GetPhysical()),
			Logical:  int64(ts.GetLogical()),
		}
		return nil
	})
	return result, err
}

func (c *grpcClient) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	var region *metapb.Region
	var leader *metapb.Peer
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.GetRegion", "key", fmt.Sprintf("%x", key))
		resp, err := client.GetRegion(ctx, &pdpb.GetRegionRequest{
			Header:    c.header(),
			RegionKey: key,
		})
		if err != nil {
			return fmt.Errorf("pdclient: get region: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: get region error: %s", resp.GetHeader().GetError().GetMessage())
		}
		region = resp.GetRegion()
		leader = resp.GetLeader()
		return nil
	})
	return region, leader, err
}

func (c *grpcClient) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	var region *metapb.Region
	var leader *metapb.Peer
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.GetRegionByID", "region-id", regionID)
		resp, err := client.GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
			Header:   c.header(),
			RegionId: regionID,
		})
		if err != nil {
			return fmt.Errorf("pdclient: get region by id: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: get region by id error: %s", resp.GetHeader().GetError().GetMessage())
		}
		region = resp.GetRegion()
		leader = resp.GetLeader()
		return nil
	})
	return region, leader, err
}

func (c *grpcClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	var store *metapb.Store
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.GetStore", "store-id", storeID)
		resp, err := client.GetStore(ctx, &pdpb.GetStoreRequest{
			Header:  c.header(),
			StoreId: storeID,
		})
		if err != nil {
			return fmt.Errorf("pdclient: get store: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: get store error: %s", resp.GetHeader().GetError().GetMessage())
		}
		store = resp.GetStore()
		return nil
	})
	return store, err
}

func (c *grpcClient) GetAllStores(ctx context.Context) ([]*metapb.Store, error) {
	var stores []*metapb.Store
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.GetAllStores")
		resp, err := client.GetAllStores(ctx, &pdpb.GetAllStoresRequest{
			Header: c.header(),
		})
		if err != nil {
			return fmt.Errorf("pdclient: get all stores: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: get all stores error: %s", resp.GetHeader().GetError().GetMessage())
		}
		stores = resp.GetStores()
		return nil
	})
	return stores, err
}

func (c *grpcClient) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error) {
	slog.Debug("pd.Bootstrap", "store-id", store.GetId(), "region-id", region.GetId())
	var result *pdpb.BootstrapResponse
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		resp, err := client.Bootstrap(ctx, &pdpb.BootstrapRequest{
			Header: c.header(),
			Store:  store,
			Region: region,
		})
		if err != nil {
			return fmt.Errorf("pdclient: bootstrap: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: bootstrap error: %s", resp.GetHeader().GetError().GetMessage())
		}
		result = resp
		return nil
	})
	return result, err
}

func (c *grpcClient) IsBootstrapped(ctx context.Context) (bool, error) {
	slog.Debug("pd.IsBootstrapped")
	resp, err := c.client.IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{
		Header: c.header(),
	})
	if err != nil {
		return false, fmt.Errorf("pdclient: is bootstrapped: %w", err)
	}
	return resp.GetBootstrapped(), nil
}

func (c *grpcClient) PutStore(ctx context.Context, store *metapb.Store) error {
	return c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.PutStore", "store-id", store.GetId())
		resp, err := client.PutStore(ctx, &pdpb.PutStoreRequest{
			Header: c.header(),
			Store:  store,
		})
		if err != nil {
			return fmt.Errorf("pdclient: put store: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: put store error: %s", resp.GetHeader().GetError().GetMessage())
		}
		return nil
	})
}

func (c *grpcClient) ReportRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) (*pdpb.RegionHeartbeatResponse, error) {
	var result *pdpb.RegionHeartbeatResponse
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.RegionHeartbeat", "region-id", req.GetRegion().GetId())
		req.Header = c.header()
		stream, err := client.RegionHeartbeat(ctx)
		if err != nil {
			return fmt.Errorf("pdclient: region heartbeat stream: %w", err)
		}
		if err := stream.Send(req); err != nil {
			return fmt.Errorf("pdclient: region heartbeat send: %w", err)
		}
		if err := stream.CloseSend(); err != nil {
			return fmt.Errorf("pdclient: region heartbeat close: %w", err)
		}
		// Wait for the server to acknowledge the heartbeat.
		resp, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("pdclient: region heartbeat recv: %w", err)
		}
		result = resp
		return nil
	})
	return result, err
}

func (c *grpcClient) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	return c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.StoreHeartbeat", "store-id", stats.GetStoreId())
		resp, err := client.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
			Header: c.header(),
			Stats:  stats,
		})
		if err != nil {
			return fmt.Errorf("pdclient: store heartbeat: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: store heartbeat error: %s", resp.GetHeader().GetError().GetMessage())
		}
		return nil
	})
}

func (c *grpcClient) AskBatchSplit(ctx context.Context, region *metapb.Region, count uint32) (*pdpb.AskBatchSplitResponse, error) {
	var result *pdpb.AskBatchSplitResponse
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.AskBatchSplit", "region-id", region.GetId(), "count", count)
		resp, err := client.AskBatchSplit(ctx, &pdpb.AskBatchSplitRequest{
			Header:     c.header(),
			Region:     region,
			SplitCount: count,
		})
		if err != nil {
			return fmt.Errorf("pdclient: ask batch split: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: ask batch split error: %s", resp.GetHeader().GetError().GetMessage())
		}
		result = resp
		return nil
	})
	return result, err
}

func (c *grpcClient) ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error {
	return c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.ReportBatchSplit", "regions", len(regions))
		resp, err := client.ReportBatchSplit(ctx, &pdpb.ReportBatchSplitRequest{
			Header:  c.header(),
			Regions: regions,
		})
		if err != nil {
			return fmt.Errorf("pdclient: report batch split: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: report batch split error: %s", resp.GetHeader().GetError().GetMessage())
		}
		return nil
	})
}

func (c *grpcClient) GetGCSafePoint(ctx context.Context) (uint64, error) {
	var safePoint uint64
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.GetGCSafePoint")
		resp, err := client.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{
			Header: c.header(),
		})
		if err != nil {
			return fmt.Errorf("pdclient: get gc safe point: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: get gc safe point error: %s", resp.GetHeader().GetError().GetMessage())
		}
		safePoint = resp.GetSafePoint()
		return nil
	})
	return safePoint, err
}

func (c *grpcClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	var newSafePoint uint64
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.UpdateGCSafePoint", "safe-point", safePoint)
		resp, err := client.UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{
			Header:    c.header(),
			SafePoint: safePoint,
		})
		if err != nil {
			return fmt.Errorf("pdclient: update gc safe point: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: update gc safe point error: %s", resp.GetHeader().GetError().GetMessage())
		}
		newSafePoint = resp.GetNewSafePoint()
		return nil
	})
	return newSafePoint, err
}

func (c *grpcClient) AllocID(ctx context.Context) (uint64, error) {
	var id uint64
	err := c.withRetry(func() error {
		c.mu.RLock()
		client := c.client
		c.mu.RUnlock()

		slog.Debug("pd.AllocID")
		resp, err := client.AllocID(ctx, &pdpb.AllocIDRequest{
			Header: c.header(),
		})
		if err != nil {
			return fmt.Errorf("pdclient: alloc id: %w", err)
		}
		if resp.GetHeader().GetError() != nil {
			return fmt.Errorf("pdclient: alloc id error: %s", resp.GetHeader().GetError().GetMessage())
		}
		id = resp.GetId()
		return nil
	})
	return id, err
}

func (c *grpcClient) GetClusterID(_ context.Context) uint64 {
	return c.clusterID
}

func (c *grpcClient) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
	}
}
