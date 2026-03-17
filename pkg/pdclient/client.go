// Package pdclient provides a client for the Placement Driver (PD) service.
// PD manages cluster metadata, region-to-store mapping, and provides the
// Timestamp Oracle (TSO) for globally unique, monotonically increasing timestamps.
package pdclient

import (
	"context"
	"fmt"
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

	// Bootstrap bootstraps the cluster with the given store and region.
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error)

	// IsBootstrapped returns whether the cluster has been bootstrapped.
	IsBootstrapped(ctx context.Context) (bool, error)

	// PutStore registers or updates a store in PD.
	PutStore(ctx context.Context, store *metapb.Store) error

	// ReportRegionHeartbeat sends region heartbeat to PD.
	// Returns any scheduling commands from PD.
	ReportRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) error

	// StoreHeartbeat sends a store-level heartbeat to PD.
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error

	// AskBatchSplit requests new region IDs for split operations.
	AskBatchSplit(ctx context.Context, region *metapb.Region, count uint32) (*pdpb.AskBatchSplitResponse, error)

	// ReportBatchSplit notifies PD of completed splits.
	ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error

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

	closed atomic.Bool
}

// NewClient creates a new PD client connected to the given endpoints.
func NewClient(ctx context.Context, cfg Config) (Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("pdclient: no endpoints configured")
	}

	// Connect to the first available endpoint.
	var conn *grpc.ClientConn
	var err error
	for _, ep := range cfg.Endpoints {
		conn, err = grpc.DialContext(ctx, ep,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err == nil {
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
	}

	// Discover cluster ID.
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

func (c *grpcClient) GetTS(ctx context.Context) (TimeStamp, error) {
	stream, err := c.client.Tso(ctx)
	if err != nil {
		return TimeStamp{}, fmt.Errorf("pdclient: tso stream: %w", err)
	}

	req := &pdpb.TsoRequest{
		Header: c.header(),
		Count:  1,
	}
	if err := stream.Send(req); err != nil {
		return TimeStamp{}, fmt.Errorf("pdclient: tso send: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return TimeStamp{}, fmt.Errorf("pdclient: tso recv: %w", err)
	}

	if resp.GetHeader().GetError() != nil {
		return TimeStamp{}, fmt.Errorf("pdclient: tso error: %s", resp.GetHeader().GetError().GetMessage())
	}

	ts := resp.GetTimestamp()
	return TimeStamp{
		Physical: int64(ts.GetPhysical()),
		Logical:  int64(ts.GetLogical()),
	}, nil
}

func (c *grpcClient) GetRegion(ctx context.Context, key []byte) (*metapb.Region, *metapb.Peer, error) {
	resp, err := c.client.GetRegion(ctx, &pdpb.GetRegionRequest{
		Header:    c.header(),
		RegionKey: key,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pdclient: get region: %w", err)
	}
	if resp.GetHeader().GetError() != nil {
		return nil, nil, fmt.Errorf("pdclient: get region error: %s", resp.GetHeader().GetError().GetMessage())
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *grpcClient) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, *metapb.Peer, error) {
	resp, err := c.client.GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
		Header:   c.header(),
		RegionId: regionID,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("pdclient: get region by id: %w", err)
	}
	if resp.GetHeader().GetError() != nil {
		return nil, nil, fmt.Errorf("pdclient: get region by id error: %s", resp.GetHeader().GetError().GetMessage())
	}
	return resp.GetRegion(), resp.GetLeader(), nil
}

func (c *grpcClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	resp, err := c.client.GetStore(ctx, &pdpb.GetStoreRequest{
		Header:  c.header(),
		StoreId: storeID,
	})
	if err != nil {
		return nil, fmt.Errorf("pdclient: get store: %w", err)
	}
	if resp.GetHeader().GetError() != nil {
		return nil, fmt.Errorf("pdclient: get store error: %s", resp.GetHeader().GetError().GetMessage())
	}
	return resp.GetStore(), nil
}

func (c *grpcClient) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error) {
	resp, err := c.client.Bootstrap(ctx, &pdpb.BootstrapRequest{
		Header: c.header(),
		Store:  store,
		Region: region,
	})
	if err != nil {
		return nil, fmt.Errorf("pdclient: bootstrap: %w", err)
	}
	if resp.GetHeader().GetError() != nil {
		return nil, fmt.Errorf("pdclient: bootstrap error: %s", resp.GetHeader().GetError().GetMessage())
	}
	return resp, nil
}

func (c *grpcClient) IsBootstrapped(ctx context.Context) (bool, error) {
	resp, err := c.client.IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{
		Header: c.header(),
	})
	if err != nil {
		return false, fmt.Errorf("pdclient: is bootstrapped: %w", err)
	}
	return resp.GetBootstrapped(), nil
}

func (c *grpcClient) PutStore(ctx context.Context, store *metapb.Store) error {
	resp, err := c.client.PutStore(ctx, &pdpb.PutStoreRequest{
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
}

func (c *grpcClient) ReportRegionHeartbeat(ctx context.Context, req *pdpb.RegionHeartbeatRequest) error {
	req.Header = c.header()
	stream, err := c.client.RegionHeartbeat(ctx)
	if err != nil {
		return fmt.Errorf("pdclient: region heartbeat stream: %w", err)
	}
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("pdclient: region heartbeat send: %w", err)
	}
	return nil
}

func (c *grpcClient) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	resp, err := c.client.StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
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
}

func (c *grpcClient) AskBatchSplit(ctx context.Context, region *metapb.Region, count uint32) (*pdpb.AskBatchSplitResponse, error) {
	resp, err := c.client.AskBatchSplit(ctx, &pdpb.AskBatchSplitRequest{
		Header:     c.header(),
		Region:     region,
		SplitCount: count,
	})
	if err != nil {
		return nil, fmt.Errorf("pdclient: ask batch split: %w", err)
	}
	if resp.GetHeader().GetError() != nil {
		return nil, fmt.Errorf("pdclient: ask batch split error: %s", resp.GetHeader().GetError().GetMessage())
	}
	return resp, nil
}

func (c *grpcClient) ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error {
	resp, err := c.client.ReportBatchSplit(ctx, &pdpb.ReportBatchSplitRequest{
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
}

func (c *grpcClient) AllocID(ctx context.Context) (uint64, error) {
	resp, err := c.client.AllocID(ctx, &pdpb.AllocIDRequest{
		Header: c.header(),
	})
	if err != nil {
		return 0, fmt.Errorf("pdclient: alloc id: %w", err)
	}
	if resp.GetHeader().GetError() != nil {
		return 0, fmt.Errorf("pdclient: alloc id error: %s", resp.GetHeader().GetError().GetMessage())
	}
	return resp.GetId(), nil
}

func (c *grpcClient) GetClusterID(_ context.Context) uint64 {
	return c.clusterID
}

func (c *grpcClient) Close() {
	if c.closed.CompareAndSwap(false, true) {
		if c.conn != nil {
			c.conn.Close()
		}
	}
}
