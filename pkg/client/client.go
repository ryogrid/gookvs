package client

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/ryogrid/gookv/pkg/pdclient"
)

// Config holds configuration for the client library.
type Config struct {
	PDAddrs       []string
	DialTimeout   time.Duration // default: 5s
	MaxRetries    int           // default: 3
	StoreCacheTTL time.Duration // default: 30s
}

// Client provides access to gookv sub-clients (RawKV, etc.).
type Client struct {
	pdClient pdclient.Client
	cache    *RegionCache
	resolver *PDStoreResolver
	sender   *RegionRequestSender
}

// NewClient creates a client connected to PD.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.PDAddrs) == 0 {
		return nil, fmt.Errorf("no PD addresses provided")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.StoreCacheTTL <= 0 {
		cfg.StoreCacheTTL = 30 * time.Second
	}

	pdClient, err := pdclient.NewClient(ctx, pdclient.Config{
		Endpoints: cfg.PDAddrs,
	})
	if err != nil {
		return nil, fmt.Errorf("create PD client: %w", err)
	}

	resolver := NewPDStoreResolver(pdClient, cfg.StoreCacheTTL)
	cache := NewRegionCache(pdClient, resolver)
	sender := NewRegionRequestSender(cache, resolver, cfg.MaxRetries, cfg.DialTimeout)

	return &Client{
		pdClient: pdClient,
		cache:    cache,
		resolver: resolver,
		sender:   sender,
	}, nil
}

// TxnKV returns a TxnKVClient for performing transactional KV operations.
func (c *Client) TxnKV() *TxnKVClient {
	return NewTxnKVClient(c.sender, c.cache, c.pdClient)
}

// RawKV returns a RawKVClient for performing Raw KV operations.
func (c *Client) RawKV() *RawKVClient {
	return &RawKVClient{
		sender: c.sender,
		cache:  c.cache,
		cf:     "",
	}
}

// Close releases all resources.
func (c *Client) Close() error {
	c.sender.Close()
	c.pdClient.Close()
	return nil
}

// buildContext creates a kvrpcpb.Context from RegionInfo for RPC requests.
func buildContext(info *RegionInfo) *kvrpcpb.Context {
	return &kvrpcpb.Context{
		RegionId:    info.Region.GetId(),
		RegionEpoch: info.Region.GetRegionEpoch(),
		Peer:        info.Leader,
	}
}
