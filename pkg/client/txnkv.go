package client

import (
	"context"
	"fmt"

	"github.com/ryogrid/gookv/pkg/pdclient"
	"github.com/ryogrid/gookv/pkg/txntypes"
)

// TxnMode defines the transaction mode.
type TxnMode int

const (
	// TxnModeOptimistic is the default optimistic transaction mode.
	TxnModeOptimistic TxnMode = iota
	// TxnModePessimistic uses pessimistic locking.
	TxnModePessimistic
)

// TxnOptions holds options for beginning a transaction.
type TxnOptions struct {
	Mode           TxnMode
	UseAsyncCommit bool
	Try1PC         bool
	LockTTL        uint64
}

// TxnOption is a functional option for configuring TxnOptions.
type TxnOption func(*TxnOptions)

// WithPessimistic enables pessimistic transaction mode.
func WithPessimistic() TxnOption {
	return func(o *TxnOptions) {
		o.Mode = TxnModePessimistic
	}
}

// WithAsyncCommit enables the async commit protocol.
func WithAsyncCommit() TxnOption {
	return func(o *TxnOptions) {
		o.UseAsyncCommit = true
	}
}

// With1PC enables the 1PC optimization for single-region transactions.
func With1PC() TxnOption {
	return func(o *TxnOptions) {
		o.Try1PC = true
	}
}

// WithLockTTL sets the lock TTL in milliseconds.
func WithLockTTL(ttl uint64) TxnOption {
	return func(o *TxnOptions) {
		o.LockTTL = ttl
	}
}

// defaultLockTTL is the default lock TTL in milliseconds.
const defaultLockTTL = 3000

// TxnKVClient provides transactional KV operations.
type TxnKVClient struct {
	sender   *RegionRequestSender
	cache    *RegionCache
	pdClient pdclient.Client
	resolver *LockResolver
}

// NewTxnKVClient creates a new TxnKVClient.
func NewTxnKVClient(sender *RegionRequestSender, cache *RegionCache, pdClient pdclient.Client) *TxnKVClient {
	c := &TxnKVClient{
		sender:   sender,
		cache:    cache,
		pdClient: pdClient,
	}
	c.resolver = NewLockResolver(sender, cache, pdClient)
	return c
}

// Begin starts a new transaction with the given options.
func (c *TxnKVClient) Begin(ctx context.Context, opts ...TxnOption) (*TxnHandle, error) {
	options := TxnOptions{
		LockTTL: defaultLockTTL,
	}
	for _, opt := range opts {
		opt(&options)
	}

	ts, err := c.pdClient.GetTS(ctx)
	if err != nil {
		return nil, fmt.Errorf("get start timestamp: %w", err)
	}

	return newTxnHandle(c, txntypes.TimeStamp(ts.ToUint64()), options), nil
}

// Close releases resources held by the TxnKVClient.
func (c *TxnKVClient) Close() error {
	return nil
}
