# Client Library — Detailed Design

## 1. Package Layout

```
pkg/client/
  ├── region_cache.go       // RegionCache: key → region → store resolution
  ├── region_cache_test.go
  ├── store_resolver.go     // PDStoreResolver: storeID → address with caching
  ├── store_resolver_test.go
  ├── request_sender.go     // RegionRequestSender: dispatch + retry
  ├── request_sender_test.go
  ├── rawkv.go              // RawKVClient: high-level Raw KV API
  ├── rawkv_test.go
  ├── client.go             // Client: top-level factory, lifecycle management
  └── client_test.go
```

All types are in `pkg/client` (public package, importable by applications).

---

## 2. RegionCache

### Purpose

Cache the mapping `key → RegionInfo{Region, LeaderPeer, StoreAddr}` to avoid PD round-trips on every request.

### Data Structures

```go
// RegionInfo holds cached region metadata with its leader and store address.
type RegionInfo struct {
    Region    *metapb.Region
    Leader    *metapb.Peer
    StoreAddr string  // resolved via PDStoreResolver
}

// RegionCache caches key-to-region mappings.
type RegionCache struct {
    mu       sync.RWMutex
    pdClient pdclient.Client
    resolver *PDStoreResolver

    // sorted slice of cached regions, ordered by StartKey.
    // Lookup via binary search on key ranges.
    regions  []*RegionInfo
    // regionID → index in regions slice for fast lookup by ID.
    byID     map[uint64]int
}
```

### Key Methods

```go
// LocateKey returns the RegionInfo for the given key.
// Returns from cache if available; queries PD on cache miss.
func (c *RegionCache) LocateKey(ctx context.Context, key []byte) (*RegionInfo, error)

// LocateRegionByID returns the RegionInfo for the given region ID.
func (c *RegionCache) LocateRegionByID(ctx context.Context, regionID uint64) (*RegionInfo, error)

// InvalidateRegion removes the cached entry for the given region ID.
// Called on EpochNotMatch or RegionNotFound errors.
func (c *RegionCache) InvalidateRegion(regionID uint64)

// UpdateLeader updates the cached leader for a region.
// Called on NotLeader errors with the hint from the error response.
func (c *RegionCache) UpdateLeader(regionID uint64, leader *metapb.Peer)

// GroupKeysByRegion groups a set of keys by their region.
// Returns map[regionID] → {RegionInfo, keys[]}.
func (c *RegionCache) GroupKeysByRegion(ctx context.Context, keys [][]byte) (map[uint64]*KeyGroup, error)
```

### Lookup Algorithm

`LocateKey` uses binary search on the sorted `regions` slice:

```
1. RLock, binary search for region where startKey <= key < endKey
2. If found and not expired → return cached RegionInfo
3. If miss → RUnlock, call pdClient.GetRegion(ctx, key)
4. Resolve store address via PDStoreResolver
5. Lock, insert into sorted slice (maintaining order), update byID map
6. Return new RegionInfo
```

### Invalidation Strategy

| Error | Action |
|-------|--------|
| `NotLeader{leader: peer}` | `UpdateLeader(regionID, peer)` — update leader in place, re-resolve store addr |
| `NotLeader{leader: nil}` | `InvalidateRegion(regionID)` — force PD re-query |
| `EpochNotMatch` | `InvalidateRegion(regionID)` — region was split/merged |
| `RegionNotFound` | `InvalidateRegion(regionID)` — region no longer exists |
| `StoreNotMatch` | `resolver.InvalidateStore(storeID)` — refresh store address |
| `KeyNotInRegion` | `InvalidateRegion(regionID)` — routing stale |

---

## 3. PDStoreResolver

### Purpose

Resolve `storeID → gRPC address` dynamically from PD, with caching.

### Data Structures

```go
type storeEntry struct {
    addr      string
    fetchedAt time.Time
}

type PDStoreResolver struct {
    mu       sync.RWMutex
    pdClient pdclient.Client
    stores   map[uint64]*storeEntry
    ttl      time.Duration  // default: 30s
}
```

### Key Methods

```go
// Resolve returns the gRPC address for the given store ID.
// Returns from cache if fresh; queries PD if stale or missing.
func (r *PDStoreResolver) Resolve(ctx context.Context, storeID uint64) (string, error)

// InvalidateStore removes the cached address for a store.
func (r *PDStoreResolver) InvalidateStore(storeID uint64)
```

---

## 4. RegionRequestSender

### Purpose

Send a single RPC to the correct store for a given region, handling errors and retries.

### Data Structures

```go
type RegionRequestSender struct {
    cache       *RegionCache
    resolver    *PDStoreResolver
    maxRetries  int           // default: 3
    dialTimeout time.Duration // default: 5s

    mu    sync.RWMutex
    conns map[string]*grpc.ClientConn // addr → conn pool
}
```

### Key Methods

```go
// SendToRegion sends an RPC to the leader of the region containing the given key.
// On retriable errors (NotLeader, EpochNotMatch), it refreshes the cache and retries.
//
// rpcFn receives a tikvpb.TikvClient and should make the gRPC call.
// It should return the response and the region_error from the response (if any).
func (s *RegionRequestSender) SendToRegion(
    ctx context.Context,
    key []byte,
    rpcFn func(client tikvpb.TikvClient) (regionErr *errorpb.Error, err error),
) error
```

### Retry Flow

```
1. info := cache.LocateKey(ctx, key)
2. conn := getOrDial(info.StoreAddr)
3. client := tikvpb.NewTikvClient(conn)
4. regionErr, err := rpcFn(client)
5. If err != nil (gRPC level) → invalidate region, retry
6. If regionErr != nil:
     NotLeader      → cache.UpdateLeader / InvalidateRegion, retry
     EpochNotMatch  → cache.InvalidateRegion, retry
     RegionNotFound → cache.InvalidateRegion, retry
     StoreNotMatch  → resolver.InvalidateStore, retry
7. If retries exhausted → return error
```

---

## 5. RawKVClient

### Purpose

High-level Raw KV API that applications use directly. Handles region routing, batch splitting, and error retry transparently.

### Data Structures

```go
type RawKVClient struct {
    sender *RegionRequestSender
    cache  *RegionCache
    cf     string // default column family
}
```

### Public API

```go
func NewRawKVClient(pdAddrs []string, opts ...Option) (*RawKVClient, error)
func (c *RawKVClient) Close() error

// Single-key operations: route to the key's region leader.
func (c *RawKVClient) Get(ctx context.Context, key []byte) ([]byte, error)
func (c *RawKVClient) Put(ctx context.Context, key, value []byte) error
func (c *RawKVClient) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error
func (c *RawKVClient) Delete(ctx context.Context, key []byte) error
func (c *RawKVClient) GetKeyTTL(ctx context.Context, key []byte) (uint64, error)

// Batch operations: group keys by region, dispatch in parallel, merge results.
func (c *RawKVClient) BatchGet(ctx context.Context, keys [][]byte) ([]KvPair, error)
func (c *RawKVClient) BatchPut(ctx context.Context, pairs []KvPair) error
func (c *RawKVClient) BatchDelete(ctx context.Context, keys [][]byte) error

// Range operations: may span regions; handled by scanning per-region.
func (c *RawKVClient) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]KvPair, error)
func (c *RawKVClient) DeleteRange(ctx context.Context, startKey, endKey []byte) error

// Advanced operations.
func (c *RawKVClient) CompareAndSwap(ctx context.Context, key, value, prevValue []byte, prevNotExist bool) (bool, []byte, error)
func (c *RawKVClient) Checksum(ctx context.Context, startKey, endKey []byte) (uint64, uint64, uint64, error)
```

### Batch Operation Flow (BatchGet example)

```
1. groups := cache.GroupKeysByRegion(ctx, keys)
2. For each group (in parallel via errgroup):
   a. info := group.RegionInfo
   b. sender.SendToRegion(ctx, info, func(client) {
        resp := client.RawBatchGet(ctx, &RawBatchGetRequest{Keys: group.Keys, Cf: cf})
        return resp.RegionError, resp.Pairs
      })
3. Merge all results into a single []KvPair
4. Return merged results
```

### Scan Across Regions

```
1. currentKey := startKey
2. Loop:
   a. info := cache.LocateKey(ctx, currentKey)
   b. scanEnd := min(endKey, info.Region.EndKey)  // don't exceed region boundary
   c. resp := sender.SendToRegion(...)  // RawScan within this region
   d. Append results
   e. If len(results) >= limit or scanEnd == endKey → done
   f. currentKey = scanEnd  // move to next region
```

---

## 6. Client Factory

```go
// Config holds configuration for the client library.
type Config struct {
    PDAddrs         []string
    DialTimeout     time.Duration // default: 5s
    MaxRetries      int           // default: 3
    StoreCacheTTL   time.Duration // default: 30s
}

// NewClient creates a top-level client with PD connection and region cache.
func NewClient(ctx context.Context, cfg Config) (*Client, error)

// Client provides access to sub-clients.
type Client struct {
    pdClient pdclient.Client
    cache    *RegionCache
    resolver *PDStoreResolver
    sender   *RegionRequestSender
}

func (c *Client) RawKV() *RawKVClient
func (c *Client) Close() error
```

---

## 7. Connection Management

The `RegionRequestSender` maintains a connection pool `map[string]*grpc.ClientConn` keyed by store address. Connections are created lazily on first use with:
- `grpc.WithTransportCredentials(insecure.NewCredentials())`
- `grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64 << 20))`
- Keepalive: 10s interval, 3s timeout

Connections are closed when `Client.Close()` is called.

---

## 8. Error Classification

```go
func isRetryableRegionError(err *errorpb.Error) bool {
    if err == nil { return false }
    return err.GetNotLeader() != nil ||
           err.GetRegionNotFound() != nil ||
           err.GetEpochNotMatch() != nil ||
           err.GetStoreNotMatch() != nil ||
           err.GetKeyNotInRegion() != nil
}
```

Non-retryable errors (returned directly to caller):
- `KeyError` (lock conflict, write conflict) — application-level
- Context cancellation / deadline exceeded
- Max retries exhausted
