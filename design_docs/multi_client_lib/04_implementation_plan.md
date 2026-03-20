# Client Library — Implementation Plan

## 1. Summary

Implement a client library in `pkg/client/` that provides TiKV-compatible multi-region routing for Raw KV operations. The library resolves keys to regions via PD, caches region/store metadata, and handles region errors with automatic retry.

## 2. Prerequisites

Before implementation, ensure:
- gookv server returns proper `region_error` in RPC responses (currently the server uses `resolveRegionID` internally and does not propagate region errors to the client — this must be fixed first).
- PD server's `GetRegion` and `GetStore` RPCs work correctly (already verified).

### 2.1 Server-Side Fix: Propagate Region Errors

Currently, `server.go` handlers resolve the region internally and call `ProposeModifies(regionID, ...)`. If the region is not found or the node is not the leader, it returns a gRPC-level error. For the client library to work properly, the server should instead return a **response-level `region_error`** so the client can distinguish routing errors from real failures.

**Changes needed in `internal/server/server.go`:**

For each RPC handler that calls `ProposeModifies`:
- Catch "region not found" → set `resp.RegionError = &errorpb.Error{RegionNotFound: &errorpb.RegionNotFound{RegionId: regionID}}`
- Catch "not leader" → set `resp.RegionError = &errorpb.Error{NotLeader: &errorpb.NotLeader{RegionId: regionID}}`
- Return the response (not a gRPC error) so the client can inspect and retry.

This is a prerequisite change. Without it, the client sees gRPC `Unavailable` errors and cannot distinguish between routing issues and real failures.

## 3. Implementation Phases

### Phase 1: Server-Side Region Error Propagation

**Files:** `internal/server/server.go`

**Tasks:**
- [ ] Add helper: `proposeModifiesWithRegionError(coord, regionID, modifies, timeout) *errorpb.Error`
- [ ] Modify Raw KV handlers (RawPut, RawDelete, RawBatchPut, RawBatchDelete) to return `region_error` in response instead of gRPC error on ProposeModifies failure
- [ ] Ensure KvGet, KvScan (read-only) also return region_error if accessed on wrong node (currently they read from local engine, which works but returns wrong data if the node doesn't have the region — this is acceptable for now since all nodes share the same engine in the current architecture)

### Phase 2: PDStoreResolver

**Files:** `pkg/client/store_resolver.go`, `pkg/client/store_resolver_test.go`

**Tasks:**
- [ ] Implement `PDStoreResolver` struct with PD-backed address resolution
- [ ] Add TTL-based caching (default 30s)
- [ ] Add `InvalidateStore(storeID)` method
- [ ] Unit tests: cache hit, cache miss, cache expiry, invalidation

### Phase 3: RegionCache

**Files:** `pkg/client/region_cache.go`, `pkg/client/region_cache_test.go`

**Tasks:**
- [ ] Implement `RegionInfo` struct
- [ ] Implement `RegionCache` with sorted slice + binary search
- [ ] `LocateKey(ctx, key)` — cache lookup with PD fallback
- [ ] `InvalidateRegion(regionID)` — cache eviction
- [ ] `UpdateLeader(regionID, leader)` — leader update in place
- [ ] `GroupKeysByRegion(ctx, keys)` — batch key grouping
- [ ] Unit tests with mock PD client: cold cache, warm cache, invalidation, split handling, key grouping

### Phase 4: RegionRequestSender

**Files:** `pkg/client/request_sender.go`, `pkg/client/request_sender_test.go`

**Tasks:**
- [ ] Implement `RegionRequestSender` with connection pool
- [ ] `SendToRegion(ctx, key, rpcFn)` — locate region, send, handle errors, retry
- [ ] Error classification: `isRetryableRegionError()`
- [ ] Error-driven cache invalidation (NotLeader, EpochNotMatch, etc.)
- [ ] Configurable max retries (default 3)
- [ ] Unit tests: success path, NotLeader retry, EpochNotMatch retry, max retries exhausted

### Phase 5: RawKVClient

**Files:** `pkg/client/rawkv.go`, `pkg/client/rawkv_test.go`, `pkg/client/client.go`

**Tasks:**
- [ ] Implement `Client` factory with PD connection
- [ ] Implement `RawKVClient` with full API:
  - Single-key: Get, Put, PutWithTTL, Delete, GetKeyTTL
  - Batch: BatchGet, BatchPut, BatchDelete (parallel per-region dispatch)
  - Range: Scan (cross-region with continuation), DeleteRange
  - Advanced: CompareAndSwap, Checksum
- [ ] Implement cross-region Scan (iterate region by region until limit or endKey)
- [ ] `Client.Close()` — cleanup connections, caches
- [ ] Unit tests with mock PD + mock server

### Phase 6: E2E Tests

**Files:** `e2e/client_lib_test.go`

**Tasks:**
- [ ] `newClientTestCluster` helper
- [ ] TestClientRegionCacheMiss
- [ ] TestClientRegionCacheHit
- [ ] TestClientRegionCacheInvalidationOnSplit
- [ ] TestClientStoreResolution
- [ ] TestClientStoreFailover
- [ ] TestClientBatchGetAcrossRegions
- [ ] TestClientBatchPutAcrossRegions
- [ ] TestClientScanAcrossRegions
- [ ] TestClientScanWithLimit
- [ ] TestClientCompareAndSwap
- [ ] TestClientRetriesOnNotLeader

### Phase 7: Documentation Update

**Tasks:**
- [ ] Update `impl_doc/08_not_yet_implemented.md` to reflect client library status
- [ ] Verify all e2e tests pass

## 4. Dependency Graph

```
Phase 1 (server fix) ─┐
                       ├─→ Phase 2 (StoreResolver) ─┐
                       │                              ├─→ Phase 4 (RequestSender) ─→ Phase 5 (RawKVClient) ─→ Phase 6 (E2E)
                       └─→ Phase 3 (RegionCache) ────┘                                                        │
                                                                                                               └─→ Phase 7 (Docs)
```

## 5. Estimated Effort

| Phase | Complexity | Estimated effort |
|-------|-----------|-----------------|
| 1. Server region error propagation | S | Small |
| 2. PDStoreResolver | S | Small |
| 3. RegionCache | M | Medium |
| 4. RegionRequestSender | M | Medium |
| 5. RawKVClient | L | Large |
| 6. E2E Tests | M | Medium |
| 7. Doc update | S | Small |

## 6. Key Design Decisions

1. **Binary search for region lookup**: O(log n) instead of the server-side O(n) linear scan. The sorted slice is maintained on insert/delete.

2. **Parallel batch dispatch**: `BatchGet`/`BatchPut` use `errgroup` to send per-region requests concurrently.

3. **Cross-region Scan**: Iterates region-by-region, sending one Scan RPC per region. This matches TiKV's approach.

4. **Connection pooling**: One `grpc.ClientConn` per store address, shared across all RPCs to that store.

5. **No TxnClient in this phase**: Transaction routing requires more complex 2PC coordination (per-region prewrite, global commit). Deferred to a future design.

6. **Lazy cache population**: Region cache starts empty and is populated on demand. No background refresh — rely on error-driven invalidation.
