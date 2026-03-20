# TODO: Remaining Unimplemented Items + Multi-Region E2E Tests

## Phase 0: Setup
- [x] Create feature branch from master
- [x] Create design documents under design_docs/additional2/
- [x] Review design documents via subagent

## Phase 1: Test Infrastructure + CLI (Items 5, 6, 11, 12)
- [x] Item 11: Engine traits conformance tests (`internal/engine/traits/conformance_test.go`)
- [x] Item 12: Codec fuzz tests (`pkg/codec/fuzz_test.go`)
- [x] Item 5: Update 08_not_yet_implemented.md (compact already done)
- [x] Item 6: CLI dump SST file parsing (`cmd/gookv-ctl/main.go`)

## Phase 2: Transaction gRPC + Raw KV (Items 1, 2, 3, 7)
- [x] Item 1: Async Commit / 1PC gRPC path integration
- [x] Item 2: KvCheckSecondaryLocks endpoint
- [x] Item 3: KvScanLock endpoint
- [x] Item 7a: RawBatchScan
- [x] Item 7b: RawGetKeyTTL + TTL encoding in RawStorage
- [x] Item 7c: RawCompareAndSwap (TTL-aware)
- [x] Item 7d: RawChecksum
- [x] E2E tests for Items 1-3 (async_commit_test.go)
- [x] E2E tests for Item 7 (raw_kv_extended_test.go)

## Phase 3: PD Resilience (Items 8, 9)
- [x] Item 9: PD leader failover / retry in pdclient
- [x] Item 8: TSO integration in server
- [x] E2E tests for PD failover and TSO

## Phase 4: PD-Coordinated Split (Item 10)
- [x] Item 10: Wire SplitCheckWorker into peer event loop + coordinator
- [x] E2E tests for automatic PD-coordinated split

## Phase 5: Multi-Region Routing + E2E Tests
- [x] Multi-region cluster test helpers
- [x] TestMultiRegionKeyRouting
- [x] TestMultiRegionIndependentLeaders
- [x] TestMultiRegionRawKV
- [x] Fix server.go ProposeModifies(1,...) hardcoded region routing (8 places)
- [x] Add groupModifiesByRegion helper for batch operations
- [x] Design doc: design_docs/additional2/07_multi_region_routing.md
- [x] TestMultiRegionTransactions (2PC spanning 2 regions)
- [x] TestMultiRegionRawKVBatchScan (BatchScan across region boundaries)
- [x] TestMultiRegionSplitWithLiveTraffic (writes during split)
- [x] TestMultiRegionPDCoordinatedSplit (automatic split via PD, single-node)
- [x] TestMultiRegionAsyncCommit (async commit across regions)
- [x] TestMultiRegionScanLock (ScanLock respects boundaries)

## Phase 6: Final Verification (prev items)
- [x] TODO.md audit — all items checked
- [x] Stale comment scan — 1 unrelated TODO in coordinator.go (flow control)
- [x] Full test suite passes (internal, pkg, e2e all green: 22.9s)
- [x] Update impl_doc/08_not_yet_implemented.md

---

## Phase 7: Client Library for Multi-Region Routing (`pkg/client/`)

Design docs: `design_docs/multi_client_lib/`

### 7.1 Server-Side Region Error Propagation (prerequisite)
- [x] Add `validateRegionContext()` helper in server.go
- [x] Modify Raw KV handlers to return `region_error` in response (not gRPC error) on routing failures
- [x] All 8 read-only handlers updated: RawGet, RawScan, RawBatchGet, RawBatchScan, RawGetKeyTTL, RawCompareAndSwap, RawChecksum, RawDeleteRange

### 7.2 PDStoreResolver (`pkg/client/store_resolver.go`)
- [x] Implement `PDStoreResolver` — dynamic storeID→address via PD `GetStore`
- [x] TTL-based caching (default 30s) with `nowFunc` for testability
- [x] `InvalidateStore(storeID)` method
- [x] Unit tests (5 tests: cache miss, hit, expiry, invalidation, not found)

### 7.3 RegionCache (`pkg/client/region_cache.go`)
- [x] Implement `RegionInfo` struct (Region + Leader + StoreAddr)
- [x] Implement `RegionCache` — sorted slice + binary search for O(log n) lookup
- [x] `LocateKey(ctx, key)` — cache lookup with PD fallback
- [x] `InvalidateRegion(regionID)` — eviction on EpochNotMatch/RegionNotFound
- [x] `UpdateLeader(regionID, leader, storeAddr)` — in-place leader update on NotLeader
- [x] `GroupKeysByRegion(ctx, keys)` — batch key grouping
- [x] Unit tests with mock PD (6 tests: cold/warm cache, multi-region, invalidation, leader update, key grouping)

### 7.4 RegionRequestSender (`pkg/client/request_sender.go`)
- [x] Implement `RegionRequestSender` with gRPC connection pool
- [x] `SendToRegion(ctx, key, rpcFn)` — locate, send, retry on region errors
- [x] Error-driven cache invalidation (NotLeader, EpochNotMatch, StoreNotMatch, KeyNotInRegion, RegionNotFound)
- [x] Configurable max retries (default 3)
- [x] `isRetryableRegionError()` helper

### 7.5 RawKVClient (`pkg/client/rawkv.go`)
- [x] Implement `Client` factory (PD connection, cache/resolver init) in `client.go`
- [x] Single-key: Get, Put, PutWithTTL, Delete, GetKeyTTL
- [x] Batch: BatchGet, BatchPut, BatchDelete (parallel per-region dispatch via errgroup)
- [x] Range: Scan (cross-region continuation), DeleteRange
- [x] Advanced: CompareAndSwap, Checksum
- [x] `Client.Close()` — cleanup all resources

### 7.6 E2E Tests (`e2e/client_lib_test.go`)
- [x] `newClientTestCluster` helper
- [x] TestClientRegionCacheMiss
- [x] TestClientRegionCacheHit
- [x] TestClientStoreResolution
- [x] TestClientBatchGetAcrossRegions
- [x] TestClientBatchPutAcrossRegions
- [x] TestClientScanAcrossRegions
- [x] TestClientScanWithLimit
- [x] TestClientCompareAndSwap
- [x] TestClientBatchDeleteAcrossRegions

### 7.7 Final Verification
- [x] TODO comment audit — only 1 pre-existing unrelated TODO in coordinator.go (flow control)
- [x] All unit tests pass (11 in pkg/client, all internal/pkg tests green)
- [x] All e2e tests pass (9 client lib tests green)
