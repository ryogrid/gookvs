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

## Phase 6: Final Verification
- [x] TODO.md audit — all items checked
- [x] Stale comment scan — 1 unrelated TODO in coordinator.go (flow control)
- [x] Full test suite passes (internal, pkg, e2e all green: 22.9s)
- [x] Update impl_doc/08_not_yet_implemented.md
