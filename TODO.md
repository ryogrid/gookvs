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
- [ ] E2E tests for Items 1-3 (async_commit_test.go)
- [ ] E2E tests for Item 7 (raw_kv_extended_test.go)

## Phase 3: PD Resilience (Items 8, 9)
- [ ] Item 9: PD leader failover / retry in pdclient
- [ ] Item 8: TSO integration in server
- [ ] E2E tests for PD failover and TSO

## Phase 4: PD-Coordinated Split (Item 10)
- [ ] Item 10: Wire SplitCheckWorker into peer event loop + coordinator
- [ ] E2E tests for automatic PD-coordinated split

## Phase 5: Multi-Region E2E Tests
- [ ] Multi-region cluster test helpers
- [ ] TestMultiRegionKeyRouting
- [ ] TestMultiRegionTransactions
- [ ] TestMultiRegionRawKVBatchScan
- [ ] TestMultiRegionSplitWithLiveTraffic
- [ ] TestMultiRegionPDCoordinatedSplit
- [ ] TestMultiRegionAsyncCommit
- [ ] TestMultiRegionScanLock

## Phase 6: Final Verification
- [ ] TODO.md audit — all items checked
- [ ] Stale comment scan (TODO/FIXME/HACK/XXX/UNIMPLEMENTED)
- [ ] Full test suite passes
- [ ] Update impl_doc/08_not_yet_implemented.md
