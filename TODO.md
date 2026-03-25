# Read Index and Region Epoch Implementation

## Phase 1: Region Epoch Validation

- [ ] 1.1 Enhance `validateRegionContext()` — add epoch check (Version + ConfVer comparison)
- [ ] 1.2 Add `validateRegionContext()` to KvGet handler
- [ ] 1.3 Add `validateRegionContext()` to KvScan handler
- [ ] 1.4 Add `validateRegionContext()` to KvBatchGet handler
- [ ] 1.5 Add `validateRegionContext()` to KvPrewrite handler
- [ ] 1.6 Add `validateRegionContext()` to KvCommit handler
- [ ] 1.7 Add `validateRegionContext()` to KvBatchRollback handler
- [ ] 1.8 Add `validateRegionContext()` to KvCleanup handler
- [ ] 1.9 Add `validateRegionContext()` to KvCheckTxnStatus handler
- [ ] 1.10 Add `validateRegionContext()` to KvPessimisticLock handler
- [ ] 1.11 Add `validateRegionContext()` to KvResolveLock handler
- [ ] 1.12 Add `validateRegionContext()` to KvScanLock handler
- [ ] 1.13 Fix data race on `Peer.region` field (add mutex or atomic.Value)
- [ ] 1.14 Write epoch validation unit tests (`internal/server/epoch_test.go`)
- [ ] 1.15 Verify client handles EpochNotMatch (no change needed)
- [ ] 1.16 Run `go vet` and `make test`

## Phase 2: Read Index Protocol

- [ ] 2.1 Add `PeerMsgTypeReadIndex` constant to `msg.go`
- [ ] 2.2 Add `ReadIndexRequest` struct to `msg.go`
- [ ] 2.3 Add `pendingRead` struct to `peer.go`
- [ ] 2.4 Add `pendingReads` map and `nextReadID` to Peer struct
- [ ] 2.5 Initialize `pendingReads` in `NewPeer()`
- [ ] 2.6 Add `NextReadID()` method to Peer
- [ ] 2.7 Add `handleReadIndexRequest()` method to Peer
- [ ] 2.8 Add `PeerMsgTypeReadIndex` case to `handleMessage()`
- [ ] 2.9 Add ReadStates processing to `handleReady()`
- [ ] 2.10 Add pending reads sweep after committed entries in `handleReady()`
- [ ] 2.11 Add leader stepdown cleanup for pending reads in `handleReady()`
- [ ] 2.12 Add `ReadIndex()` method to StoreCoordinator
- [ ] 2.13 Add ReadIndex call to KvGet handler (before storage read)
- [ ] 2.14 Add ReadIndex call to KvScan handler
- [ ] 2.15 Add ReadIndex call to KvBatchGet handler
- [ ] 2.16 Gate all ReadIndex calls on `coordinator != nil` (standalone bypass)
- [ ] 2.17 Write ReadIndex peer unit tests (`internal/raftstore/readindex_test.go`)
- [ ] 2.18 Write coordinator ReadIndex unit tests (`internal/server/coordinator_readindex_test.go`)
- [ ] 2.19 Run `go vet` and `make test`

## Phase 3: E2E and Integration Tests

- [ ] 3.1 Write bank transfer conservation E2E test (`e2e/bank_transfer_test.go`)
- [ ] 3.2 Run `make test-e2e`
- [ ] 3.3 Run transaction integrity demo 3 times — all PASS

## Phase 4: Final Verification

- [ ] 4.1 Run `go vet ./...` — no issues
- [ ] 4.2 Run `make test` 3 times — all pass, no flaky failures
- [ ] 4.3 Run `make test-e2e` — all pass
- [ ] 4.4 Verify TODO.md completeness — all items checked
- [ ] 4.5 Search codebase for TODO comments — none unresolved
- [ ] 4.6 Report deferred items

## Deferred Items (Out of Scope)

- Leader Lease optimization (future performance optimization)
- ReadIndex for KvCheckSecondaryLocks (idempotent lock resolution)
- ReadIndex for KvScanLock (idempotent lock scanning)
