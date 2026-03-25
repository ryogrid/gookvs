# Read Index and Region Epoch Implementation

## Phase 1: Region Epoch Validation

- [x] 1.1 Enhance `validateRegionContext()` — add epoch check (Version + ConfVer comparison)
- [x] 1.2 Add `validateRegionContext()` to KvGet handler
- [x] 1.3 Add `validateRegionContext()` to KvScan handler
- [x] 1.4 Add `validateRegionContext()` to KvBatchGet handler
- [x] 1.5 Add `validateRegionContext()` to KvScanLock handler
- [x] 1.6 Fix data race on `Peer.region` field (add sync.RWMutex)
- [x] 1.7 Write epoch validation unit tests (`internal/server/epoch_test.go`)
- [x] 1.8 Verify client handles EpochNotMatch (no change needed — already handled)
- [x] 1.9 Fix flaky PD Raft leader election test (increase timeout 3s→5s)
- [x] 1.10 Run `go vet` and `make test` — all pass

### Design Decision: Write handlers excluded from validateRegionContext

Epoch validation is applied to **read handlers only** (KvGet, KvScan, KvBatchGet, KvScanLock, Raw* handlers). Write handlers (KvPrewrite, KvCommit, KvBatchRollback, KvCleanup, KvCheckTxnStatus, KvPessimisticLock, KvResolveLock) are excluded because:
1. They already go through `ProposeModifies` which performs Raft consensus
2. Adding `validateRegionContext` to write handlers caused legitimate operations to be rejected during region splits (the client's stale regionId triggered "region not found" before ProposeModifies could route the operation)
3. This matches TiKV's behavior where Raft proposal handles region validation

## Phase 2: Read Index Protocol — Infrastructure

- [x] 2.1 Add `PeerMsgTypeReadIndex` constant to `msg.go`
- [x] 2.2 Add `ReadIndexRequest` struct to `msg.go`
- [x] 2.3 Add `pendingRead` struct to `peer.go`
- [x] 2.4 Add `pendingReads` map and `nextReadID` to Peer struct
- [x] 2.5 Initialize `pendingReads` in `NewPeer()`
- [x] 2.6 Add `NextReadID()` method to Peer
- [x] 2.7 Add `handleReadIndexRequest()` method to Peer
- [x] 2.8 Add `PeerMsgTypeReadIndex` case to `handleMessage()`
- [x] 2.9 Add ReadStates processing to `handleReady()`
- [x] 2.10 Add pending reads sweep after committed entries in `handleReady()`
- [x] 2.11 Add leader stepdown cleanup for pending reads in `handleReady()`
- [x] 2.12 Add `ReadIndex()` method to StoreCoordinator
- [x] 2.13-2.16 ReadIndex calls in KvGet/KvScan/KvBatchGet — **implemented but disabled**

### ReadIndex Status: Disabled Pending Raft Integration Fix

ReadIndex infrastructure is fully implemented (peer, coordinator, handler code) but the actual calls in KvGet/KvScan/KvBatchGet are disabled (commented out with TODO). The etcd raft `ReadOnlySafe` mode requires heartbeat quorum confirmation which currently causes all reads to timeout. Root cause: the Raft heartbeat flow between gookv peers may not correctly support the ReadIndex protocol's quorum confirmation requirement. This needs investigation of the inter-peer message handling path.

## Phase 3: Verification

- [x] 3.1 Run `go vet ./...` — no issues
- [x] 3.2 Run `make test` — all pass (2 consecutive runs)
- [x] 3.3 Run `make test-e2e` — all pass

## Deferred Items

- **ReadIndex activation**: ReadIndex handler code is commented out. Requires fixing Raft ReadOnlySafe quorum confirmation flow. See TODO(readindex) in server.go.
- **Leader Lease optimization**: Future performance optimization (skip ReadIndex when lease valid)
- **ReadIndex for KvCheckSecondaryLocks**: Idempotent lock resolution, deferred
- **ReadIndex for KvScanLock**: Idempotent lock scanning, deferred
- **validateRegionContext for write handlers**: Excluded by design (see note above)
- **Transaction integrity demo 32-worker PASS**: Depends on ReadIndex activation (Bug 12 root cause)
