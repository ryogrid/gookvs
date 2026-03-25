# Read Index and Region Epoch — Complete Implementation

## Phase 1: Region Epoch Validation (DONE)

- [x] 1.1-1.10 All epoch validation items complete

## Phase 2: Read Index Activation (DONE)

- [x] 2.1 Fix sendRaftMessage loopback for same-store peers
- [x] 2.2 Enable ReadIndex in KvGet (uncomment)
- [x] 2.3 Enable ReadIndex in KvScan (uncomment)
- [x] 2.4 Enable ReadIndex in KvBatchGet (uncomment)
- [x] 2.5 Add ReadIndex to KvCheckSecondaryLocks
- [x] 2.6 Add ReadIndex to KvScanLock
- [x] 2.7 Re-add validateRegionContext to 7 write handlers

## Phase 3: Leader Lease Optimization (DONE)

- [x] 3.1 Add leaseExpiry and leaseValid fields to Peer
- [x] 3.2 Add IsLeaseValid() method
- [x] 3.3 Extend lease in handleReady on leadership confirmation
- [x] 3.4 Check lease in coordinator.ReadIndex() before Raft round-trip

## Phase 4: Verification (PARTIAL)

- [x] 4.1 go vet clean
- [x] 4.2 make test — 3 consecutive passes
- [x] 4.3 make test-e2e — all pass
- [ ] 4.4 Transaction integrity demo — 3 consecutive PASSes (32 workers)
      **BLOCKED**: ReadIndex now works (AppliedIndex fix resolved timeout).
      Remaining blocker: Bug 12 (commit to wrong node after split) causes
      $50-$100 balance divergence. See 03_current_issues.md for details.
- [x] 4.5 No TODO(readindex) comments remaining
- [x] 4.6 TODO.md fully checked
