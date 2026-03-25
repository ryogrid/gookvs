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
- Note: Leader Lease disabled — does not guarantee appliedIndex >= commitIndex.
  ReadOnlySafe provides both leadership and apply guarantees.

## Phase 3.5: ReadIndex Bug Fixes (DONE)

- [x] 3.5.1 Fix AppliedIndex never updated (root cause of ReadOnlySafe timeout)
- [x] 3.5.2 Propose no-op before ReadIndex for committedEntryInCurrentTerm gate
- [x] 3.5.3 ErrMailboxFull retry in HandleRaftMessage
- [x] 3.5.4 Batch mailbox drain in Peer Run loop

## Phase 3.6: Bug 12 — Commit to Wrong Node After Split (DONE)

- [x] 3.6.1 Rewrite commitSecondaries: per-key SendToRegion with retry
- [x] 3.6.2 TxnLockNotFound accepted as success (lock already resolved)
- [x] 3.6.3 KvCommit validates key range via validateRegionContext
- [x] 3.6.4 Fix validateRegionContext key encoding (codec.EncodeBytes)

## Phase 4: Verification

- [x] 4.1 go vet clean
- [x] 4.2 make test — 3 consecutive passes
- [x] 4.3 make test-e2e — all pass
- [ ] 4.4 Transaction integrity demo — 3 consecutive PASSes (32 workers)
      Bug 12 fix reduced deviation from $50-$200 to $8-$95.
      Remaining issue: cross-region prewrite interleaving or Raft apply
      timing allows concurrent transactions to both succeed without
      detecting conflicts. See 03_current_issues.md.
- [x] 4.5 No TODO(readindex) comments remaining
- [x] 4.6 TODO.md fully checked
