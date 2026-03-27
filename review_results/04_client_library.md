# Code Review: Client Library (`pkg/client/`)

**Reviewer**: Claude Opus 4.6 (1M context)
**Date**: 2026-03-27
**Scope**: `pkg/client/` -- committer, txn, request_sender, region_cache, lock_resolver, rawkv, txnkv, client, store_resolver (9 files, ~2200 lines)

---

## Summary

The client library implements region-aware RPC routing, a 2PC committer, transactional and raw KV APIs, a region cache, and a lock resolver. The overall design is sound and follows TiKV client patterns well. However, there are several bugs and correctness issues, including two that could cause **data loss or data inconsistency** under region splits.

**Severity counts**: Critical: 3, High: 4, Medium: 6, Low: 4

---

## Critical Issues

### C1. Region cache does not evict overlapping stale regions after split

**File**: `pkg/client/region_cache.go`, `insertLocked()` (lines 132-154)

When a region splits (e.g., region 1 [a, z) splits into region 2 [a, m) and region 3 [m, z)), the cache learns about region 2 or 3 from PD but never removes the stale region 1 entry. `insertLocked` only replaces entries with the **same region ID**; it does not check for and evict entries whose key ranges overlap the new region.

This means subsequent lookups via `findInCache` may still find the stale region 1 (since its StartKey still matches), and RPCs will be routed with the old epoch. The server will reject with `EpochNotMatch`, but this wastes retries. Worse, if the timing is such that the stale region is used during a commit, the commit can be sent to the wrong store.

**Fix**: When inserting a new region, scan for and remove any existing regions whose key ranges overlap the new region's range.

### C2. `BatchGet` sends all keys to a single region (data correctness)

**File**: `pkg/client/txn.go`, `kvBatchGet()` (lines 198-231)

The `kvBatchGet` method routes **all keys** using only `keys[0]`:
```go
err := t.client.sender.SendToRegion(ctx, keys[0], func(...) {
    resp, err := client.KvBatchGet(ctx, &kvrpcpb.BatchGetRequest{
        ...
        Keys: keys,  // ALL keys, not just this region's keys
    })
```

If the keys span multiple regions, all keys are sent to the region that owns `keys[0]`. The server will either:
- Return `KeyNotInRegion` for some keys (but the code does not handle partial failures here), or
- Silently ignore keys outside its range, causing missing results.

The `BatchGet` caller at line 152 does **not** group keys by region before calling `kvBatchGet`, unlike `RawKVClient.BatchGet` which correctly groups by region. This is a data correctness bug -- reads will silently return incomplete results when keys span regions.

**Fix**: Group `remoteKeys` by region (using `GroupKeysByRegion`) and issue per-region `KvBatchGet` RPCs, similar to how `RawKVClient.BatchGet` works.

### C3. `commitSecondaries` silently swallows TxnLockNotFound, leaving orphan locks

**File**: `pkg/client/committer.go`, `commitSecondariesPerKey()` (lines 337-378)

In the per-key fallback path, after exhausting 5 `TxnLockNotFound` retries, the code does:
```go
if lockNotFoundRetries <= 5 {
    return &errorpb.Error{...}, nil  // retry
}
return nil, nil  // silently succeeds
```

Returning `nil, nil` makes the caller believe the commit succeeded, but the lock was never committed. This key now has an orphan lock that will block future readers until lock resolution cleans it up, or the data is lost.

In the batched path (lines 264-331), the fallback to `commitSecondariesPerKey` after a "batch TxnLockNotFound" error is correct, but if `commitSecondariesPerKey` itself fails, the error is only logged (line 373), not propagated.

Since the primary is already committed, there is no good recovery here -- but the system should at least log at ERROR level and consider retry with backoff rather than silently accepting the loss.

---

## High Severity Issues

### H1. `isPrimaryCommitted` swallows errors and returns false

**File**: `pkg/client/committer.go`, `isPrimaryCommitted()` (lines 213-234)

```go
func (c *twoPhaseCommitter) isPrimaryCommitted(ctx context.Context) bool {
    var committed bool
    _ = c.client.sender.SendToRegion(ctx, c.primary, func(...) {
```

If `SendToRegion` returns an error (network issue, retries exhausted), `committed` stays `false`, and the caller in `execute()` (line 88) will **roll back a transaction whose primary is actually committed**. This is a data consistency violation -- the primary is committed, but the client rolls back, leaving an inconsistent state where the primary's write is visible but all other state is rolled back.

**Fix**: Return an error from `isPrimaryCommitted` and handle the ambiguous case (e.g., retry the check, or refuse to roll back if the check itself failed).

### H2. `primaryKey()` is non-deterministic for pessimistic transactions

**File**: `pkg/client/txn.go`, `primaryKey()` (lines 316-322)

```go
func (t *TxnHandle) primaryKey() []byte {
    for k := range t.membuf {
        return []byte(k)  // Go map iteration order is random
    }
    return nil
}
```

This function iterates a Go map, which has random iteration order. Each call to `acquirePessimisticLock` (via `Set`/`Delete` in pessimistic mode) can pick a **different** primary key. All pessimistic locks should reference the same primary, but this function may return different keys on different calls.

Meanwhile, `newTwoPhaseCommitter` (used at commit time) uses `selectPrimary()` which deterministically picks the lexicographically smallest key. So the primary used during pessimistic locking may differ from the primary used during prewrite/commit. This will cause `CheckTxnStatus` to look at the wrong key.

**Fix**: Cache the primary key on first call, or always use the deterministic sort-based selection.

### H3. `rawkv.DeleteRange` does not span regions

**File**: `pkg/client/rawkv.go`, `DeleteRange()` (lines 341-361)

`DeleteRange` sends a single `RawDeleteRange` RPC routed by `startKey`, but if the range `[startKey, endKey)` spans multiple regions, only the first region's portion will be deleted. Unlike `Scan` which correctly iterates across region boundaries, `DeleteRange` does not.

**Fix**: Iterate across regions similar to `Scan`, sending per-region `RawDeleteRange` requests.

### H4. `rawkv.Checksum` does not span regions

**File**: `pkg/client/rawkv.go`, `Checksum()` (lines 394-419)

Same issue as H3. The checksum is only computed for the first region that `startKey` falls into, not the full `[startKey, endKey)` range.

---

## Medium Severity Issues

### M1. `prewriteRegion` resolves locks but returns `ErrWriteConflict` regardless

**File**: `pkg/client/committer.go`, `prewriteRegion()` (lines 193-201)

When a prewrite encounters a lock, the code resolves the lock, then returns `ErrWriteConflict`:
```go
if keyErr.GetLocked() != nil {
    if lockInfo := keyErr.GetLocked(); lockInfo.GetLockVersion() > 0 {
        _ = c.client.resolver.ResolveLocks(ctx, []*kvrpcpb.LockInfo{lockInfo})
    }
    return nil, ErrWriteConflict
}
```

After resolving the lock, the prewrite should **retry** rather than failing with `ErrWriteConflict`. The lock may have been from a crashed transaction, not a real conflict. This causes unnecessary transaction failures.

### M2. `lockNotFoundRetries` counter is captured by closure, survives across `SendToRegion` retries

**File**: `pkg/client/committer.go`, lines 294 and 344

`lockNotFoundRetries` is declared outside the `SendToRegion` callback. `SendToRegion` itself retries on region errors (up to `maxRetries`). The counter accumulates across both internal retries (region error retries within `SendToRegion`) and explicit lock-not-found retries. If there are 3 region-error retries each triggering 2 lock-not-found errors, the counter hits 6 and the fallback triggers even though no single region endpoint was tried more than twice.

This is not critical but can cause premature fallback to per-key commit.

### M3. `isRetryableRegionError` function is dead code

**File**: `pkg/client/request_sender.go`, lines 132-142

The function `isRetryableRegionError` is defined but never called anywhere in the codebase. The retry logic in `handleRegionError` duplicates this check. Dead code should be removed.

### M4. `LockResolver.resolving` map grows unboundedly

**File**: `pkg/client/lock_resolver.go`, lines 22-23, 69

The `resolving` map entries are cleaned up in the defer (line 72-77), so there is no true leak. However, under high contention with many concurrent lock resolutions, the map's underlying hash table can grow large and never shrink (Go maps do not shrink). For long-lived clients this may waste memory.

### M5. `Scan` uses `SendToRegion` which may route to wrong region after retry

**File**: `pkg/client/rawkv.go`, `Scan()` (lines 274-338)

The scan loop computes `scanEnd` based on the region info from `LocateKey` (line 279-286), but then `SendToRegion` may retry the RPC if a region error occurs. After cache invalidation and retry, the region may have changed (e.g., after a split), but `scanEnd` is not recomputed. This could cause the scan to use a stale region boundary as the end key, potentially requesting keys beyond the new region's range.

### M6. No context deadline on background rollback/commit-secondary operations

**File**: `pkg/client/committer.go`, lines 70, 78, 92, 95, 100

`context.Background()` is used for rollback and `commitSecondaries`. These operations have no deadline, so if a store is unreachable, they will retry up to `maxRetries` with no timeout, blocking the caller indefinitely (modulo individual RPC timeouts, which are not set on the context).

---

## Low Severity Issues

### L1. No `Close()` method on `RegionRequestSender` connection cache for graceful shutdown logging

Connections are closed in `Close()` but connection errors during close are not logged. If a connection close fails, it is silently ignored.

### L2. `time.Sleep(200ms)` in `Get` lock resolution loop is a fixed sleep

**File**: `pkg/client/txn.go`, line 99

A fixed 200ms sleep between lock resolution retries is used. Exponential backoff would be more appropriate to avoid thundering herd on popular locked keys.

### L3. `time.Sleep(100ms)` in `SendToRegion` retry loop

**File**: `pkg/client/request_sender.go`, line 92

Similarly, a fixed 100ms backoff between retries in `SendToRegion` could benefit from exponential backoff with jitter.

### L4. `BatchGet` in `TxnHandle` only retries 3 times vs `Get`'s 20 times

**File**: `pkg/client/txn.go`, lines 83 vs 181

`Get` retries lock resolution up to 20 times; `BatchGet` only retries 3 times. The inconsistency may cause `BatchGet` to fail unnecessarily under lock contention where `Get` would have succeeded.

---

## Design Observations (Non-Issues)

1. **Connection pooling**: The `getOrDial` pattern with double-checked locking is correct and idiomatic.

2. **Region cache binary search**: The `findInCache` binary search implementation is correct.

3. **gRPC connection reuse**: The decision not to close connections on region errors (line 74 comment) is the right call -- gRPC handles reconnection internally.

4. **1PC optimization guard**: The check at `commitPrimary` line 239 correctly skips commit if 1PC already succeeded.

5. **Lock resolver dedup**: The channel-based dedup pattern in `resolveSingleLock` is a clean implementation.

6. **Test coverage**: The existing tests cover membuf operations, primary key selection, state machine guards, mutation grouping, region cache, and store resolver. Good unit test foundation. Missing: integration tests for multi-region scenarios, lock resolution, and 2PC error paths.

---

## Recommended Priority

1. **C2** (BatchGet single-region routing) -- most likely to cause user-visible incorrect results
2. **C1** (stale region cache after split) -- causes unnecessary retries and potential misdirected RPCs
3. **H2** (non-deterministic primary key) -- causes correctness issues in pessimistic mode
4. **H1** (isPrimaryCommitted error handling) -- can cause committed transactions to be rolled back
5. **C3** (silent TxnLockNotFound swallowing) -- orphan locks degrade availability
6. **H3/H4** (DeleteRange/Checksum single-region) -- silent data loss for cross-region operations
7. **M1** (prewrite lock resolution no retry) -- unnecessary transaction failures
