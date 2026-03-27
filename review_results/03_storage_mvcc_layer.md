# Storage/MVCC Layer Code Review

**Reviewer**: Claude Opus 4.6 (1M context)
**Date**: 2026-03-27
**Scope**: Storage facade, raw storage, MVCC reader/scanner/txn/point_getter/key, transaction actions, async commit, pessimistic locking, latch manager, scheduler, concurrency manager, GC

---

## Critical Issues

### BUG-01: Scanner backward scan bound conditions are swapped (scanner.go:63-74)

**Severity**: High
**File**: `internal/storage/mvcc/scanner.go`, lines 63-74

The condition guards for backward scan iterator bounds check the wrong config field:

```go
} else {
    // Backward scan: swap bounds.
    if cfg.UpperBound != nil {                                    // BUG: should be cfg.LowerBound
        writeOpts.LowerBound = EncodeKey(cfg.LowerBound, txntypes.TSMax)
        lockOpts.LowerBound = EncodeLockKey(cfg.LowerBound)
    }
    if cfg.LowerBound != nil {                                    // BUG: should be cfg.UpperBound
        writeOpts.UpperBound = EncodeKey(cfg.UpperBound, 0)
        lockOpts.UpperBound = EncodeLockKey(cfg.UpperBound)
    }
}
```

When only one bound is set (common case), this will:
1. Skip setting the needed bound entirely (the condition for it is false)
2. Set the other bound using a nil key (the condition for the nil field is true)

**Fix**: Swap the conditions:
- `if cfg.LowerBound != nil` for setting `LowerBound`
- `if cfg.UpperBound != nil` for setting `UpperBound`

---

### BUG-02: PointGetter does not skip pessimistic locks (point_getter.go:74-86)

**Severity**: High
**File**: `internal/storage/mvcc/point_getter.go`, lines 74-86

The PointGetter checks for locks in SI mode but does not skip pessimistic locks. Pessimistic locks are documented as "invisible to readers" (pessimistic.go line 9), and the Scanner correctly skips them (scanner.go:265), but PointGetter does not:

```go
if lock != nil && lock.StartTS <= pg.ts {
    if !pg.bypassLocks[lock.StartTS] {
        return nil, &LockError{Key: key, Lock: lock}   // Returns error even for pessimistic locks
    }
}
```

A pessimistic lock (LockTypePessimistic) should not block readers because no data has been written yet (no prewrite). This causes spurious `LockError` returns for keys that have only pessimistic locks, blocking reads unnecessarily.

**Fix**: Add `lock.LockType != txntypes.LockTypePessimistic` to the condition, matching the Scanner's behavior.

---

### BUG-03: Async commit prewrite conflict check only examines one write record (async_commit.go:44-52)

**Severity**: High
**File**: `internal/storage/txn/async_commit.go`, lines 44-52

The regular `Prewrite` (actions.go:88-111) iterates through up to `SeekBound*2` write records to skip Rollback/Lock records and find actual data-changing writes. In contrast, `PrewriteAsyncCommit` only checks the single most recent write record:

```go
write, commitTS, err := reader.SeekWrite(key, txntypes.TSMax)
if write != nil && commitTS > props.StartTS {
    if write.WriteType != txntypes.WriteTypeRollback && write.WriteType != txntypes.WriteTypeLock {
        return ErrWriteConflict
    }
}
```

If the most recent write is a Rollback but an older write (still with `commitTS > startTS`) is a Put, the conflict is missed. This can allow a prewrite to succeed when it should be rejected, leading to lost updates.

The same single-check issue exists in `PrewriteAndCommit1PC` (async_commit.go:158-168) and `AcquirePessimisticLock` (pessimistic.go:58-66).

---

### BUG-04: Latch spin-wait without backoff or blocking (storage.go, multiple locations)

**Severity**: Medium
**File**: `internal/server/storage.go`, all latch acquisition sites (lines 185-186, 227-228, 257-258, etc.)

Every latch acquisition in Storage uses a tight spin loop:
```go
for !s.latches.Acquire(lock, cmdID) {
}
```

The `Latches.Acquire` returns false when a slot is owned by another command, but there is no blocking mechanism to wait for the slot to become free. The `latchSlot.wakeCh` channel exists in the data structure (latch.go:23) but is never used. The `Release` method returns wake-up command IDs but callers in `storage.go` ignore the return value (`defer s.latches.Release(lock, cmdID)` discards it).

This means:
1. CPU burns spinning while waiting for latches
2. No fairness guarantee -- a long-running command can starve waiters
3. The wait queue in latchSlot is populated but waiters are never notified through the channel

The TxnScheduler correctly handles wake-ups (scheduler.go:259-262), but Storage's standalone-mode methods do not.

---

## Moderate Issues

### ISSUE-05: CleanupModifies silently drops lock on primary status check failure (storage.go:481-483)

**Severity**: Medium
**File**: `internal/server/storage.go`, lines 481-483

When `CheckTxnStatus` for the primary key fails, `CleanupModifies` removes the lock without writing a rollback record:

```go
if err != nil {
    // Primary check failed -- force-remove the lock.
    mvccTxn.UnlockKey(key, keyLock.LockType == txntypes.LockTypePessimistic)
    return 0, mvccTxn.Modifies, nil, guard
}
```

This is dangerous: removing a lock without a rollback record means a late-arriving commit for this transaction could succeed (no rollback record to prevent it). Compare with the standalone `Cleanup` (storage.go:422-425), which correctly writes a rollback via `txn.Rollback`.

---

### ISSUE-06: GC does not hold latches, risking concurrent modification (gc.go:224-282)

**Severity**: Medium
**File**: `internal/storage/gc/gc.go`, lines 224-282

`GCWorker.processTask` reads a snapshot and writes modifications without acquiring latches on the keys being GC'd. If a concurrent transaction is committing or rolling back on the same key, the GC batch write could race with the transaction's batch write. While the GC state machine is designed to only delete versions below the safe point, lack of latch coordination could lead to:
1. GC reading a snapshot that doesn't yet reflect a concurrent commit, then deleting a version that the concurrent transaction references
2. Interleaved write batches producing inconsistent state

TiKV runs GC through the Raft layer (serialized per region), which provides this guarantee. In standalone mode, latches should be acquired per-key or per-batch.

---

### ISSUE-07: GC state machine does not remove the Delete marker in gcStateRemoveIdempotent (gc.go:100-104)

**Severity**: Low-Medium
**File**: `internal/storage/gc/gc.go`, lines 100-104

When GC encounters a `WriteTypeDelete` in `gcStateRemoveIdempotent`, it keeps the Delete marker:

```go
case txntypes.WriteTypeDelete:
    // Keep the Delete marker, but transition to remove all older versions.
    state = gcStateRemoveAll
    ts = commitTS - 1
    continue
```

In TiKV, a Delete marker below the safe point with no older Put versions is eligible for removal (since the key is logically non-existent). Keeping it permanently wastes space for deleted keys.

---

### ISSUE-08: Scan with limit==0 becomes unbounded scan (storage.go:125)

**Severity**: Low
**File**: `internal/server/storage.go`, line 125

```go
for limit == 0 || uint32(len(results)) < limit {
```

When `limit == 0`, the loop has no bound and will scan the entire range. If the intent is "no limit", this is correct but should be documented. If callers expect 0 to mean "return nothing", this is a bug. The same pattern appears in `RawStorage.Scan` (raw_storage.go:210).

---

## Design Observations

### OBS-01: ConcurrencyManager is instantiated but unused in Storage

`Storage.concMgr` is created in `NewStorage` (storage.go:33) but never referenced by any Storage method. The `UpdateMaxTS` / `LockKey` / `IsKeyLocked` APIs are never called from the storage layer. This means async commit's `MinCommitTS` calculation in `PrewriteAsyncCommit` uses only `MaxCommitTS` passed by the caller rather than the concurrency manager's tracked `max_ts`. If no caller ever calls `UpdateMaxTS`, the async commit safety invariant (min_commit_ts > max read timestamp) is not enforced.

### OBS-02: bytesEqual reimplements bytes.Equal (raw_storage.go:416-426)

`bytesEqual` in `raw_storage.go` is a manual reimplementation of `bytes.Equal` from the standard library. The standard library version is optimized with compiler intrinsics. Should use `bytes.Equal` instead.

### OBS-03: Scanner forward scan upper bound uses TSMax (scanner.go:60)

For forward scan, `writeOpts.UpperBound = EncodeKey(cfg.UpperBound, txntypes.TSMax)`. Since TSMax encodes to all-zero bytes (descending encoding), this produces the smallest possible encoded key for `cfg.UpperBound`, which correctly makes the upper bound exclusive. This is correct.

### OBS-04: MvccReader.SeekWrite creates iterator per call (reader.go:45-46)

Each call to `SeekWrite` creates and destroys a new iterator. In hot paths like `Prewrite` conflict checking (which calls `SeekWrite` up to `SeekBound*2` times in a loop), this is wasteful. Consider caching the iterator in the reader.

### OBS-05: Cleanup vs CleanupModifies have divergent logic

`Cleanup` (storage.go:380-444) and `CleanupModifies` (storage.go:449-507) implement the same operation but with different logic flows and different error handling. `Cleanup` first calls `CheckTxnStatus` before loading the lock; `CleanupModifies` loads the lock first. This divergence makes maintenance error-prone.

---

## Summary

| ID | Severity | Category | File | Description |
|---|---|---|---|---|
| BUG-01 | High | Data consistency | scanner.go | Backward scan bound conditions swapped |
| BUG-02 | High | Data consistency | point_getter.go | Pessimistic locks not skipped by PointGetter |
| BUG-03 | High | Data consistency | async_commit.go | Conflict check only examines one write record |
| BUG-04 | Medium | Performance/Liveness | storage.go | Spin-wait latch acquisition without backoff |
| ISSUE-05 | Medium | Data consistency | storage.go | Lock removed without rollback record on error |
| ISSUE-06 | Medium | Data consistency | gc.go | GC runs without latches |
| ISSUE-07 | Low-Medium | Space leak | gc.go | Delete markers never removed by GC |
| ISSUE-08 | Low | API semantics | storage.go | limit==0 means unbounded scan |
| OBS-01 | Info | Completeness | storage.go | ConcurrencyManager unused |
| OBS-02 | Info | Code quality | raw_storage.go | Manual bytesEqual vs stdlib bytes.Equal |
| OBS-04 | Info | Performance | reader.go | Iterator created per SeekWrite call |
| OBS-05 | Info | Maintainability | storage.go | Cleanup/CleanupModifies logic divergence |

**High-severity bugs (BUG-01 through BUG-03)** should be fixed before any correctness testing, as they can cause silent data loss or snapshot isolation violations.
