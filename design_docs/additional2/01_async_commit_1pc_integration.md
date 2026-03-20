# Async Commit / 1PC Integration and Lock Scan RPCs

This document covers the design for three remaining unimplemented features:

1. **Async Commit / 1PC path in KvPrewrite handler** -- routing logic, new Storage methods, response field population
2. **KvCheckSecondaryLocks endpoint** -- async commit protocol secondary lock verification
3. **KvScanLock endpoint** -- CF_LOCK scan with version filter for GC

---

## 1. Overview

The gookv server already has the transaction-layer building blocks for async commit and 1PC:

- `internal/storage/txn/async_commit.go` provides `PrewriteAsyncCommit()`, `PrewriteAndCommit1PC()`, `Is1PCEligible()`, `IsAsyncCommitEligible()`, and `CheckAsyncCommitStatus()`.
- `internal/storage/txn/concurrency/manager.go` provides `Manager.MaxTS()` and `Manager.UpdateMaxTS()` for tracking the maximum observed timestamp, which is required for computing `min_commit_ts`.
- The Lock struct (`pkg/txntypes/lock.go`) already carries `UseAsyncCommit`, `Secondaries`, and `MinCommitTS` fields.

What is missing is the **gRPC handler wiring** -- the `KvPrewrite` handler in `internal/server/server.go` currently only calls the standard `Prewrite()` path and ignores the `use_async_commit`, `try_one_pc`, `secondaries`, and `max_commit_ts` fields from `PrewriteRequest`. The response fields `min_commit_ts` and `one_pc_commit_ts` are never populated.

Additionally, two lock-related RPCs have no implementation:

- `KvCheckSecondaryLocks` -- used by the async commit resolution protocol to verify that all secondary locks are present.
- `KvScanLock` -- used by GC to find all locks older than a safe point.

---

## 2. Current State

### 2.1 What Exists

| Component | File | Status |
|-----------|------|--------|
| `PrewriteAsyncCommit()` | `internal/storage/txn/async_commit.go` | Implemented. Writes lock with `UseAsyncCommit=true`, `Secondaries`, and computed `MinCommitTS`. |
| `PrewriteAndCommit1PC()` | `internal/storage/txn/async_commit.go` | Implemented. Writes commit records directly to CF_WRITE, skipping CF_LOCK entirely. |
| `Is1PCEligible()` | `internal/storage/txn/async_commit.go` | Implemented. Checks mutation count (<= 64) and total size (< 256KB). |
| `IsAsyncCommitEligible()` | `internal/storage/txn/async_commit.go` | Implemented. Checks mutation count (<= 256). |
| `CheckAsyncCommitStatus()` | `internal/storage/txn/async_commit.go` | Implemented. Reads primary lock or commit record to determine async commit status. |
| `ConcurrencyManager.MaxTS()` | `internal/storage/txn/concurrency/manager.go` | Implemented. Returns current max observed timestamp. |
| `Storage.Prewrite()` | `internal/server/storage.go` | Implemented. Standard 2PC prewrite only. |
| `Storage.PrewriteModifies()` | `internal/server/storage.go` | Implemented. Standard 2PC prewrite for cluster mode. |
| `KvPrewrite` handler | `internal/server/server.go` | Implemented but only calls standard `Prewrite()`. Ignores async commit / 1PC request fields. |
| `KvCheckSecondaryLocks` | `internal/server/server.go` | Not implemented (falls through to `UnimplementedTikvServer`). |
| `KvScanLock` | `internal/server/server.go` | Not implemented (falls through to `UnimplementedTikvServer`). |
| `scanLocksForTxn()` | `internal/server/storage.go` | Implemented as a private helper for `ResolveLock`. Iterates CF_LOCK, filters by `startTS`. |

### 2.2 Proto Definitions (from `proto/kvrpcpb.proto`)

**PrewriteRequest** (relevant fields):
```protobuf
bool use_async_commit = 11;
repeated bytes secondaries = 12;
bool try_one_pc = 13;
uint64 max_commit_ts = 14;
```

**PrewriteResponse** (relevant fields):
```protobuf
uint64 min_commit_ts = 3;
uint64 one_pc_commit_ts = 4;
```

**CheckSecondaryLocksRequest**:
```protobuf
message CheckSecondaryLocksRequest {
  Context context = 1;
  repeated bytes keys = 2;
  uint64 start_version = 3;
}
```

**CheckSecondaryLocksResponse**:
```protobuf
message CheckSecondaryLocksResponse {
  errorpb.Error region_error = 1;
  KeyError error = 2;
  repeated LockInfo locks = 3;
  uint64 commit_ts = 4;
}
```

**ScanLockRequest**:
```protobuf
message ScanLockRequest {
  Context context = 1;
  uint64 max_version = 2;
  bytes start_key = 3;
  uint32 limit = 4;
  bytes end_key = 5;
}
```

**ScanLockResponse**:
```protobuf
message ScanLockResponse {
  errorpb.Error region_error = 1;
  KeyError error = 2;
  repeated LockInfo locks = 3;
}
```

---

## 3. Detailed Design

### 3.1 Item 1: Async Commit / 1PC Path in KvPrewrite

#### 3.1.1 Routing Logic in the Handler

The `KvPrewrite` handler in `internal/server/server.go` must inspect the request to determine which path to take. The decision tree is:

```
if req.TryOnePc && Is1PCEligible(mutations, 64):
    -> 1PC path
elif req.UseAsyncCommit && IsAsyncCommitEligible(mutations, 256):
    -> Async Commit path
else:
    -> Standard 2PC path (existing code)
```

The 1PC check comes first because it is the most optimistic path. If 1PC fails eligibility, fall through to async commit. If async commit also fails eligibility, fall through to standard 2PC.

#### 3.1.2 New Storage Methods

Add two new methods to `Storage` in `internal/server/storage.go`:

**`PrewriteAsyncCommit()`** -- standalone mode:

```go
func (s *Storage) PrewriteAsyncCommit(
    mutations []txn.Mutation,
    primary []byte,
    startTS txntypes.TimeStamp,
    lockTTL uint64,
    secondaries [][]byte,
    maxCommitTS txntypes.TimeStamp,
) (minCommitTS txntypes.TimeStamp, errs []error)
```

Implementation:
1. Collect keys, acquire latches.
2. Take snapshot, create `MvccReader` and `MvccTxn`.
3. Read `s.concMgr.MaxTS()` to get the current max observed timestamp.
4. For each mutation, call `txn.PrewriteAsyncCommit()` with `AsyncCommitPrewriteProps`:
   - `UseAsyncCommit = true`
   - `Secondaries = secondaries` (only for the primary key mutation)
   - `MaxCommitTS = max(maxCommitTS, concMgr.MaxTS())`
5. If all succeed, apply modifies. Track the maximum `MinCommitTS` across all written locks (each lock computes its own `MinCommitTS`).
6. Return `minCommitTS` (the maximum of all per-key `MinCommitTS` values) and `errs`.

**`PrewriteAsyncCommitModifies()`** -- cluster mode (returns modifies without applying):

Same logic as above but returns `(minCommitTS, modifies, errs)` instead of applying.

**`Prewrite1PC()`** -- standalone mode:

```go
func (s *Storage) Prewrite1PC(
    mutations []txn.Mutation,
    primary []byte,
    startTS txntypes.TimeStamp,
    lockTTL uint64,
    maxCommitTS txntypes.TimeStamp,
) (commitTS txntypes.TimeStamp, errs []error)
```

Implementation:
1. Collect keys, acquire latches.
2. Take snapshot, create `MvccReader` and `MvccTxn`.
3. Compute `commitTS = max(startTS + 1, concMgr.MaxTS() + 1)`. If `maxCommitTS > 0 && commitTS > maxCommitTS`, fall back to standard 2PC (return `commitTS = 0` as signal).
4. Call `txn.PrewriteAndCommit1PC()` with `OnePCProps{StartTS, CommitTS, Primary, LockTTL}`.
5. If all succeed, apply modifies and return `commitTS`.

**`Prewrite1PCModifies()`** -- cluster mode:

Same logic but returns `(commitTS, modifies, errs)`.

#### 3.1.3 Updated KvPrewrite Handler

```go
func (svc *tikvService) KvPrewrite(ctx context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
    resp := &kvrpcpb.PrewriteResponse{}

    // Convert proto mutations to internal format (existing code).
    mutations := convertMutations(req.GetMutations())
    startTS := txntypes.TimeStamp(req.GetStartVersion())
    primary := req.GetPrimaryLock()
    lockTTL := req.GetLockTtl()

    // Update concurrency manager with the caller's timestamp.
    svc.server.storage.ConcMgr().UpdateMaxTS(startTS)

    // --- 1PC path ---
    if req.GetTryOnePc() && txn.Is1PCEligible(mutations, 64) {
        maxCommitTS := txntypes.TimeStamp(req.GetMaxCommitTs())

        if coord := svc.server.coordinator; coord != nil {
            commitTS, modifies, errs := svc.server.storage.Prewrite1PCModifies(mutations, primary, startTS, lockTTL, maxCommitTS)
            // handle errs, propose modifies via Raft
            if commitTS != 0 {
                resp.OnePcCommitTs = uint64(commitTS)
                return resp, nil
            }
            // commitTS == 0 means 1PC ineligible due to maxCommitTS constraint; fall through
        } else {
            commitTS, errs := svc.server.storage.Prewrite1PC(mutations, primary, startTS, lockTTL, maxCommitTS)
            // handle errs
            if commitTS != 0 {
                resp.OnePcCommitTs = uint64(commitTS)
                return resp, nil
            }
        }
    }

    // --- Async Commit path ---
    if req.GetUseAsyncCommit() && txn.IsAsyncCommitEligible(mutations, 256) {
        secondaries := req.GetSecondaries()
        maxCommitTS := txntypes.TimeStamp(req.GetMaxCommitTs())

        if coord := svc.server.coordinator; coord != nil {
            minCommitTS, modifies, errs := svc.server.storage.PrewriteAsyncCommitModifies(mutations, primary, startTS, lockTTL, secondaries, maxCommitTS)
            // handle errs, propose modifies via Raft
            if minCommitTS != 0 {
                resp.MinCommitTs = uint64(minCommitTS)
            }
            return resp, nil
        } else {
            minCommitTS, errs := svc.server.storage.PrewriteAsyncCommit(mutations, primary, startTS, lockTTL, secondaries, maxCommitTS)
            // handle errs
            if minCommitTS != 0 {
                resp.MinCommitTs = uint64(minCommitTS)
            }
            return resp, nil
        }
    }

    // --- Standard 2PC path (existing code) ---
    // ... (unchanged)
}
```

#### 3.1.4 ConcMgr Access

Add a public accessor to `Storage`:

```go
func (s *Storage) ConcMgr() *concurrency.Manager {
    return s.concMgr
}
```

The concurrency manager's `MaxTS()` is used to compute `min_commit_ts` for async commit. The handler should call `UpdateMaxTS(startTS)` on every prewrite request so that the max_ts reflects the most recent reader/writer timestamp.

#### 3.1.5 MinCommitTS Computation

For async commit, the `min_commit_ts` for each key is:

```
min_commit_ts = max(start_ts + 1, max_ts_from_concurrency_manager + 1)
```

This is already implemented in `PrewriteAsyncCommit()` at line 76-80 of `async_commit.go`. The `MaxCommitTS` field in `AsyncCommitPrewriteProps` is used for this purpose.

The response's `min_commit_ts` field should be set to the **maximum** of all per-key `min_commit_ts` values. This is because the client needs to know the earliest safe commit timestamp for the entire transaction.

For 1PC, the `one_pc_commit_ts` is computed similarly:

```
commit_ts = max(start_ts + 1, max_ts + 1)
```

If `max_commit_ts > 0` and the computed `commit_ts` exceeds it, 1PC is abandoned (the timestamp constraint from schema change cannot be satisfied).

---

### 3.2 Item 2: KvCheckSecondaryLocks

#### 3.2.1 Protocol Semantics

`KvCheckSecondaryLocks` is part of the async commit resolution protocol. When a transaction coordinator (or a resolver that encounters an async-commit lock) needs to determine whether the transaction committed or rolled back, it:

1. Checks the primary key's lock/write status (via `CheckTxnStatus`).
2. Checks all secondary keys via `KvCheckSecondaryLocks`.

For each key in the request:
- If a lock exists with matching `start_version` -> report the lock (transaction still in progress).
- If the lock is missing, check CF_WRITE for a commit record with matching `start_ts`:
  - If a commit record exists -> the transaction was committed at `commit_ts`.
  - If no commit record (or only a rollback) -> the key was rolled back. Write a rollback tombstone to prevent late-arriving prewrites.

The response contains:
- `locks`: one `LockInfo` per key that still has a matching lock (nil entries for keys where the lock is gone).
- `commit_ts`: non-zero if any of the keys has been committed.

If `commit_ts > 0`, the resolver knows the transaction committed and can finalize remaining keys. If all locks are gone and `commit_ts == 0`, the transaction was rolled back.

#### 3.2.2 Storage Method

Add to `internal/server/storage.go`:

```go
type CheckSecondaryLocksResult struct {
    Locks    []*txntypes.Lock  // nil entry means lock missing for that key
    CommitTS txntypes.TimeStamp
}

func (s *Storage) CheckSecondaryLocks(keys [][]byte, startTS txntypes.TimeStamp) (*CheckSecondaryLocksResult, error)
```

Implementation:
1. Acquire latches on all keys.
2. Take snapshot, create `MvccReader`.
3. For each key:
   a. Call `reader.LoadLock(key)`.
   b. If lock exists and `lock.StartTS == startTS`:
      - Append lock to result.
   c. Else (lock missing or belongs to different txn):
      - Call `reader.GetTxnCommitRecord(key, startTS)`.
      - If committed -> set `result.CommitTS = commitTS`, append nil to locks.
      - If not committed -> write a rollback tombstone for protection, append nil to locks.
4. Apply any rollback modifies atomically.
5. Return result.

Also add `CheckSecondaryLocksModifies()` for cluster mode that returns `(result, modifies, error)`.

#### 3.2.3 Handler

```go
func (svc *tikvService) KvCheckSecondaryLocks(ctx context.Context, req *kvrpcpb.CheckSecondaryLocksRequest) (*kvrpcpb.CheckSecondaryLocksResponse, error) {
    resp := &kvrpcpb.CheckSecondaryLocksResponse{}

    startTS := txntypes.TimeStamp(req.GetStartVersion())
    keys := req.GetKeys()

    if coord := svc.server.coordinator; coord != nil {
        result, modifies, err := svc.server.storage.CheckSecondaryLocksModifies(keys, startTS)
        if err != nil {
            resp.Error = errToKeyError(err)
            return resp, nil
        }
        if len(modifies) > 0 {
            if err := coord.ProposeModifies(1, modifies, 10*time.Second); err != nil {
                return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
            }
        }
        // populate resp from result
    } else {
        result, err := svc.server.storage.CheckSecondaryLocks(keys, startTS)
        if err != nil {
            resp.Error = errToKeyError(err)
            return resp, nil
        }
        // populate resp from result
    }

    // Convert result.Locks to LockInfo protos
    for _, lock := range result.Locks {
        if lock != nil {
            resp.Locks = append(resp.Locks, &kvrpcpb.LockInfo{
                PrimaryLock: lock.Primary,
                LockVersion: uint64(lock.StartTS),
                LockTtl:     lock.TTL,
                Key:         ...,  // from the request keys
                MinCommitTs: uint64(lock.MinCommitTS),
                UseAsyncCommit: lock.UseAsyncCommit,
                Secondaries:    lock.Secondaries,
            })
        }
        // nil locks are simply omitted from the response
    }
    if result.CommitTS != 0 {
        resp.CommitTs = uint64(result.CommitTS)
    }

    return resp, nil
}
```

#### 3.2.4 Rollback Tombstone

When a secondary key's lock is missing and no commit record exists, the implementation must write a rollback record to prevent a late-arriving prewrite from succeeding. This uses the existing `txn.Rollback()` function:

```go
mvccTxn := mvcc.NewMvccTxn(startTS)
txn.Rollback(mvccTxn, reader, key, startTS)
// collect mvccTxn.Modifies
```

This is critical for correctness: without the tombstone, a slow prewrite could arrive after the resolution has decided to roll back the transaction.

---

### 3.3 Item 3: KvScanLock

#### 3.3.1 Protocol Semantics

`KvScanLock` scans CF_LOCK for all locks with `start_ts <= max_version`. This is used by:
- GC to find old locks that need resolution before garbage collection.
- Lock resolvers that need to find all locks in a region.

Request parameters:
- `max_version`: Return locks where `lock.start_ts <= max_version`.
- `start_key`: Begin scanning from this key (inclusive).
- `end_key`: Stop scanning at this key (exclusive). Empty means scan to end.
- `limit`: Maximum number of locks to return. 0 means no limit.

#### 3.3.2 Storage Method

Add to `internal/server/storage.go`:

```go
func (s *Storage) ScanLock(
    maxVersion txntypes.TimeStamp,
    startKey, endKey []byte,
    limit uint32,
) ([]*ScanLockResult, error)

type ScanLockResult struct {
    Key  []byte
    Lock *txntypes.Lock
}
```

Implementation:
1. Take a snapshot.
2. Create an iterator on CF_LOCK with bounds:
   - `LowerBound = EncodeLockKey(startKey)` if `startKey` is non-empty.
   - `UpperBound = EncodeLockKey(endKey)` if `endKey` is non-empty.
3. Iterate from first valid position:
   ```go
   for iter.SeekToFirst(); iter.Valid(); iter.Next() {
       lockData := iter.Value()
       lock, err := txntypes.UnmarshalLock(lockData)
       if err != nil { continue }  // skip corrupt entries

       if lock.StartTS > maxVersion {
           continue  // filter by max_version
       }

       userKey, err := mvcc.DecodeLockKey(iter.Key())
       if err != nil { continue }

       results = append(results, &ScanLockResult{Key: userKey, Lock: lock})

       if limit > 0 && uint32(len(results)) >= limit {
           break
       }
   }
   ```
4. Return results.

This is read-only, so no latches or write batches are needed.

Note: The existing private method `scanLocksForTxn()` in `storage.go` is similar but filters by exact `startTS` match. The new `ScanLock` method filters by `startTS <= maxVersion`, which is the correct GC semantics.

#### 3.3.3 Handler

```go
func (svc *tikvService) KvScanLock(ctx context.Context, req *kvrpcpb.ScanLockRequest) (*kvrpcpb.ScanLockResponse, error) {
    resp := &kvrpcpb.ScanLockResponse{}

    maxVersion := txntypes.TimeStamp(req.GetMaxVersion())
    results, err := svc.server.storage.ScanLock(
        maxVersion,
        req.GetStartKey(),
        req.GetEndKey(),
        req.GetLimit(),
    )
    if err != nil {
        resp.Error = errToKeyError(err)
        return resp, nil
    }

    for _, r := range results {
        resp.Locks = append(resp.Locks, &kvrpcpb.LockInfo{
            PrimaryLock: r.Lock.Primary,
            LockVersion: uint64(r.Lock.StartTS),
            Key:         r.Key,
            LockTtl:     r.Lock.TTL,
            LockType:    toLockTypeProto(r.Lock.LockType),
            MinCommitTs: uint64(r.Lock.MinCommitTS),
        })
    }

    return resp, nil
}
```

#### 3.3.4 BatchCommands Support

Add `KvScanLock` and `KvCheckSecondaryLocks` to the `handleBatchCmd` switch in `server.go`:

```go
case *tikvpb.BatchCommandsRequest_Request_ScanLock:
    r, _ := svc.KvScanLock(ctx, cmd.ScanLock)
    resp.Cmd = &tikvpb.BatchCommandsResponse_Response_ScanLock{ScanLock: r}

case *tikvpb.BatchCommandsRequest_Request_CheckSecondaryLocks:
    r, _ := svc.KvCheckSecondaryLocks(ctx, cmd.CheckSecondaryLocks)
    resp.Cmd = &tikvpb.BatchCommandsResponse_Response_CheckSecondaryLocks{CheckSecondaryLocks: r}
```

---

## 4. Integration with Dual-Mode (Standalone / Cluster)

All three items follow the existing dual-mode pattern established by `KvPrewrite`, `KvCommit`, `KvResolveLock`, etc.:

| Mode | Write Operations | Read Operations |
|------|-----------------|-----------------|
| **Standalone** | `Storage.XxxMethod()` applies modifies directly via `ApplyModifies()`. | Reads directly from engine snapshot. |
| **Cluster** | `Storage.XxxModifies()` returns `[]mvcc.Modify`, handler proposes via `coord.ProposeModifies()`. | Reads directly from engine snapshot (reads do not go through Raft). |

Specific considerations:

- **1PC in cluster mode**: The `Prewrite1PCModifies()` returns commit-record modifications (writes to CF_WRITE and CF_DEFAULT). These are proposed via Raft and applied on all replicas. No CF_LOCK writes are generated.
- **Async Commit in cluster mode**: The `PrewriteAsyncCommitModifies()` returns lock modifications with async-commit metadata. The lock is replicated via Raft. The async resolution (second phase) happens lazily when readers encounter async-commit locks.
- **CheckSecondaryLocks in cluster mode**: May generate rollback tombstone writes. These must be proposed via Raft for replication.
- **ScanLock**: Read-only. No cluster/standalone distinction needed. Always reads from local engine snapshot.

---

## 5. E2E Test Plan

### 5.1 Test File

`e2e/async_commit_1pc_test.go`

### 5.2 Test Cases

#### 5.2.1 Async Commit Happy Path

1. Start a gookv server (standalone mode).
2. Send `KvPrewrite` with `use_async_commit=true`, `secondaries=[keyB, keyC]`, mutations for `[keyA (primary), keyB, keyC]`.
3. Verify response has `min_commit_ts > 0` and `one_pc_commit_ts == 0`.
4. Verify locks exist in CF_LOCK with `UseAsyncCommit=true` and primary lock has `Secondaries` populated.
5. Send `KvCommit` for all keys with `commit_ts = min_commit_ts`.
6. Verify `KvGet` returns correct values at `commit_ts`.

#### 5.2.2 1PC Happy Path

1. Send `KvPrewrite` with `try_one_pc=true`, a small number of mutations (e.g., 2 keys).
2. Verify response has `one_pc_commit_ts > 0` and no errors.
3. Verify `KvGet` returns correct values at `one_pc_commit_ts`.
4. Verify no locks remain in CF_LOCK (1PC skips locks).

#### 5.2.3 1PC Fallback to 2PC

1. Send `KvPrewrite` with `try_one_pc=true` and `max_commit_ts` set to a value lower than `start_ts`.
2. The handler should detect 1PC is infeasible and fall through to standard 2PC.
3. Verify response has `one_pc_commit_ts == 0`, `min_commit_ts == 0`, and locks are present in CF_LOCK.

#### 5.2.4 CheckSecondaryLocks -- All Locks Present

1. Prewrite an async-commit transaction with primary `keyA` and secondaries `[keyB, keyC]`.
2. Call `KvCheckSecondaryLocks` with keys `[keyB, keyC]` and `start_version = startTS`.
3. Verify response has 2 locks, `commit_ts == 0`.

#### 5.2.5 CheckSecondaryLocks -- Committed Transaction

1. Prewrite and commit an async-commit transaction.
2. Call `KvCheckSecondaryLocks` with the secondary keys.
3. Verify response has `commit_ts > 0`, locks list is empty or has nil entries.

#### 5.2.6 CheckSecondaryLocks -- Rolled Back Secondary

1. Prewrite an async-commit transaction.
2. Roll back one secondary key via `KvBatchRollback`.
3. Call `KvCheckSecondaryLocks`.
4. Verify the rolled-back key returns nil lock, `commit_ts == 0`.
5. Verify a rollback tombstone was written for the rolled-back key.

#### 5.2.7 ScanLock -- Basic Scan

1. Prewrite multiple transactions with different `start_ts` values: TS=10, TS=20, TS=30.
2. Call `KvScanLock` with `max_version=25`.
3. Verify only locks with `start_ts <= 25` are returned (TS=10 and TS=20).

#### 5.2.8 ScanLock -- With Limit and Pagination

1. Prewrite 100 keys with `start_ts=10`.
2. Call `KvScanLock` with `max_version=10`, `limit=30`.
3. Verify exactly 30 locks returned.
4. Call again with `start_key` set to the key after the last returned lock.
5. Verify the next batch of locks.

#### 5.2.9 ScanLock -- With Key Range

1. Prewrite keys `a`, `b`, `c`, `d`, `e` with `start_ts=10`.
2. Call `KvScanLock` with `start_key=b`, `end_key=d`, `max_version=10`.
3. Verify only locks for `b` and `c` are returned.

#### 5.2.10 Cluster Mode Integration

1. Start a 3-node Raft cluster.
2. Run the async commit happy path test against the leader.
3. Verify data is replicated by reading from a follower after leader step-down.

---

## 6. Implementation Order

1. Add `ConcMgr()` accessor to `Storage`.
2. Implement `Storage.ScanLock()` and `KvScanLock` handler (simplest, read-only).
3. Implement `Storage.CheckSecondaryLocks()` / `CheckSecondaryLocksModifies()` and `KvCheckSecondaryLocks` handler.
4. Implement `Storage.PrewriteAsyncCommit()` / `PrewriteAsyncCommitModifies()` and update `KvPrewrite` handler for async commit path.
5. Implement `Storage.Prewrite1PC()` / `Prewrite1PCModifies()` and update `KvPrewrite` handler for 1PC path.
6. Add BatchCommands routing for `KvScanLock` and `KvCheckSecondaryLocks`.
7. Write E2E tests.
