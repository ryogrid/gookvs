# Bug 12: Commit Sent to Wrong Node After Split — Design Document

## 1. Problem Statement

After a region split, `commitSecondaries` may send `KvCommit` RPCs to a node that does not hold the secondary's prewrite lock. The commit silently fails with `TxnLockNotFound`, leaving the secondary lock as an orphan. Since the primary is already committed, the net effect is a partial transaction: debit applied but credit lost (or vice versa), causing balance divergence in the transaction integrity demo.

### Observed symptoms

- Phase 3 total balance deviates by $50–$200 (both directions)
- VERIFY MISMATCH shows `errVF=<nil> errVT=<nil>` — both reads succeed but values don't match replayed amounts
- Server trace confirms: prewrite on Node2, commit on Node1 → `LOCK_NOT_FOUND`

### Affected demo metrics

| Metric | Expected | Actual |
|--------|----------|--------|
| Phase 3 balance | $100,000 | $100,073 (varies) |
| Discrepant accounts | 0 | 20–30 per run |
| Orphan locks after cleanup | 0 | 2–6 |

## 2. Root Cause Analysis

### The commit flow

```
Prewrite phase:
  1. groupMutationsByRegion() → GroupKeysByRegion() → LocateKey() for each key
  2. For each region group: SendToRegion(key, prewriteFunc)
  3. RegionInfo is used only for building the RPC context
  4. RegionInfo is DISCARDED after grouping (groupMutationsByRegion returns map[uint64][]mutationWithKey)

Commit phase:
  5. commitPrimary() → SendToRegion(primaryKey, commitFunc) ← uses fresh LocateKey
  6. commitSecondaries() → GroupKeysByRegion(secondaryKeys) ← FRESH region lookup
  7. For each region group: SendToRegion(group.Keys[0], commitFunc)
```

### The race window

```
Timeline:

  T=100  Prewrite: LocateKey("acct:0500") → Region 1, leader=Node2
         Prewrite lock written on Node2 ✓

  T=150  Region 1 splits into Region 1 [..acct:0498) and Region 1004 [acct:0498..)
         "acct:0500" now belongs to Region 1004, leader=Node1

  T=200  commitSecondaries: GroupKeysByRegion(["acct:0500"])
         LocateKey("acct:0500"):
           Case A: Stale cache → Region 1, leader=Node2 → WRONG
           Case B: Fresh cache → Region 1004, leader=Node1 → May be correct
                   (but Node1 may not have the lock yet if Raft hasn't replicated)

  T=200  KvCommit sent to Node2 for "acct:0500"
         Node2: lock not found in Region 1 (key moved to Region 1004) → TxnLockNotFound
```

### Why the current error handling doesn't help

`commitSecondaries` (committer.go:269-278) handles `TxnLockNotFound` by calling `ResolveLocks`. However:

1. `ResolveLocks` checks the primary status (committed) and decides to commit the secondary
2. But it sends the resolve/commit to the same stale region → same node → same failure
3. The resolve result is ignored (`_ = c.client.resolver.ResolveLocks(...)`)
4. The secondary lock remains on the correct node, never committed

### Code references

| Location | File | Lines |
|----------|------|-------|
| `commitSecondaries` | `pkg/client/committer.go` | 227–289 |
| `groupMutationsByRegion` | `pkg/client/committer.go` | 332–359 |
| `GroupKeysByRegion` | `pkg/client/region_cache.go` | 193–210 |
| `KeyGroup` struct | `pkg/client/region_cache.go` | 24–27 |
| `SendToRegion` (retry loop) | `pkg/client/request_sender.go` | 53–95 |
| `KvCommit` handler | `internal/server/server.go` | ~449–493 |
| `handleRegionError` | `pkg/client/request_sender.go` | 97–130 |

## 3. Fix Design

### Approach: Use `SendToRegion` per key with retry

The simplest and most robust fix: instead of grouping secondary keys and sending one RPC per group, send each secondary key individually via `SendToRegion`. `SendToRegion` already has retry logic with cache invalidation on region errors.

The key insight: `SendToRegion` calls `LocateKey` on each retry attempt (request_sender.go:56), so even if the first attempt hits a stale cache entry, the retry will re-query PD and find the correct region.

Additionally, the server-side `KvCommit` handler should return `KeyNotInRegion` (a retriable region error) when the lock is not found, instead of `TxnLockNotFound` (a non-retriable transaction error). This triggers `SendToRegion`'s automatic retry with cache refresh.

### Changes

#### Change 1: `commitSecondaries` — per-key SendToRegion with retry

**File:** `pkg/client/committer.go`

Replace the current group-and-send approach with per-key `SendToRegion`:

```go
func (c *twoPhaseCommitter) commitSecondaries(ctx context.Context) {
    // ... (skip if 1PC, collect secondaryKeys as before) ...

    var wg sync.WaitGroup
    for _, key := range secondaryKeys {
        key := key
        wg.Add(1)
        go func() {
            defer wg.Done()
            err := c.client.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
                resp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
                    Context:       buildContext(info),
                    StartVersion:  uint64(c.startTS),
                    Keys:          [][]byte{key},
                    CommitVersion: uint64(c.commitTS),
                })
                if err != nil {
                    return nil, err
                }
                if resp.GetRegionError() != nil {
                    return resp.GetRegionError(), nil
                }
                if resp.GetError() != nil {
                    if resp.GetError().GetTxnLockNotFound() != nil {
                        // Lock was resolved by another transaction or
                        // the key is in a different region after split.
                        // Return as NotLeader to trigger retry with fresh region lookup.
                        return &errorpb.Error{
                            NotLeader: &errorpb.NotLeader{RegionId: info.Region.GetId()},
                        }, nil
                    }
                }
                return nil, nil
            })
            if err != nil {
                slog.Warn("commitSecondary failed", "key", key, "startTS", c.startTS, "err", err)
            }
        }()
    }
    wg.Wait()
}
```

**Trade-off:** Per-key RPCs increase the number of round-trips compared to batched RPCs. For a typical transaction with 2 keys (1 primary + 1 secondary), this is zero overhead. For transactions with many secondaries, the overhead is bounded by `numWorkers` goroutines.

#### Change 2: Server-side `KvCommit` — return region error on lock-not-found after split

**File:** `internal/server/server.go`

In the `KvCommit` handler, when `TxnLockNotFound` is returned, check if the key belongs to the current region. If not, return `KeyNotInRegion` instead:

```go
if resp.GetError().GetTxnLockNotFound() != nil {
    // Check if the key is actually in this region's range.
    // If not, the client used a stale region cache after a split.
    region := coord.GetPeer(regionID).Region()
    if !keyInRegionRange(key, region) {
        resp.Error = nil
        resp.RegionError = &errorpb.Error{
            KeyNotInRegion: &errorpb.KeyNotInRegion{...},
        }
    }
}
```

This makes `SendToRegion` automatically retry with a fresh region lookup.

### Why not: preserve RegionInfo from prewrite

An alternative approach is to store the `KeyGroup` results from the prewrite phase and reuse them for commit. This was considered but rejected because:

1. **Still fragile**: Even with preserved RegionInfo, the region's leader may have changed (leader transfer during split). The stored `StoreAddr` may point to a non-leader.
2. **No retry**: The prewrite-preserved RegionInfo has no retry mechanism. If the leader moved, the commit fails without recourse.
3. **Complexity**: Requires threading `KeyGroup` through `prewrite` → `twoPhaseCommitter` → `commitSecondaries`, changing multiple function signatures.
4. **SendToRegion already handles this**: The existing retry-with-cache-invalidation mechanism in `SendToRegion` is the correct abstraction for handling split-induced routing changes.

## 4. Impact Assessment

| Aspect | Before fix | After fix |
|--------|-----------|-----------|
| Commit routing | Stale after split | Self-correcting via retry |
| RPC count per secondary | 1 (batched) | 1 per key (but retries if needed) |
| Error handling | `TxnLockNotFound` silently ignored | Retried as region error |
| Balance conservation | $100,000 ± $200 | $100,000 exact |
