# PD Layer Code Review

**Reviewer:** Claude Opus 4.6 (1M context)
**Date:** 2026-03-27
**Scope:** `internal/pd/` (13 files, ~2,590 lines), `pkg/pdclient/` (2 files, ~1,021 lines)

---

## Summary

The PD layer is well-architected, with clean separation between single-node and Raft-replicated modes, buffered allocators (TSOBuffer, IDBuffer) to amortize Raft consensus cost, and a multi-step move tracker for safe region rebalancing. The code quality is generally high. However, there are several bugs ranging from critical data-consistency violations to moderate correctness issues.

**Critical bugs found:** 4
**Moderate bugs found:** 5
**Minor issues found:** 6

---

## Critical Bugs

### C1. Proposal Index Tracking Race — Pending Callback Misdelivery

**File:** `internal/pd/raft_peer.go`, lines 250-255
**Severity:** Critical

```go
// propose()
if err := p.rawNode.Propose(data); err != nil { ... }
lastIdx, _ := p.storage.LastIndex()
expectedIdx := lastIdx + 1
if proposal.Callback != nil {
    p.pendingProposals[expectedIdx] = proposal.Callback
}
```

The expected index is computed *after* `Propose()` by reading `storage.LastIndex() + 1`. But `rawNode.Propose()` appends the entry to the unstable log inside the RawNode, and those entries are only flushed to PDRaftStorage during `handleReady()` (via `SaveReady`). At the time `LastIndex()` is called, the storage may not yet reflect the proposed entry.

In a single-proposer scenario (all proposals go through the Run goroutine), this *might* work because `handleReady()` is called synchronously after `handleMessage()`. However, if a Ready batch includes entries from both a received Raft message *and* the local proposal, or if there are multiple proposals queued before a single Ready cycle, the index calculation becomes stale.

**The real danger:** When a leader receives entries from itself (e.g., after a leader change, entries from the previous term being committed), the index tracking can be off-by-N, causing callbacks to be delivered for the wrong proposals. This would cause ID allocation or TSO allocation to return results for a different command.

**Fix:** Track proposals by the data content or use a monotonic proposal ID embedded in the command, not by guessing the log index.

### C2. Non-Atomic Bootstrap in Raft Mode — Partial Bootstrap on Failure

**File:** `internal/pd/server.go`, lines 574-605
**Severity:** Critical

```go
// Propose SetBootstrapped(true).
bTrue := true
cmd := PDCommand{Type: CmdSetBootstrapped, Bootstrapped: &bTrue}
if _, err := s.raftPeer.ProposeAndWait(ctx, cmd); err != nil {
    return nil, err
}
// Propose PutStore.
if store := req.GetStore(); store != nil {
    cmd = PDCommand{Type: CmdPutStore, Store: store}
    if _, err := s.raftPeer.ProposeAndWait(ctx, cmd); err != nil {
        return nil, err  // <<< Bootstrapped=true committed, but no store registered
    }
}
// Propose PutRegion.
if region := req.GetRegion(); region != nil { ... }
```

Bootstrap is performed as three separate Raft proposals. If the first (SetBootstrapped) succeeds but the second (PutStore) or third (PutRegion) fails (e.g., leader change mid-bootstrap, context timeout), the cluster is left in a partially bootstrapped state: `bootstrapped=true` but with no store or region registered. Subsequent bootstrap attempts will be rejected with "already bootstrapped", leaving the cluster permanently wedged.

**Fix:** Combine all three operations into a single Raft proposal (e.g., a `CmdBootstrap` command that atomically sets all three).

### C3. ReportBatchSplit in Raft Mode Loses Leader Information

**File:** `internal/pd/server.go`, lines 858-864
**Severity:** Critical

In single-node mode, `ReportBatchSplit` correctly infers leaders:
```go
// Single-node mode:
leader = region.GetPeers()[0]
s.meta.PutRegion(region, leader)
```

But in Raft mode, leader is not set:
```go
// Raft mode:
cmd := PDCommand{Type: CmdPutRegion, Region: region}  // Leader field is nil!
```

After a split in Raft mode, the newly created regions will have no leader information in PD's metadata. This means:
- `GetRegion`/`GetRegionByID` will return `nil` for the leader peer
- The scheduler cannot make scheduling decisions (leader balance, region balance)
- Clients cannot determine which store to send requests to

**Fix:** Set `cmd.Leader` to `region.GetPeers()[0]` (or the reporting peer) in the Raft mode path, matching the single-node behavior.

### C4. Snapshot Does Not Include `storeLastHeartbeat` — Store States Reset on Snapshot Apply

**File:** `internal/pd/snapshot.go`
**Severity:** Critical

The `PDSnapshot` struct and `GenerateSnapshot()`/`ApplySnapshot()` do not include `storeLastHeartbeat` (the map of `storeID -> time.Time`). When a follower receives a snapshot:

1. All store metadata is restored, including `storeStates`
2. But `storeLastHeartbeat` remains empty
3. On the next `updateStoreStates()` tick, every store has no heartbeat recorded
4. All stores transition to `StoreStateDown`
5. The scheduler begins unnecessary replica repair for every region

This creates a cascade of unnecessary scheduling commands after any snapshot transfer.

**Fix:** Add `StoreLastHeartbeat map[uint64]int64` (unix millis) to `PDSnapshot` and serialize/deserialize it in `GenerateSnapshot()`/`ApplySnapshot()`.

---

## Moderate Bugs

### M1. TSO Monotonicity Violation on Logical Overflow

**File:** `internal/pd/server.go`, lines 1199-1221
**Severity:** Moderate

```go
func (t *TSOAllocator) Allocate(count int) (*pdpb.Timestamp, error) {
    t.mu.Lock()
    defer t.mu.Unlock()
    now := time.Now().UnixMilli()
    if now > t.physical {
        t.physical = now
        t.logical = 0
    }
    t.logical += int64(count)
    if t.logical >= (1 << 18) {
        t.physical++
        t.logical = int64(count)  // <<< starts at count, not 1
    }
    return &pdpb.Timestamp{
        Physical: t.physical,
        Logical:  int64(t.logical),
    }, nil
}
```

When logical overflows (>= 2^18), the physical is incremented by 1 and logical is reset to `count`. But this `physical+1` value may be *less than* `time.Now().UnixMilli()` in the future, which is fine. However, the returned timestamp after overflow has `logical = count`, which starts the range at an arbitrary value rather than building from 0 correctly. If `count` is large (e.g., a TSOBuffer batch of 1000), the first allocation after overflow wastes IDs [1, count-1].

More importantly: if the physical clock *advances* between two calls (both entering the `now > t.physical` branch), and the second call's `now` happens to equal the artificially-incremented `physical+1` from the overflow case, the logical counter is reset to 0 and reallocated from `count` — potentially producing a timestamp identical to one already issued.

**Fix:** On overflow, use `t.physical = max(t.physical+1, now)` and set `t.logical = count` consistently. Or better: advance physical to `now` after overflow and re-check.

### M2. TSOBuffer GetTS Can Return Stale Physical Component

**File:** `internal/pd/tso_buffer.go`, lines 36-63
**Severity:** Moderate

```go
func (b *TSOBuffer) GetTS(ctx context.Context, count int) (*pdpb.Timestamp, error) {
    ...
    b.logical += int64(count)
    b.remain -= count
    return &pdpb.Timestamp{
        Physical: b.physical,
        Logical:  b.logical,
    }, nil
}
```

The TSOBuffer caches a batch of timestamps with a fixed physical component. If the buffer has remaining capacity, it keeps returning timestamps with the same physical value. Over time (especially with a large batch size of 1000 and low traffic), the physical component can become significantly stale (seconds behind wall clock time).

While the resulting timestamps are still *monotonically increasing* (which is the critical invariant), they will appear to be from the past. For a TSO-based MVCC system, this means GC safe point calculations and "read at latest" semantics may see timestamps that are older than expected, potentially causing reads to return stale data if GC runs concurrently.

**Impact:** Low under normal load (buffer depletes quickly), but can be problematic during low-traffic periods or after a fresh batch allocation.

### M3. `scheduleExcessReplicaShedding` May Remove the Leader Peer

**File:** `internal/pd/scheduler.go`, lines 156-187
**Severity:** Moderate

```go
func (s *Scheduler) scheduleExcessReplicaShedding(regionID uint64, region *metapb.Region) *ScheduleCommand {
    ...
    // Find the peer on the store with the most regions.
    var removePeer *metapb.Peer
    maxCount := -1
    for _, peer := range region.GetPeers() {
        count := regionCounts[peer.GetStoreId()]
        if count > maxCount {
            maxCount = count
            removePeer = peer
        }
    }
    ...
    return &ScheduleCommand{
        RegionID: regionID,
        ChangePeer: &pdpb.ChangePeer{
            Peer:       removePeer,
            ChangeType: eraftpb.ConfChangeType_RemoveNode,
        },
    }
}
```

This function does not check whether `removePeer` is the current leader. Removing the leader peer directly will cause an election disruption. The `leader` parameter is not even passed to this function.

**Fix:** Accept the `leader` parameter and exclude the leader's store from candidates for removal.

### M4. `scheduleRegionBalance` Returns AddPeer But Source Peer Is Never Actually Removed

**File:** `internal/pd/scheduler.go`, lines 190-290
**Severity:** Moderate

When `moveTracker` is nil (or not set), `scheduleRegionBalance` returns an AddPeer command but never issues a corresponding RemovePeer. The region will permanently have one extra replica. This only happens when `moveTracker` is nil, but the scheduler constructor allows this path.

Even when `moveTracker` is non-nil, the initial response is AddPeer only. The removal depends on subsequent heartbeats advancing the move state machine. If heartbeats stop (store crash, network partition), the move stalls indefinitely until `CleanupStale` runs (10 min timeout). This is by design but worth noting.

### M5. MockClient TSO Has Race Condition

**File:** `pkg/pdclient/mock.go`, lines 64-80
**Severity:** Moderate

```go
func (c *MockClient) GetTS(_ context.Context) (TimeStamp, error) {
    logical := c.tsoLogical.Add(1)           // atomic
    physical := c.tsoPhysical.Load()         // atomic, but not in same critical section

    if logical >= (1 << 18) {
        c.tsoPhysical.Add(1)                 // race: multiple goroutines can increment
        c.tsoLogical.Store(0)                // race: Store(0) after Add(1) by another goroutine
        physical = c.tsoPhysical.Load()
        logical = 0
    }
    ...
}
```

Under concurrent access, multiple goroutines can simultaneously detect `logical >= (1 << 18)`, each incrementing `tsoPhysical` and resetting `tsoLogical`. This can produce:
- Duplicate timestamps (two goroutines both return `logical=0` with the same physical)
- Skipped physical values (physical incremented multiple times)
- Non-monotonic timestamps (goroutine A gets physical=2,logical=5; goroutine B gets physical=1,logical=3)

**Fix:** Use a mutex instead of individual atomics, or use `CompareAndSwap` loops.

---

## Minor Issues

### m1. `GetSafePoint` Uses Mutex Instead of RWMutex

**File:** `internal/pd/server.go`, line 1256
**Severity:** Minor

```go
func (g *GCSafePointManager) GetSafePoint() uint64 {
    g.mu.Lock()       // Should be g.mu.RLock()
    defer g.mu.Unlock()
    return g.safePoint
}
```

Read-only access should use `RLock()` for better concurrency. `GCSafePointManager.mu` is a `sync.Mutex`, not a `sync.RWMutex`, so this also requires changing the type.

### m2. `IsBootstrapped` Not Forwarded in Raft Mode — Serves Potentially Stale Data

**File:** `internal/pd/server.go`, lines 608-613

`IsBootstrapped` reads directly from the local `MetadataStore` without forwarding to the leader. On a follower that has not yet received the bootstrap replication, this returns `false` even though the cluster is bootstrapped. This could cause a client to attempt a redundant bootstrap.

### m3. `GetRegion`/`GetRegionByID`/`GetAllStores`/`GetStore`/`GetGCSafePoint` Serve Stale Data on Followers

**File:** `internal/pd/server.go`, various handlers
**Severity:** Minor

All read-only RPCs read directly from the local `MetadataStore` without checking whether this node is the leader or forwarding to the leader. In Raft mode, followers may have arbitrarily stale data. While this is a common trade-off for read performance, it should be documented as eventually-consistent reads.

### m4. Client `GetTS` Creates a New Stream Per Call

**File:** `pkg/pdclient/client.go`, lines 332-370
**Severity:** Minor

Each `GetTS` call creates a new bidirectional streaming RPC, sends one request, receives one response, and implicitly closes the stream. This incurs significant gRPC overhead (HTTP/2 stream creation per call). The TSO API is designed for streaming precisely to avoid this overhead.

**Fix:** Maintain a long-lived TSO stream and multiplex requests over it, reconnecting on failure.

### m5. `grpc.Dial` and `grpc.DialContext` Are Deprecated

**File:** `internal/pd/forward.go` line 43, `internal/pd/transport.go` line 121, `pkg/pdclient/client.go` line 157
**Severity:** Minor

`grpc.Dial` and `grpc.DialContext` are deprecated in favor of `grpc.NewClient`. The deprecated APIs have different default behaviors (e.g., `WithBlock` semantics).

### m6. `readEntriesFromEngine` Does Sequential Key Lookups Instead of Range Scan

**File:** `internal/pd/raft_storage.go`, lines 402-418
**Severity:** Minor

```go
for idx := lo; idx < hi; idx++ {
    data, err := s.engine.Get(cfnames.CFRaft, keys.RaftLogKey(s.clusterID, idx))
    ...
}
```

Each entry is fetched with a separate `Get` call. For large ranges, this is O(n) random reads. An iterator-based range scan would be more efficient. This is used as a fallback when entries are not in the cache, so it occurs primarily on restart recovery.

---

## Design Observations (Not Bugs)

### D1. Linear Scan for GetRegionByKey

`MetadataStore.GetRegionByKey` does a linear scan over all regions (line 1165). With hundreds of thousands of regions, this becomes a bottleneck. The code comments acknowledge this: "Linear scan for simplicity. In production, use a B-tree."

### D2. Scheduler Evaluates One Region Per Heartbeat

The scheduler only evaluates the region that just heartbeated. There is no background sweep to detect imbalances for regions that heartbeat infrequently. This is standard TiKV behavior but means scheduling latency depends on heartbeat frequency.

### D3. No Conf State in InitialState

`PDRaftStorage.InitialState()` always returns an empty `ConfState{}`. This works because the etcd/raft library uses the initial peer list from Bootstrap for the first term, but on recovery, the ConfState should reflect the actual cluster membership. Since PD clusters have static membership (no dynamic member changes), this is acceptable but fragile.

### D4. No Pending Proposal Cleanup on Leader Change

When a leader loses leadership, `pendingProposals` in `PDRaftPeer` are never cleaned up. Callbacks for in-flight proposals on the old leader will hang indefinitely (the channel in `ProposeAndWait` is never signaled). The caller's context timeout is the only safety net. Adding a `leaderChangeFunc` step to drain pending proposals with `ErrNotLeader` would provide faster failure notification.

---

## Files Reviewed

| File | Lines | Status |
|------|-------|--------|
| `internal/pd/server.go` | 1269 | Reviewed |
| `internal/pd/raft_peer.go` | 504 | Reviewed |
| `internal/pd/raft_storage.go` | 435 | Reviewed |
| `internal/pd/scheduler.go` | 311 | Reviewed |
| `internal/pd/move_tracker.go` | 210 | Reviewed |
| `internal/pd/forward.go` | 188 | Reviewed |
| `internal/pd/snapshot.go` | 151 | Reviewed |
| `internal/pd/apply.go` | 100 | Reviewed |
| `internal/pd/command.go` | 94 | Reviewed |
| `internal/pd/peer_service.go` | 103 | Reviewed |
| `internal/pd/id_buffer.go` | 79 | Reviewed |
| `internal/pd/tso_buffer.go` | 100 | Reviewed |
| `internal/pd/transport.go` | 146 | Reviewed |
| `pkg/pdclient/client.go` | 708 | Reviewed |
| `pkg/pdclient/mock.go` | 313 | Reviewed |
