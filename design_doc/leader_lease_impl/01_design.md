# Leader Lease Read — Detailed Design

## 1. Problem Statement

Every read operation (KvGet, KvScan) performs a full ReadIndex Raft round-trip (~65ms), dominated by:
1. **`Propose(nil)`** in `handleReadIndexRequest()` — forces a no-op commit before ReadIndex (etcd/raft requires `committedEntryInCurrentTerm()`) — adds ~30ms
2. **ReadIndex heartbeat round-trip** — leader confirms quorum via heartbeat before returning ReadState — adds ~30ms
3. **Pebble WAL sync** — 8ms per handleReady iteration, limits event loop throughput

A valid leader lease allows reads to bypass BOTH the `Propose(nil)` AND the ReadIndex round-trip, serving reads directly from the local engine.

## 2. TiKV's Leader Lease Implementation

### 2.1 Lease States
TiKV uses a three-state lease:
- **Valid**: Leader can serve local reads without ReadIndex
- **Suspect**: Lease may be invalid (set during leader transfer); reads fall back to ReadIndex
- **Expired**: Lease has expired; reads fall back to ReadIndex

### 2.2 Lease Duration
```
max_lease = election_timeout - safety_margin
          = 10s - 1s = 9s (TiKV default)
```

The lease duration MUST be strictly less than the election timeout. If a new leader is elected, the old leader's lease will have expired before the new leader starts serving.

### 2.3 Lease Renewal
TiKV renews the lease when:
- An entry committed in the current term (any write or ReadIndex confirmation)
- The new expiry = `propose_time + max_lease`
- Proactive renewal via `CheckLeaderLeaseTick` when approaching expiry

### 2.4 Lease Invalidation
- **Leader stepdown**: Lease expires immediately
- **Leader transfer** (`MsgTimeoutNow`): Lease enters Suspect state
- **Prepare-merge**: Lease enters Suspect state
- **Split in progress**: Renewal suppressed
- **check_quorum failure**: `in_lease()` returns false → Suspect

### 2.5 Safety Guarantee
The invariant is: **if the old leader's lease is Valid, no new leader can have been elected yet.** This holds because:
1. New leader election requires election timeout to pass since last heartbeat
2. Old leader's lease expires BEFORE election timeout
3. etcd/raft's `CheckQuorum` ensures the leader steps down if quorum is lost

## 3. Current gookv Lease Infrastructure

### 3.1 What Exists (`internal/raftstore/peer.go`)

**Fields** (lines 148-149):
```go
leaseExpiryNanos atomic.Int64 // Unix nanoseconds
leaseValid       atomic.Bool
```

**IsLeaseValid()** (lines 377-388):
```go
func (p *Peer) IsLeaseValid() bool {
    if !p.leaseValid.Load() { return false }
    if time.Now().UnixNano() > p.leaseExpiryNanos.Load() {
        p.leaseValid.Store(false)
        return false
    }
    return true
}
```

**Lease extension in handleReady():**
- On leadership gain (line 663): `leaseExpiryNanos = now + electionTimeout * 4/5`
- On ReadState confirmation (line 774): same extension
- On leader stepdown (line 674): `leaseValid.Store(false)`

**Disabled in coordinator.ReadIndex()** (line 390):
```go
// Leader lease is implemented but disabled: the lease confirms leadership
// but does not guarantee that all committed Raft entries have been applied
// to the engine. ReadIndex (ReadOnlySafe) guarantees both leadership AND
// that appliedIndex >= readIndex, preventing stale reads.
```

### 3.2 What's Missing

1. **The `appliedIndex >= readIndex` check**: Lease confirms leadership but not that all committed data is applied. Without this check, a read could miss recently committed writes.
2. **Lease invalidation on ConfChange/Split**: Not explicitly handled (relies on timeout).
3. **Configuration flag** to enable/disable lease reads.
4. **Integration point in coordinator.ReadIndex()**: The bypass logic.

### 3.3 The appliedIndex Problem

The comment in coordinator.go identifies the core challenge: ReadIndex guarantees `appliedIndex >= readIndex` (the read sees all committed data). Lease only guarantees leadership. The solution: **check `appliedIndex >= committedIndex` before serving a lease read.**

If `appliedIndex < committedIndex`, there are unapplied entries. A lease read at this point could miss committed but not-yet-applied writes. The fix: only serve lease reads when `appliedIndex == committedIndex` (the state machine is fully caught up).

## 4. Proposed Design

### 4.1 Lease Read Flow

```
coordinator.ReadIndex(regionID, timeout):
  1. peer.IsLeader() check           // fast-fail if not leader
  2. peer.IsLeaseValid() check       // NEW: lease validity
  3. peer.IsAppliedCurrent() check   // NEW: appliedIndex >= committedIndex
  4. If 2 AND 3: return nil          // LEASE READ: no Raft round-trip!
  5. Else: fall through to ReadIndexBatcher (existing path)
```

### 4.2 New Method: `Peer.IsAppliedCurrent()`

**File**: `internal/raftstore/peer.go`

**IMPORTANT**: `rawNode.Status()` is NOT goroutine-safe — it reads Raft internal state without locks. We MUST NOT call it from the coordinator goroutine. Instead, add an atomic field updated by the peer goroutine:

```go
// In Peer struct:
committedIndex atomic.Uint64  // updated in handleReady() from peer goroutine only

// In handleReady(), after processing HardState:
if !raft.IsEmptyHardState(rd.HardState) {
    p.committedIndex.Store(rd.HardState.Commit)
}

// Safe to call from any goroutine:
func (p *Peer) IsAppliedCurrent() bool {
    return p.storage.AppliedIndex() >= p.committedIndex.Load()
}
```

Both `AppliedIndex()` (mutex-protected) and `committedIndex` (atomic) are safe for cross-goroutine access. Note: under the async apply path, `AppliedIndex` may lag behind `committedIndex` during write bursts, causing lease reads to fall back to ReadIndex more frequently. This is safe (conservative) but reduces lease hit rate during heavy writes.

### 4.3 Lease Duration Calculation & Election Timeout Prerequisite

**CRITICAL**: The current election timeout (10 ticks × 1ms = 10ms) is too close to the Pebble WAL sync latency (~8ms). This leaves only 2ms of safety margin, which can be consumed by timing imprecision in `time.Now()` placement within `handleReady()`. The lease renewal timestamp is set AFTER WAL sync completes, not when the heartbeat was actually sent.

**Prerequisite**: Increase `RaftElectionTimeoutTicks` to **50** (= 50ms with 1ms tick) before enabling lease reads. This provides:

```go
electionTimeout = 50 * 1ms = 50ms
leaseDuration   = 50ms * 4/5 = 40ms
heartbeat       = 2 * 1ms = 2ms (effective ~5ms due to handleReady cost)
```

With 50ms election timeout and 40ms lease:
- Safety margin = 50ms - 40ms = 10ms (> 8ms WAL sync)
- Heartbeat = 2ms (effective ~5ms) — leader detects quorum loss quickly
- Lease hit rate is high: 160ms is much longer than inter-request intervals

`RaftHeartbeatTicks` stays at 2, so heartbeat interval = 2 effective ticks = ~10ms.

### 4.4 Configuration Flag

**File**: `internal/server/coordinator.go` — `StoreCoordinatorConfig`

```go
type StoreCoordinatorConfig struct {
    // ...existing fields...
    EnableLeaseRead bool // default: true
}
```

When `EnableLeaseRead` is false, `coordinator.ReadIndex()` always falls through to ReadIndexBatcher (current behavior). The flag is on the coordinator config (not PeerConfig) because the lease read decision is a server-level concern, not a Raft-level one.

### 4.5 Lease Invalidation Points

The existing invalidation (leader stepdown + time expiry) is sufficient because:
- Lease duration (8ms) is much shorter than any ConfChange/Split/Merge operation
- ConfChange/Split trigger Raft state changes that naturally cause lease expiry
- `CheckQuorum` in etcd/raft ensures the leader steps down if quorum is lost

For additional safety, explicitly invalidate on:
- **ConfChange apply** (`applyConfChangeEntry`): `leaseValid.Store(false)`
- **Split apply** (`applySplitAdminEntry`): `leaseValid.Store(false)`

### 4.6 Eliminating Propose(nil)

With lease reads, `Propose(nil)` is no longer needed in the lease-valid path. For the fallback ReadIndex path, `Propose(nil)` is STILL needed (etcd/raft requires it). But the common case (lease valid) skips it entirely.

### 4.7 Integration with ReadIndexBatcher

Lease reads **bypass the batcher entirely**. The check happens in `coordinator.ReadIndex()` BEFORE the batcher is consulted:

```go
func (sc *StoreCoordinator) ReadIndex(regionID uint64, timeout time.Duration) error {
    peer := ...
    if !peer.IsLeader() { return "not leader" }

    // Lease read: bypass batcher and ReadIndex entirely.
    if sc.cfg.EnableLeaseRead && peer.IsLeaseValid() && peer.IsAppliedCurrent() {
        leaseReadTotal.Inc()
        return nil  // No readindexDuration observation on this path
    }

    // Fall through to ReadIndex via batcher.
    start := time.Now()
    defer func() { readindexDuration.Observe(time.Since(start).Seconds()) }()
    batcher := sc.getOrCreateBatcher(regionID, peer)
    return batcher.Wait(timeout)
}
```

Note: `readindexDuration` is NOT observed on the lease-read path to avoid distorting the histogram with ~0µs entries.

### 4.8 Lease Renewal — Only on Quorum Confirmation

The lease is renewed ONLY on:
1. **Leadership gain** (election win) — quorum voted for this leader
2. **ReadState confirmation** (ReadIndex response) — quorum confirmed leadership via heartbeat

**Lease is NOT renewed on committed entries.** Commit confirms that entries are replicated, but does NOT confirm continued leadership at the current moment. Renewing on commit could extend the lease into a period where the leader has lost quorum contact, creating a stale-read window.

This is consistent with TiKV's approach, which renews lease on heartbeat responses (quorum round-trips), not on commit acknowledgments.

## 5. Safety Analysis

### 5.1 Leader Transfer
- Old leader receives `MsgTimeoutNow` → stepdown → `leaseValid = false`
- Even without explicit handling, lease (40ms) expires before election timeout (50ms)
- New leader starts fresh with no lease → first reads use ReadIndex

### 5.2 Network Partition
- Leader loses quorum → heartbeats fail → `CheckQuorum` steps leader down → lease invalidated
- Lease (40ms) expires before election timeout (50ms), with 10ms safety margin (> 8ms WAL sync)
- Stale reads are impossible because the lease is shorter than the partition detection time

### 5.3 Region Split
- Split is a Raft admin entry → applied inline in `handleReady()`
- Explicit `leaseValid.Store(false)` in `applySplitAdminEntry()`
- New child regions start with no lease → use ReadIndex initially

### 5.4 Clock Drift
- gookv runs on a single machine (current deployment) → no clock drift
- For distributed deployment: lease uses `time.Now()` (monotonic on Linux) → safe
- Safety margin (20% of election timeout) provides additional buffer

### 5.5 Applied Lag
- `IsAppliedCurrent()` prevents stale reads from unapplied entries
- If apply is slow (Pebble WAL sync), reads fall back to ReadIndex
- Under normal load, apply catches up quickly and lease reads resume

## 6. Performance Expectations

| Scenario | Before | After |
|----------|--------|-------|
| Read with valid lease + applied current | ~65ms (ReadIndex) | **<1ms** (local read) |
| Read with valid lease + apply lag | ~65ms | ~65ms (fallback to ReadIndex) |
| Read after leader transfer | ~65ms | ~65ms (lease expired, ReadIndex) |
| Sustained read workload | ~65ms per read | **<1ms per read** (lease continuously renewed) |

## 7. Files Summary

| File | Change |
|------|--------|
| `internal/raftstore/peer.go` | Add `committedIndex atomic.Uint64` field, update in `handleReady()`. Add `IsAppliedCurrent()`. Explicit lease invalidation in `applyConfChangeEntry()` success paths and `applySplitAdminEntry()`. Increase `RaftElectionTimeoutTicks` to 200. |
| `internal/server/coordinator.go` | Add `EnableLeaseRead` to `StoreCoordinatorConfig`. Add lease read bypass in `ReadIndex()` (skip `readindexDuration` on lease path). Add `leaseReadTotal` metric increment. |
| `internal/server/metrics.go` | Add `leaseReadTotal` counter. |
