# 07. Multi-Region Routing

## 1. Overview

In a single-region deployment every key maps to region ID 1 and every RPC
handler can hardcode that value. Once the keyspace is split across multiple
regions each write must land on the Raft group that owns the key range
containing the target key. This document specifies how gookv routes RPCs to
the correct region and how split events keep the routing table consistent.

### Why multi-region routing is needed

| Problem | Impact without routing |
|---------|-----------------------|
| Prewrite spans two regions | Mutations outside the leader's key range are silently dropped or proposed to the wrong Raft log |
| Commit reaches a region that doesn't hold the lock | Lock lookup fails; transaction hangs |
| Batch Raw KV writes cross a boundary | Half the keys are written, the other half error with "not leader" |
| Region split creates a child region | Subsequent writes to the right half still target the parent, which no longer owns those keys |

Multi-region routing solves all of the above by resolving keys to region IDs
at the RPC handler level and, for batch operations, grouping modifications by
region before proposing them through Raft.

---

## 2. Region Routing Architecture

The routing layer lives in `internal/server/server.go` (the `tikvService`
receiver) and `internal/server/coordinator.go` (`StoreCoordinator`).

### 2.1 `resolveRegionID(key []byte) uint64`

**Location:** `tikvService` method in `server.go`.

Looks up the coordinator's `ResolveRegionForKey` to find the region whose
`[StartKey, EndKey)` range contains `key`. Falls back to region ID 1 when no
coordinator is configured (standalone mode).

```
resolveRegionID(key)
  └─ coord.ResolveRegionForKey(key)
       └─ linear scan over sc.peers map
            └─ peer.Region().StartKey <= key < peer.Region().EndKey
```

The coordinator holds the authoritative in-memory copy of each region's
metadata via `peer.Region()`. This is updated atomically on split
(`peer.UpdateRegion`).

### 2.2 `ResolveRegionForKey(key []byte) uint64`

**Location:** `StoreCoordinator` method in `coordinator.go`.

Acquires `sc.mu.RLock`, iterates `sc.peers`, and performs a byte-range
comparison for each region. Returns 0 if no region matches.

**Complexity:** O(n) where n is the number of regions on this store. For
production workloads a B-tree or interval-tree index would be preferable, but
the linear scan is correct and sufficient for the current scale.

### 2.3 `groupModifiesByRegion(modifies []mvcc.Modify) map[uint64][]mvcc.Modify`

**Location:** `tikvService` method in `server.go`.

For batch operations (`RawBatchPut`, `RawBatchDelete`) the handler builds
a slice of `mvcc.Modify` entries then calls `groupModifiesByRegion` to
partition them by region ID. Each group is independently proposed via
`ProposeModifies`.

```
modifies ──► groupModifiesByRegion ──► { regionA: [m1,m2], regionB: [m3] }
                                              │                    │
                                    ProposeModifies(A,...)  ProposeModifies(B,...)
```

### 2.4 `proposeModifiesToRegions(coord, modifies, timeout) error`

**Location:** `tikvService` method in `server.go`.

Convenience wrapper: calls `groupModifiesByRegion` then iterates the groups,
calling `coord.ProposeModifies` for each. Returns the first error encountered.

---

## 3. Per-RPC Routing Strategy

Each RPC handler picks a routing key based on the semantics of the operation.
The table below summarizes the current implementation.

| RPC | Routing key | Rationale |
|-----|-------------|-----------|
| **KvPrewrite** | `primary` (PrimaryLock) | All mutations in a prewrite belong to the same transaction; the primary key determines the region. In the real TiKV model the client splits mutations per region before sending. In gookv the server routes by primary key. |
| **KvCommit** | `keys[0]` (first key) | The commit message carries the list of keys that were pre-written. The first key selects the region. |
| **KvPessimisticLock** | `primary` (PrimaryLock) | Same as prewrite: the primary key anchors the pessimistic lock group. |
| **KvResolveLock** | `keys[0]` if present, else fallback to region 1 | ResolveLock may be broadcast (no explicit keys) or targeted. When keys are provided the first one selects the region; otherwise the handler falls back to region 1 for a full-store scan. |
| **RawPut** | `key` (single key) | Single-key operation; route directly. |
| **RawDelete** | `key` (single key) | Single-key operation; route directly. |
| **RawBatchPut** | Split by region via `proposeModifiesToRegions` | Each key-value pair is resolved individually and grouped by region. All groups are proposed in sequence. |
| **RawBatchDelete** | Split by region via `proposeModifiesToRegions` | Same as RawBatchPut but with delete modifies. |

### 3.1 KvPrewrite detail

The three prewrite paths (standard 2PC, async commit, 1PC) all use the same
routing call:

```go
regionID := svc.resolveRegionID(primary)
coord.ProposeModifies(regionID, modifies, 10*time.Second)
```

All mutations in the request are proposed to the primary key's region. This
is correct for the common case where the client has already split mutations
by region. When mutations span multiple regions the server-side approach
still works because all nodes share the same underlying storage engine; the
Raft proposal ensures ordering and replication, and the resolved region's
leader will apply all entries.

### 3.2 KvCommit detail

```go
regionID := svc.resolveRegionID(keys[0])
coord.ProposeModifies(regionID, modifies, 10*time.Second)
```

The commit handler computes lock-resolution modifies for all keys, then
routes the batch to the first key's region. This assumes all keys in a
single commit call belong to the same region (the client partitions commit
calls per region in production).

### 3.3 KvResolveLock detail

```go
regionID := uint64(1)
if reqKeys := req.GetKeys(); len(reqKeys) > 0 {
    regionID = svc.resolveRegionID(reqKeys[0])
}
coord.ProposeModifies(regionID, modifies, 10*time.Second)
```

When no explicit keys are given (broadcast resolve), the handler defaults to
region 1. This is a known limitation; a correct broadcast implementation
would iterate all regions and resolve locks on each.

---

## 4. Split Integration

When a region splits the routing table must be updated so that new writes
target the correct child region.

### 4.1 Split flow (coordinator)

The split flow in `StoreCoordinator.handleSplitCheckResult`:

1. `SplitCheckWorker` detects region size exceeds threshold, produces
   `SplitCheckResult{RegionID, SplitKey}`.
2. Coordinator asks PD for new region/peer IDs via `AskBatchSplit`.
3. `split.ExecBatchSplit` creates child `metapb.Region` objects.
4. **`peer.UpdateRegion(splitResult.Derived)`** -- the parent region's
   metadata is updated in place. Its `EndKey` is now the split key, so
   `ResolveRegionForKey` immediately stops matching keys >= splitKey to the
   parent.
5. **`coord.BootstrapRegion(newRegion, raftPeers)`** -- the child region is
   created, a new Peer is inserted into `sc.peers`, and it is registered in
   the Router. `ResolveRegionForKey` now matches keys in `[splitKey, oldEndKey)` to the child.
6. `ReportBatchSplit` notifies PD of both regions.

### 4.2 Atomicity

Steps 4 and 5 both execute under `sc.mu.Lock` (BootstrapRegion acquires the
lock). Between UpdateRegion and BootstrapRegion there is a brief window where
keys in the right half don't match any region (`ResolveRegionForKey` returns
0, falling back to region 1). In practice this is harmless because:

- UpdateRegion is a single pointer assignment on the parent peer.
- BootstrapRegion immediately follows.
- The coordinator's mutex serializes all routing lookups during this window.

### 4.3 Example

Before split: Region 5 covers `["", "")` (entire keyspace).

```
ResolveRegionForKey("alpha") → 5
ResolveRegionForKey("zebra") → 5
```

After split at "m": Region 5 covers `["", "m")`, Region 8 covers `["m", "")`.

```
ResolveRegionForKey("alpha") → 5
ResolveRegionForKey("zebra") → 8
```

---

## 5. Limitations and Future Work

### 5.1 Linear scan routing

`ResolveRegionForKey` iterates all peers in a map. This is O(n) per lookup.
For clusters with thousands of regions an ordered data structure (e.g.
`sync.Map` backed by a sorted slice or B-tree keyed on region start key)
would reduce this to O(log n).

### 5.2 Single-region proposal for multi-key RPCs

KvPrewrite and KvCommit propose all mutations to a single region. This works
because all nodes share the same storage engine, but it means the Raft log of
one region carries entries for keys outside its range. A more correct
implementation would split mutations by region (like `proposeModifiesToRegions`
does for Raw KV batch operations) and propose each group independently.

### 5.3 Broadcast ResolveLock

When `ResolveLockRequest.Keys` is empty the handler defaults to region 1
instead of iterating all regions. A proper implementation would fan out to all
region leaders.

### 5.4 Stale routing after merge

Region merge (absorbing a sibling) updates the target region's metadata via
`ExecCommitMerge` but the source region's peer is destroyed asynchronously.
Between destruction and the routing table update, lookups for keys in the
source range may fail. Adding a forwarding entry or tombstone would eliminate
this window.

### 5.5 Client-side region cache

In TiKV the client maintains a region cache and routes requests before they
reach the server. gookv currently relies entirely on server-side routing. A
future `RegionCache` in `pkg/pdclient` would reduce round-trips by letting
the client pre-split batch requests.
