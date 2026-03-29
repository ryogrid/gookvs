# Unimplemented Features and Known Limitations

This document lists features and capabilities that are not yet implemented or have known limitations in gookv.

## Summary Table

| # | Category | Feature | Status | Impact |
|---|----------|---------|--------|--------|
| 1 | gRPC / Coprocessor | BatchCoprocessor | Not implemented | Low — single-region Coprocessor covers core functionality |
| 2 | Client Library | TSO batching | Not implemented | Low — optimization only |
| 3 | Raftstore | Streaming snapshot generation | Not implemented | Medium — OOM risk for large regions |
| 4 | Raftstore | Region epoch validation in handleScheduleMessage | Not implemented | Low — Raft's built-in rejection provides safety |
| 5 | PD | Store heartbeat capacity fields | Not implemented | Low — scheduling works without capacity data |
| 6 | PD | PD Raft dynamic membership change | Not implemented | Medium — requires full cluster restart for topology changes |
| 7 | Raftstore | Split key selection from dominant CF | Deferred | Low — splits may be slightly unbalanced |
| 8 | Raftstore | Leader lease read path | Not implemented | Medium — ReadIndex used instead (extra round-trip) |
| 9 | Server | Flow control integration | Not implemented | Low — `IsBusy()` always returns false |
| 10 | PD Client | `grpc.DialContext` + `WithBlock` migration | Deferred | Low — deprecated API, functionally correct |
| 11 | PD / Bootstrap | MaxPeerCount must match initial voter count | Known constraint | High — ConfChange churn destabilizes cluster if mismatched |

---

## 1. BatchCoprocessor

`BatchCoprocessor` is a server-streaming RPC that dispatches a coprocessor request across multiple regions in a single call. Only `Coprocessor` (unary) and `CoprocessorStream` (server-streaming, single region) are currently implemented. `BatchCoprocessor` falls through to `UnimplementedTikvServer`.

This is primarily a performance optimization for multi-region queries. The existing single-region RPCs cover all core functionality.

## 2. Client TSO Batching

The client library (`pkg/client/`) issues a `GetTS` RPC to PD for every transaction begin and every commit timestamp allocation. TiKV's client batches multiple `GetTS` requests into a single PD round-trip and dispenses timestamps from a local buffer.

Implementing this would reduce PD round-trips under high concurrency but is not required for correctness.

## 3. Streaming Snapshot Generation

The current snapshot implementation (`internal/raftstore/snapshot.go`) reads all region data into memory before serializing. For large regions (hundreds of MB), this can cause OOM.

TiKV uses streaming snapshot generation that reads and sends data in chunks. gookv should implement a similar chunked approach for production use with large datasets.

## 4. Region Epoch Validation in handleScheduleMessage

When the PD scheduler sends commands (TransferLeader, ChangePeer) to a peer via `handleScheduleMessage` (`internal/raftstore/peer.go`), the region epoch is not validated against the current region state. The implementation relies on Raft's built-in rejection of stale configuration changes.

Explicit epoch validation would catch stale commands earlier, but the current approach is safe — Raft will reject any command that conflicts with the current configuration.

## 5. Store Heartbeat Capacity Fields

The `StoreHeartbeat` RPC sends `pdpb.StoreStats` to PD, but the `Capacity`, `Available`, and `UsedSize` fields are not populated. PD's scheduling decisions (region balance, store placement) currently operate without disk capacity information.

These fields should query the underlying filesystem for accurate capacity reporting.

## 6. PD Raft Dynamic Membership Change

The PD cluster topology is fixed at startup via the `--initial-cluster` flag. All PD nodes must be configured with the same initial cluster map. There is no mechanism for adding or removing PD nodes at runtime.

To change the PD cluster topology, all PD nodes must be stopped and restarted with updated `--initial-cluster` flags. This is acceptable for the typical 3-or-5-node PD deployment but prevents online PD scaling.

KVS nodes support dynamic join via PD, but PD nodes themselves cannot be dynamically added or removed.

## 7. Split Key Selection from Dominant CF

The split check worker (`internal/raftstore/split/checker.go`) scans CF_DEFAULT, CF_WRITE, and CF_LOCK sequentially, picking the split key from whichever CF's accumulated size first reaches the half-size threshold. This can lead to slightly unbalanced splits when data is unevenly distributed across column families.

A more accurate approach would track per-CF sizes and select the split key from the dominant CF. This is deferred because the impact is low — splits may be slightly unbalanced but data integrity is unaffected.

## 8. Leader Lease Read Path

Leader lease is partially implemented: the lease is confirmed via Raft heartbeat responses and the expiry is tracked (`leaseExpiryNanos` in `peer.go`). However, the `appliedIndex >= commitIndex` check is missing, which means a lease-based read could return stale data if applied index lags behind.

The ReadIndex (ReadOnlySafe) path is used instead, which guarantees both leadership and data freshness at the cost of an additional Raft round-trip. Enabling the lease path requires adding the applied index check.

**Reference:** `internal/server/coordinator.go:294`

## 9. Flow Control Integration

`StoreCoordinator.IsBusy()` always returns `false`. This function is intended to signal backpressure when the store is under heavy load (e.g., too many pending proposals, high apply queue depth), but no flow control metrics are currently integrated.

**Reference:** `internal/server/coordinator.go:503`

## 10. `grpc.DialContext` + `WithBlock` Migration

Three sites in `pkg/pdclient/client.go` use the deprecated `grpc.DialContext` with `grpc.WithBlock()` for connection establishment. The modern `grpc.NewClient` API does not support blocking dial, requiring a redesign to non-blocking connection establishment with retry logic.

All other `grpc.Dial` call sites have been migrated to `grpc.NewClient`. These three sites remain because removing `WithBlock()` changes the connection establishment semantics and requires careful testing of PD client reconnection behavior.

## 11. MaxPeerCount Must Match Initial Voter Count

gookv bootstraps the initial region with **all nodes** listed in `--initial-cluster` as Raft voters. The PD scheduler's `MaxPeerCount` (default 3, configurable via `--max-peer-count`) controls how many replicas each region should have.

**Known constraint:** If `MaxPeerCount < number of initial voters`, the PD scheduler immediately begins removing excess peers via ConfChange to reduce the replica count. This causes:

- **Continuous ConfChange churn**: confVer keeps incrementing as peers are added/removed
- **Leadership instability**: Raft leaders may step down during ConfChange transitions
- **Client routing failures**: clients receive "not leader" / "epoch not match" errors as cached region metadata becomes stale faster than retries can converge
- **Potential data loss on restart**: newly added peers (via ConfChange) may not have received snapshots before the cluster destabilizes

**Workaround:** Always set `--max-peer-count` equal to the number of KVS nodes when all nodes are initial voters. The e2elib (`pkg/e2elib/cluster.go`) automatically sets `MaxPeerCount = NumNodes` for test clusters.

**Proper fix (not yet implemented):** Bootstrap the initial region with only `min(NumNodes, MaxPeerCount)` voters, and let the remaining nodes join as empty stores that PD can schedule regions to. This matches TiKV's architecture where the initial Raft group has 3 members regardless of cluster size.
