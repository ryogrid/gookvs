# Server/RPC Layer Code Review

**Reviewer**: Claude Opus 4.6 (1M context)
**Date**: 2026-03-27
**Scope**: `internal/server/` -- server.go, coordinator.go, pd_worker.go, pd_resolver.go, resolver.go, raftcmd.go, store_ident.go, flow/flow.go, status/status.go, transport/transport.go

---

## Summary

The server/RPC layer is well-structured with clear separation between gRPC service handlers (server.go), Raft coordination (coordinator.go), PD communication (pd_worker.go), and inter-node transport (transport/transport.go). The code follows a consistent dual-mode pattern (standalone direct-write vs. cluster Raft-propose) across all transaction RPCs. Region epoch validation, ReadIndex for linearizable reads, and key-range validation are applied systematically. There are several issues related to concurrency safety, resource lifecycle, error handling, and a correctness concern in the loopback Raft message routing.

---

## Critical Issues

### C1. Loopback sends route to wrong region for target peer (coordinator.go:770-783)

`sendRaftMessage` uses the *source* peer's `regionID` parameter when delivering loopback messages via `sc.router.Send(regionID, peerMsg)`. In a multi-region single-node setup, if peer A (region 1) sends a Raft message to peer B (region 2) on the same store, the message is routed to region 1's mailbox instead of region 2's. This only matters for cross-region Raft messages on the same node. In practice the `regionID` passed in is always the source region and the `msg.To` is a peer within that same region, so this is likely correct for normal operation. However, the same pattern is used in `HandleRaftMessage` where `msg.GetRegionId()` is the correct target -- the inconsistency should be verified.

**Verdict**: Not a bug in practice (Raft messages are always within a single region), but the parameter naming could be clearer. Low risk.

### C2. KVPessimisticRollback bypasses Raft in cluster mode (server.go:804-816)

`KVPessimisticRollback` calls `svc.server.storage.PessimisticRollbackKeys()` directly without the cluster-mode Raft propose path. Every other write RPC (Prewrite, Commit, BatchRollback, Cleanup, ResolveLock, PessimisticLock) computes modifications and proposes via Raft when a coordinator is present. This handler directly mutates storage, meaning pessimistic lock rollbacks are not replicated to followers.

**File**: `internal/server/server.go:804-816`

```go
func (svc *tikvService) KVPessimisticRollback(...) {
    // Missing: if coord := svc.server.coordinator; coord != nil { ... ProposeModifies ... }
    errs := svc.server.storage.PessimisticRollbackKeys(req.GetKeys(), startTS, forUpdateTS)
```

**Impact**: In a Raft cluster, pessimistic lock rollbacks are applied only to the leader's local engine, not replicated. On leader failover, stale pessimistic locks remain on followers, potentially blocking transactions.

### C3. KvTxnHeartBeat bypasses Raft in cluster mode (server.go:819-830)

Same pattern as C2. `KvTxnHeartBeat` calls `svc.server.storage.TxnHeartBeat()` directly. If TxnHeartBeat modifies lock TTL in the engine, that modification is not replicated via Raft.

**File**: `internal/server/server.go:819-830`

### C4. KvTxnHeartBeat and KVPessimisticRollback skip region validation (server.go:804-830)

Neither handler calls `validateRegionContext()`. In a multi-region cluster, requests for these RPCs could be processed by the wrong region leader without being rejected, leading to operations on keys that don't belong to the local region.

**File**: `internal/server/server.go:804` and `server.go:819`

### C5. ProposeModifies callback never invoked on error (coordinator.go:317-343)

`ProposeModifies` creates a `doneCh` and a `RaftCommand` with a callback that signals `doneCh`. If the Raft proposal fails internally (e.g., the peer drops the command because it's no longer leader, or the entry is rejected), the callback is never called. The caller then waits until the 10-second timeout, returning a timeout error instead of the actual failure. This also means the `doneCh` goroutine and channel leak for the duration of the timeout.

More critically, if `router.Send` succeeds but the peer loop drops the command (e.g., term change), the caller blocks until timeout with no way to learn the real reason.

**File**: `internal/server/coordinator.go:317-343`

### C6. Transport creates a new gRPC stream per Send call (transport.go:78-102)

`RaftClient.Send()` opens a new gRPC stream (`client.Raft(ctx)`), sends one message, then calls `CloseAndRecv()` for *every single Raft message*. This is extremely expensive -- stream setup involves HTTP/2 header negotiation. TiKV maintains long-lived streams and batches messages. With N regions each ticking at ~100ms, this could create hundreds of stream open/close operations per second per store pair.

**File**: `internal/server/transport/transport.go:78-102`

**Impact**: High latency for Raft message delivery, excessive connection overhead, potential performance degradation under load.

---

## Potential Issues

### P1. ReadPool.Stop() doesn't drain pending tasks (flow/flow.go:112-114)

`ReadPool.Stop()` closes `stopCh` which causes workers to exit, but any tasks currently queued in `taskCh` are abandoned. Additionally, `Submit()` on line 68 can block forever on `rp.taskCh <- func(){}` after `Stop()` is called if the channel is full and workers have exited. There is no mechanism to reject submissions after stop.

**File**: `internal/server/flow/flow.go:66-75, 112-114, 116-125`

### P2. ReadPool worker has a race between taskCh and stopCh (flow/flow.go:116-125)

When both `taskCh` and `stopCh` are ready, Go's `select` picks randomly. A stopped pool may still execute tasks from the channel after `Stop()` returns to the caller, which could lead to use-after-close scenarios if the tasks reference resources being torn down.

**File**: `internal/server/flow/flow.go:116-125`

### P3. Connection pool always uses index 0 (transport.go:269-301)

`connPool.get()` hardcodes `idx := 0`, ignoring the pool `size` parameter. The `RaftClientConfig.PoolSize` and `HashRegionForConn()` function exist but are never used. `newConnPool` is always called with size=1 on line 249, making the pool effectively single-connection despite the API suggesting otherwise.

**File**: `internal/server/transport/transport.go:249, 269-301`

### P4. PDStoreResolver TOCTOU between read and write (pd_resolver.go:52-75)

Two concurrent `ResolveStore` calls for the same storeID can both find the cache miss (line 57), both query PD, and both update the cache. The second write overwrites the first. This is benign (both should get the same address), but if PD returns different addresses during a store migration, the final cached value is non-deterministic.

**File**: `internal/server/pd_resolver.go:52-75`

### P5. FlowController.ShouldDrop uses non-seeded global rand (flow/flow.go:163)

`rand.Float64()` uses the global `math/rand` source. In Go versions before 1.20, this was seeded to 0 by default, making the "probabilistic" dropping deterministic. In Go 1.20+, global rand is auto-seeded, so this is fine. However, under high concurrency, the global rand has a mutex which could become a bottleneck.

**File**: `internal/server/flow/flow.go:163`

### P6. KvDeleteRange hardcodes region ID 1 (server.go:1541)

`KvDeleteRange` proposes to `region ID 1` regardless of the key range. If the range spans multiple regions, only region 1's Raft group processes it. The keys outside region 1's range would still be deleted locally (since `ApplyModifies` doesn't filter by region), but the deletion is only replicated to region 1's followers.

**File**: `internal/server/server.go:1541`

### P7. RawPut/RawDelete skip region validation (server.go:1194-1233)

`RawPut` and `RawDelete` in cluster mode resolve the region ID from the key but never call `validateRegionContext()`. This means no epoch check or leader check is performed before proposing. The `ProposeModifies` call does check `IsLeader()` but not the epoch from the client request.

**File**: `internal/server/server.go:1194-1233`

### P8. PDWorker.SetCoordinator has no synchronization (pd_worker.go:115-117)

`SetCoordinator` writes `w.coordinator` without any lock, while `sendStoreHeartbeat` reads `w.coordinator` concurrently (line 177). If `SetCoordinator` is called after `Run()`, this is a data race.

**File**: `internal/server/pd_worker.go:115-117`

### P9. peerTaskProcessorLoop spawns unbounded goroutines (pd_worker.go:290-306)

Each peer task spawns `go w.sendRegionHeartbeat(data)` (line 303) to avoid blocking the channel. Under heavy region heartbeat traffic, this can spawn thousands of concurrent goroutines, each making a PD RPC with a 5-second timeout. There is no rate limiting or concurrency cap.

**File**: `internal/server/pd_worker.go:303`

### P10. BatchCommands processes sub-commands serially (server.go:1601-1622)

`BatchCommands` processes each sub-command in the batch sequentially on line 1614. The whole point of `BatchCommands` is to amortize RPC overhead by processing multiple commands concurrently. Serial processing negates this benefit and adds latency proportional to the batch size.

**File**: `internal/server/server.go:1601-1622`

### P11. BatchCommands swallows errors from sub-command handlers (server.go:1630-1699)

In `handleBatchCmd`, errors returned by sub-command handlers (e.g., `svc.KvGet(ctx, cmd.Get)`) are silently discarded with `r, _ :=`. If a handler returns `(nil, err)`, the client receives a nil response for that sub-command with no indication of failure.

**File**: `internal/server/server.go:1630-1699`

### P12. Snapshot receiver accumulates all data in memory (server.go:1783-1815)

The `Snapshot` handler reads all chunks into a `bytes.Buffer` in memory before passing to `HandleSnapshotMessage`. For large regions, this could consume significant memory. TiKV streams snapshot data to disk.

**File**: `internal/server/server.go:1783-1815`

### P13. applyEntriesForPeer silently drops unmarshal failures (coordinator.go:202-204)

When `req.Unmarshal(entry.Data)` fails, the entry is silently skipped with `continue`. A corrupted or incompatible entry format would cause data loss without any indication. The slog.Warn on line 217 only fires for ApplyModifies failures, not unmarshal failures.

**File**: `internal/server/coordinator.go:202-204`

---

## Observations

1. **Consistent dual-mode pattern**: The standalone/cluster branching is well-implemented across all transaction RPCs. The "compute modifies, then propose" pattern cleanly separates MVCC logic from Raft replication.

2. **Region epoch validation is thorough**: `validateRegionContext` checks region existence, leader status, epoch version/confVer, and key-in-region -- all with proper structured error responses. The additional per-mutation key validation in `KvPrewrite` (lines 290-322) is a good defense-in-depth measure.

3. **ReadIndex usage is correct**: Read RPCs (KvGet, KvScan, KvBatchGet, KvCheckSecondaryLocks, KvScanLock) all perform ReadIndex before reading from the local engine, ensuring linearizable reads.

4. **Latch guard lifecycle**: The `defer svc.server.storage.ReleaseLatch(guard)` pattern is consistently applied across all write handlers, ensuring latches are released even on error paths.

5. **Code quality**: The codebase is well-commented with clear documentation of design decisions (e.g., the ReadIndex vs. leader lease comment on coordinator.go:241-245, the epoch-aware filtering explanation on coordinator.go:191-194).

6. **Error mapping**: `proposeErrorToRegionError` uses string matching on error messages (lines 1162-1191), which is fragile. Typed errors or error codes would be more robust.

7. **store_ident.go**: Clean and correct. File permissions 0644 are appropriate for a store identity file.

8. **raftcmd.go**: Bidirectional conversion between Modify and Request is symmetric and handles all three modify types (Put, Delete, DeleteRange).

9. **status.go**: Properly configured HTTP server with timeouts, graceful shutdown, and mutex-protected state. The pprof endpoints are correctly registered.

10. **Static resolver**: Properly uses RWMutex for concurrent read access with defensive copy in constructor.
