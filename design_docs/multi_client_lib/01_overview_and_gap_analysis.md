# Client Library for Multi-Region Routing — Overview and Gap Analysis

## 1. Problem Statement

gookv currently has no client-side region routing. All routing is handled server-side via `StoreCoordinator.ResolveRegionForKey()`. This works when every node holds every region, but fails in a distributed deployment where regions are spread across different stores. A proper client library must resolve `key → region → leader store → gRPC address` before sending any RPC.

## 2. TiKV Client Architecture Reference

TiKV's Go client (`tikv-client-go`) implements a layered routing stack:

```
Application
  └─ Transaction / RawKVClient  (high-level API)
       └─ RegionRequestSender   (retry + error handling)
            └─ RegionCache      (key → region → leader store)
                 ├─ PD Client   (GetRegion, GetStore, GetTS)
                 └─ StorePool   (gRPC connection management)
```

Key behaviors:
- **RegionCache** maps key ranges to `(Region, LeaderStore)` tuples, cached locally.
- **Error-driven invalidation**: `NotLeader` → update leader; `EpochNotMatch` → evict region; `StoreNotMatch` → refresh store address.
- **Batch splitting**: Multi-key requests are split per region and dispatched in parallel.
- **TSO batching**: Timestamps are fetched in batches and dispensed locally.

## 3. gookv Current State

### What exists

| Component | Location | Status |
|-----------|----------|--------|
| PD Client (`GetRegion`, `GetStore`, `GetTS`, etc.) | `pkg/pdclient/client.go` | Fully implemented with retry/failover |
| PD Mock Client | `pkg/pdclient/mock.go` | Fully implemented |
| Server-side `ResolveRegionForKey` | `internal/server/coordinator.go` | O(n) linear scan |
| Server-side `resolveRegionID` | `internal/server/server.go` | Used by all RPC handlers |
| `groupModifiesByRegion` / `proposeModifiesToRegions` | `internal/server/server.go` | Batch ops split per region |
| Proto region error types | `proto/errorpb.proto` | `NotLeader`, `RegionNotFound`, `EpochNotMatch`, `StoreNotMatch`, `KeyNotInRegion` |
| Response `region_error` field | `proto/kvrpcpb.proto` | Present on all KV RPC responses |
| Static Store Resolver (Raft transport) | `internal/server/resolver.go` | `StaticStoreResolver` for server-to-server |
| Raft transport connection pool | `internal/server/transport/transport.go` | `RaftClient` with lazy dial + pooling |

### What is missing

| Component | Description | Priority |
|-----------|-------------|----------|
| **RegionCache** | Client-side cache: key → (region, leader peer, store address). Epoch-aware invalidation. | P0 |
| **PDStoreResolver** | Dynamic `storeID → address` resolution via PD `GetStore`, with caching. | P0 |
| **RegionRequestSender** | Send RPC to correct store, handle region errors, retry with cache refresh. | P0 |
| **RawKVClient** | High-level Raw KV API: `Get`, `Put`, `Delete`, `Scan`, `BatchGet`, `BatchPut`, `BatchScan`, `CAS`, `Checksum`. Auto-routes per key. | P0 |
| **TxnClient** | High-level transaction API: `Begin`, `Get`, `Set`, `Delete`, `Commit`. 2PC with per-region prewrite/commit. | P1 |
| **Batch key grouping** | Group multi-key requests by region, dispatch in parallel, merge results. | P0 |
| **TSO batching** | Batch `GetTS` calls, dispense from local buffer. | P2 |

## 4. Scope for This Design

This design covers the **P0 items** — the minimum set needed for a functional multi-region client:

1. `RegionCache` — key-to-region-to-store resolution with caching and invalidation
2. `PDStoreResolver` — dynamic store address lookup
3. `RegionRequestSender` — RPC dispatch with error-driven retry
4. `RawKVClient` — high-level Raw KV API with automatic region routing
5. Batch key grouping for multi-key operations
6. E2E tests validating the full client→PD→store flow

TxnClient (P1) and TSO batching (P2) are deferred to a follow-up design.
