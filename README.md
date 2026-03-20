# gookv

A Go-based distributed transactional key-value store modeled after [TiKV](https://github.com/tikv/tikv). It implements the TiKV wire protocol (kvproto) and provides MVCC-based transactions using the Percolator two-phase commit protocol.

<p align="center">
  <img src="gookv_logo.jpg" alt="gookv logo" />
</p>

## Project Overview

gookv reproduces the core architecture of TiKV in Go:

- **Codec layer** — order-preserving memcomparable key encoding
- **Storage engine** — Pebble-backed KV engine with column family emulation
- **MVCC** — multi-version concurrency control with snapshot isolation, read-committed support, and range scanner
- **Transaction layer** — Percolator 2PC, async commit, 1PC optimization, pessimistic transactions, transaction scheduler
- **MVCC garbage collection** — three-state GC worker for old version cleanup
- **Raw KV API** — non-transactional get/put/delete/scan/batch operations with TTL, compare-and-swap, and checksum support
- **Client library** — multi-region client (`pkg/client`) with automatic region routing, PD-backed store discovery, connection pooling, and retry logic
- **Raft consensus** — etcd/raft integration with region-based routing and inter-node transport
- **Raft lifecycle** — log compaction, snapshot generation/transfer/application, configuration changes (AddNode/RemoveNode), store worker for dynamic peer creation
- **Region management** — region split (PD-coordinated, size-based with midpoint key), region merge (prepare/commit/rollback)
- **Placement Driver (PD)** — TSO allocation, cluster metadata, store/region heartbeats, split scheduling, replica repair and leader balance scheduling, GC safe point centralization, multi-endpoint failover with retry
- **Coprocessor** — push-down execution with RPN expressions, table scan, selection, aggregation
- **gRPC server** — TiKV-compatible RPC interface via `pingcap/kvproto`
- **HTTP status server** — pprof, Prometheus metrics, health checks
- **Configuration** — TOML config with validation, CLI flag overrides
- **Logging** — structured logging with slow-log routing and file rotation
- **Flow control** — EWMA-based backpressure and memory quota
- **Admin CLI** — `gookv-ctl` for inspecting data, MVCC info, LSM compaction, SST file parsing, region listing

## Directory Structure

```
cmd/
  gookv-server/          # Server binary entry point
  gookv-ctl/             # Admin CLI (scan, get, mvcc, dump, size, compact, region, SST parsing)
  gookv-pd/              # Placement Driver server binary
pkg/                      # Public packages (importable by external code)
  codec/                  # Memcomparable byte/number encoding
  keys/                   # User key <-> internal key encoding (DataKey, RaftLogKey, etc.)
  cfnames/                # Column family constants (default, lock, write, raft)
  txntypes/               # Lock, Write, Mutation structs with binary serialization
  pdclient/               # Placement Driver client (gRPC + mock, multi-endpoint failover)
  client/                 # Multi-region client library (RawKVClient, RegionCache, PDStoreResolver)
internal/                 # Private implementation packages
  config/                 # TOML config loading, validation, ReadableSize/Duration types
  log/                    # Structured logging, LogDispatcher, SlowLogHandler, file rotation
  pd/                     # PD server (TSO allocator, metadata store, 16 gRPC RPCs)
  engine/
    traits/               # KvEngine, Snapshot, WriteBatch, Iterator interfaces
    rocks/                # Pebble-backed engine with CF emulation via key prefixing
  storage/
    mvcc/                 # MVCC key encoding, MvccTxn, MvccReader, PointGetter, Scanner
    txn/                  # Percolator actions (Prewrite, Commit, Rollback, CheckTxnStatus)
      latch/              # Deadlock-free key latch system
      concurrency/        # In-memory lock table + max_ts tracking
      scheduler/          # Transaction command dispatcher
      async_commit.go     # Async commit, 1PC optimization
      pessimistic.go      # Pessimistic lock acquire, upgrade, rollback
    gc/                   # MVCC garbage collection worker (three-state machine)
  raftstore/              # Raft peer loop, PeerStorage, message types
    router/               # Region-based message routing (sync.Map)
    split/                # Region split checker (size-based, midpoint key)
    raftlog_gc.go         # Raft log compaction (GC tick, background deletion)
    snapshot.go           # Raft snapshot generation, transfer, and application
    conf_change.go        # Raft configuration changes (AddNode, RemoveNode)
    merge.go              # Region merge (PrepareMerge, CommitMerge, RollbackMerge)
    store_worker.go       # Store worker utilities (CleanupRegionData)
  server/                 # gRPC server, TikvService implementation, Storage bridge
    coordinator.go        # StoreCoordinator (peer lifecycle, region bootstrap, PD-coordinated split)
    storage.go            # Transaction-aware storage bridge (2PC, async commit, 1PC, pessimistic, scan lock)
    raw_storage.go        # Raw KV storage (non-transactional API with TTL, CAS, checksum)
    pd_worker.go          # PD heartbeat worker (store/region heartbeats, split reporting)
    transport/            # Inter-node Raft transport, connection pooling, message batching
    status/               # HTTP status server (pprof, metrics, health, config)
    flow/                 # Flow control, EWMA backpressure, memory quota
  coprocessor/            # Push-down execution: RPN, TableScan, Selection, Aggregation
e2e/                      # End-to-end integration tests (cluster, PD, Raw KV, GC, etc.)
proto/                    # Reference .proto files from kvproto
design_docs/              # Architecture and design documents (00-08)
tasks/                    # Project tracking (todo.md)
```

## Dependencies

| Module | Purpose |
|--------|---------|
| `github.com/cockroachdb/pebble` v1.1.5 | Pure-Go KV store (no CGo), replaces RocksDB |
| `github.com/pingcap/kvproto` | TiKV protocol buffer definitions (pre-generated Go code) |
| `go.etcd.io/etcd/raft/v3` v3.5.17 | Raft consensus implementation |
| `google.golang.org/grpc` v1.59.0 | gRPC framework |
| `github.com/stretchr/testify` v1.11.1 | Test assertions |
| `github.com/BurntSushi/toml` v1.6.0 | TOML config parsing |
| `gopkg.in/natefinch/lumberjack.v2` v2.2.1 | Log file rotation |

Go version: 1.22.2
Module path: `github.com/ryogrid/gookv`

## Usage

For building, running, configuring, and testing gookv, see **[USAGE.md](USAGE.md)**.

## Programmatic Go Client Example
The `pkg/client` package provides a high-level client with automatic multi-region routing, PD-backed store discovery, and transparent retry on region errors.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ryogrid/gookv/pkg/client"
)

func main() {
	ctx := context.Background()

	// Connect via PD (handles region discovery and store resolution automatically)
	c, err := client.NewClient(ctx, client.Config{
		PDAddrs: []string{"127.0.0.1:2379"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	rawkv := c.RawKV()

	// Single-key operations
	if err := rawkv.Put(ctx, []byte("key1"), []byte("value1")); err != nil {
		log.Fatal(err)
	}

	val, notFound, err := rawkv.Get(ctx, []byte("key1"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: value=%s notFound=%v\n", val, notFound)

	// TTL support
	if err := rawkv.PutWithTTL(ctx, []byte("temp"), []byte("expires"), 60); err != nil {
		log.Fatal(err)
	}

	// Batch operations (transparently split across regions)
	pairs := []client.KvPair{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	}
	if err := rawkv.BatchPut(ctx, pairs); err != nil {
		log.Fatal(err)
	}

	// Cross-region scan
	results, err := rawkv.Scan(ctx, []byte("a"), []byte("z"), 100)
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range results {
		fmt.Printf("  %s = %s\n", kv.Key, kv.Value)
	}

	// Atomic compare-and-swap
	swapped, prev, err := rawkv.CompareAndSwap(ctx,
		[]byte("key1"), []byte("value2"), []byte("value1"), false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("CAS: swapped=%v prev=%s\n", swapped, prev)
}
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Client Library                                │
│          pkg/client: RawKVClient, RegionCache,                       │
│          PDStoreResolver, RegionRequestSender                        │
│          (multi-region routing, retry, connection pool)               │
├──────────────────────────────────────────────────────────────────────┤
│                          gRPC Server                                 │
│                   (tikvpb.TikvServer interface)                      │
│  Txn: KvGet, KvScan, KvPrewrite, KvCommit, Pessimistic, Resolve,    │
│       KvCheckSecondaryLocks, KvScanLock                              │
│  Raw: RawGet, RawPut, RawDelete, RawScan, Batch*, TTL, CAS, Cksum   │
│  Coprocessor, BatchCommands, Raft, BatchRaft, Snapshot, KvGC,         │
│  KvDeleteRange                                                        │
│  Region validation: validateRegionContext (leader, epoch, key range) │
├───────────────────┬──────────────────────────────────────────────────┤
│  HTTP Status      │            Flow Control                          │
│  (pprof, metrics, │  (EWMA backpressure, memory quota)               │
│   health, config) │                                                  │
├───────────────────┴──────────────────────────────────────────────────┤
│                       Storage Bridge                                 │
│          (latch acquisition, MVCC transaction management,            │
│           async commit, 1PC, Raw KV with TTL/CAS/checksum)           │
├──────────────┬────────────────┬──────────────────────────────────────┤
│ Transaction  │  Coprocessor   │          Raftstore                   │
│ - Percolator │  - RPN engine  │  - etcd/raft peers                   │
│ - Async      │  - TableScan   │  - PeerStorage                      │
│   commit/1PC │  - Selection   │  - Router + Transport                │
│ - Pessimistic│  - Aggregation │  - StoreCoordinator                  │
│ - Scheduler  │                │  - Raft log compaction               │
├──────────────┤                │  - Snapshot gen/transfer/apply       │
│    MVCC      │                │  - Store worker (dynamic peers)      │
│ - MvccReader │                │  - Conf change (Add/Remove)          │
│ - PointGetter│                │  - Region split (PD-coordinated)     │
│ - MvccTxn    │                │  - Region merge                      │
│ - Scanner    │                │    (prepare/commit/rollback)         │
├──────────────┤                ├──────────────────────────────────────┤
│   GC Worker  │                │       Placement Driver (PD)          │
│ - 3-state GC │                │  - TSO allocator                     │
│ - KvGC RPC   │                │  - Metadata store                    │
│              │                │  - Heartbeat processing              │
│              │                │  - Split scheduling                  │
│              │                │  - Scheduling (replica/leader)       │
│              │                │  - GC safe point                     │
│              │                │  - Multi-endpoint failover           │
├──────────────┴────────────────┴──────────────────────────────────────┤
│                    Engine (traits.KvEngine)                           │
│               Pebble backend with CF emulation                       │
│               (default, lock, write, raft)                           │
├──────────────────────────────────────────────────────────────────────┤
│        Config (TOML)     │     Logging (slog + lumberjack)           │
└──────────────────────────────────────────────────────────────────────┘
```

## Implemented User Stories

| ID | Component | Status |
|----|-----------|--------|
| IMPL-001 | Proto generation (kvproto integration) | Done |
| IMPL-002 | pkg/codec (memcomparable encoding) | Done |
| IMPL-003 | pkg/keys (key encoding) | Done |
| IMPL-004 | pkg/cfnames (column families) | Done |
| IMPL-005 | pkg/txntypes (Lock, Write, Mutation) | Done |
| IMPL-006 | internal/engine/traits (interfaces) | Done |
| IMPL-007 | internal/engine/rocks (Pebble backend) | Done |
| IMPL-008 | pkg/pdclient (PD client) | Done |
| IMPL-009 | internal/raftstore (Raft consensus) | Done |
| IMPL-010 | internal/storage/mvcc (MVCC) | Done |
| IMPL-011 | internal/storage/txn (transactions) | Done |
| IMPL-012 | internal/server (gRPC server) | Done |
| IMPL-013 | internal/config (configuration system) | Done |
| IMPL-014 | internal/log (logging and diagnostics) | Done |
| IMPL-015 | internal/server/transport (Raft transport) | Done |
| IMPL-016 | internal/server/status (HTTP status server) | Done |
| IMPL-017 | internal/server/flow (flow control) | Done |
| IMPL-018 | internal/coprocessor (push-down execution) | Done |
| IMPL-019 | internal/storage/txn (async commit and 1PC) | Done |
| IMPL-020 | internal/storage/txn (pessimistic transactions) | Done |
| IMPL-021 | cmd/gookv-ctl (admin CLI) | Done |
| IMPL-022 | WriteBatch SavePoint (copy-on-savepoint) | Done |
| IMPL-023 | Raw KV API (RawGet/Put/Delete/Scan/Batch) | Done |
| IMPL-024 | MVCC Scanner (range queries) | Done |
| IMPL-025 | TxnScheduler (command dispatcher) | Done |
| IMPL-026 | RPC wiring (pessimistic lock, resolve, heartbeat) | Done |
| IMPL-027 | Raft log compaction (GC tick, background deletion) | Done |
| IMPL-028 | Raft snapshot (SST export/ingest, async generation) | Done |
| IMPL-029 | Coprocessor gRPC integration | Done |
| IMPL-030 | MVCC GC worker (three-state machine, KvGC RPC) | Done |
| IMPL-031 | PD server (TSO, metadata, 16 gRPC RPCs) | Done |
| IMPL-032 | Raft configuration changes (AddNode/RemoveNode) | Done |
| IMPL-033 | PD integration (heartbeats, split reporting) | Done |
| IMPL-034 | Region split (size-based checker, batch split) | Done |
| IMPL-035 | Region merge (prepare/commit/rollback) | Done |
| IMPL-036 | CleanupRegionData (peer destruction cleanup) | Done |
| IMPL-037 | End-to-end integration test suite | Done |
| IMPL-038 | Async commit / 1PC gRPC path integration (KvPrewrite routing, PD-based TSO) | Done |
| IMPL-039 | KvCheckSecondaryLocks, KvScanLock RPCs | Done |
| IMPL-040 | Raw KV extensions (RawBatchScan, RawGetKeyTTL, RawCompareAndSwap, RawChecksum) | Done |
| IMPL-041 | Raw KV TTL support (per-key expiry encoding, automatic filtering) | Done |
| IMPL-042 | PD client multi-endpoint failover and retry | Done |
| IMPL-043 | PD-coordinated region split (split check tick, AskBatchSplit, child bootstrap) | Done |
| IMPL-044 | Region validation in gRPC handlers (validateRegionContext) | Done |
| IMPL-045 | Engine traits conformance test suite (17 test cases) | Done |
| IMPL-046 | Codec fuzz tests (6 fuzz targets) | Done |
| IMPL-047 | CLI improvements (compact with full LSM compaction, dump --sst for SST parsing) | Done |
| IMPL-048 | Client library for multi-region routing (pkg/client) | Done |
| IMPL-049 | Snapshot transfer (end-to-end streaming via gRPC) | Done |
| IMPL-050 | Store worker goroutine (dynamic peer creation/destruction) | Done |
| IMPL-051 | Significant messages (Unreachable, SnapshotStatus, MergeResult) | Done |
| IMPL-052 | GC safe point PD centralization (pdclient methods, KvGC integration) | Done |
| IMPL-053 | KvDeleteRange (ModifyTypeDeleteRange, gRPC handler, Raft serialization) | Done |
| IMPL-054 | PD scheduling (Scheduler, replica repair, leader balance, PDWorker delivery) | Done |

## Known Limitations

The following features are intentionally deferred or not yet fully connected. See `impl_doc/08_not_yet_implemented.md` for details.

### gRPC RPCs

| RPC | Status | Notes |
|-----|--------|-------|
| `BatchCoprocessor` | Stub | Only single-region `Coprocessor` and `CoprocessorStream` are implemented. Multi-region dispatch is not wired. |

### Client Library (`pkg/client`)

| Feature | Status | Notes |
|---------|--------|-------|
| TxnClient (2PC cross-region) | Not implemented | Only `RawKVClient` is provided. A transactional client with per-region prewrite/commit coordination is deferred to future design. |
| TSO batching | Not implemented | Each `GetTS` call opens a new stream. Batching multiple allocations into a single RPC is a low-priority optimization. |

### Placement Driver (PD)

| Feature | Status | Notes |
|---------|--------|-------|
| Periodic PD leader refresh | Not implemented | `Config.UpdateInterval` is defined but not consumed. The client relies on connection-time endpoint discovery and error-driven reconnection. |

### Raftstore

| Feature | Status | Notes |
|---------|--------|-------|
| Casual / Start messages | Not implemented | `PeerMsgTypeCasual` and `PeerMsgTypeStart` are defined but ignored in `handleMessage`. |

## Acknowledgments

The architecture and implementation of gookv were designed with reference to the [TiKV](https://github.com/tikv/tikv) source code. TiKV is licensed under the Apache License 2.0 — see [TiKV LICENSE](https://github.com/tikv/tikv/blob/de241946c52851ac996e1f1d1047a9d3c914f149/LICENSE).

## License

See the project root for license information.
