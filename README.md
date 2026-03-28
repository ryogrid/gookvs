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
- **Placement Driver (PD)** — TSO allocation, cluster metadata, store/region heartbeats, split scheduling, replica repair and leader balance scheduling, GC safe point centralization, multi-endpoint failover with retry, Raft-replicated multi-node PD cluster for high availability
- **Dynamic node addition** — join mode for adding new KVS nodes to a running cluster via PD, with automatic region rebalancing (balance scheduler, excess replica shedding, 3-step move protocol)
- **Performance optimizations** — RaftLogWriter (cross-region I/O coalescing, 1 fsync per batch), ApplyWorkerPool (async apply pipeline), NoSync apply (Raft log replay guarantees durability)
- **Standalone mode** — single-node operation without PD for development and testing
- **E2E test library** — `pkg/e2elib` provides PostgreSQL TAP-style test helpers (GokvNode, PDNode, GokvCluster, PDCluster, PortAllocator) with 75+ external-binary-based tests
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
  gookv-ctl/             # Admin CLI (scan, get, mvcc, dump, size, compact, region, SST parsing, store list/status)
  gookv-pd/              # Placement Driver server binary
pkg/                      # Public packages (importable by external code)
  codec/                  # Memcomparable byte/number encoding
  keys/                   # User key <-> internal key encoding (DataKey, RaftLogKey, etc.)
  cfnames/                # Column family constants (default, lock, write, raft)
  txntypes/               # Lock, Write, Mutation structs with binary serialization
  pdclient/               # Placement Driver client (gRPC + mock, multi-endpoint failover)
  client/                 # Multi-region client library (RawKVClient, TxnKVClient, RegionCache, LockResolver)
  e2elib/                 # E2E test library (GokvNode, PDNode, GokvCluster, PDCluster, PortAllocator)
internal/                 # Private implementation packages
  config/                 # TOML config loading, validation, ReadableSize/Duration types
  log/                    # Structured logging, LogDispatcher, SlowLogHandler, file rotation
  pd/                     # PD server (TSO allocator, metadata store, 16 gRPC RPCs, Raft replication for HA, move tracker)
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
    raft_log_writer.go    # Cross-region Raft log batch writer (I/O coalescing)
    apply_worker.go       # Async apply worker pool (propose-apply pipeline)
    router/               # Region-based message routing (sync.Map) + peer-to-region mapping
    split/                # Region split checker (size-based, midpoint key)
    raftlog_gc.go         # Raft log compaction (GC tick, background deletion)
    snapshot.go           # Raft snapshot generation, transfer, and application
    conf_change.go        # Raft configuration changes (AddNode, RemoveNode)
    merge.go              # Region merge (PrepareMerge, CommitMerge, RollbackMerge)
    store_worker.go       # Store worker utilities (CleanupRegionData)
  server/                 # gRPC server, TikvService implementation, Storage bridge
    coordinator.go        # StoreCoordinator (peer lifecycle, region bootstrap, PD-coordinated split, snapshot send semaphore)
    pd_resolver.go        # PD-based store resolver for dynamic node discovery
    store_ident.go        # Store identity persistence for join mode
    storage.go            # Transaction-aware storage bridge (2PC, async commit, 1PC, pessimistic, scan lock)
    raw_storage.go        # Raw KV storage (non-transactional API with TTL, CAS, checksum)
    pd_worker.go          # PD heartbeat worker (store/region heartbeats, split reporting)
    transport/            # Inter-node Raft transport, connection pooling, message batching
    status/               # HTTP status server (pprof, metrics, health, config)
    flow/                 # Flow control, EWMA backpressure, memory quota
  coprocessor/            # Push-down execution: RPN, TableScan, Selection, Aggregation
e2e/                      # Internal e2e tests (Raft cluster, GC, region merge)
e2e_external/             # External-binary e2e tests (75+ tests using pkg/e2elib)
```

## Dependencies

| Module | Purpose |
|--------|---------|
| `github.com/cockroachdb/pebble` v1.1.5 | Pure-Go KV store (no CGo), replaces RocksDB |
| `github.com/pingcap/kvproto` | TiKV protocol buffer definitions (pre-generated Go code) |
| `go.etcd.io/etcd/raft/v3` v3.5.17 | Raft consensus implementation |
| `google.golang.org/grpc` v1.79.3 | gRPC framework (`grpc.NewClient` API) |
| `github.com/stretchr/testify` v1.11.1 | Test assertions |
| `github.com/BurntSushi/toml` v1.6.0 | TOML config parsing |
| `gopkg.in/natefinch/lumberjack.v2` v2.2.1 | Log file rotation |

Go version: 1.25.0
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

	// --- Transactional KV (cross-region 2PC) ---
	txnkv := c.TxnKV()

	txn, err := txnkv.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}

	txn.Set(ctx, []byte("account:A"), []byte("900"))
	txn.Set(ctx, []byte("account:B"), []byte("1100"))

	if err := txn.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Transaction committed")
}
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Client Library                                │
│          pkg/client: RawKVClient, TxnKVClient, RegionCache,           │
│          PDStoreResolver, RegionRequestSender, LockResolver           │
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
│ - Scheduler  │                │  - RaftLogWriter (I/O coalescing)    │
├──────────────┤                │  - ApplyWorkerPool (async apply)     │
│    MVCC      │                │  - Raft log compaction               │
│ - MvccReader │                │  - Snapshot gen/transfer/apply       │
│ - PointGetter│                │  - Store worker (dynamic peers)      │
│ - MvccTxn    │                │  - Conf change (Add/Remove)          │
│ - Scanner    │                │  - Region split (PD-coordinated)     │
│              │                │  - Region merge                      │
├──────────────┤                ├──────────────────────────────────────┤
│   GC Worker  │                │       Placement Driver (PD)          │
│ - 3-state GC │                │  - TSO allocator                     │
│ - KvGC RPC   │                │  - Metadata store                    │
│              │                │  - Heartbeat processing              │
│              │                │  - Split scheduling                  │
│              │                │  - Scheduling (replica/leader)       │
│              │                │  - GC safe point                     │
│              │                │  - Multi-endpoint failover           │
│              │                │  - Raft replication (multi-node HA)  │
├──────────────┴────────────────┴──────────────────────────────────────┤
│                    Engine (traits.KvEngine)                           │
│       Pebble backend with CF emulation (default, lock, write, raft)  │
│       Commit() = fsync (Raft log)  |  CommitNoSync() (apply path)    │
├──────────────────────────────────────────────────────────────────────┤
│        Config (TOML)     │     Logging (slog + lumberjack)           │
└──────────────────────────────────────────────────────────────────────┘
```

## Known Limitations

See `gookv-design/10_not_yet_implemented.md` for full details.

| Category | Feature | Notes |
|----------|---------|-------|
| gRPC | BatchCoprocessor | Stub only; single-region Coprocessor works |
| Client | TSO batching | Each GetTS is a separate RPC; low-priority optimization |
| Raftstore | Streaming snapshot generation | Holds all data in memory; OOM risk for large regions |
| Raftstore | Leader lease read path | `appliedIndex >= commitIndex` check missing; ReadIndex used instead |
| Raftstore | Split key selection from dominant CF | Deferred; low impact |
| PD | Dynamic membership change | Fixed topology at startup; requires restart to change |
| PD | Store heartbeat capacity fields | Capacity/Available/UsedSize not populated |
| Server | Flow control | `IsBusy()` always returns false |

## Acknowledgments

The architecture and implementation of gookv were designed with reference to the [TiKV](https://github.com/tikv/tikv) source code. TiKV is licensed under the Apache License 2.0 — see [TiKV LICENSE](https://github.com/tikv/tikv/blob/de241946c52851ac996e1f1d1047a9d3c914f149/LICENSE).

## License

See the project root for license information.
