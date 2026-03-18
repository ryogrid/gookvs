# gookvs

A Go-based distributed transactional key-value store modeled after [TiKV](https://github.com/tikv/tikv). It implements the TiKV wire protocol (kvproto) and provides MVCC-based transactions using the Percolator two-phase commit protocol.

<p align="center">
  <img src="gookvs_logo.jpg" alt="gookvs logo" />
</p>

## Project Overview

gookvs reproduces the core architecture of TiKV in Go:

- **Codec layer** — order-preserving memcomparable key encoding
- **Storage engine** — Pebble-backed KV engine with column family emulation
- **MVCC** — multi-version concurrency control with snapshot isolation, read-committed support, and range scanner
- **Transaction layer** — Percolator 2PC, async commit, 1PC optimization, pessimistic transactions, transaction scheduler
- **MVCC garbage collection** — three-state GC worker for old version cleanup
- **Raw KV API** — non-transactional get/put/delete/scan/batch operations
- **Raft consensus** — etcd/raft integration with region-based routing and inter-node transport
- **Raft lifecycle** — log compaction, snapshot generation/application, configuration changes (AddNode/RemoveNode)
- **Region management** — region split (size-based with midpoint key), region merge (prepare/commit/rollback)
- **Placement Driver (PD)** — TSO allocation, cluster metadata, store/region heartbeats, split scheduling
- **Coprocessor** — push-down execution with RPN expressions, table scan, selection, aggregation
- **gRPC server** — TiKV-compatible RPC interface via `pingcap/kvproto`
- **HTTP status server** — pprof, Prometheus metrics, health checks
- **Configuration** — TOML config with validation, CLI flag overrides
- **Logging** — structured logging with slow-log routing and file rotation
- **Flow control** — EWMA-based backpressure and memory quota
- **Admin CLI** — `gookvs-ctl` for inspecting data, MVCC info, compaction, region listing

## Directory Structure

```
cmd/
  gookvs-server/          # Server binary entry point
  gookvs-ctl/             # Admin CLI (scan, get, mvcc, dump, size, compact, region)
  gookvs-pd/              # Placement Driver server binary
pkg/                      # Public packages (importable by external code)
  codec/                  # Memcomparable byte/number encoding
  keys/                   # User key <-> internal key encoding (DataKey, RaftLogKey, etc.)
  cfnames/                # Column family constants (default, lock, write, raft)
  txntypes/               # Lock, Write, Mutation structs with binary serialization
  pdclient/               # Placement Driver client (gRPC + mock)
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
    coordinator.go        # StoreCoordinator (peer lifecycle, region bootstrap, message dispatch)
    raw_storage.go        # Raw KV storage (non-transactional API)
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
Module path: `github.com/ryogrid/gookvs`

## Building

```bash
# Build all binaries
make build

# This produces:
#   ./gookvs-server
#   ./gookvs-ctl

# Build PD server separately
go build -o gookvs-pd ./cmd/gookvs-pd
```

## Running the Server

```bash
# Start with default configuration
./gookvs-server --data-dir /tmp/gookvs-data

# Start with a TOML config file
./gookvs-server --config gookvs.toml

# Override specific settings via CLI flags
./gookvs-server \
  --addr 127.0.0.1:20160 \
  --status-addr 127.0.0.1:20180 \
  --data-dir /var/lib/gookvs \
  --pd-endpoints 127.0.0.1:2379,127.0.0.1:2381
```

The server exposes:
- **gRPC** on `--addr` (default `127.0.0.1:20160`) — TiKV-compatible KV RPCs
- **HTTP** on `--status-addr` (default `127.0.0.1:20180`) — pprof, metrics, health

Shut down gracefully with `SIGINT` or `SIGTERM`.

## Running the PD Server

```bash
# Start the Placement Driver server
./gookvs-pd --addr 0.0.0.0:2379

# PD provides:
#   - TSO (timestamp oracle) allocation
#   - Cluster metadata management (store/region CRUD)
#   - Region scheduling (split ID allocation, heartbeat processing)
#   - GC safe point management
```

## Running a Cluster

gookvs supports running multiple nodes as a Raft cluster on a single machine. Data written to any leader node is replicated to all other nodes via Raft consensus.

### Quick Start (Makefile)

```bash
# Build and start a 5-node cluster
make cluster-start

# Wait a few seconds for leader election, then verify cross-node replication
make cluster-verify

# Stop the cluster and clean up data
make cluster-stop
```

### Cluster Ports

| Node | gRPC Port | Status Port | Data Directory |
|------|-----------|-------------|----------------|
| 1 | 20160 | 20180 | /tmp/gookvs-cluster/node1 |
| 2 | 20161 | 20181 | /tmp/gookvs-cluster/node2 |
| 3 | 20162 | 20182 | /tmp/gookvs-cluster/node3 |
| 4 | 20163 | 20183 | /tmp/gookvs-cluster/node4 |
| 5 | 20164 | 20184 | /tmp/gookvs-cluster/node5 |

### Manual Cluster Startup

```bash
# Start 5 nodes (run each in a separate terminal or background)
CLUSTER="1=127.0.0.1:20160,2=127.0.0.1:20161,3=127.0.0.1:20162,4=127.0.0.1:20163,5=127.0.0.1:20164"

./gookvs-server --store-id 1 --addr 127.0.0.1:20160 --status-addr 127.0.0.1:20180 \
  --data-dir /tmp/gookvs-cluster/node1 --initial-cluster $CLUSTER &

./gookvs-server --store-id 2 --addr 127.0.0.1:20161 --status-addr 127.0.0.1:20181 \
  --data-dir /tmp/gookvs-cluster/node2 --initial-cluster $CLUSTER &

./gookvs-server --store-id 3 --addr 127.0.0.1:20162 --status-addr 127.0.0.1:20182 \
  --data-dir /tmp/gookvs-cluster/node3 --initial-cluster $CLUSTER &

./gookvs-server --store-id 4 --addr 127.0.0.1:20163 --status-addr 127.0.0.1:20183 \
  --data-dir /tmp/gookvs-cluster/node4 --initial-cluster $CLUSTER &

./gookvs-server --store-id 5 --addr 127.0.0.1:20164 --status-addr 127.0.0.1:20184 \
  --data-dir /tmp/gookvs-cluster/node5 --initial-cluster $CLUSTER &
```

### Cross-Node Verification

The `make cluster-verify` target (or `go run scripts/cluster-verify.go`) performs:

1. Health check on all 5 nodes via HTTP status endpoints
2. Write a key-value pair to the leader node via gRPC (KvPrewrite + KvCommit)
3. Read the same key from a different (follower) node via gRPC (KvGet)
4. Verify the value matches, confirming Raft replication is working

### Cluster CLI Flags

| Flag | Description |
|------|-------------|
| `--store-id` | Unique store ID for this node (required for cluster mode) |
| `--initial-cluster` | Cluster topology: `storeID=addr,...` (e.g., `1=127.0.0.1:20160,2=127.0.0.1:20161`) |

## Using the Admin CLI

```bash
# Scan keys in the default column family
./gookvs-ctl scan --db /tmp/gookvs-data --cf default --limit 20

# Get a single key (hex-encoded)
./gookvs-ctl get --db /tmp/gookvs-data --cf default --key 68656c6c6f

# Show MVCC versions for a key
./gookvs-ctl mvcc --db /tmp/gookvs-data --key 68656c6c6f

# Dump raw key-value pairs (with optional --decode for human-readable output)
./gookvs-ctl dump --db /tmp/gookvs-data --cf write --limit 50 --decode

# Show approximate data size
./gookvs-ctl size --db /tmp/gookvs-data

# Trigger manual compaction
./gookvs-ctl compact --db /tmp/gookvs-data --cf default

# List regions
./gookvs-ctl region --db /tmp/gookvs-data
```

## Running Tests

```bash
# Run unit and integration tests
make test

# Run end-to-end tests
make test-e2e

# Run go vet
make vet

# Run tests for a specific package
go test ./internal/server/... -v -count=1
go test ./pkg/codec/... -v -count=1
```

### Test Summary

| Package | Tests | Description |
|---------|------:|-------------|
| `pkg/codec` | 23 | Byte/number encoding round-trips and ordering |
| `pkg/keys` | 16 | Key format, ranges, region ID extraction |
| `pkg/cfnames` | 3 | Column family constants |
| `pkg/txntypes` | 12 | Lock/Write serialization round-trips |
| `pkg/pdclient` | 16 | PD client operations (mock) |
| `internal/config` | 22 | Config loading, validation, diff, clone |
| `internal/log` | 22 | Log dispatch, slow log, file rotation |
| `internal/pd` | 13 | PD server (TSO, metadata, heartbeats) |
| `internal/engine/traits` | 3 | Interface compliance |
| `internal/engine/rocks` | 38 | CRUD, CF, snapshot, batch, iterator, bounds |
| `internal/storage/mvcc` | 38 | Multi-version reads, SI/RC isolation, scanner |
| `internal/storage/txn` | 45 | Percolator, async commit, 1PC, pessimistic txns |
| `internal/storage/txn/latch` | 11 | Latch acquire/release, concurrent safety |
| `internal/storage/txn/concurrency` | 7 | Lock table, max_ts tracking |
| `internal/storage/txn/scheduler` | 9 | Transaction command dispatcher |
| `internal/storage/gc` | 8 | MVCC garbage collection worker |
| `internal/raftstore` | 88 | Peer lifecycle, storage, conf change, snapshot, merge, log GC, cleanup |
| `internal/raftstore/router` | 14 | Concurrent message routing |
| `internal/raftstore/split` | 11 | Region split checker |
| `internal/server` | 99 | gRPC handlers, coordinator, raw KV, PD worker |
| `internal/server/transport` | 22 | Raft transport, connection pool, batching |
| `internal/server/status` | 13 | HTTP status endpoints |
| `internal/server/flow` | 25 | Flow control, EWMA, memory quota |
| `internal/coprocessor` | 45 | RPN, table scan, selection, aggregation |
| `cmd/gookvs-ctl` | 20 | Admin CLI commands |
| `e2e` | 35 | End-to-end: cluster, PD, Raw KV, GC, txn RPCs, region ops |
| **Total** | **601** | **27 packages, all passing** |

## Client-Server Verification

### Starting the Server

```bash
./gookvs-server --data-dir /tmp/gookvs-demo --addr 127.0.0.1:20160
```

### Using grpcurl

```bash
# List available services
grpcurl -plaintext 127.0.0.1:20160 list

# The server exposes the tikvpb.Tikv service with these RPCs:
#   Transactional: KvGet, KvScan, KvBatchGet, KvPrewrite, KvCommit,
#     KvBatchRollback, KvCleanup, KvCheckTxnStatus,
#     KvPessimisticLock, KVPessimisticRollback,
#     KvTxnHeartBeat, KvResolveLock, KvGC
#   Raw KV: RawGet, RawPut, RawDelete, RawScan,
#     RawBatchGet, RawBatchPut, RawBatchDelete, RawDeleteRange
#   Coprocessor: Coprocessor, CoprocessorStream
#   Raft: Raft, BatchRaft
#   Batch: BatchCommands (bidirectional streaming)
```

### Programmatic Go Client Example

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Connect to a running gookvs-server
	conn, err := grpc.Dial(
		"127.0.0.1:20160",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := tikvpb.NewTikvClient(conn)
	ctx := context.Background()

	// Phase 1: Prewrite
	prewriteResp, err := client.KvPrewrite(ctx, &kvrpcpb.PrewriteRequest{
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Op_Put, Key: []byte("hello"), Value: []byte("world")},
		},
		PrimaryLock:  []byte("hello"),
		StartVersion: 10,
		LockTtl:      5000,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Prewrite errors: %v\n", prewriteResp.GetErrors())

	// Phase 2: Commit
	commitResp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{[]byte("hello")},
		StartVersion:  10,
		CommitVersion: 20,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Commit error: %v\n", commitResp.GetError())

	// Read back
	getResp, err := client.KvGet(ctx, &kvrpcpb.GetRequest{
		Key:     []byte("hello"),
		Version: 30,
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Get: key=%s value=%s not_found=%v\n",
		"hello", string(getResp.GetValue()), getResp.GetNotFound())
}
```

```bash
# Expected output:
# Prewrite errors: []
# Commit error: <nil>
# Get: key=hello value=world not_found=false
```

### Checking Server Health

```bash
# Health endpoint
curl http://127.0.0.1:20180/status

# Prometheus metrics
curl http://127.0.0.1:20180/metrics

# Current config (JSON)
curl http://127.0.0.1:20180/config

# pprof
go tool pprof http://127.0.0.1:20180/debug/pprof/profile?seconds=10
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          gRPC Server                                 │
│                   (tikvpb.TikvServer interface)                      │
│  Txn: KvGet, KvScan, KvPrewrite, KvCommit, Pessimistic, Resolve     │
│  Raw: RawGet, RawPut, RawDelete, RawScan, Batch*                    │
│  Coprocessor, BatchCommands, Raft, BatchRaft, KvGC                   │
├───────────────────┬──────────────────────────────────────────────────┤
│  HTTP Status      │            Flow Control                          │
│  (pprof, metrics, │  (EWMA backpressure, memory quota)               │
│   health, config) │                                                  │
├───────────────────┴──────────────────────────────────────────────────┤
│                       Storage Bridge                                 │
│          (latch acquisition, MVCC transaction management,            │
│           engine coordination, Raw KV storage)                       │
├──────────────┬────────────────┬──────────────────────────────────────┤
│ Transaction  │  Coprocessor   │          Raftstore                   │
│ - Percolator │  - RPN engine  │  - etcd/raft peers                   │
│ - Async      │  - TableScan   │  - PeerStorage                      │
│   commit/1PC │  - Selection   │  - Router + Transport                │
│ - Pessimistic│  - Aggregation │  - StoreCoordinator                  │
│ - Scheduler  │                │  - Raft log compaction               │
├──────────────┤                │  - Snapshot gen/apply                │
│    MVCC      │                │  - Conf change (Add/Remove)          │
│ - MvccReader │                │  - Region split (size-based)         │
│ - PointGetter│                │  - Region merge                      │
│ - MvccTxn    │                │    (prepare/commit/rollback)         │
│ - Scanner    │                │                                      │
├──────────────┤                ├──────────────────────────────────────┤
│   GC Worker  │                │       Placement Driver (PD)          │
│ - 3-state GC │                │  - TSO allocator                     │
│ - KvGC RPC   │                │  - Metadata store                    │
│              │                │  - Heartbeat processing              │
│              │                │  - Split scheduling                  │
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
| IMPL-021 | cmd/gookvs-ctl (admin CLI) | Done |
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

## Acknowledgments

The architecture and implementation of gookvs were designed with reference to the [TiKV](https://github.com/tikv/tikv) source code. TiKV is licensed under the Apache License 2.0 — see [TiKV LICENSE](https://github.com/tikv/tikv/blob/de241946c52851ac996e1f1d1047a9d3c914f149/LICENSE).

## License

See the project root for license information.
