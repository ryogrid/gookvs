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

## Building

```bash
# Build all binaries
make build

# This produces:
#   ./gookv-server
#   ./gookv-ctl
#   ./gookv-pd
```

## Running the Server

```bash
# Start with default configuration
./gookv-server --data-dir /tmp/gookv-data

# Start with a TOML config file
./gookv-server --config gookv.toml

# Override specific settings via CLI flags
./gookv-server \
  --addr 127.0.0.1:20160 \
  --status-addr 127.0.0.1:20180 \
  --data-dir /var/lib/gookv \
  --pd-endpoints 127.0.0.1:2379,127.0.0.1:2381
```

The server exposes:
- **gRPC** on `--addr` (default `127.0.0.1:20160`) — TiKV-compatible KV RPCs
- **HTTP** on `--status-addr` (default `127.0.0.1:20180`) — pprof, metrics, health

Shut down gracefully with `SIGINT` or `SIGTERM`.

## Running the PD Server

```bash
# Start the Placement Driver server
./gookv-pd --addr 0.0.0.0:2379

# PD provides:
#   - TSO (timestamp oracle) allocation
#   - Cluster metadata management (store/region CRUD)
#   - Region scheduling (split ID allocation, heartbeat processing)
#   - GC safe point management
```

## Logging

gookv uses Go's `log/slog` structured logging with file rotation via lumberjack.

### CLI Flags

| Flag | Description |
|------|-------------|
| `--log-level` | Log level: `debug`, `info`, `warn`, `error` (overrides config/default) |
| `--log-file` | Log file path (overrides config/default) |

### Examples

```bash
# Enable debug logging (includes gRPC call traces)
./gookv-server --data-dir /tmp/gookv-data --log-level debug

# Write logs to a custom file
./gookv-server --data-dir /tmp/gookv-data --log-file /var/log/gookv/server.log

# PD server with debug logging
./gookv-pd --addr 0.0.0.0:2379 --log-level debug

# PD server with custom log file
./gookv-pd --addr 0.0.0.0:2379 --log-file /var/log/gookv/pd.log
```

### TOML Configuration (gookv-server)

```toml
[log]
level = "info"       # debug, info, warn, error
format = "text"      # text or json

[log.file]
filename = "server.log"   # relative to <data-dir>/log/
max-size = 300            # MB per file before rotation
max-backups = 5
max-days = 28
```

CLI flags (`--log-level`, `--log-file`) override TOML settings.

### Default Log Paths

| Binary | Default log path |
|--------|-----------------|
| `gookv-server` | `<data-dir>/log/server.log` |
| `gookv-pd` | `<data-dir>/log/pd.log` |

## Running a Cluster

gookv supports running multiple nodes as a Raft cluster on a single machine. Data written to any leader node is replicated to all other nodes via Raft consensus.

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
| 1 | 20160 | 20180 | /tmp/gookv-cluster/node1 |
| 2 | 20161 | 20181 | /tmp/gookv-cluster/node2 |
| 3 | 20162 | 20182 | /tmp/gookv-cluster/node3 |
| 4 | 20163 | 20183 | /tmp/gookv-cluster/node4 |
| 5 | 20164 | 20184 | /tmp/gookv-cluster/node5 |

### Manual Cluster Startup

```bash
# Start 5 nodes (run each in a separate terminal or background)
CLUSTER="1=127.0.0.1:20160,2=127.0.0.1:20161,3=127.0.0.1:20162,4=127.0.0.1:20163,5=127.0.0.1:20164"

./gookv-server --store-id 1 --addr 127.0.0.1:20160 --status-addr 127.0.0.1:20180 \
  --data-dir /tmp/gookv-cluster/node1 --initial-cluster $CLUSTER &

./gookv-server --store-id 2 --addr 127.0.0.1:20161 --status-addr 127.0.0.1:20181 \
  --data-dir /tmp/gookv-cluster/node2 --initial-cluster $CLUSTER &

./gookv-server --store-id 3 --addr 127.0.0.1:20162 --status-addr 127.0.0.1:20182 \
  --data-dir /tmp/gookv-cluster/node3 --initial-cluster $CLUSTER &

./gookv-server --store-id 4 --addr 127.0.0.1:20163 --status-addr 127.0.0.1:20183 \
  --data-dir /tmp/gookv-cluster/node4 --initial-cluster $CLUSTER &

./gookv-server --store-id 5 --addr 127.0.0.1:20164 --status-addr 127.0.0.1:20184 \
  --data-dir /tmp/gookv-cluster/node5 --initial-cluster $CLUSTER &
```

### Cross-Node Verification

The `make cluster-verify` target (or `go run scripts/cluster-verify/main.go`) performs:

1. Health check on all 5 nodes via HTTP status endpoints
2. Write a key-value pair to the leader node via gRPC (KvPrewrite + KvCommit)
3. Read the same key from a different (follower) node via gRPC (KvGet)
4. Verify the value matches, confirming Raft replication is working

### Cluster CLI Flags

| Flag | Description |
|------|-------------|
| `--store-id` | Unique store ID for this node (required for cluster mode) |
| `--initial-cluster` | Cluster topology: `storeID=addr,...` (e.g., `1=127.0.0.1:20160,2=127.0.0.1:20161`) |

## Running a Cluster with PD

A full production-like cluster consists of a PD (Placement Driver) server for cluster coordination plus multiple gookv-server nodes connected to it.

### Quick Start (Makefile)

```bash
# Build and start PD + 5-node cluster
make pd-cluster-start

# Wait a few seconds for leader election, then verify cross-node replication
make pd-cluster-verify

# Stop all processes and clean up data
make pd-cluster-stop
```

### PD Cluster Ports

| Component | Address | Description |
|-----------|---------|-------------|
| PD | 127.0.0.1:2379 | Placement Driver (TSO, metadata, scheduling) |
| Node 1 | 127.0.0.1:20160 (gRPC), :20180 (status) | KV server |
| Node 2 | 127.0.0.1:20161 (gRPC), :20181 (status) | KV server |
| Node 3 | 127.0.0.1:20162 (gRPC), :20182 (status) | KV server |
| Node 4 | 127.0.0.1:20163 (gRPC), :20183 (status) | KV server |
| Node 5 | 127.0.0.1:20164 (gRPC), :20184 (status) | KV server |

### Manual Startup

```bash
# 1. Start the PD server
./gookv-pd --addr 127.0.0.1:2379 --cluster-id 1 --data-dir /tmp/gookv-pd-cluster/pd &

# Wait for PD to be ready
sleep 1

# 2. Start 5 nodes (each connected to PD via --pd-endpoints)
CLUSTER="1=127.0.0.1:20160,2=127.0.0.1:20161,3=127.0.0.1:20162,4=127.0.0.1:20163,5=127.0.0.1:20164"
PD="127.0.0.1:2379"

./gookv-server --store-id 1 --addr 127.0.0.1:20160 --status-addr 127.0.0.1:20180 \
  --data-dir /tmp/gookv-pd-cluster/node1 --pd-endpoints $PD --initial-cluster $CLUSTER &

./gookv-server --store-id 2 --addr 127.0.0.1:20161 --status-addr 127.0.0.1:20181 \
  --data-dir /tmp/gookv-pd-cluster/node2 --pd-endpoints $PD --initial-cluster $CLUSTER &

./gookv-server --store-id 3 --addr 127.0.0.1:20162 --status-addr 127.0.0.1:20182 \
  --data-dir /tmp/gookv-pd-cluster/node3 --pd-endpoints $PD --initial-cluster $CLUSTER &

./gookv-server --store-id 4 --addr 127.0.0.1:20163 --status-addr 127.0.0.1:20183 \
  --data-dir /tmp/gookv-pd-cluster/node4 --pd-endpoints $PD --initial-cluster $CLUSTER &

./gookv-server --store-id 5 --addr 127.0.0.1:20164 --status-addr 127.0.0.1:20184 \
  --data-dir /tmp/gookv-pd-cluster/node5 --pd-endpoints $PD --initial-cluster $CLUSTER &
```

### Manual Verification

```bash
# Verify PD is running (requires grpcurl)
grpcurl -plaintext 127.0.0.1:2379 list
# Should show: pdpb.PD

# Check PD leader
grpcurl -plaintext 127.0.0.1:2379 pdpb.PD/GetMembers

# Check node health
for port in 20180 20181 20182 20183 20184; do
  curl -s http://127.0.0.1:$port/status && echo " (port $port: OK)"
done

# Run the verification script
go run scripts/pd-cluster-verify/main.go
```

### Stopping Manually

```bash
# Kill all server processes
pkill -f gookv-server
pkill -f gookv-pd

# Clean up data
rm -rf /tmp/gookv-pd-cluster
```

### PD Cluster CLI Flags

| Flag | Description |
|------|-------------|
| `--store-id` | Unique store ID for this node (required for cluster mode) |
| `--initial-cluster` | Cluster topology: `storeID=addr,...` |
| `--pd-endpoints` | Comma-separated PD addresses (e.g., `127.0.0.1:2379`) |

## Using the Admin CLI

```bash
# Scan keys in the default column family
./gookv-ctl scan --db /tmp/gookv-data --cf default --limit 20

# Get a single key (hex-encoded)
./gookv-ctl get --db /tmp/gookv-data --cf default --key 68656c6c6f

# Show MVCC versions for a key
./gookv-ctl mvcc --db /tmp/gookv-data --key 68656c6c6f

# Dump raw key-value pairs (with optional --decode for human-readable output)
./gookv-ctl dump --db /tmp/gookv-data --cf write --limit 50 --decode

# Dump entries directly from an SST file (no running database required)
./gookv-ctl dump --sst /tmp/gookv-data/000042.sst --limit 50

# Show approximate data size
./gookv-ctl size --db /tmp/gookv-data

# Trigger full LSM compaction
./gookv-ctl compact --db /tmp/gookv-data --cf default

# Flush WAL only (no compaction)
./gookv-ctl compact --db /tmp/gookv-data --flush-only

# List regions
./gookv-ctl region --db /tmp/gookv-data
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

# Run codec fuzz tests (short duration)
go test ./pkg/codec/... -fuzz=FuzzEncodeBytes -fuzztime=10s
go test ./pkg/codec/... -fuzz=FuzzEncodeUint64 -fuzztime=10s
```

## Client-Server Verification

### Starting the Server

```bash
./gookv-server --data-dir /tmp/gookv-demo --addr 127.0.0.1:20160
```

### Using grpcurl

```bash
# List available services
grpcurl -plaintext 127.0.0.1:20160 list

# The server exposes the tikvpb.Tikv service with these RPCs:
#   Transactional: KvGet, KvScan, KvBatchGet, KvPrewrite, KvCommit,
#     KvBatchRollback, KvCleanup, KvCheckTxnStatus,
#     KvCheckSecondaryLocks, KvScanLock,
#     KvPessimisticLock, KVPessimisticRollback,
#     KvTxnHeartBeat, KvResolveLock, KvGC, KvDeleteRange
#   Raw KV: RawGet, RawPut, RawDelete, RawScan,
#     RawBatchGet, RawBatchPut, RawBatchDelete, RawDeleteRange,
#     RawBatchScan, RawGetKeyTTL, RawCompareAndSwap, RawChecksum
#   Coprocessor: Coprocessor, CoprocessorStream
#   Raft: Raft, BatchRaft, Snapshot
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
	// Connect to a running gookv-server
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

### Using the Client Library (`pkg/client`)

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
