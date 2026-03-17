# gookvs

A Go-based distributed transactional key-value store modeled after [TiKV](https://github.com/tikv/tikv). It implements the TiKV wire protocol (kvproto) and provides MVCC-based transactions using the Percolator two-phase commit protocol.

## Project Overview

gookvs reproduces the core architecture of TiKV in Go:

- **Codec layer** — order-preserving memcomparable key encoding
- **Storage engine** — Pebble-backed KV engine with column family emulation
- **MVCC** — multi-version concurrency control with snapshot isolation and read-committed support
- **Transaction layer** — Percolator 2PC, async commit, 1PC optimization, pessimistic transactions
- **Raft consensus** — etcd/raft integration with region-based routing and inter-node transport
- **Coprocessor** — push-down execution with RPN expressions, table scan, selection, aggregation
- **gRPC server** — TiKV-compatible RPC interface via `pingcap/kvproto`
- **HTTP status server** — pprof, Prometheus metrics, health checks
- **Configuration** — TOML config with validation, CLI flag overrides
- **Logging** — structured logging with slow-log routing and file rotation
- **Flow control** — EWMA-based backpressure and memory quota
- **Admin CLI** — `gookvs-ctl` for inspecting data, MVCC info, compaction

## Directory Structure

```
cmd/
  gookvs-server/          # Server binary entry point
  gookvs-ctl/             # Admin CLI (scan, get, mvcc, dump, size, compact)
pkg/                      # Public packages (importable by external code)
  codec/                  # Memcomparable byte/number encoding
  keys/                   # User key <-> internal key encoding (DataKey, RaftLogKey, etc.)
  cfnames/                # Column family constants (default, lock, write, raft)
  txntypes/               # Lock, Write, Mutation structs with binary serialization
  pdclient/               # Placement Driver client (gRPC + mock)
internal/                 # Private implementation packages
  config/                 # TOML config loading, validation, ReadableSize/Duration types
  log/                    # Structured logging, LogDispatcher, SlowLogHandler, file rotation
  engine/
    traits/               # KvEngine, Snapshot, WriteBatch, Iterator interfaces
    rocks/                # Pebble-backed engine with CF emulation via key prefixing
  storage/
    mvcc/                 # MVCC key encoding, MvccTxn, MvccReader, PointGetter
    txn/                  # Percolator actions (Prewrite, Commit, Rollback, CheckTxnStatus)
      latch/              # Deadlock-free key latch system
      concurrency/        # In-memory lock table + max_ts tracking
      async_commit.go     # Async commit, 1PC optimization
      pessimistic.go      # Pessimistic lock acquire, upgrade, rollback
  raftstore/              # Raft peer loop, PeerStorage, message types
    router/               # Region-based message routing (sync.Map)
  server/                 # gRPC server, TikvService implementation, Storage bridge
    transport/            # Inter-node Raft transport, connection pooling, message batching
    status/               # HTTP status server (pprof, metrics, health, config)
    flow/                 # Flow control, EWMA backpressure, memory quota
  coprocessor/            # Push-down execution: RPN, TableScan, Selection, Aggregation
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
# Build both server and CLI binaries
make build

# This produces:
#   ./gookvs-server
#   ./gookvs-ctl
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

## Using the Admin CLI

```bash
# Scan keys in the default column family
./gookvs-ctl scan --db /tmp/gookvs-data --cf default --limit 20

# Get a single key (hex-encoded)
./gookvs-ctl get --db /tmp/gookvs-data --cf default --key 68656c6c6f

# Show MVCC versions for a key
./gookvs-ctl mvcc --db /tmp/gookvs-data --key 68656c6c6f

# Dump raw key-value pairs
./gookvs-ctl dump --db /tmp/gookvs-data --cf write --limit 50

# Show approximate data size
./gookvs-ctl size --db /tmp/gookvs-data

# Trigger manual compaction
./gookvs-ctl compact --db /tmp/gookvs-data --cf default
```

## Running Tests

```bash
# Run all tests (verbose, no cache)
make test

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
| `internal/engine/traits` | 3 | Interface compliance |
| `internal/engine/rocks` | 38 | CRUD, CF, snapshot, batch, iterator, bounds |
| `internal/storage/mvcc` | 20 | Multi-version reads, SI/RC isolation |
| `internal/storage/txn` | 45 | Percolator, async commit, 1PC, pessimistic txns |
| `internal/storage/txn/latch` | 11 | Latch acquire/release, concurrent safety |
| `internal/storage/txn/concurrency` | 7 | Lock table, max_ts tracking |
| `internal/raftstore` | 19 | Bootstrap, election, proposal, peer storage |
| `internal/raftstore/router` | 14 | Concurrent message routing |
| `internal/server` | 18 | Full gRPC end-to-end tests |
| `internal/server/transport` | 22 | Raft transport, connection pool, batching |
| `internal/server/status` | 13 | HTTP status endpoints |
| `internal/server/flow` | 25 | Flow control, EWMA, memory quota |
| `internal/coprocessor` | 45 | RPN, table scan, selection, aggregation |
| **Total** | **394** | **20 packages, all passing** |

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
#   KvGet, KvScan, KvBatchGet
#   KvPrewrite, KvCommit
#   KvBatchRollback, KvCleanup, KvCheckTxnStatus
#   BatchCommands (bidirectional streaming)
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
┌─────────────────────────────────────────────────────────┐
│                     gRPC Server                         │
│              (tikvpb.TikvServer interface)               │
│  KvGet, KvScan, KvPrewrite, KvCommit, BatchCommands     │
├──────────────────────┬──────────────────────────────────┤
│   HTTP Status Server │        Flow Control              │
│  (pprof, metrics,    │  (EWMA backpressure,             │
│   health, config)    │   memory quota)                  │
├──────────────────────┴──────────────────────────────────┤
│                    Storage Bridge                        │
│       (latch acquisition, MVCC transaction               │
│        management, engine coordination)                  │
├───────────────┬─────────────────┬───────────────────────┤
│  Transaction  │   Coprocessor   │      Raftstore        │
│  - Percolator │  - RPN engine   │  - etcd/raft          │
│  - Async      │  - TableScan    │  - PeerStorage        │
│    commit/1PC │  - Selection    │  - Router             │
│  - Pessimistic│  - Aggregation  │  - Transport          │
├───────────────┤                 │  (connection pool,    │
│     MVCC      │                 │   message batching)   │
│  - MvccReader │                 │                       │
│  - PointGetter│                 │                       │
│  - MvccTxn    │                 │                       │
├───────────────┴─────────────────┴───────────────────────┤
│               Engine (traits.KvEngine)                   │
│          Pebble backend with CF emulation                │
│          (default, lock, write, raft)                    │
├─────────────────────────────────────────────────────────┤
│     Config (TOML)  │  Logging (slog + lumberjack)       │
└─────────────────────────────────────────────────────────┘
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

## License

See the project root for license information.
