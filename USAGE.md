# gookv Usage Guide

For project overview, architecture, and design details, see **[README.md](README.md)**.

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

### Adding a Node to an Existing Cluster

```bash
# Start a new node that joins an existing cluster via PD.
# No --initial-cluster needed. Store ID is allocated from PD automatically.
./gookv-server \
  --pd-endpoints 127.0.0.1:2379 \
  --addr 127.0.0.1:20165 \
  --status-addr 127.0.0.1:20185 \
  --data-dir /tmp/gookv-pd-cluster/node6

# Verify the new node is registered:
./gookv-ctl store list --pd 127.0.0.1:2379
```

PD will automatically schedule region replicas onto the new node. Use `gookv-ctl store list` to monitor the process.

### PD Cluster CLI Flags

| Flag | Description |
|------|-------------|
| `--store-id` | Unique store ID for this node (required for bootstrap cluster mode; optional in join mode — allocated from PD automatically) |
| `--initial-cluster` | Cluster topology: `storeID=addr,...` (required for bootstrap; not needed for join mode) |
| `--pd-endpoints` | Comma-separated PD addresses (e.g., `127.0.0.1:2379`) |

## Cross-Region Transaction Demo

Demonstrates cross-region 2PC transactions: single-region txn, region split, then cross-region atomic commit.

### Prerequisites

- Go installed
- Ports 2389, 20170-20172, 20190-20192 available

### Running

```bash
# Build and start PD + 3-node cluster with small split thresholds
make txn-demo-start

# Run the demo (3 scenarios: baseline txn, split, cross-region 2PC)
make txn-demo-verify

# Stop and clean up
make txn-demo-stop
```

### Ports

| Component | Address |
|-----------|---------|
| PD | 127.0.0.1:2389 |
| Node 1 | 127.0.0.1:20170 (gRPC), :20190 (status) |
| Node 2 | 127.0.0.1:20171 (gRPC), :20191 (status) |
| Node 3 | 127.0.0.1:20172 (gRPC), :20192 (status) |

### Demo Configuration

The demo cluster uses `scripts/txn-demo/config.toml` with low thresholds to trigger region splits quickly with minimal data:

```toml
[raft-store]
raft-base-tick-interval = "100ms"   # Fast ticks for quick leader election
raft-heartbeat-ticks = 2
raft-election-timeout-ticks = 10
region-max-size = "2KB"             # Low threshold to trigger split quickly
region-split-size = "1KB"
split-check-tick-interval = "2s"    # Check for oversized regions every 2s
pd-heartbeat-tick-interval = "5s"   # Frequent heartbeats for demo responsiveness
```

### What Each Scenario Demonstrates

1. **Single-Region Transaction (Baseline)**: Connects to PD, verifies the cluster starts with a single region, then performs a 2PC transaction setting `account:alice=1000` and `account:bob=1000`. Reads back both keys in a new transaction to confirm correctness.

2. **Region Split**: Writes ~15 keys of ~150 bytes each via `RawKVClient` to exceed the 1KB split threshold. Polls PD until the region count increases (up to 60s timeout). Prints the before/after region layout showing each region's ID, key range, peer stores, and leader.

3. **Cross-Region 2PC**: Waits for the region count to stabilize (3 consecutive stable polls). Picks keys guaranteed to be in different regions — `account:alice` in the first region and a key derived from the last region's start key. Pre-warms both regions with RawKV writes to ensure they are writable, then initializes balances with separate single-region transactions. Performs a cross-region balance transfer (100 units) via 2PC. Verifies the result with a retry loop to handle async secondary commits.

The cluster uses 3 KVS nodes with 3-node Raft groups. After split, both regions have 3 peers on the same 3 stores but run as separate Raft groups with potentially different leaders.

### Expected Output

The demo prints structured output with scenario banners (`--- Scenario 1/3: ...`), numbered steps, region layout tables, and explicit PASS/FAIL per scenario. A final summary reports how many scenarios passed (e.g., `All 3 scenarios passed.`). The program exits with code 0 on full success, 1 if any scenario fails.

## Dynamic Node Addition Demo

Demonstrates dynamic horizontal scaling: new KVS nodes join a running cluster via PD, and regions split across the expanded cluster.

### Prerequisites

- Go installed
- Ports 2399, 20270-20273, 20290-20293 available

### Running

```bash
# Build and start PD + 3-node bootstrap cluster
make scale-demo-start

# Run the demo (2 scenarios: initial state, add node + split)
make scale-demo-verify

# Stop and clean up
make scale-demo-stop
```

### Ports

| Component | Address |
|-----------|---------|
| PD | 127.0.0.1:2399 |
| Node 1 (bootstrap) | 127.0.0.1:20270 (gRPC), :20290 (status) |
| Node 2 (bootstrap) | 127.0.0.1:20271 (gRPC), :20291 (status) |
| Node 3 (bootstrap) | 127.0.0.1:20272 (gRPC), :20292 (status) |
| Node 4 (join) | 127.0.0.1:20273 (gRPC), :20293 (status) |

### What Each Scenario Demonstrates

1. **Initial Cluster State**: Verifies the cluster starts with a single region `["", "")` spanning all keys, hosted on 3 bootstrap nodes. Prints store and region topology.

2. **Add Node + Region Split + Data Verification**: Starts 1 new KVS node in **join mode** (`--pd-endpoints` only, no `--initial-cluster`). The node automatically receives a store ID from PD and registers itself. After confirming 4 stores are registered, the demo writes data via `RawKVClient` to exceed the 1KB split threshold, polls PD until a region split is detected, then writes and reads back a test value in each region to verify data correctness. Prints the final region layout showing key ranges, peer members, and leaders.

Join mode works because `gookv-server` detects that `--pd-endpoints` is provided without `--initial-cluster`, connects to PD, allocates a store ID via `AllocID()`, and starts with an empty region set — PD then schedules region replicas onto the new node via heartbeat responses.

### Expected Output

The demo prints structured output with scenario banners (`--- Scenario 1/2: ...`), numbered steps, store/region topology tables, and explicit PASS/FAIL per scenario. The program exits with code 0 on full success, 1 if any scenario fails.

## Using the Admin CLI

gookv-ctl commands fall into two categories: **offline commands** (`scan`, `get`, `mvcc`, `dump`, `size`, `compact`, `region`) that read directly from a data directory via `--db` and work without a running cluster, and **online commands** (`store list`, `store status`) that communicate with a running PD server via `--pd`.

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

# List all stores in the cluster (requires running PD)
./gookv-ctl store list --pd 127.0.0.1:2379

# Show details for a specific store (requires running PD)
./gookv-ctl store status --pd 127.0.0.1:2379 --store-id 1
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
