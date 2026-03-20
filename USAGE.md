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
