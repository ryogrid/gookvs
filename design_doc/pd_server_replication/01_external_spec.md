# PD Server Raft Replication: External Specification

## 1. Overview

PD (Placement Driver) is the cluster metadata and coordination service for gookv. It provides timestamp allocation (TSO), store/region metadata management, ID allocation, GC safe point tracking, and scheduling decisions.

Today, PD runs as a single-node, in-memory server (`internal/pd/server.go`). This design adds Raft-based replication so PD can run as a 3- or 5-node cluster, providing fault tolerance for the control plane without changing any client-facing interfaces.

## 2. Deployment Topologies

| Topology | Nodes | Fault Tolerance | Use Case |
|---|---|---|---|
| Single-node (default) | 1 | None | Development, testing. No `--initial-cluster` flag. No Raft overhead. Full backward compatibility with current behavior. |
| 3-node cluster | 3 | 1 node failure | Recommended for production. |
| 5-node cluster | 5 | 2 node failures | High-availability production environments. |

Only odd node counts are supported. Even counts (2, 4) are rejected at startup because they provide no additional fault tolerance over the next lower odd count while increasing resource usage.

## 3. New CLI Flags (`cmd/gookv-pd`)

Existing flags are unchanged:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--addr` | string | `0.0.0.0:2379` | Client gRPC listen address (serves `pdpb.PD` service) |
| `--data-dir` | string | `/tmp/gookv-pd` | Data directory for metadata |
| `--cluster-id` | uint64 | `1` | Cluster ID |
| `--log-level` | string | `info` | Log level: debug, info, warn, error |
| `--log-file` | string | (auto) | Log file path |

New flags for multi-node mode:

| Flag | Type | Default | Description |
|---|---|---|---|
| `--pd-id` | uint64 | (none) | This PD node's unique ID within the PD cluster. Required when `--initial-cluster` is specified. Must match one of the IDs in `--initial-cluster`. |
| `--initial-cluster` | string | (none) | Cluster topology string defining all PD nodes and their Raft peer addresses. Format: `ID1=HOST1:PORT1,ID2=HOST2:PORT2,...`. When omitted, PD starts in single-node mode with no Raft. |
| `--peer-port` | string | `0.0.0.0:2380` | Listen address for PD-to-PD Raft peer communication. Only used when `--initial-cluster` is specified. Ignored in single-node mode. |
| `--client-cluster` | string | (none) | Client-facing addresses for all PD nodes. Format: `1=host:port,2=host:port,...`. Used for leader forwarding (followers need to know the leader's client gRPC address). Required when `--initial-cluster` is specified. |

Startup validation rules:
- If `--initial-cluster` is specified, `--pd-id` is required. Startup fails with an error if `--pd-id` is missing.
- The `--pd-id` value must appear as one of the IDs in `--initial-cluster`. Startup fails if it does not match.
- The `--initial-cluster` string must contain an odd number of entries (1, 3, or 5). Even counts are rejected.
- All PD nodes in a cluster must use the same `--cluster-id` value.

## 4. Port Allocation

Each PD node uses two ports in multi-node mode:

| Port | Flag | Default | Protocol | Purpose |
|---|---|---|---|---|
| Client port | `--addr` | `0.0.0.0:2379` | gRPC | Serves the `pdpb.PD` gRPC service for KVS nodes and clients |
| Peer port | `--peer-port` | `0.0.0.0:2380` | gRPC | Internal PD-to-PD Raft consensus messages (AppendEntries, RequestVote, snapshots) |

In single-node mode, only the client port is opened. The peer port listener is not started.

> **Note:** `--peer-port` is the local bind address for Raft peer communication, while the entries in `--initial-cluster` are the advertised addresses that other nodes use to connect. In typical deployments these are the same value, but they may differ when running behind NAT or in containerized environments.

Recommended port allocation for a local 3-node cluster:

| Node | Client Port (`--addr`) | Peer Port (`--peer-port`) |
|---|---|---|
| PD 1 | 127.0.0.1:2379 | 127.0.0.1:2380 |
| PD 2 | 127.0.0.1:2381 | 127.0.0.1:2382 |
| PD 3 | 127.0.0.1:2383 | 127.0.0.1:2384 |

## 5. Client Behavior

KVS nodes (`cmd/gookv-server`) connect to PD via the `--pd-endpoints` flag, which accepts a comma-separated list of PD client addresses:

```
--pd-endpoints 127.0.0.1:2379,127.0.0.1:2381,127.0.0.1:2383
```

The existing PD client (`pkg/pdclient/client.go`) already handles multi-endpoint failover:
- `Config.Endpoints` accepts multiple addresses.
- `NewClient()` connects to the first available endpoint.
- `reconnect()` rotates through endpoints in round-robin order when a connection fails.
- `withRetry()` provides exponential backoff with reconnection between retries.

No client-side code changes are required for multi-PD support. The `pdclient.Client` interface (16 methods) remains unchanged.

## 6. RPC Routing

All 16 RPCs served by `pdpb.PD` are categorized as either read or write operations. This determines their routing behavior in a replicated PD cluster.

### Read RPCs (served locally on any node)

These RPCs read from the local replicated state machine. Because Raft guarantees that committed entries are applied in the same order on all nodes, any node (leader or follower) can serve reads after applying up to its commit index.

| RPC | Description |
|---|---|
| `GetStore` | Look up store metadata by ID |
| `GetAllStores` | List all registered stores |
| `GetRegion` | Find region containing a given key |
| `GetRegionByID` | Look up region metadata by ID |
| `IsBootstrapped` | Check whether cluster has been bootstrapped |
| `GetGCSafePoint` | Read current GC safe point |
| `GetMembers` | List PD cluster members and leader |

### Write RPCs (forwarded to Raft leader)

These RPCs mutate cluster state. They must be proposed through the Raft log to ensure consistency.

If the receiving node IS the Raft leader, the request is proposed directly into the Raft log, and the response is returned after the entry is committed and applied.

If the receiving node is a follower, the request is proxied to the current Raft leader via a gRPC call. The follower acts as a transparent proxy: it forwards the request, waits for the leader's response, and returns it to the client.

| RPC | Description |
|---|---|
| `Bootstrap` | Initialize cluster with first store and region |
| `PutStore` | Register or update a store |
| `AllocID` | Allocate a globally unique ID |
| `Tso` | Allocate globally unique timestamps |
| `StoreHeartbeat` | Process store-level heartbeat and stats |
| `RegionHeartbeat` | Process region heartbeat, return scheduling commands |
| `AskBatchSplit` | Allocate new region/peer IDs for a split |
| `ReportBatchSplit` | Record completed region splits |
| `UpdateGCSafePoint` | Advance the GC safe point |

### Tso Special Handling

`Tso` (timestamp allocation) is latency-critical and called on every transaction. To avoid per-request Raft consensus overhead:

1. The Raft leader pre-allocates a timestamp range (e.g., 1000 timestamps) by proposing a single Raft entry that reserves `[current_max, current_max + batch_size)`.
2. Individual `Tso` requests are served from this local buffer without further Raft round-trips.
3. When the buffer is exhausted, a new range is allocated through Raft.
4. On leader change, the new leader allocates a fresh range. Any unused timestamps from the old leader's buffer are safely discarded (timestamps only need to be unique and monotonically increasing, not contiguous).

## 7. GetMembers Enhancement

The `GetMembers` RPC currently returns a single member (the server itself). After replication, it is enhanced to return full cluster membership information.

Response includes:
- **All PD cluster members**: Each member reports its `pd-id`, client URL (`--addr`), and peer URL (`--peer-port`).
- **Current Raft leader**: The `leader` field identifies which member is the current Raft leader by its `pd-id`.
- **Raft term**: The current Raft term, useful for leader change detection.

Clients can use this information to:
- Discover all PD endpoints (in case the initial endpoint list is incomplete).
- Identify the leader for direct connection (optimization to avoid follower-to-leader proxying; not required for correctness).

## 8. Backward Compatibility

| Scenario | Behavior |
|---|---|
| `--initial-cluster` NOT specified | PD starts in single-node mode. No Raft state machine, no peer port, no consensus overhead. All 16 RPCs are handled directly as they are today. This is the default and preserves full backward compatibility. |
| `--initial-cluster` IS specified | PD enters replicated mode. Raft state machine is initialized, peer port is opened, read/write routing is enabled per Section 6. |
| KVS nodes (`gookv-server`) | Require no changes. They already support multi-endpoint PD via `--pd-endpoints`. The existing `pdclient.Client` failover logic handles replicated PD transparently. |
| `pdclient.Client` interface | Unchanged. All 16 methods retain the same signatures and semantics. |
| `GetMembers` response | Enhanced with additional fields (peer URLs, all members list). Existing clients that only read the `leader` field continue to work. New fields are additive. |

## 9. Example Deployments

### Single-Node (unchanged)

```bash
./gookv-pd --addr 127.0.0.1:2379 --cluster-id 1
```

No new flags needed. Behavior is identical to today.

### 3-Node Cluster

```bash
# Node 1
./gookv-pd --pd-id 1 --addr 127.0.0.1:2379 --peer-port 127.0.0.1:2380 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383" \
  --cluster-id 1 --data-dir /tmp/pd1

# Node 2
./gookv-pd --pd-id 2 --addr 127.0.0.1:2381 --peer-port 127.0.0.1:2382 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383" \
  --cluster-id 1 --data-dir /tmp/pd2

# Node 3
./gookv-pd --pd-id 3 --addr 127.0.0.1:2383 --peer-port 127.0.0.1:2384 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383" \
  --cluster-id 1 --data-dir /tmp/pd3
```

KVS nodes connect to all PD client endpoints:

```bash
./gookv-server --pd-endpoints 127.0.0.1:2379,127.0.0.1:2381,127.0.0.1:2383 ...
```

### 5-Node Cluster

```bash
# Node 1
./gookv-pd --pd-id 1 --addr 127.0.0.1:2379 --peer-port 127.0.0.1:2380 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384,4=127.0.0.1:2386,5=127.0.0.1:2388" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383,4=127.0.0.1:2385,5=127.0.0.1:2387" \
  --cluster-id 1 --data-dir /tmp/pd1

# Node 2
./gookv-pd --pd-id 2 --addr 127.0.0.1:2381 --peer-port 127.0.0.1:2382 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384,4=127.0.0.1:2386,5=127.0.0.1:2388" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383,4=127.0.0.1:2385,5=127.0.0.1:2387" \
  --cluster-id 1 --data-dir /tmp/pd2

# Node 3
./gookv-pd --pd-id 3 --addr 127.0.0.1:2383 --peer-port 127.0.0.1:2384 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384,4=127.0.0.1:2386,5=127.0.0.1:2388" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383,4=127.0.0.1:2385,5=127.0.0.1:2387" \
  --cluster-id 1 --data-dir /tmp/pd3

# Node 4
./gookv-pd --pd-id 4 --addr 127.0.0.1:2385 --peer-port 127.0.0.1:2386 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384,4=127.0.0.1:2386,5=127.0.0.1:2388" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383,4=127.0.0.1:2385,5=127.0.0.1:2387" \
  --cluster-id 1 --data-dir /tmp/pd4

# Node 5
./gookv-pd --pd-id 5 --addr 127.0.0.1:2387 --peer-port 127.0.0.1:2388 \
  --initial-cluster "1=127.0.0.1:2380,2=127.0.0.1:2382,3=127.0.0.1:2384,4=127.0.0.1:2386,5=127.0.0.1:2388" \
  --client-cluster "1=127.0.0.1:2379,2=127.0.0.1:2381,3=127.0.0.1:2383,4=127.0.0.1:2385,5=127.0.0.1:2387" \
  --cluster-id 1 --data-dir /tmp/pd5
```

## 10. Failure Modes

| Failure | Impact | Recovery |
|---|---|---|
| **PD leader failure** | Write RPCs become temporarily unavailable. Read RPCs continue on surviving nodes. | Raft elects a new leader (typically within 1-3 seconds, governed by election timeout). Clients automatically reconnect via endpoint rotation (`reconnect()` in `pkg/pdclient/client.go`). No manual intervention required. |
| **PD minority failure** (e.g., 1 of 3 nodes down) | No impact on availability. Quorum (majority) is maintained. Raft continues to commit entries. | Failed node can be restarted. It catches up via Raft log replay or snapshot. |
| **PD majority failure** (e.g., 2 of 3 nodes down) | Cluster becomes unavailable for all write RPCs. Read RPCs may still be served by the surviving node from its local state, but the data may become stale. | KVS nodes continue serving existing data with cached region/store metadata, but cannot: allocate new timestamps (Tso), split regions (AskBatchSplit), register new stores (PutStore), or advance GC safe point. Recovery requires restoring a majority of PD nodes. |
| **Network partition** | Raft leader requires quorum for writes. A leader isolated from the majority steps down after election timeout. The majority partition elects a new leader and continues operating. | The minority partition (including a stale leader) cannot serve write RPCs. Clients connected to the minority partition fail over to endpoints in the majority partition via `reconnect()`. |
| **Slow follower** | No impact on availability. Raft leader does not wait for all followers, only a quorum. | The slow follower catches up asynchronously. If it falls too far behind, it receives a snapshot from the leader. |
