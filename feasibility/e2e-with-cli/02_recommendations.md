# Recommendations: CLI-Based E2E Testing Strategy

Date: 2026-03-29

## 1. Recommended Approach: Hybrid Architecture

A pure CLI replacement is not feasible. **48% of tests (36 of 75)** require raw gRPC protocol-level access that a CLI cannot and should not provide. The recommended architecture is a **two-tier hybrid**:

| Tier | Tool | Tests | Purpose |
|---|---|---|---|
| **Tier 1: CLI** | `gookv-cli` + shell scripts | 23 tests (31%) today, up to 39 (52%) with CLI additions | High-level functional validation: "does PUT/GET/SCAN/TXN work?" |
| **Tier 2: Library** | `pkg/e2elib` + `pkg/client` + raw gRPC | 36 tests (48%) permanently | Protocol correctness: "does KvPrewrite set MinCommitTs correctly?" |

The remaining 16 tests (21%) are "partial" -- their KV operations can use CLI but they also need process management (StopNode, RestartNode, AddNode). These should stay as Go tests with a shell helper for the CLI portions, or remain fully library-based.

## 2. Tests to Convert First (Phase 1)

Start with the two files that have the highest CLI coverage and zero gRPC dependencies:

### 2.1 client_lib_test.go (9 tests) -- Priority 1

All 9 tests use exclusively `RawKVClient` methods that map 1:1 to CLI commands:

| Test | CLI Commands Needed |
|---|---|
| `TestClientRegionCacheMiss` | `PUT key1 val1`, `GET key1` |
| `TestClientRegionCacheHit` | `PUT a 1`, `PUT b 2`, `GET a`, `GET b` |
| `TestClientStoreResolution` | `PUT key val`, `GET key` |
| `TestClientBatchGetAcrossRegions` | `PUT apple 1`, `PUT mango 2`, `BGET apple mango` |
| `TestClientBatchPutAcrossRegions` | `BPUT a 1 z 2`, `GET a`, `GET z` |
| `TestClientScanAcrossRegions` | `PUT a 0`, ..., `SCAN "" "" LIMIT 100` |
| `TestClientScanWithLimit` | (same setup), `SCAN "" "" LIMIT 3` |
| `TestClientCompareAndSwap` | `PUT zebra old`, `CAS zebra new old`, `GET zebra` |
| `TestClientBatchDeleteAcrossRegions` | `PUT alpha v1`, `PUT zeta v2`, `BDELETE alpha zeta`, `GET alpha`, `GET zeta` |

**Estimated effort:** 1-2 days. Each test becomes a shell script that starts a cluster, runs CLI commands, and asserts output.

### 2.2 cluster_raw_kv_test.go (2 tests) -- Priority 2

| Test | CLI Commands Needed |
|---|---|
| `TestClusterRawKVOperations` | `PUT cluster-key-1 cluster-val-1`, `GET cluster-key-1`, `DELETE cluster-key-1`, `GET cluster-key-1` |
| `TestClusterRawKVBatchPutAndScan` | 20x `PUT batch-cluster-NN val-NN`, `SCAN batch-cluster-00 batch-cluster-99 LIMIT 100` |

**Estimated effort:** 0.5 days.

### 2.3 Phase 1 Totals

- **11 tests converted** (17% of total)
- **0 new CLI commands needed**
- **Eliminates `pkg/client` import** from `client_lib_test.go`

## 3. CLI Commands to Add for Phase 2

Phase 2 targets admin/PD tests. Adding these 4 low-effort commands unlocks 12 more tests:

| Command | Syntax | Backed By | Tests Unlocked |
|---|---|---|---|
| `ALLOC ID` | `ALLOC ID` | `pdclient.AllocID` | `TestPDReplication_IDAllocMonotonicity`, `TestPDReplication_SingleNodeCompat` (partial), `TestPDServerBootstrapAndTSO` (partial) |
| `GC SAFEPOINT SET <ts>` | `GC SAFEPOINT SET 1000` | `pdclient.UpdateGCSafePoint` | `TestPDClusterGCSafePoint`, `TestPDReplication_GCSafePoint` |
| `IS BOOTSTRAPPED` | `IS BOOTSTRAPPED` | `pdclient.IsBootstrapped` | `TestPDReplication_Bootstrap`, `TestPDReplication_LeaderElection` |
| `PUT STORE <id> <addr>` | `PUT STORE 2 127.0.0.1:20161` | `pdclient.PutStore` | `TestPDReplication_WriteForwarding`, `TestPDServerStoreAndRegionMetadata` (partial) |

Adding `BOOTSTRAP <storeID> <addr> <regionID>` (medium effort) would unlock an additional 7 tests, but this is a bootstrap-only operation that may not be worth the CLI complexity.

## 4. PostgreSQL TAP Comparison

PostgreSQL's Test Anything Protocol (TAP) framework for `pg_basebackup`, `pg_dump`, replication, etc. uses a similar hybrid:

| Aspect | PostgreSQL TAP | gookv Proposed |
|---|---|---|
| High-level tests | Shell (`psql -c "SELECT ..."`) + TAP assertions | Shell (`gookv-cli -e "PUT ..."`) + grep/diff assertions |
| Protocol tests | `libpq` C tests | `pkg/e2elib` + raw gRPC Go tests |
| Process control | `PostgreSQL::Test::Cluster` Perl module | `e2elib.GokvCluster` Go harness |
| Polling/waiting | `poll_query_until()` Perl helper | `WaitForCondition` / shell loop |
| Output parsing | Regex on `psql` output | Regex on `gookv-cli` output |

Key lesson from PostgreSQL: they did **not** try to replace `libpq` integration tests with `psql`. Protocol-level tests stayed as C/library tests. High-level regression tests (backup, restore, replication setup) used the CLI.

gookv should follow the same principle: **CLI for behavioral validation, library for protocol correctness**.

## 5. Test Harness Design

For CLI-based tests, a lightweight shell harness:

```bash
#!/usr/bin/env bash
# test_client_put_get.sh
set -euo pipefail

# Start cluster (reuse existing Go helper or a shell wrapper)
source "$(dirname "$0")/harness.sh"
start_cluster 3

# Test: PUT and GET
cli_exec "PUT mykey myvalue"
assert_output "$(cli_exec 'GET mykey')" "myvalue"

# Test: BGET
cli_exec "PUT apple 1"
cli_exec "PUT mango 2"
output=$(cli_exec "BGET apple mango")
assert_contains "$output" "apple"
assert_contains "$output" "mango"

stop_cluster
echo "PASS: client_put_get"
```

The harness would provide:
- `start_cluster N` -- launches N gookv-server nodes + PD
- `cli_exec CMD` -- runs `gookv-cli --addr <addr> -e "CMD"` and captures output
- `assert_output`, `assert_contains`, `assert_not_found` -- output matchers
- `wait_for CMD PATTERN TIMEOUT` -- polling wrapper for `WaitForCondition` equivalent
- `stop_node N`, `restart_node N` -- process management for partial tests

## 6. Risks and Trade-offs

### 6.1 Advantages of CLI-Based Tests

1. **No Go compilation dependency** for running tests -- any machine with the binaries can run the suite
2. **Closer to user experience** -- tests exercise the actual CLI parsing + formatting path
3. **Faster test development** for simple scenarios (shell scripts vs Go test boilerplate)
4. **Better integration test isolation** -- CLI connects externally, avoiding in-process state leaks

### 6.2 Risks

| Risk | Severity | Mitigation |
|---|---|---|
| Output format changes break assertions | Medium | Use structured output (`--format json` flag, not yet implemented) |
| Shell parsing fragility | Medium | Keep assertions simple; prefer exact-match over regex |
| Slower execution (process spawn per command) | Low | Batch commands with `-e "CMD1; CMD2; CMD3"` or stdin pipe |
| Loss of test precision (no structured error types) | Medium | Keep protocol tests in Go; CLI tests only assert success/value |
| Process management complexity in shell | High | Keep process-lifecycle tests in Go; only convert pure-client tests |
| Cross-platform portability | Low | Target Linux only (consistent with CI) |

### 6.3 What NOT to Convert

These categories should permanently remain as Go library tests:

1. **Protocol-level tests** (36 tests): All tests using `DialTikvClient` for raw gRPC calls or missing CLI commands (`Bootstrap`, `PutStore`, `AllocID`, etc.) -- `KvPrewrite`, `KvCommit`, `KvScanLock`, `KvPessimisticLock`, `KvCheckSecondaryLocks`, etc.
2. **Tests requiring response field inspection**: Tests that check `prewriteResp.GetOnePcCommitTs()`, `prewriteResp.GetMinCommitTs()`, or `scanResp.GetLocks()`.
3. **Concurrent/parallel tests**: `TestPDReplication_ConcurrentWritesFromMultipleClients` uses goroutines with per-node clients.
4. **Per-node targeting tests**: Tests that connect to specific follower nodes (`cluster.ClientForNode(i)`) -- the CLI connects to one endpoint.

## 7. Implementation Roadmap

| Phase | Scope | Tests Converted | Cumulative | Effort |
|---|---|---|---|---|
| **Phase 1** | Convert `client_lib_test.go` + `cluster_raw_kv_test.go` | 11 | 11 (17%) | 2 days |
| **Phase 2** | Add 4 CLI commands; convert simple PD/admin tests | 8 | 19 (29%) | 3 days |
| **Phase 3** | Build shell harness with process mgmt; convert partial tests | 5 | 24 (32%) | 3 days |
| **Steady state** | Remaining 51 tests stay as Go library tests | -- | 24 CLI + 51 library | -- |

**Total investment: ~8 days** for 32% coverage via CLI, with the remaining 68% staying as Go tests for protocol correctness. This is comparable to PostgreSQL's ratio where roughly 40% of their e2e tests use `psql` and 60% use `libpq`/C.

## 8. Decision Matrix

| If your goal is... | Then... |
|---|---|
| Prove the CLI works end-to-end | Convert Phase 1 (11 tests, 2 days) |
| Reduce Go build dependency for CI smoke tests | Convert Phase 1 + Phase 2 (19 tests, 5 days) |
| Full PostgreSQL TAP-style framework | All 3 phases (24 tests, 8 days) |
| Eliminate `pkg/client` from test package entirely | Not feasible (36 tests require raw gRPC or PD admin APIs permanently) |
