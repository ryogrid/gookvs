# Revised Recommendations: CLI-Based E2E Testing Strategy

Date: 2026-03-29
Revision: 2 (relaxed constraints)

## 1. Updated Hybrid Architecture

Under relaxed constraints, the architecture shifts from a two-tier to a **three-tier** model:

| Tier | Tool | Tests | % | Purpose |
|---|---|---|---|---|
| **Tier 1: Pure CLI** | `gookv-cli` with existing + new commands | 32 (A+B) | 43% | No Go code changes needed for assertions |
| **Tier 2: Hybrid CLI+Go** | e2elib CLI wrappers + process management | 31 (C+D) | 41% | Process lifecycle in Go; KV/PD assertions via CLI |
| **Tier 3: Library-only** | `pkg/e2elib` + raw gRPC | 12 (F) | 16% | Protocol correctness verification |

### 1.1 Tier Boundaries

**Tier 1 (Pure CLI)** — Tests where the entire test body can be expressed as CLI commands:
- All `client_lib_test.go` tests (9)
- All `cluster_raw_kv_test.go` tests (2)
- Simple cluster/PD read tests (13)
- Raw KV tests with `--addr` flag (8)

**Tier 2 (Hybrid)** — Tests that need Go for process management but CLI for assertions:
- Tests using `StopNode`/`RestartNode`/`AddNode` (15)
- Tests using `BOOTSTRAP`, `PUT STORE`, `ALLOC ID` etc. via CLI (16)

**Tier 3 (Library-only)** — Tests that inspect protocol-internal gRPC response fields:
- async_commit protocol tests (4)
- txn_rpc protocol tests (5)
- Multi-region protocol tests (3)

### 1.2 Comparison with First Study

| Aspect | Study 1 | Study 2 |
|---|---|---|
| Tiers | 2 (CLI + Library) | 3 (Pure CLI + Hybrid + Library) |
| CLI-only tests | 23 (31%) | 32 (43%) |
| CLI-involved tests | 23 (31%) | **63 (84%)** |
| Library-only tests | 52 (69%) | **12 (16%)** |
| New CLI commands | 8 | 10 |
| e2elib changes | 0 | 8 new wrapper functions |

## 2. Implementation Roadmap

### Phase 1: Foundation (4 days)

**Goal:** `--addr` flag + e2elib CLI wrappers → convert 32 Category A+B tests

| Task | Effort | Tests Enabled |
|---|---|---|
| Add `--addr` flag to gookv-cli | 0.5 day | — |
| Add `BSCAN` command to gookv-cli | 0.5 day | — |
| Implement e2elib CLI wrapper functions (CLIExec, CLIPut, CLIGet, CLINodeExec, CLIWaitForCondition, etc.) | 1 day | — |
| Convert `client_lib_test.go` (9 tests) | 0.5 day | 9 |
| Convert `cluster_raw_kv_test.go` (2 tests) | 0.25 day | 2 |
| Convert `raw_kv_test.go` (3 tests) | 0.25 day | 3 |
| Convert `raw_kv_extended_test.go` (4 tests) | 0.5 day | 4 |
| Convert remaining Category A tests (14 across 6 files) | 0.5 day | 14 |

**Phase 1 result:** 32 tests converted (43%), `--addr` flag operational, e2elib CLI layer established.

### Phase 2: PD Admin Commands (5 days)

**Goal:** Add PD CLI commands → convert 16 Category C tests

| Task | Effort | Tests Enabled |
|---|---|---|
| Add `IS BOOTSTRAPPED` command | 0.25 day | — |
| Add `BOOTSTRAP` command | 0.5 day | — |
| Add `PUT STORE` command | 0.25 day | — |
| Add `ALLOC ID` command | 0.25 day | — |
| Add `GC SAFEPOINT SET` command | 0.25 day | — |
| Add `ASK SPLIT` + `REPORT SPLIT` commands | 1 day | — |
| Add `STORE HEARTBEAT` command | 0.5 day | — |
| Convert `pd_server_test.go` (4 tests) | 0.5 day | 4 |
| Convert `pd_replication_test.go` Category C tests (10 tests) | 1 day | 10 |
| Convert `region_split_test.go` (1 test) | 0.25 day | 1 |
| Convert `pd_cluster_integration_test.go` GCSafePoint (1 test) | 0.25 day | 1 |

**Phase 2 result:** 48 tests converted (64%), all PD admin operations available via CLI.

### Phase 3: Process Management Hybrid (4 days)

**Goal:** Convert Category D tests (process mgmt in Go + CLI for assertions)

| Task | Effort | Tests Enabled |
|---|---|---|
| Enhance e2elib CLI wrappers for process mgmt coordination (CLIWaitForStoreCount, CLIWaitForRegionLeader, CLIParallel) | 1 day | — |
| Convert `cluster_server_test.go` NodeFailure + LeaderFailover (2 tests) | 0.5 day | 2 |
| Convert `add_node_test.go` (4 tests) | 0.5 day | 4 |
| Convert `restart_replay_test.go` (3 tests) | 0.5 day | 3 |
| Convert `pd_leader_discovery_test.go` LeaderFailover (1 test) | 0.25 day | 1 |
| Convert remaining pd_replication Category D tests (4 tests) | 0.75 day | 4 |
| Convert `multi_region_routing_test.go` SplitWithLiveTraffic (1 test) | 0.5 day | 1 |

**Phase 3 result:** 63 tests converted (84%), all non-protocol tests using CLI.

### Phase 4: Verification and Polish (3 days)

| Task | Effort |
|---|---|
| Run full e2e_external suite, fix CLI output parsing edge cases | 1 day |
| Add `--format json` to gookv-cli for structured output (reduces parsing fragility) | 1 day |
| Documentation: update USAGE.md with new commands, update test README | 0.5 day |
| Performance testing: verify CLI subprocess overhead is acceptable | 0.5 day |

### Roadmap Summary

| Phase | Duration | Tests Converted | Cumulative | % |
|---|---|---|---|---|
| Phase 1: Foundation | 4 days | 32 | 32 | 43% |
| Phase 2: PD Admin | 5 days | 16 | 48 | 64% |
| Phase 3: Process Mgmt | 4 days | 15 | 63 | 84% |
| Phase 4: Polish | 3 days | 0 | 63 | 84% |
| **Total** | **16 days** | **63** | **63** | **84%** |

The remaining 12 tests (16%) permanently stay as Go library tests.

## 3. Effort Estimate

| Item | Days |
|---|---|
| gookv-cli: `--addr` flag | 0.5 |
| gookv-cli: 9 new commands | 3.5 |
| gookv-cli: `--format json` (optional) | 1 |
| e2elib: CLI wrapper functions | 2 |
| Test conversion: Category A (24 tests) | 1.5 |
| Test conversion: Category B (8 tests) | 1 |
| Test conversion: Category C (16 tests) | 2 |
| Test conversion: Category D (15 tests) | 2.5 |
| Verification and debugging | 2 |
| **Total** | **~16 days** |

## 4. Which Tests to Convert First

### Recommended Order

**Highest ROI first** — maximize converted test count per unit effort:

1. **client_lib_test.go (9 tests)** — Zero new CLI commands needed. Pure PUT/GET/SCAN/BGET/CAS. Easiest conversion. Eliminates `pkg/client` import from this file.

2. **cluster_raw_kv_test.go (2 tests)** — Same: zero new commands needed.

3. **raw_kv_test.go (3 tests)** — Needs only `--addr` flag (one-time effort).

4. **raw_kv_extended_test.go (4 tests)** — Needs `--addr` + `BSCAN` command.

5. **Simple PD read tests** (14 tests across 6 files) — Already replaceable with existing STORE LIST, STORE STATUS, REGION, TSO commands.

After these 32 tests (Phase 1), the CLI framework is proven and the remaining conversions follow the same patterns.

## 5. Risk Analysis

| Risk | Severity | Mitigation |
|---|---|---|
| CLI output format changes break test assertions | High | Add `--format json` for structured output; use JSON parsing in e2elib wrappers |
| Shell subprocess overhead slows test suite | Medium | Batch multiple commands per CLI invocation (`-e "CMD1; CMD2; CMD3"`); reuse connections |
| ASK SPLIT / REPORT SPLIT output parsing complexity | Medium | CLI outputs split IDs in a parseable format (JSON or table); e2elib provides `CLIAskSplit` helper |
| Race conditions in CLI-based polling vs library-based | Medium | Keep same polling interval (200ms); add jitter; use `WaitForCondition` wrapper |
| Debugging difficulty (CLI output vs Go errors) | Medium | CLI wrappers log full command + output on failure; `t.Logf` for diagnostics |
| Per-node `--pd` targeting for PD cluster tests | Low | Already supported: `gookv-cli --pd <addr>` connects to specified PD endpoint |

## 6. Decision Matrix

| Goal | Recommended Scope | Effort | Coverage |
|---|---|---|---|
| Quick proof-of-concept | Phase 1 only (--addr + Category A+B) | 4 days | 43% (32 tests) |
| Eliminate most `pkg/client` usage | Phase 1 + Phase 2 | 9 days | 64% (48 tests) |
| Maximum CLI coverage | All 4 phases | 16 days | 84% (63 tests) |
| Eliminate `pkg/client` entirely | Not feasible | — | 12 tests (16%) permanently require raw gRPC |

### Recommended Minimum Viable Conversion

**Phase 1 + Phase 2 (9 days, 64% coverage)** provides the best effort-to-coverage ratio:
- Proves the CLI-based testing architecture end-to-end
- Covers all "easy" conversions (existing CLI commands)
- Covers all "medium" conversions (new PD commands)
- Leaves only process-management-hybrid tests for Phase 3 (which can be done later if the approach proves valuable)

## 7. Architecture Diagram

```
┌─────────────────────────────────────────────────┐
│                 Test Functions                    │
│                                                  │
│  ┌──────────┐  ┌──────────┐  ┌───────────────┐ │
│  │ Tier 1   │  │ Tier 2   │  │   Tier 3      │ │
│  │ Pure CLI │  │ Hybrid   │  │ Library-only  │ │
│  │ 32 tests │  │ 31 tests │  │  12 tests     │ │
│  └────┬─────┘  └────┬─────┘  └───────┬───────┘ │
│       │              │                │          │
└───────┼──────────────┼────────────────┼──────────┘
        │              │                │
        ▼              ▼                ▼
  ┌───────────┐  ┌───────────┐  ┌───────────────┐
  │ e2elib    │  │ e2elib    │  │ e2elib        │
  │ CLI       │  │ CLI       │  │ DialTikvClient│
  │ Wrappers  │  │ Wrappers  │  │ raw gRPC      │
  │           │  │     +     │  │               │
  │           │  │ Process   │  │ pkg/client    │
  │           │  │ Mgmt (Go) │  │ pkg/pdclient  │
  └─────┬─────┘  └─────┬─────┘  └───────┬───────┘
        │              │                │
        ▼              ▼                ▼
  ┌───────────────────────────────────────────────┐
  │              gookv-cli binary                  │
  │  (--pd / --addr / -e "CMD" / -c)              │
  │                                                │
  │  Raw KV: PUT GET DELETE SCAN BGET BPUT ...     │
  │  Txn:    BEGIN SET GET DELETE COMMIT ROLLBACK  │
  │  Admin:  STORE LIST/STATUS, REGION, TSO, ...   │
  │  PD:     BOOTSTRAP, PUT STORE, ALLOC ID, ...   │
  └──────────────────┬────────────────────────────┘
                     │
                     ▼
  ┌───────────────────────────────────────────────┐
  │        gookv-server / gookv-pd                 │
  │         (external binaries)                    │
  └───────────────────────────────────────────────┘
```

## 8. Conclusion

Under relaxed constraints, **84% of e2e_external tests can eliminate direct `pkg/client` and `pkg/pdclient` dependencies**. The remaining 16% (12 tests) are protocol-correctness tests that fundamentally require raw gRPC access — this matches the PostgreSQL model where `libpq` protocol tests stay in C while behavioral regression tests use `psql`.

The recommended implementation path is Phase 1+2 (9 days, 64% coverage) as a minimum viable conversion, with Phase 3 (4 additional days) providing the full 84% coverage for teams that want maximum CLI-based testing.

Key insight from this revised study: the original study's 48% "permanently library-only" figure was inflated because it treated all `DialTikvClient` usage as irreplaceable. Under relaxed constraints, **most `DialTikvClient` usage is for standard CRUD operations that CLI can handle** — only the protocol-inspection use cases (versioned reads, lock scanning, response field checking) are truly irreplaceable.
