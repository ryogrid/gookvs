# Remaining Unimplemented and Incomplete Features in gookv

## 1. Overview

This document tracks features in the gookv codebase that are not yet fully implemented or remain partially complete. Items are verified against the Go source code.

As of 2026-03-20, the previous 12 remaining items have been addressed in branch `feat/remaining-items-and-multiregion-e2e`. The following features were implemented:

- **Async Commit / 1PC gRPC path** — `KvPrewrite` handler routes to 1PC or async commit paths based on request flags.
- **KvCheckSecondaryLocks** — Full handler with lock inspection and commit detection.
- **KvScanLock** — Iterates CF_LOCK with version filter and limit.
- **CLI `compact`** — Already implemented with `CompactAll()`/`CompactCF()` and `--flush-only` flag.
- **CLI `dump` SST parsing** — `--sst` flag for direct SST file reading via `pebble/sstable`.
- **Raw KV partial RPCs** — `RawBatchScan`, `RawGetKeyTTL` (with full TTL encoding), `RawCompareAndSwap` (TTL-aware), `RawChecksum` (CRC64 XOR).
- **TSO integration** — Server uses PD-allocated timestamps for 1PC commitTS and async commit maxCommitTS.
- **PD leader failover / retry** — Endpoint rotation, exponential backoff, reconnection on failure.
- **PD-coordinated split** — Split check ticker in peer, SplitCheckWorker wired in coordinator, AskBatchSplit/ExecBatchSplit/ReportBatchSplit flow.
- **Engine traits conformance tests** — 17 test cases covering WriteBatch, Snapshot, Iterator, cross-CF, concurrency.
- **Codec fuzz tests** — 6 fuzz targets for bytes and number codecs using `testing.F`.

Additionally, the **Client Library for Multi-Region Routing** has been implemented in `pkg/client/`:

- **PDStoreResolver** — Dynamic `storeID → gRPC address` resolution via PD, with TTL caching.
- **RegionCache** — Client-side `key → region → leader store` cache with sorted-slice binary search and error-driven invalidation.
- **RegionRequestSender** — gRPC connection pool with automatic retry on region errors (NotLeader, EpochNotMatch, etc.).
- **RawKVClient** — Full Raw KV API: Get, Put, PutWithTTL, Delete, GetKeyTTL, BatchGet, BatchPut, BatchDelete, Scan (cross-region), DeleteRange, CompareAndSwap, Checksum.
- **Server-side region validation** — `validateRegionContext()` added to 8 read-only Raw KV handlers for proper `region_error` propagation.
- **9 E2E tests** validating region routing, batch operations, scan across regions, and CAS.

## 2. Remaining Items

| # | Category | Feature | Status | Notes |
|---|----------|---------|--------|-------|
| 1 | gRPC / Coprocessor | BatchCoprocessor | Not implemented | Only `Coprocessor` and `CoprocessorStream` are wired. `BatchCoprocessor` remains a stub. |
| 2 | Client Library | TxnClient (2PC cross-region) | Not implemented | Transaction client with per-region prewrite/commit coordination. Deferred to future design. |
| 3 | Client Library | TSO batching | Not implemented | Batch `GetTS` calls and dispense from local buffer. Low priority optimization. |

### 2.1 BatchCoprocessor

`BatchCoprocessor` is a server-streaming RPC that dispatches a coprocessor request across multiple regions in a single call. Only `Coprocessor` (unary) and `CoprocessorStream` (server-streaming, single region) are currently implemented. `BatchCoprocessor` falls through to `UnimplementedTikvServer`.

This is a low-priority item since the single-region `Coprocessor` and `CoprocessorStream` RPCs cover the core functionality. `BatchCoprocessor` would primarily be a performance optimization for multi-region queries.
