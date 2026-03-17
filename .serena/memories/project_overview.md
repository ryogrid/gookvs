# Project Overview

**TiKV** - A distributed transactional key-value database powered by Rust and Raft.
Version: 9.0.0-beta.2, Apache-2.0 licensed.

## Tech Stack
- **Language**: Rust (nightly-2026-01-30)
- **Build**: Cargo + Makefile
- **Storage engine**: RocksDB
- **Consensus**: Raft
- **RPC**: gRPC (protobuf)
- **Allocator**: jemalloc (default), with tcmalloc/mimalloc/snmalloc options

## Code Structure
- `/src/` - Main TiKV server (config, coprocessor, import, server, storage/mvcc/txn)
- `/components/` - Modular libraries (raftstore, engine_rocks, engine_traits, cdc, backup, encryption, pd_client, tikv_util, etc.)
- `/cmd/` - Binary entry points (tikv-server, tikv-ctl)
- `/tests/` - Integration tests
- `/fuzz/` - Fuzzing targets

## Key Features (Cargo)
- Default: test-engine-kv-rocksdb, test-engine-raft-raft-engine
- failpoints, testexport, mem-profiling, openssl-vendored, pprof-fp
