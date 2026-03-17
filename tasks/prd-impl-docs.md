# PRD: TiKV Implementation Documentation Generation

## Introduction

Generate comprehensive English implementation documentation for the TiKV distributed key-value store repository. The documentation lives under `impl_docs/` and serves as the complete build specification for an AI coding agent (Claude Code orchestrated by Ralph) to reimplement TiKV from scratch in Rust — without access to the original source code.

The documentation must capture algorithms, data formats, concurrency models, correctness invariants, design rationale, and interface boundaries with enough precision that each subsystem can be rebuilt from its corresponding document plus the architecture overview.

## Goals

- Produce 8 self-contained specification documents under `impl_docs/`, each covering a major TiKV subsystem
- Extract precise details from actual source code (`src/`, `components/`) — no vague summaries
- Enable a Rust-capable AI agent to reimplement each subsystem using only the generated docs
- Documents are produced one at a time, iteratively refined before moving to the next
- Depth is adaptive: proportional to subsystem complexity
- Detail level is mixed: byte-level for formats/encoding, algorithmic pseudocode for core logic, high-level for orchestration
- Ambiguities in source code are resolved with reasonable inferences, clearly marked as `[Inferred]`
- External interfaces (gRPC, PD, TiDB) are documented as abstract boundaries, not exact wire-compatibility specs

## User Stories

### US-001: Create architecture overview document
**Description:** As an AI agent, I want an architecture overview so that I understand TiKV's overall structure, component relationships, and request lifecycle before diving into subsystems.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/architecture_overview.md`
- [ ] Documents overall system architecture: TiKV as a distributed transactional KV store
- [ ] Includes component dependency graph: crate responsibilities and inter-crate dependencies
- [ ] Documents thread/async-task model: which components run on which threads/runtimes, communication channels
- [ ] Traces request lifecycle for both KV read and KV write (gRPC ingress -> Raft -> storage -> response)
- [ ] Describes cluster topology: PD-based discovery, region assignment, leader balancing
- [ ] Explains design philosophy and key trade-offs (Percolator over 2PC variants, region-based sharding rationale)
- [ ] Inferences from code are marked with `[Inferred]`
- [ ] Cross-references other `impl_docs/` documents where subsystem details are expanded

### US-002: Create key encoding and data formats document
**Description:** As an AI agent, I want byte-level specifications for all key encoding schemes and data formats so that I can implement compatible storage layers.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/key_encoding_and_data_formats.md`
- [ ] Documents key encoding scheme: user keys -> internal keys transformation (the `keys` component), with byte-level format
- [ ] Specifies MVCC key format: version/timestamp encoding, ordering guarantees, with exact byte layout
- [ ] Documents Lock, Write, and Default column family layouts and what each stores, with field-level detail
- [ ] Describes on-disk format considerations: RocksDB key-value layout, SST file usage patterns
- [ ] Summarizes key `.proto` file definitions and their role in the system
- [ ] Documents serialization conventions used across components
- [ ] All format specifications include byte-level diagrams or tables where applicable

### US-003: Create Raft and replication document
**Description:** As an AI agent, I want full Raft and region management specifications so that I can implement the consensus and replication layer.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/raft_and_replication.md`
- [ ] Documents Raft protocol implementation: election, log replication, snapshotting, conf-change — as algorithmic pseudocode
- [ ] Specifies region abstraction: key range mapping, region epoch semantics
- [ ] Documents region lifecycle: creation, split, merge — exact sequences and conditions
- [ ] Describes raftstore message processing: batch-system architecture, PeerMsg/StoreMsg dispatch model
- [ ] Specifies snapshot generation and application: data included, format, transfer protocol
- [ ] Documents raftstore-v2 differences from v1: architectural changes and motivation
- [ ] Lists safety invariants that prevent data loss or corruption

### US-004: Create transaction and MVCC document
**Description:** As an AI agent, I want implementable Percolator protocol specifications and MVCC details so that I can build the transaction layer.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/transaction_and_mvcc.md`
- [ ] Specifies full Percolator protocol: prewrite/commit/rollback/check-txn-status as algorithmic pseudocode
- [ ] Documents lock types (pessimistic, optimistic, shared) and their state machines
- [ ] Specifies write record types (Put, Delete, Lock, Rollback) and their semantics
- [ ] Documents MvccTxn and MvccReader abstractions: responsibilities and interaction patterns
- [ ] Describes conflict detection: how write conflicts and lock conflicts are detected and resolved
- [ ] Documents timestamp oracle interaction: start_ts and commit_ts acquisition and usage
- [ ] Specifies GC and compaction filter: old version cleanup mechanism
- [ ] Documents resolved timestamp: computation algorithm and role in CDC/stale reads
- [ ] Describes concurrency control at the storage layer

### US-005: Create coprocessor document
**Description:** As an AI agent, I want the coprocessor push-down computation specifications so that I can implement the query execution layer.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/coprocessor.md`
- [ ] Documents coprocessor request model: how TiDB pushes down computation
- [ ] Specifies DAG executor pipeline: executor types (TableScan, IndexScan, Selection, Aggregation, TopN, Limit), interfaces, and composition
- [ ] Documents expression evaluation framework: representation and evaluation
- [ ] Describes batch vs row-based execution model differences
- [ ] Specifies Coprocessor V2: plugin loading, sandboxing, API surface
- [ ] Documents memory and time quota enforcement mechanisms
- [ ] Defines the interface pattern for adding new executor types or built-in functions

### US-006: Create gRPC API and server document
**Description:** As an AI agent, I want the server layer specification so that I can implement request routing, flow control, and inter-node communication.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/grpc_api_and_server.md`
- [ ] Lists all gRPC services and RPCs with their semantics (as abstract interface boundaries)
- [ ] Documents request routing: how each RPC maps to internal storage/txn operations
- [ ] Describes flow control: backpressure, rate limiting, request queuing mechanisms
- [ ] Documents connection and session management
- [ ] Specifies status server: HTTP endpoints and diagnostics
- [ ] Describes PD client protocol: heartbeat, region reporting, scheduling command handling (as abstract boundary)
- [ ] Documents inter-node Raft message transport

### US-007: Create CDC and backup document
**Description:** As an AI agent, I want CDC and backup subsystem specifications so that I can implement change capture and data protection features.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/cdc_and_backup.md`
- [ ] Documents CDC architecture: event generation from Raft apply, delegate model, event filtering/assembly
- [ ] Specifies CDC protocol: change event streaming to downstream (as abstract boundary)
- [ ] Describes raftstore observer pattern: how CDC and backup register for Raft state changes
- [ ] Documents full backup process: SST file export, external storage abstraction (S3/GCS/Azure as interface)
- [ ] Specifies backup-stream (PITR): log backup architecture, checkpoint management, incremental change capture
- [ ] Documents resolved timestamp propagation: region-level to store-level aggregation

### US-008: Create resource control, security, and config document
**Description:** As an AI agent, I want resource control, security, and configuration specifications so that I can implement operational infrastructure.

**Acceptance Criteria:**
- [ ] Creates `impl_docs/resource_control_security_config.md`
- [ ] Documents resource control: quota model, priority scheduling, token allocation/consumption
- [ ] Specifies encryption at rest: key management hierarchy (master key, data keys), what is encrypted, rotation
- [ ] Documents TLS: certificate handling, mutual TLS configuration, security component API
- [ ] Specifies configuration system: config struct hierarchy, defaults, runtime config change propagation (online config)
- [ ] Documents logging and diagnostics conventions

## Functional Requirements

- FR-1: Each document must be generated by thoroughly reading the relevant source code under `src/` and `components/`, not by summarizing comments or README files alone
- FR-2: Documents are produced sequentially, one at a time, starting with `architecture_overview.md` and proceeding through US-002 to US-008 in order
- FR-3: Each completed document is reviewed/refined before moving to the next
- FR-4: Format specifications (key encoding, MVCC layout, protobuf structures) must include byte-level detail: field offsets, byte order, length encoding
- FR-5: Core algorithms (Raft, Percolator, conflict detection, GC) must be expressed as implementable pseudocode with clear pre/post-conditions
- FR-6: Orchestration and architectural descriptions use high-level prose with component interaction diagrams (in text/ASCII form)
- FR-7: Where source code is ambiguous or undocumented, the agent makes reasonable inferences and marks them with `[Inferred]`
- FR-8: External system interfaces (gRPC API, PD protocol, TiDB interaction) are documented as abstract boundaries — describing what the interface does, not exact wire-level compatibility
- FR-9: Each document cross-references related documents when there are interface dependencies (e.g., transaction doc references key encoding doc for MVCC key format)
- FR-10: All documents are written in English and saved under the `impl_docs/` directory
- FR-11: Document depth is proportional to subsystem complexity — simple subsystems get concise treatment, complex ones (Raft, transactions) get exhaustive detail

## Non-Goals

- No implementation of any TiKV code — this PRD covers documentation generation only
- No exact wire-compatibility specifications for external protocols (gRPC, PD) — abstract boundaries only
- No tutorial-style or narrative writing — these are build specifications
- No documentation of test infrastructure or CI/CD pipelines
- No documentation of third-party library internals (RocksDB internals, gRPC library internals)
- No translation to languages other than English
- No generation of code scaffolding or skeleton projects alongside documentation

## Technical Considerations

- The TiKV codebase is large (~500k+ lines of Rust). The agent must navigate `src/` and `components/` efficiently, using symbolic code exploration tools rather than reading entire files
- Key components to analyze: `components/keys/`, `components/raftstore/`, `components/raftstore-v2/`, `src/storage/`, `src/storage/mvcc/`, `src/storage/txn/`, `src/coprocessor/`, `src/server/`, `components/cdc/`, `components/backup/`, `components/backup-stream/`, `components/encryption/`, `components/resource_control/`, `components/security/`, `src/config/`
- Protobuf definitions (`.proto` files) need to be located and summarized — check for `kvproto` or similar proto dependencies
- The iterative one-at-a-time approach allows the agent to build understanding progressively, with earlier documents informing later ones
- Each document should target the context window of the consuming AI agent — structure with clear headers and numbered references for easy lookup

## Success Metrics

- Each document contains sufficient detail that an AI agent can implement the corresponding subsystem without accessing TiKV source code
- All key algorithms are expressed as pseudocode with pre/post-conditions
- All data formats have byte-level specifications
- Ambiguities are flagged as `[Inferred]` — not silently guessed
- Documents are internally consistent and cross-reference each other correctly
- A Rust developer reviewing any document can confirm it accurately reflects the TiKV implementation

## Open Questions

- Should the architecture overview document include a recommended implementation order for the clone project (i.e., which subsystem to build first)?
- How should the agent handle features that depend on external crates with significant logic (e.g., `raft-rs`)? Document the expected behavior or reference the external crate?
- Should version-specific behavior (e.g., raftstore v1 vs v2) be documented as separate specifications or as a single spec with version annotations?
- What is the protobuf source location — is `kvproto` vendored or a git dependency? This affects how `.proto` files are analyzed.
