# PRD: gookvs Design Documentation Generation

## Introduction

Generate comprehensive English design documents for **gookvs**, a TiKV-like distributed key-value store implemented in Go. The documents are placed under `design_docs/` and serve as the complete design specification for Claude Code agents (orchestrated by Ralph) to implement gookvs iteratively using a test-first approach.

gookvs is NOT a port of TiKV. It is a new Go system that maintains **external interface compatibility** with TiKV (gRPC API, PD protocol) while using **Go-idiomatic internal design** (goroutines, channels, interfaces, Go ecosystem libraries). RocksDB is used as the storage engine.

## Goals

- Produce 9 design documents under `design_docs/`, each covering a major gookvs subsystem
- Reference `impl_docs/` (TiKV implementation docs) as the primary source, with TiKV source code (`src/`, `components/`) as a secondary oracle
- Start with a 1:1 mapping to `impl_docs/` structure but diverge where Go idioms warrant it
- Include key Go interface and struct definitions with method signatures — enough detail to write tests against, not every field
- Use mermaid diagrams extensively in every document (architecture, sequence, state machine, data flow)
- Present 2-3 Go library options per major function with pros/cons comparison tables
- Maintain `08_priority_and_scope.md` incrementally alongside other documents
- Enable test-first implementation: specify observable behavior, interfaces, and invariants rather than internal implementation steps

## User Stories

### US-001: Create architecture overview document
**Description:** As an AI agent, I want an architecture overview of gookvs so that I understand the Go-based system structure, package layout, goroutine model, and request lifecycle before implementing subsystems.

**Acceptance Criteria:**
- [ ] Creates `design_docs/00_architecture_overview.md`
- [ ] Documents overall gookvs system architecture as a Go-based distributed transactional KV store
- [ ] Includes component dependency graph with recommended Go package structure (`cmd/`, `pkg/`, `internal/`)
- [ ] Documents goroutine model: which components run in which goroutines, channel-based communication patterns
- [ ] Contains mermaid sequence diagrams tracing KV read and KV write request lifecycles (gRPC -> Raft -> storage -> response)
- [ ] Describes cluster topology: PD-based discovery, region assignment, leader balancing
- [ ] Explains key design decisions: where gookvs diverges from TiKV's internal design and why
- [ ] Contains mermaid diagrams for: system architecture, component dependency graph, request lifecycle sequences, goroutine model
- [ ] References `impl_docs/architecture_overview.md` as source; diverges where Go idioms warrant it
- [ ] Provides 2-3 library options with pros/cons table for key infrastructure (gRPC framework, logging, metrics)

### US-002: Create key encoding and data formats document
**Description:** As an AI agent, I want byte-level key encoding and data format specifications so that I can implement storage layers compatible with TiKV's external key format.

**Acceptance Criteria:**
- [ ] Creates `design_docs/01_key_encoding_and_data_formats.md`
- [ ] Documents key encoding scheme: user keys to internal keys (compatible with TiKV's external format for client compatibility)
- [ ] Specifies MVCC key format: version/timestamp encoding, ordering guarantees
- [ ] Documents Lock, Write, and Default column family layouts
- [ ] Provides Go struct definitions with key method signatures for internal data representations
- [ ] Presents 2-3 RocksDB Go binding options with pros/cons comparison table
- [ ] Documents protobuf message handling: reuse of TiKV's .proto definitions
- [ ] Contains mermaid diagrams for: key encoding transformation flow, column family data layout
- [ ] References `impl_docs/key_encoding_and_data_formats.md` as source

### US-003: Create Raft and replication document
**Description:** As an AI agent, I want Raft and region management design specifications so that I can implement the consensus and replication layer using Go Raft libraries.

**Acceptance Criteria:**
- [ ] Creates `design_docs/02_raft_and_replication.md`
- [ ] Documents Raft implementation strategy with 2-3 Go Raft library options (e.g., etcd/raft, hashicorp/raft) with pros/cons table
- [ ] Specifies region abstraction: key range mapping, region epoch semantics
- [ ] Documents region lifecycle: creation, split, merge with exact sequences and conditions
- [ ] Describes Go channel-based message dispatch model (replacing TiKV's batch-system)
- [ ] Specifies snapshot generation and application
- [ ] Lists safety invariants that prevent data loss or corruption
- [ ] Contains mermaid diagrams for: region lifecycle state machine, message dispatch flow, Raft replication sequence
- [ ] Provides key Go interface definitions (Region, RaftNode, MessageRouter) with method signatures
- [ ] References `impl_docs/raft_and_replication.md` as source; diverges for Go channel-based architecture

### US-004: Create transaction and MVCC document
**Description:** As an AI agent, I want Percolator protocol and MVCC design specifications so that I can implement the transaction layer with testable Go interfaces.

**Acceptance Criteria:**
- [ ] Creates `design_docs/03_transaction_and_mvcc.md`
- [ ] Specifies Percolator protocol: prewrite/commit/rollback/check-txn-status algorithms
- [ ] Documents lock types (pessimistic, optimistic, shared) and their state machines
- [ ] Specifies write record types (Put, Delete, Lock, Rollback) and semantics
- [ ] Provides Go interface definitions for MVCC reader/writer with method signatures
- [ ] Describes conflict detection and resolution as observable behavior
- [ ] Documents timestamp oracle interaction
- [ ] Specifies GC mechanism and resolved timestamp computation
- [ ] Contains mermaid diagrams for: transaction lifecycle sequence, lock state machines, conflict resolution flow
- [ ] References `impl_docs/transaction_and_mvcc.md` as source

### US-005: Create coprocessor document
**Description:** As an AI agent, I want the coprocessor push-down computation design so that I can implement the query execution layer with composable Go executor interfaces.

**Acceptance Criteria:**
- [ ] Creates `design_docs/04_coprocessor.md`
- [ ] Documents coprocessor request handling model
- [ ] Specifies DAG executor pipeline: executor types (TableScan, IndexScan, Selection, Aggregation, TopN, Limit) with Go interface definitions and composition pattern
- [ ] Documents expression evaluation framework
- [ ] Specifies memory and time quota enforcement mechanisms
- [ ] Contains mermaid diagrams for: executor pipeline flow, request processing sequence
- [ ] References `impl_docs/coprocessor.md` as source

### US-006: Create gRPC API and server document
**Description:** As an AI agent, I want the server layer design so that I can implement a gRPC server compatible with TiKV's external API.

**Acceptance Criteria:**
- [ ] Creates `design_docs/05_grpc_api_and_server.md`
- [ ] Lists complete gRPC API surface (must be compatible with TiKV's external API)
- [ ] Documents request routing from RPCs to internal operations
- [ ] Describes flow control and backpressure mechanisms using Go patterns
- [ ] Documents PD client protocol as abstract boundary
- [ ] Specifies inter-node Raft message transport
- [ ] Provides Go gRPC server setup and middleware design with interface definitions
- [ ] Presents 2-3 options for gRPC middleware/interceptor patterns with pros/cons
- [ ] Contains mermaid diagrams for: request routing flow, gRPC service hierarchy
- [ ] References `impl_docs/grpc_api_and_server.md` as source

### US-007: Create CDC and backup document
**Description:** As an AI agent, I want CDC and backup subsystem designs so that I can implement change capture and data protection with Go-native patterns.

**Acceptance Criteria:**
- [ ] Creates `design_docs/06_cdc_and_backup.md`
- [ ] Documents CDC architecture in Go: event generation, streaming via goroutines/channels
- [ ] Describes Raft observer pattern adapted to Go interfaces
- [ ] Documents backup process: SST export, external storage abstraction
- [ ] Specifies log backup (PITR) architecture
- [ ] Presents 2-3 options for external storage abstraction libraries (S3/GCS/Azure) with pros/cons
- [ ] Contains mermaid diagrams for: CDC event flow, backup process sequence
- [ ] References `impl_docs/cdc_and_backup.md` as source

### US-008: Create resource control, security, and config document
**Description:** As an AI agent, I want resource control, security, and configuration designs so that I can implement operational infrastructure using Go ecosystem conventions.

**Acceptance Criteria:**
- [ ] Creates `design_docs/07_resource_control_security_config.md`
- [ ] Documents resource control: quota model, priority scheduling
- [ ] Specifies encryption at rest: key management hierarchy
- [ ] Documents TLS configuration
- [ ] Specifies configuration system: Go struct hierarchy, runtime config change propagation
- [ ] Presents 2-3 options for logging library (slog, zerolog, zap) with pros/cons table
- [ ] Contains mermaid diagrams for: resource control flow, config propagation
- [ ] References `impl_docs/resource_control_security_config.md` as source

### US-009: Create and maintain priority and scope document
**Description:** As a user deciding implementation scope, I want a comprehensive priority ranking of all gookvs components so that I can determine what to build and in what order.

**Acceptance Criteria:**
- [ ] Creates `design_docs/08_priority_and_scope.md`
- [ ] Organizes ALL components/features into 4 tiers: Tier 1 (Essential), Tier 2 (Important), Tier 3 (Nice-to-have), Tier 4 (Optional)
- [ ] Each component/feature includes: tier justification, estimated complexity (S/M/L/XL), and dependencies on other components
- [ ] Provides recommended implementation order within each tier
- [ ] Document is updated incrementally as other design documents are completed — each iteration adds/refines entries based on the subsystem just designed
- [ ] Includes a summary table: component name | tier | complexity | dependencies | status (designed/pending)
- [ ] Accounts for test-first implementation: notes which TiKV test areas map to each component for test adaptation

## Functional Requirements

- FR-1: Each design document must reference the corresponding `impl_docs/` document as primary source, with TiKV source code (`src/`, `components/`) as secondary oracle
- FR-2: Document structure starts as 1:1 mapping with `impl_docs/` but diverges where Go idioms, library availability, or design simplification warrant it — divergences must be noted with rationale
- FR-3: Every document must include mermaid-format diagrams for all major concepts (minimum 2 diagrams per document)
- FR-4: Key Go interfaces and structs must be provided with method signatures — sufficient to write test cases against, but not exhaustive field listings
- FR-5: For each major infrastructure function, present 2-3 Go library options with a pros/cons comparison table (columns: library name, pros, cons, recommendation)
- FR-6: All designs must reflect Go-idiomatic patterns: interfaces, struct embedding, goroutine concurrency, channel communication, context cancellation, error return values
- FR-7: External interfaces (gRPC API, PD protocol, protobuf messages) must be compatible with TiKV — internal data structures and concurrency patterns may differ freely
- FR-8: RocksDB must be used as the storage engine; document via Go bindings
- FR-9: Documents must specify observable behavior and invariants (not just implementation steps) to support test-first development
- FR-10: `08_priority_and_scope.md` must be updated incrementally as each other document is completed
- FR-11: Each document must be self-contained enough that an agent can implement the subsystem using only that document plus `00_architecture_overview.md`
- FR-12: Cross-reference other `design_docs/` documents when interface dependencies exist
- FR-13: All documents are in English markdown, saved under `design_docs/`

## Non-Goals

- No actual Go code implementation — this PRD covers design document generation only
- No direct Rust-to-Go porting or translation of TiKV code
- No exact replication of TiKV's internal data structures or concurrency model
- No wire-level compatibility specification for internal protocols — only external API compatibility
- No documentation of TiKV's test infrastructure (tests will be adapted separately during implementation)
- No benchmarking or performance testing design — focus on correctness and functionality first
- No CI/CD pipeline or deployment design

## Design Considerations

- **Library comparison tables** are a key output: implementing agents need to make informed library choices. Each table should include: library name, GitHub stars/activity (if known), API style, pros, cons, and a soft recommendation
- **Mermaid diagrams** should use consistent styling across documents: same node shapes for same component types, consistent color coding if used
- **Go interface definitions** should follow Go naming conventions: single-method interfaces where possible, `er` suffix (Reader, Writer, Scheduler), exported types for public APIs

## Technical Considerations

- The `impl_docs/` directory contains 8 documents covering TiKV's internals — these are the primary reference
- TiKV source code is a large Rust codebase (~500k+ lines) — use symbolic exploration tools, do not read entire files
- Key TiKV components to cross-reference: `components/keys/`, `components/raftstore/`, `src/storage/`, `src/storage/mvcc/`, `src/storage/txn/`, `src/coprocessor/`, `src/server/`, `components/cdc/`, `components/backup/`
- Protobuf definitions from TiKV's kvproto dependency should be reused directly for external API compatibility
- Go library landscape to evaluate: etcd/raft vs hashicorp/raft, gorocksdb vs grocksdb, various cloud storage SDKs
- The iterative one-at-a-time approach with incremental priority updates allows progressive design refinement

## Success Metrics

- Each design document contains sufficient Go interface definitions and behavioral specifications that an AI agent can write test cases without additional context
- All documents include mermaid diagrams that accurately represent the designed system
- Library comparison tables cover all major infrastructure choices with actionable recommendations
- `08_priority_and_scope.md` provides a clear, tiered ranking that enables scope decision-making
- A Go developer reviewing any document can confirm it describes a feasible, idiomatic Go implementation
- Design documents are internally consistent and cross-reference each other correctly
- Divergences from TiKV's internal design are documented with rationale

## Open Questions

- Should gookvs support both raftstore v1 and v2 designs, or pick one? (This affects complexity significantly)
- Which kvproto version should be targeted for external API compatibility?
- Should the Coprocessor V2 plugin system (WASM/native) be included in gookvs design, or is the V1 DAG executor sufficient?
- How should the Go module structure handle the large number of subsystems — monorepo with `internal/` packages, or separate Go modules?
- For the test-first approach: should design docs include a "suggested test scenarios" section per component to guide test adaptation from TiKV?
