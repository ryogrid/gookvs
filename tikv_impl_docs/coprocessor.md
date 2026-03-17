# Coprocessor: Push-Down Computation

This document specifies TiKV's coprocessor subsystem — the mechanism by which TiDB pushes query computation down to the storage layer. It covers the request model, DAG executor pipeline, expression evaluation, batch execution, the Coprocessor V2 plugin system, and resource enforcement.

**Cross-references:** [Architecture Overview](architecture_overview.md) §5 request lifecycle; [Key Encoding](key_encoding_and_data_formats.md) §3–4 for CF layouts; [Transaction and MVCC](transaction_and_mvcc.md) §6 for snapshot isolation.

---

## 1. Coprocessor Request Model

### 1.1 How TiDB Pushes Down Computation

TiDB compiles SQL queries into physical plans. For operations that can be executed close to the data — table scans, index scans, filtering, aggregation, top-N — TiDB serializes a sub-plan as a protobuf `DagRequest` and sends it to TiKV as a coprocessor request. TiKV executes the sub-plan against its local data and returns encoded rows, avoiding the cost of transferring raw data to TiDB for processing.

**Entry point:** `src/coprocessor/endpoint.rs` — the `Endpoint` struct receives gRPC coprocessor requests and dispatches them.

### 1.2 Request Types

Three request types are dispatched based on `coppb::Request.tp`:

| Type Code | Name | Purpose |
|-----------|------|---------|
| `REQ_TYPE_DAG` (103) | DAG Request | Query execution — the primary path |
| `REQ_TYPE_ANALYZE` (104) | Analyze Request | Table/index statistics collection for the optimizer |
| `REQ_TYPE_CHECKSUM` (105) | Checksum Request | Data integrity verification |

A raw `coppb::Request` contains:
- **`context`**: Request metadata — region ID, peer, isolation level, timestamps
- **`data`**: Serialized protobuf payload (`DagRequest`, `AnalyzeReq`, or `ChecksumRequest`)
- **`ranges`**: Key ranges to scan (region-local)
- **`start_ts`**: Transaction start timestamp for MVCC reads

### 1.3 DagRequest Structure

The `DagRequest` (defined in `tipb` protobuf) carries:

| Field | Description |
|-------|-------------|
| `executors` | Ordered array of executor descriptors forming the execution pipeline |
| `output_offsets` | Which output columns TiDB wants returned |
| `start_ts_fallback` | Fallback timestamp if `start_ts` is 0 in the outer request |
| `encode_type` | Result encoding format (row-by-row or chunk) |
| `collect_execution_summaries` | Whether to collect per-executor performance statistics |
| `paging_size` | Optional pagination size for incremental result delivery |
| `is_cache_enabled` | Whether query result caching is active |

### 1.4 Request Processing Flow

```
gRPC coprocessor request
  │
  ▼
parse_request_and_check_memory_locks_impl()
  ├── Deserialize protobuf (with recursion limit check)
  ├── Check in-memory locks (ConcurrencyManager)
  └── Dispatch by request type
  │
  ▼
RequestHandlerBuilder closure created
  │
  ▼
Schedule on read pool (YATP thread pool)
  ├── Memory quota check
  └── Concurrency semaphore acquisition
  │
  ▼
Retrieve MVCC snapshot (with timeout)
  │
  ▼
Build handler (BatchDagHandler / AnalyzeContext / ChecksumContext)
  │
  ▼
handler.handle_request()
  ├── Build executor tree from DagRequest.executors
  ├── Loop: next_batch(batch_size)
  │     ├── Pull batch from executor pipeline
  │     ├── Encode rows into response
  │     ├── Check deadline
  │     └── Collect statistics
  └── Return SelectResponse
  │
  ▼
Pack into coppb::Response
  ├── Set cache flags if applicable
  ├── Record metrics and slow log
  └── Return to gRPC layer
```

### 1.5 Streaming Requests

When `is_streaming=true`, the endpoint calls `handle_streaming_request()` repeatedly instead of `handle_request()`. Each call returns `(Option<Response>, bool)` — the next batch of rows and whether the stream is finished. This supports cursor-style result delivery for large result sets. Streaming uses `stream_batch_row_limit` (rather than `batch_row_limit`) and supports paging via `paging_size` for resumable scans.

### 1.6 Handler Builder Pattern

The endpoint uses a lazy builder pattern: it creates a `RequestHandlerBuilder<Snapshot>` closure that captures the parsed request but defers handler construction until after the snapshot is retrieved. This decouples snapshot acquisition (async, may timeout) from handler construction (synchronous, cheap).

---

## 2. DAG Executor Pipeline

### 2.1 Executor Types

All executors implement the `BatchExecutor` trait (§3.1) and form a pull-based pipeline. The first executor must be a scan (leaf) operator; subsequent executors wrap their predecessor.

**Leaf (Scan) Executors:**

| Executor | Location | Description |
|----------|----------|-------------|
| `BatchTableScanExecutor` | `tidb_query_executors/src/table_scan_executor.rs` | Scans base table rows from CF_DEFAULT/CF_WRITE. Handles row format decoding, column defaults, primary key extraction. Supports forward and backward scanning. |
| `BatchIndexScanExecutor` | `tidb_query_executors/src/index_scan_executor.rs` | Scans index entries. Decodes index key structure, supports unique index optimization for point lookups, and fills common handle keys for index-lookup joins. |

**Processing Executors:**

| Executor | Location | Description |
|----------|----------|-------------|
| `BatchSelectionExecutor` | `selection_executor.rs` | WHERE clause filtering. Evaluates RPN predicate expressions and modifies `logical_rows` to exclude non-matching rows — no data copying. |
| `BatchProjectionExecutor` | `projection_executor.rs` | SELECT column extraction/transformation. Evaluates RPN expressions to compute output columns. Optimization: column-ref-only projections skip copying. |
| `BatchLimitExecutor` | `limit_executor.rs` | LIMIT/OFFSET. Truncates `logical_rows` without touching physical data. When the source is a scan executor, the limit is pushed down to avoid over-reading. |
| `BatchTopNExecutor` | `top_n_executor.rs` | ORDER BY ... LIMIT. Uses a heap to select top-N rows efficiently. |
| `BatchPartitionTopNExecutor` | `partition_top_n_executor.rs` | Partitioned top-N for window function support. |

**Aggregation Executors:**

| Executor | Location | Description |
|----------|----------|-------------|
| `BatchSimpleAggregationExecutor` | `simple_aggr_executor.rs` | Non-grouped aggregation (GROUP BY absent). Produces a single output row with aggregate values (COUNT, SUM, AVG, etc.). |
| `BatchFastHashAggregationExecutor` | `fast_hash_aggr_executor.rs` | Hash-based GROUP BY — fast path. Uses direct-mapped hash tables optimized for integer group keys. Chosen when all aggregate functions support it. |
| `BatchSlowHashAggregationExecutor` | `slow_hash_aggr_executor.rs` | Hash-based GROUP BY — generic fallback. Handles complex data types and unsupported fast-path cases. |
| `BatchStreamAggregationExecutor` | `stream_aggr_executor.rs` | GROUP BY over pre-sorted input. More memory-efficient than hash aggregation when data arrives ordered by group keys. |

**Composite Executor:**

| Executor | Location | Description |
|----------|----------|-------------|
| `BatchIndexLookUpExecutor` | Not in main executor list | Two-stage: index scan → table row lookup by primary key. Used when the index alone does not provide all needed columns. |

### 2.2 Executor Composition

Executors are built bottom-up from the `DagRequest.executors` array by `BatchExecutorsRunner::build_executors()` in `runner.rs`:

```
build_executors(executor_descriptors, storage, ranges, ...):
  1. Match first descriptor → must be TableScan or IndexScan
     → Construct scan executor with storage, ranges, column info
  2. For each subsequent descriptor:
     → Match type (Selection, Projection, Aggregation, TopN, Limit, ...)
     → Construct executor wrapping the previous one as its source
     → Wrap with WithSummaryCollector for per-executor metrics
  3. Return Box<dyn BatchExecutor>
```

The resulting structure is a nested chain: the top executor pulls from its child, which pulls from its child, and so on down to the scan executor that reads from the storage engine.

### 2.3 Executor Support Checking

Before building, `BatchExecutorsRunner::check_supported()` validates that all executor descriptors in the request are implementable. Unsupported operators (e.g., Join, Window, Sort without limit) cause the request to be rejected — TiDB falls back to executing those operators locally.

### 2.4 Execution Loop

The `BatchExecutorsRunner` drives the pipeline in a loop (`handle_request` / `handle_streaming_request`):

```
loop:
  result = top_executor.next_batch(batch_size)
  encode result.physical_columns[result.logical_rows] into response
  if result.is_drained == Drain or PagingDrain:
    break
  check deadline — return DeadlineExceeded if expired
  collect per-executor statistics
  batch_size = min(batch_size * BATCH_GROW_FACTOR, MAX_BATCH_SIZE)
```

The batch size starts small and grows by `BATCH_GROW_FACTOR = 2` to amortize startup cost while avoiding excessive memory use.

---

## 3. Batch Execution Model

### 3.1 BatchExecutor Trait

All executors implement this trait (defined in `components/tidb_query_executors/src/interface.rs`):

```rust
pub trait BatchExecutor: Send {
    /// Output schema (column types).
    fn schema(&self) -> &[FieldType];

    /// Pull the next batch of rows. scan_rows hints how many to read.
    async fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult;

    /// Collect execution statistics into dest.
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats);

    /// Collect storage-level statistics.
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats);

    /// Return the key range scanned so far.
    fn take_scanned_range(&mut self) -> IntervalRange;

    /// Whether results can be cached.
    fn can_be_cached(&self) -> bool;
}
```

### 3.2 BatchExecuteResult

Each `next_batch()` call returns:

```rust
pub struct BatchExecuteResult {
    /// Column-oriented data storage.
    pub physical_columns: LazyBatchColumnVec,

    /// Indices of valid rows within physical_columns (post-filtering).
    pub logical_rows: Vec<usize>,

    /// Whether the source is exhausted.
    pub is_drained: Result<BatchExecIsDrain>,
}

pub enum BatchExecIsDrain {
    Remain,       // More data available
    Drain,        // Source exhausted
    PagingDrain,  // Paging boundary reached
}
```

### 3.3 Column-Oriented Storage

Data is stored column-by-column in `LazyBatchColumnVec` (from `tidb_query_datatype`), not as row tuples. Each column is a `LazyBatchColumn` that supports lazy decoding — raw bytes are only decoded to typed values when an executor actually accesses them. This design enables:

- **Vectorized operations**: Functions process entire column vectors at once
- **Cache efficiency**: Sequential access within a column is cache-friendly
- **Reduced allocation**: Columns are reusable buffers

### 3.4 Logical Rows Optimization

The `logical_rows: Vec<usize>` field is a key optimization. Rather than copying surviving rows after a filter, the `BatchSelectionExecutor` simply produces a new `logical_rows` vector containing the indices of rows that passed the predicate. Downstream executors iterate using these indices, avoiding data movement entirely. This is particularly effective for filter-heavy workloads.

### 3.5 Batch vs Row-Based Execution

TiKV uses **batch execution exclusively** (the row-based executor path has been superseded). Key differences from a hypothetical row-at-a-time model:

| Aspect | Row-Based | Batch (TiKV's model) |
|--------|-----------|---------------------|
| Granularity | One row per `next()` call | 32–256+ rows per `next_batch()` call |
| Data layout | Row tuples | Column vectors (`LazyBatchColumnVec`) |
| Filtering | Copy surviving rows | Modify `logical_rows` indices |
| Expression eval | Per-row function calls | Vectorized over entire batch |
| Function call overhead | High (per row) | Amortized across batch |

---

## 4. Expression Evaluation Framework

### 4.1 RPN (Reverse Polish Notation) Model

TiDB expression trees are converted to a flat RPN (postfix) representation for efficient batch evaluation. This conversion happens in `RpnExpressionBuilder::build_from_expr_tree()`.

**RPN Expression Structure** (`components/tidb_query_expr/src/types/expr.rs`):

```rust
pub struct RpnExpression(Vec<RpnExpressionNode>);

pub enum RpnExpressionNode {
    FnCall {
        func_meta: RpnFnMeta,       // Function implementation pointer
        args_len: usize,             // Number of arguments to pop from stack
        field_type: FieldType,       // Output type
        metadata: Box<dyn Any + Send>, // Pre-computed function metadata
    },
    Constant {
        value: ScalarValue,          // Literal value
        field_type: FieldType,
    },
    ColumnRef {
        offset: usize,              // Column index in input schema
    },
}
```

### 4.2 Stack-Based Evaluation

`RpnExpression::eval()` (in `types/expr_eval.rs`) uses a stack-based evaluator:

```
eval(schema, is_arrow_encodable, physical_columns, logical_rows):
  stack = []
  for node in rpn_nodes:
    match node:
      ColumnRef(offset) → push reference to physical_columns[offset]
      Constant(value)   → push scalar value (broadcasts to all rows)
      FnCall(meta, n)   → pop n args from stack
                           call meta.fn_ptr(ctx, output_rows, args, extra, metadata)
                           push result VectorValue onto stack
  return stack.pop()  // final result
```

The evaluator operates on entire batches: each function call processes all rows in the batch at once, enabling vectorized implementations.

### 4.3 RpnFnMeta: Function Metadata

Each built-in function is described by an `RpnFnMeta` struct (`types/function.rs`):

```rust
pub struct RpnFnMeta {
    pub name: &'static str,
    pub validator_ptr: fn(expr: &Expr) -> Result<()>,
    pub metadata_expr_ptr: fn(expr: &mut Expr) -> Result<Box<dyn Any>>,
    pub fn_ptr: fn(
        ctx: &mut EvalContext,
        output_rows: usize,
        args: &[RpnStackNode],
        extra: &mut RpnFnCallExtra,
        metadata: &dyn Any,
    ) -> Result<VectorValue>,
}
```

- **`validator_ptr`**: Validates argument types at expression build time
- **`metadata_expr_ptr`**: Pre-computes metadata from the expression tree (e.g., compiled regex patterns)
- **`fn_ptr`**: The actual evaluation function, operating on batched arguments

### 4.4 Function Arguments

Functions receive arguments as `RpnStackNode` values, which may be:

- **`ScalarArg<T>`**: A single scalar value that broadcasts to all rows
- **`VectorArg<T, C>`**: A per-row vector value with NULL tracking via bit vectors

The `RpnFnArg` trait provides unified access, and the `#[rpn_fn]` macro generates the dispatch code.

### 4.5 Built-in Function Registry

The function dispatcher `map_expr_node_to_rpn_func()` in `components/tidb_query_expr/src/lib.rs` maps `ScalarFuncSig` protobuf enum values to `RpnFnMeta` implementations. There are **1500+ function variants** (including type-specialized versions) organized by domain:

| Domain | Examples |
|--------|----------|
| **Arithmetic** | `PlusInt` (4 variants by signedness), `MinusInt`, `MultiplyInt`, `DivideReal`, `ModInt` |
| **Comparison** | `LtInt`, `LeReal`, `GtDecimal`, `GeString`, `EqJson`, `NullEq` |
| **String** | `Concat`, `Length`, `LTrim`, `Upper`, `Lower`, `Substr`, `Replace`, `Like` (per charset/collation) |
| **Math** | `Sin`, `Cos`, `Log`, `Sqrt`, `Pow`, `Abs`, `Ceil`, `Floor`, `Round` |
| **Time/Date** | `DateFormat`, `DayOfWeek`, `TimestampDiff`, `AddDatetime`, `ExtractDatetime` |
| **JSON** | `JsonExtract`, `JsonSet`, `JsonMerge`, `JsonType`, `JsonContains` |
| **Control Flow** | `If`, `IfNull`, `CaseWhen`, `Coalesce` |
| **Type Casting** | `CastIntAsReal`, `CastStringAsDecimal`, `CastJsonAsString` (~70+ cast combinations) |
| **Encryption** | `Md5`, `Sha1`, `AesEncrypt`, `AesDecrypt` |

Many functions are polymorphic — dispatched to type-specialized implementations based on field type, signedness, charset, or collation. For example, `PlusInt` dispatches to one of four implementations based on whether the left and right operands are signed or unsigned.

### 4.6 Adding New Built-in Functions

The pattern for adding a new scalar function:

1. **Implement the function** in the appropriate `impl_*.rs` file under `tidb_query_expr/src/`:
   ```rust
   #[rpn_fn]
   pub fn my_function(arg1: &Int, arg2: &Real) -> Result<Option<Int>> {
       // implementation
   }
   ```

2. The **`#[rpn_fn]` procedural macro** (from `tidb_query_codegen`) automatically generates:
   - A `my_function_fn_meta() -> RpnFnMeta` function
   - Argument type validation logic
   - Metadata constructor
   - Batch-capable dispatch wrapper

3. **Register in the dispatcher** — add a case to `map_expr_node_to_rpn_func()`:
   ```rust
   ScalarFuncSig::MyFunction => my_function_fn_meta(),
   ```

The `#[rpn_fn]` macro supports annotations for:
- `nullable` — function handles NULLs itself (receives `Option<&T>`)
- `capture = [ctx]` — function receives `EvalContext` for warnings/errors
- `metadata_mapper` — custom metadata pre-computation
- `min_args` / `max_args` — variadic argument support

---

## 5. Aggregation Functions

### 5.1 Aggregation Trait Hierarchy

Aggregation functions use a two-trait pattern (`components/tidb_query_aggr/src/`):

```
AggrFunction              ← Factory: creates state objects
  └─ AggrFunctionState    ← Generic interface (type-erased, accepts all types)
       └─ ConcreteAggrFunctionState  ← Type-safe specialization
```

- **`AggrFunction`** creates new `AggrFunctionState` instances (one per group)
- **`AggrFunctionState`** provides generic update/push_result methods
- **`ConcreteAggrFunctionState`** provides type-specialized `update()` and `push_result()` — the actual implementation

The `#[derive(AggrFunction)]` macro (from `tidb_query_codegen`) bridges both traits, generating the type-erased wrapper that dispatches to the concrete implementation.

### 5.2 Built-in Aggregation Functions

| Function | Description |
|----------|-------------|
| `COUNT` | Counts non-NULL values |
| `SUM` | Accumulates numeric values |
| `AVG` | Computes average (sum + count internally) |
| `MIN` / `MAX` | Tracks extreme values |
| `FIRST_VALUE` | Retains first value in group (for stream aggregation) |
| `BIT_AND` / `BIT_OR` / `BIT_XOR` | Bitwise aggregation operations |
| `VARIANCE` / `STD_DEV` | Statistical variance and standard deviation |
| `GROUP_CONCAT` | String concatenation with separator within groups |

### 5.3 Aggregation Executor Selection

The executor is chosen based on the query shape:

```
No GROUP BY                    → BatchSimpleAggregationExecutor
GROUP BY, input not sorted:
  All group keys are integers  → BatchFastHashAggregationExecutor  (fast path)
  Otherwise                    → BatchSlowHashAggregationExecutor  (generic)
GROUP BY, input pre-sorted     → BatchStreamAggregationExecutor
```

`BatchFastHashAggregationExecutor` uses direct-mapped hash tables for integer keys, avoiding hashing overhead. `BatchSlowHashAggregationExecutor` handles arbitrary key types with a generic hash map.

### 5.4 Update Semantics

Aggregation states support three update modes:

- **`update(ctx, value)`** — single value (one row)
- **`update_vector(ctx, physical_values, logical_rows)`** — vectorized batch update
- **`update_repeat(ctx, value, repeat_times)`** — repeated value (optimization for broadcast scalars)

---

## 6. Statistics Collection (Analyze)

The `AnalyzeContext` handles `REQ_TYPE_ANALYZE` requests for optimizer statistics. Located in `src/coprocessor/statistics/`.

### 6.1 Analyze Types

| Type | Description |
|------|-------------|
| `TypeColumn` | Column-level statistics (histograms, NDV) |
| `TypeIndex` | Index-level statistics |
| `TypeMixed` | Both column and index statistics in one pass |
| `TypeFullSampling` | Complete table scan for comprehensive statistics |

### 6.2 Statistics Data Structures

- **Histogram** (`histogram.rs`): Equi-depth histograms for value distribution estimation
- **CM Sketch** (`cmsketch.rs`): Count-min sketch for cardinality and frequency estimation
- **FM Sketch** (`fmsketch.rs`): Flajolet-Martin sketch for distinct value estimation

These data structures are serialized and sent back to TiDB for storage in the `mysql.stats_*` system tables.

---

## 7. Coprocessor V2: Plugin System

### 7.1 Overview

Coprocessor V2 (`src/coprocessor_v2/`) provides a plugin-based extension system for custom computation on TiKV nodes. Unlike the built-in coprocessor which executes TiDB's fixed set of SQL operators, V2 allows loading arbitrary Rust dynamic libraries that operate on raw key-value data.

### 7.2 Plugin Loading

**`PluginRegistry`** (`plugin_registry.rs`) manages the plugin lifecycle:

```
load_plugin(file_path):
  1. Load dynamic library via libloading
  2. Resolve _plugin_create() symbol → constructor
  3. Validate build compatibility:
     - API version (coprocessor_plugin_api crate version)
     - Rust compiler version (must match exactly)
     - Target triple (e.g., x86_64-unknown-linux-gnu)
  4. Call constructor → Box<dyn CoprocessorPlugin>
  5. Store in HashMap<String, LoadedPlugin> by plugin name
  6. Return plugin name
```

**Hot-Reloading:**

`start_hot_reloading(plugin_directory)` spawns a filesystem watcher thread that monitors a directory for `.so`/`.dylib`/`.dll` files:
- **File created**: Automatically load the plugin
- **File renamed**: Update the registered path
- **File deleted/modified**: Log warning (plugin remains loaded)

### 7.3 Plugin API Surface

Plugins implement the `CoprocessorPlugin` trait (from `coprocessor_plugin_api`):

```rust
pub trait CoprocessorPlugin: Send + Sync {
    fn on_raw_coprocessor_request(
        &self,
        ranges: Vec<Range<Key>>,        // Key ranges within the region
        request: RawRequest,             // Raw byte payload (Vec<u8>)
        storage: &dyn RawStorage,        // Storage access abstraction
    ) -> PluginResult<RawResponse>;      // Raw byte response (Vec<u8>)
}
```

**RawStorage trait** — the storage interface exposed to plugins:

| Operation | Signature | Description |
|-----------|-----------|-------------|
| `get` | `async fn get(key) -> Result<Option<Vec<u8>>>` | Single key lookup |
| `batch_get` | `async fn batch_get(keys) -> Result<Vec<KvPair>>` | Multi-key lookup |
| `scan` | `async fn scan(range) -> Result<Vec<KvPair>>` | Range scan |
| `put` | `async fn put(key, value) -> Result<()>` | Single write |
| `batch_put` | `async fn batch_put(pairs) -> Result<()>` | Batch write |
| `delete` | `async fn delete(key) -> Result<()>` | Single delete |
| `batch_delete` | `async fn batch_delete(keys) -> Result<()>` | Batch delete |
| `delete_range` | `async fn delete_range(range) -> Result<()>` | Range delete |

The `RawStorageImpl` (`raw_storage_impl.rs`) wraps TiKV's `Storage` struct to implement this trait, ensuring plugins interact with the storage layer through a controlled, stable interface.

### 7.4 Sandboxing Approach

Coprocessor V2 uses **process-level isolation** (not OS-level sandboxing):

- **ABI compatibility**: Strict version matching (rustc, target triple, API version) prevents memory layout mismatches
- **Memory allocation**: Plugins use TiKV's allocator via `HostAllocatorPtr`
- **Storage boundary**: Plugins access data only through the `RawStorage` trait, not raw engine handles
- **Region boundary**: Storage operations are scoped to the request's region
- **Error propagation**: Plugin panics/errors do not crash TiKV — errors are caught and returned as `PluginError`

**Limitations [Inferred]:**
- No process-level sandboxing (plugins run in the TiKV process)
- No automatic CPU/memory resource limits on plugin execution
- No timeout enforcement within plugin logic (relies on request-level deadlines)
- A misbehaving plugin can consume unbounded resources

### 7.5 V2 Request Flow

```
Client → gRPC raw_coprocessor() → Service::raw_coprocessor()
  │
  ▼
future_raw_coprocessor() (src/server/service/kv.rs)
  │
  ▼
copr_v2::Endpoint::handle_request_impl()
  ├── Look up plugin by req.copr_name
  ├── Validate req.copr_version_req (semver constraint)
  ├── Create RawStorageImpl wrapping request context
  ├── Convert key ranges from protobuf
  └── Call plugin.on_raw_coprocessor_request(ranges, data, storage)
  │
  ▼
Plugin executes custom logic (may call storage methods)
  │
  ▼
Result → RawCoprocessorResponse
  ├── Region errors extracted and propagated
  └── Plugin errors returned as string messages
```

---

## 8. Memory and Time Quota Enforcement

### 8.1 Memory Quota

**Global quota per endpoint:**
- `MemoryQuota` (arc-wrapped, shared across all concurrent requests)
- Configured via `end_point_memory_quota` (total capacity in bytes)
- Each request tracked via `OwnedAllocated<Arc<MemoryQuota>>`

**Enforcement:**
- `read_pool_spawn_with_memory_quota_check()` checks quota before scheduling execution
- Returns `Error::MemoryQuotaExceeded` if the global limit is reached
- Memory guard (`MemoryTraceGuard<Response>`) wraps the response, releasing tracked memory when the response is sent

### 8.2 Time Quota (Deadline)

Each request carries a `Deadline` computed as `request_start + max_handle_duration`:

| Configuration | Default | Description |
|---------------|---------|-------------|
| `end_point_request_max_handle_duration` | 60s | Maximum execution time per request |
| `end_point_slow_log_threshold` | 1s | Threshold for slow request logging |

**Enforcement points:**
1. Snapshot acquisition — wrapped in `async_timeout()`
2. After each batch in the executor loop — `Deadline::check()` returns `Error::DeadlineExceeded`
3. gRPC layer — request-level timeout

### 8.3 Concurrency Control

**Semaphore-based limiting:**
- `Semaphore` with `end_point_max_concurrency` permits
- **Light task optimization**: Requests estimated to take < 5ms bypass the semaphore, avoiding queueing delay for cheap operations
- Heavy tasks must acquire a permit before execution

**Interceptor chain** applied around request execution futures:
1. `concurrency_limiter::limit_concurrency()` — enforces the semaphore
2. `deadline::check_deadline()` — early deadline check before execution begins

### 8.4 Resource Control Integration

For resource group QoS:
- `ResourceGroupManager` tracks per-resource-group quotas
- `ResourceLimiter` enforces rate limits
- `TaskMetadata` tracks resource consumption per request
- Integration with the quota limiter enables throttling based on resource group priority

---

## 9. Query Result Caching

Located in `src/coprocessor/cache.rs`.

When `is_cache_enabled=true` in the request, the coprocessor supports client-side cache validation:

- **Cache key**: Derived from request data, key ranges, and data version
- **`cache_if_match_version`**: Client sends its cached version; if it matches the current data version, TiKV returns a cache-hit response without re-executing
- **`cache_last_version`**: Server returns the current version so the client can cache results

`CachedRequestHandler` intercepts requests to serve cached results when valid, avoiding executor pipeline construction entirely.

---

## 10. Configuration

Key coprocessor configuration parameters (from `src/coprocessor/config_manager.rs`):

| Parameter | Description |
|-----------|-------------|
| `end_point_batch_row_limit` | Maximum rows per batch for unary requests |
| `end_point_stream_batch_row_limit` | Maximum rows per batch for streaming requests |
| `end_point_stream_channel_size` | Channel capacity for streaming response delivery |
| `end_point_max_concurrency` | Maximum concurrent coprocessor requests (semaphore permits) |
| `end_point_memory_quota` | Total memory limit across all concurrent requests |
| `end_point_perf_level` | Performance monitoring level (affects detail of collected metrics) |
| `end_point_recursion_limit` | Protobuf deserialization recursion depth limit |
| `end_point_request_max_handle_duration` | Maximum execution time per request |
| `end_point_slow_log_threshold` | Threshold for logging slow requests |

---

## 11. Interface Pattern for Extension

### 11.1 Adding a New Executor Type

1. **Implement `BatchExecutor`** in a new file under `tidb_query_executors/src/`
2. **Add construction logic** in `runner.rs`'s `build_executors()` match arm
3. **Add support check** in `check_supported()` for the new `ExecType` variant
4. **Wire to protobuf**: The executor type must have a corresponding `tipb::ExecType` enum value

### 11.2 Adding a New Aggregation Function

1. **Define state struct** implementing `ConcreteAggrFunctionState` with `update()` and `push_result()`
2. **Derive `AggrFunction`** using the `#[derive(AggrFunction)]` macro
3. **Register in `AllAggrDefinitionParser`** — add a case in `parser.rs` for the new function name
4. The aggregation executor dispatches to the function via the `AggrFunctionState` trait

### 11.3 Adding a Coprocessor V2 Plugin

1. **Create a Rust cdylib** depending on `coprocessor_plugin_api`
2. **Implement `CoprocessorPlugin`** trait with `on_raw_coprocessor_request()`
3. **Export constructor** via `declare_plugin!()` macro (creates `_plugin_create` symbol)
4. **Deploy**: Place the `.so` in the plugin directory for hot-reload, or configure static loading

---

## 12. Key Source Locations

| Component | Path |
|-----------|------|
| Coprocessor endpoint | `src/coprocessor/endpoint.rs` |
| DAG handler | `src/coprocessor/dag/mod.rs` |
| Executor runner | `components/tidb_query_executors/src/runner.rs` |
| BatchExecutor trait | `components/tidb_query_executors/src/interface.rs` |
| Table scan executor | `components/tidb_query_executors/src/table_scan_executor.rs` |
| Index scan executor | `components/tidb_query_executors/src/index_scan_executor.rs` |
| Selection executor | `components/tidb_query_executors/src/selection_executor.rs` |
| Aggregation executors | `components/tidb_query_executors/src/{simple,fast_hash,slow_hash,stream}_aggr_executor.rs` |
| RPN expression model | `components/tidb_query_expr/src/types/expr.rs` |
| Expression evaluator | `components/tidb_query_expr/src/types/expr_eval.rs` |
| Function dispatcher | `components/tidb_query_expr/src/lib.rs` |
| RpnFnMeta definition | `components/tidb_query_expr/src/types/function.rs` |
| Aggregation traits | `components/tidb_query_aggr/src/lib.rs` |
| Aggregation parser | `components/tidb_query_aggr/src/parser.rs` |
| Data types | `components/tidb_query_datatype/src/` |
| Code generation macros | `components/tidb_query_codegen/src/rpn_function.rs` |
| Statistics/Analyze | `src/coprocessor/statistics/` |
| Cache | `src/coprocessor/cache.rs` |
| Configuration | `src/coprocessor/config_manager.rs` |
| Coprocessor V2 endpoint | `src/coprocessor_v2/endpoint.rs` |
| Plugin registry | `src/coprocessor_v2/plugin_registry.rs` |
| Plugin storage API | `src/coprocessor_v2/raw_storage_impl.rs` |
| Plugin API crate | `components/coprocessor_plugin_api/src/` |
