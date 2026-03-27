# Review 06: Entry Points, Configuration, Codec, and Type Packages

**Reviewer:** Claude Opus 4.6 (1M context)
**Date:** 2026-03-27
**Scope:** `cmd/`, `internal/config/`, `pkg/codec/`, `pkg/keys/`, `pkg/txntypes/`, `internal/coprocessor/`

---

## Summary

Overall these packages are well-structured and follow Go idioms. The codec and key encoding logic is correct and consistent with TiKV's format. However, there are several bugs ranging from a critical data corruption issue in the coprocessor's float64 datum encoding, to startup mode logic gaps in the server entry point, and missing validation in configuration.

**Critical: 1** | **High: 4** | **Medium: 6** | **Low: 5**

---

## Critical Issues

### C-1: Float64 datum encoding uses wrong struct field (data corruption)

**File:** `internal/coprocessor/endpoint.go`, line 609
**Severity:** Critical

The `encodeDatum` function for `KindFloat64` reads `d.I64` instead of converting `d.F64` to its IEEE 754 bit representation:

```go
case KindFloat64:
    buf := make([]byte, 9)
    buf[0] = 0x03
    binary.BigEndian.PutUint64(buf[1:], uint64(d.I64)) // BUG: should use math.Float64bits(d.F64)
    return buf
```

The `Float64Datum` constructor sets `d.F64`, not `d.I64`. So `d.I64` will always be zero (or whatever garbage the struct zero-value is). Every float64 value will be encoded as 0.

The corresponding `decodeDatum` at line 652 has the same bug in reverse -- it stores the raw bits into `I64` instead of converting via `math.Float64frombits`:

```go
case 0x03: // Float64
    v := int64(binary.BigEndian.Uint64(data[1:9]))
    return Datum{Kind: KindFloat64, I64: v}, 9, nil  // BUG: should set F64
```

**Fix:** Use `math.Float64bits(d.F64)` for encoding and `math.Float64frombits(uint64(...))` for decoding, storing into `d.F64`.

---

## High Severity Issues

### H-1: `--store-id=0` with `--initial-cluster` silently creates a store with ID 0

**File:** `cmd/gookv-server/main.go`, lines 151-281
**Severity:** High

When `--initial-cluster` is provided but `--store-id` is omitted (defaults to 0), the server proceeds with `*storeID = 0`. Store ID 0 is typically invalid in TiKV/PD (PD uses 0 as "unset"). This leads to:
- `StoreCoordinatorConfig.StoreID` = 0
- The PD bootstrap registers a store with ID 0
- Raft peer IDs of 0 are created (line 270: `peers = append(peers, &metapb.Peer{Id: sid, StoreId: sid})`)

Raft explicitly forbids peer ID 0 (`etcd/raft` will panic or reject it).

**Fix:** Validate that `*storeID != 0` when `--initial-cluster` is provided, similar to how `gookv-pd` validates `--pd-id`.

### H-2: SelectionExecutor incorrectly treats non-null non-int64 datums as falsy

**File:** `internal/coprocessor/coprocessor.go`, lines 357-358
**Severity:** High

The selection filter checks:
```go
if val.IsNull() || val.I64 == 0 {
    pass = false
    break
}
```

If the predicate evaluation returns a `Uint64Datum` or `Float64Datum`, the code checks `val.I64` which is the wrong field. A `Uint64Datum(1)` has `I64 == 0` (zero value), so it would be incorrectly filtered out. While the current comparison functions always return `Int64Datum`, this is a latent bug if any expression path returns a different kind.

**Fix:** Convert to a truthiness check that handles all datum kinds properly (e.g., check the appropriate field based on `val.Kind`).

### H-3: `ReadableSize.UnmarshalText` does not support fractional values

**File:** `internal/config/config.go`, lines 42-73
**Severity:** High

The parser uses `%d` (integer) format for the numeric portion, so values like `"1.5GB"` or `"0.5MB"` will fail to parse. More importantly, this means the `UnmarshalText` function does not validate that there are no trailing characters after the numeric value. For example, `"128XMB"` would parse as `128MB` because `Sscanf("%d")` stops at the first non-digit.

**Fix:** Use `strconv.ParseUint` with validation that the entire numeric portion is consumed, or at minimum verify no unexpected characters remain.

### H-4: `parseInitialCluster` in `gookv-server` silently ignores invalid entries

**File:** `cmd/gookv-server/main.go`, lines 442-460
**Severity:** High

Unlike `gookv-pd`'s `parseClusterString` which returns errors for malformed entries, the server's `parseInitialCluster` silently skips entries with parsing errors (e.g., non-numeric store IDs, missing `=` sign). This can cause a cluster to bootstrap with fewer peers than intended, leading to split-brain or consensus failures. The only check is `len(clusterMap) == 0` (line 154), which only catches the case where *all* entries are invalid.

**Fix:** Return errors for individual parse failures, matching `gookv-pd`'s behavior, or at minimum log warnings for skipped entries.

---

## Medium Severity Issues

### M-1: Duplicate code between bootstrap mode and join mode

**File:** `cmd/gookv-server/main.go`, lines 163-172 and 333-342
**Severity:** Medium (maintainability)

The peer config override block is duplicated verbatim between bootstrap and join modes. Any future config field added to one path may be forgotten in the other.

**Recommendation:** Extract into a helper function like `applyPeerConfigOverrides(cfg, peerCfg)`.

### M-2: `EncodeRPNExpression` does not handle all constant types

**File:** `internal/coprocessor/endpoint.go`, lines 500-528
**Severity:** Medium

`EncodeRPNExpression` only encodes `KindInt64` and `KindString` constants. If a constant is `KindUint64`, `KindFloat64`, `KindBytes`, or `KindNull`, it silently produces no output for that node. The decode side has no corresponding cases for Uint64, Float64, Bytes, or Null constants either. This means roundtrip encode/decode will silently lose data for these types.

### M-3: `HashAggrExecutor.buildGroupKey` has ambiguous group key encoding

**File:** `internal/coprocessor/coprocessor.go`, lines 916-940
**Severity:** Medium

The group key is built by concatenating `fmt.Fprintf` formatted values separated by `|`. This has two problems:
1. An int64 value `100` and a string value `"100"` would produce the same group key, causing incorrect grouping across different types.
2. String values containing `|` would cause incorrect splitting (e.g., `"a|b"` as a single value vs. two values `"a"` and `"b"`).

**Fix:** Include the type tag in the group key, and use a non-ambiguous separator or length-prefixed encoding.

### M-4: Config validation does not check `StatusAddr`

**File:** `internal/config/config.go`, `Validate()` method, lines 240-291
**Severity:** Medium

`Server.Addr` is validated for non-emptiness, but `Server.StatusAddr` is not. An empty status address would cause the HTTP status server to fail on startup. The validation should also check for obviously invalid addresses (e.g., missing port).

### M-5: `RaftLogKeyRange` end key is `RaftStateKey`, which may include the state key

**File:** `pkg/keys/keys.go`, lines 114-117
**Severity:** Medium (correctness note)

The comment says `RaftStateSuffix (0x02) > RaftLogSuffix (0x01)` to justify using `RaftStateKey` as the exclusive upper bound. This is correct for the range `[RaftLogKey(regionID, 0), RaftStateKey(regionID))` because the suffix byte 0x02 > 0x01. However, this means the range also includes any key with suffix 0x01 and a log index > what's stored. This is actually fine since Pebble/RocksDB uses byte comparison, but the approach is fragile -- if a new suffix 0x01XX is added between log and state suffixes, it would be incorrectly included.

### M-6: `AggrState.Update` for MIN/MAX uses `Sum` field for comparison tracking

**File:** `internal/coprocessor/coprocessor.go`, lines 709-724
**Severity:** Medium

The `AggrState` for `AggrMin` and `AggrMax` reuses the `Sum` field to track the current min/max float64 value for comparison, while storing the actual datum in `Min`/`Max`. This is confusing and would break if the min/max datum is a string or bytes type that cannot be meaningfully compared via `ToFloat64()`.

---

## Low Severity Issues

### L-1: `gookv-ctl` uses `os.Exit` in subcommand handlers instead of returning errors

**File:** `cmd/gookv-ctl/main.go`, throughout
**Severity:** Low

All command functions call `os.Exit(1)` on error, making the `RunCommand` function (line 730) unusable for actual testing since it can never signal failure through its return value -- the process would exit first.

### L-2: `gookv-pd` always creates log directory even if logging to stdout

**File:** `cmd/gookv-pd/main.go`, lines 74-77
**Severity:** Low

The log directory is created unconditionally, even when the user might only want console output. This is a minor issue since the directory creation is harmless.

### L-3: Unexported `ReadableSize` constant does not support `TB`

**File:** `internal/config/config.go`, lines 35-40, 49-64
**Severity:** Low

The `UnmarshalText` method handles GB, MB, KB, B but not TB. While unlikely to be needed now, it's a gap compared to TiKV's config parser.

### L-4: `Lock.Marshal` skips short values when they are explicitly empty (length 0)

**File:** `pkg/txntypes/lock.go`, line 69
**Severity:** Low

The condition `len(l.ShortValue) > 0` means a non-nil but empty `ShortValue` (`[]byte{}`) will not be serialized. In TiKV, an empty short value is semantically meaningful for `WriteTypeDelete` (delete tombstone with inline empty value). However, in practice, a nil vs empty distinction is unlikely to matter since the delete type is inferred from `WriteType`.

### L-5: `evalArith` integer overflow detection uses float64 comparison

**File:** `internal/coprocessor/coprocessor.go`, lines 650-658
**Severity:** Low

The check `result >= math.MinInt64 && result <= math.MaxInt64` uses float64 comparison. Since `float64` cannot represent all `int64` values exactly (it only has 53 bits of mantissa), this check is imprecise for very large int64 values. For example, `math.MaxInt64` as float64 is rounded to `9.223372036854776e+18`, which is actually `math.MaxInt64 + 1`. The `int64()` cast would then overflow.

---

## Positive Observations

1. **Codec correctness:** The memcomparable byte encoding (`pkg/codec/bytes.go`) correctly implements TiKV's encoding scheme. The ascending/descending variants, padding logic, and roundtrip behavior are all sound.

2. **Number encoding:** The `encodeInt64ToComparable` and `encodeFloat64ToComparable` functions (`pkg/codec/number.go`) correctly implement sign-bit flipping for comparable encoding. The float64 encoding properly handles negative numbers by flipping all bits, and positive numbers by flipping only the sign bit.

3. **Lock/Write serialization:** The binary serialization format (`pkg/txntypes/lock.go`, `write.go`) is well-implemented with proper tag-based extensibility, matching TiKV's format. The use of varint for timestamps and lengths is efficient.

4. **Key encoding:** The key layout in `pkg/keys/keys.go` is clean and correct, with proper separation of local/data prefixes and region-scoped key construction.

5. **Configuration system:** The TOML-based config with CLI overrides pattern in `cmd/gookv-server/main.go` is well-designed. The `LoadFromFile` function correctly applies defaults first, then overrides with file values.

6. **Graceful shutdown:** Both `gookv-server` and `gookv-pd` properly handle SIGINT/SIGTERM with ordered cleanup.

---

## Recommendations

1. **Fix C-1 immediately** -- the float64 datum encoding bug will corrupt any coprocessor response containing floating-point values.
2. **Add validation for store-id=0** (H-1) to prevent silent cluster misconfiguration.
3. **Unify `parseInitialCluster`** between server and PD to use the same error-reporting implementation (H-4).
4. **Add roundtrip tests** for all codec paths: `EncodeBytes/DecodeBytes` with empty input, max-length groups, and `EncodeUint64/DecodeUint64` with 0, MaxUint64, and boundary values.
5. **Add roundtrip tests for datum encoding** in the coprocessor to catch the float64 bug and any future similar issues.
