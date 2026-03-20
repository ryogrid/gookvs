# CLI and Test Improvements (Items 5, 6, 11, 12)

## 1. Overview

This document covers four improvement items for the gookv project:

| Item | Category | Description | Status |
|------|----------|-------------|--------|
| 5 | CLI | `compact` command with real Pebble compaction | Already implemented |
| 6 | CLI | SST file dump via `gookv-ctl dump --sst` | New |
| 11 | Testing | Engine traits conformance test suite | New |
| 12 | Testing | Codec fuzz tests for `pkg/codec` | New |

Items 5 and 6 relate to the admin CLI (`cmd/gookv-ctl/main.go`). Items 11 and 12 introduce structured test patterns for two foundational packages.

## 2. Item 5: CLI Compact Command (Already Implemented)

The `compact` command has already been implemented in `cmd/gookv-ctl/main.go` (function `cmdCompact`, lines 519-558) with full Pebble compaction support. The implementation matches the design from `design_docs/additional_impl/09_cli_improvements.md`:

- **`--cf <name>`**: Compact a specific column family via `eng.CompactCF(cf)`.
- **`--flush-only`**: Old behavior via `eng.SyncWAL()`.
- **Default (no flags)**: Compact all CFs via `eng.CompactAll()`.

The engine methods `Compact()`, `CompactCF()`, and `CompactAll()` are implemented on `*rocks.Engine` (`internal/engine/rocks/engine.go`, lines 210-234). The `openDBRocks()` helper (line 84) returns the concrete `*rocks.Engine` type to access these methods without polluting the `traits.KvEngine` interface.

**No further work required for Item 5.** Documentation update only: the design doc `09_cli_improvements.md` should be marked as completed for the compact section.

## 3. Item 6: SST File Dump

### 3.1 Purpose

Add a `--sst <path>` flag to the existing `dump` command that reads a Pebble SST file directly, without opening the full database. This is useful for:

- Inspecting SST files from backups or snapshots without a running database.
- Debugging compaction output files.
- Examining SST files from crashed or corrupted databases where `pebble.Open()` might fail.

### 3.2 Current `dump` Command

The existing `cmdDump` function (`cmd/gookv-ctl/main.go`, lines 361-415) supports:

```
Flags:
  --db <path>      Path to data directory (required)
  --cf <name>      Column family (default: "default")
  --limit <int>    Maximum entries (default: 50)
  --decode         Decode MVCC keys and record values
  --start <hex>    Start key in hex (inclusive)
  --end <hex>      End key in hex (exclusive)
```

It opens the full database, creates an iterator for the specified CF, and dumps entries in either raw hex or decoded MVCC format.

### 3.3 Proposed Addition

Add a `--sst <path>` flag that bypasses database opening and reads the SST file directly:

```
New flag:
  --sst <path>     Read directly from an SST file (no --db required)
```

When `--sst` is specified:
- `--db` becomes optional (and is ignored).
- `--cf` is still used for MVCC decode logic (the CF prefix byte is embedded in SST keys).
- `--decode` still controls MVCC decoding.

### 3.4 SST Reading via Pebble

Pebble provides `sstable.NewReader()` for direct SST file reading. The API:

```go
import "github.com/cockroachdb/pebble/sstable"

func readSST(path string) error {
    f, err := os.Open(path)
    if err != nil {
        return err
    }
    defer f.Close()

    stat, err := f.Stat()
    if err != nil {
        return err
    }

    readable, err := sstable.NewSimpleReadable(f)
    if err != nil {
        return err
    }

    reader, err := sstable.NewReader(readable, sstable.ReaderOptions{})
    if err != nil {
        return err
    }
    defer reader.Close()

    iter, err := reader.NewIter(nil /* lower */, nil /* upper */)
    if err != nil {
        return err
    }
    defer iter.Close()

    // Iterate all entries.
    for key, val := iter.First(); key != nil; key, val = iter.Next() {
        // key is *pebble.InternalKey, val is pebble.LazyValue
        userKey := key.UserKey
        value, _, err := val.Value(nil)
        if err != nil {
            continue
        }
        // Process userKey and value...
    }
    return nil
}
```

### 3.5 Key Interpretation in SST Context

SST files from gookv's Pebble database contain keys with the CF prefix byte prepended (see `internal/engine/rocks/engine.go`, `prefixKey()`). When reading an SST file:

1. The first byte of each key identifies the column family:
   - `0x00`: CFDefault
   - `0x01`: CFLock
   - `0x02`: CFWrite
   - `0x03`: CFRaft
2. The remaining bytes are the user key (MVCC-encoded for default/write/lock, local-format for raft).

The dump logic should strip the CF prefix and then apply the same `dumpDecoded()` function already used for database iteration.

### 3.6 SST Metadata Display

Before iterating entries, display SST file metadata:

```
SST File: /path/to/000042.sst
  Size:           4.2 MB
  Entry Count:    12,345
  Smallest Key:   00616263... (CF: default)
  Largest Key:    02ffffffff... (CF: write)
  Compression:    snappy
---
```

This metadata is available from `reader.Properties`:

```go
props := reader.Properties
fmt.Printf("Entry Count: %d\n", props.NumEntries)
fmt.Printf("Raw Key Size: %d\n", props.RawKeySize)
fmt.Printf("Raw Value Size: %d\n", props.RawValueSize)
fmt.Printf("Compression: %s\n", props.CompressionName)
```

### 3.7 Implementation in `cmdDump`

```go
func cmdDump(args []string) {
    fs := flag.NewFlagSet("dump", flag.ExitOnError)
    dbPath := fs.String("db", "", "Path to data directory")
    cf := fs.String("cf", cfnames.CFDefault, "Column family")
    limit := fs.Int("limit", 50, "Maximum entries")
    decode := fs.Bool("decode", false, "Decode MVCC keys and record values")
    startHex := fs.String("start", "", "Start key in hex (inclusive)")
    endHex := fs.String("end", "", "End key in hex (exclusive)")
    sstPath := fs.String("sst", "", "Read directly from SST file")
    fs.Parse(args)

    if *sstPath != "" {
        cmdDumpSST(*sstPath, *cf, *limit, *decode)
        return
    }

    if *dbPath == "" {
        fmt.Fprintln(os.Stderr, "Error: --db or --sst is required")
        os.Exit(1)
    }

    // ... existing database dump logic ...
}

func cmdDumpSST(sstPath, cf string, limit int, decode bool) {
    // 1. Open SST file.
    // 2. Print metadata.
    // 3. Iterate entries, optionally filtering by CF prefix.
    // 4. For each entry, strip CF prefix and call dumpDecoded() or print raw hex.
}
```

### 3.8 CF Filtering in SST Mode

When `--cf` is specified in SST mode, only entries whose CF prefix byte matches are displayed. When `--cf` is omitted, all entries are shown with the CF name prefixed:

```
[CF: default] 616263...    48656c6c6f...
[CF: write]   616263...    500a00...
[CF: lock]    616263...    010a...
```

### 3.9 Testing

| Test | Description |
|------|-------------|
| `TestCmdDumpSST` | Create a Pebble DB, write data, flush to SST, dump the SST file, verify output |
| `TestCmdDumpSSTDecode` | Same as above with `--decode`, verify MVCC decoded output |
| `TestCmdDumpSSTCFFilter` | Write to multiple CFs, dump SST with `--cf write`, verify only write CF entries appear |
| `TestCmdDumpSSTMetadata` | Verify SST metadata (entry count, sizes) is printed |
| `TestCmdDumpSSTNotFound` | Non-existent SST path returns error |

### 3.10 Files to Modify

| File | Change |
|------|--------|
| `cmd/gookv-ctl/main.go` | Add `--sst` flag to `cmdDump`, add `cmdDumpSST` function |
| `cmd/gookv-ctl/ctl_test.go` | Add SST dump test cases |
| `go.mod` | May need to add `github.com/cockroachdb/pebble/sstable` import (already available as transitive dep) |

## 4. Item 11: Engine Traits Conformance Tests

### 4.1 Purpose

The current test file `internal/engine/traits/traits_test.go` contains only minimal compile-time checks (3 tests, 32 lines):

```go
func TestInterfaceCompilability(t *testing.T) {
    var _ KvEngine = nil
    var _ Snapshot = nil
    var _ WriteBatch = nil
    var _ Iterator = nil
}

func TestErrors(t *testing.T) { ... }
func TestIterOptionsDefaults(t *testing.T) { ... }
```

There are no behavioral tests verifying that an actual `KvEngine` implementation satisfies the semantic contract of each interface method. A conformance test suite ensures that any engine implementation (Pebble-backed, in-memory, or future alternatives) correctly implements the trait contracts.

### 4.2 ConformanceSuite Pattern

The conformance suite is parameterized by a factory function that creates a fresh engine instance. This allows the same test suite to run against different implementations:

```go
// Package conformance provides behavioral tests for traits.KvEngine implementations.
package conformance

import (
    "testing"

    "github.com/ryogrid/gookv/internal/engine/traits"
)

// EngineFactory creates a fresh KvEngine for testing.
// The returned cleanup function closes and removes the engine.
type EngineFactory func(t *testing.T) (traits.KvEngine, func())

// RunAll runs the complete conformance test suite against the given factory.
func RunAll(t *testing.T, factory EngineFactory) {
    t.Run("WriteBatch", func(t *testing.T) {
        t.Run("Atomicity", func(t *testing.T) { testWriteBatchAtomicity(t, factory) })
        t.Run("SavePointRollback", func(t *testing.T) { testWriteBatchSavePoint(t, factory) })
        t.Run("Clear", func(t *testing.T) { testWriteBatchClear(t, factory) })
        t.Run("CountAndDataSize", func(t *testing.T) { testWriteBatchCountDataSize(t, factory) })
    })
    t.Run("Snapshot", func(t *testing.T) {
        t.Run("Isolation", func(t *testing.T) { testSnapshotIsolation(t, factory) })
        t.Run("IteratorConsistency", func(t *testing.T) { testSnapshotIterator(t, factory) })
    })
    t.Run("Iterator", func(t *testing.T) {
        t.Run("Boundaries", func(t *testing.T) { testIteratorBoundaries(t, factory) })
        t.Run("SeekForPrev", func(t *testing.T) { testIteratorSeekForPrev(t, factory) })
        t.Run("EmptyCF", func(t *testing.T) { testIteratorEmptyCF(t, factory) })
    })
    t.Run("CrossCF", func(t *testing.T) {
        t.Run("Isolation", func(t *testing.T) { testCrossCFIsolation(t, factory) })
    })
    t.Run("DeleteRange", func(t *testing.T) {
        t.Run("Basic", func(t *testing.T) { testDeleteRange(t, factory) })
        t.Run("ViaWriteBatch", func(t *testing.T) { testDeleteRangeWriteBatch(t, factory) })
    })
    t.Run("Concurrent", func(t *testing.T) {
        t.Run("ReadWriteAccess", func(t *testing.T) { testConcurrentAccess(t, factory) })
    })
}
```

### 4.3 Registering the Pebble Engine

In `internal/engine/rocks/engine_test.go`, add:

```go
func TestConformanceSuite(t *testing.T) {
    conformance.RunAll(t, func(t *testing.T) (traits.KvEngine, func()) {
        dir := t.TempDir()
        eng, err := rocks.Open(dir)
        require.NoError(t, err)
        return eng, func() { eng.Close() }
    })
}
```

### 4.4 Test Case Details

#### 4.4.1 WriteBatch Atomicity

Verifies that a committed write batch applies all operations atomically, and an uncommitted batch applies nothing.

```go
func testWriteBatchAtomicity(t *testing.T, factory EngineFactory) {
    eng, cleanup := factory(t)
    defer cleanup()

    // Batch with multiple puts.
    wb := eng.NewWriteBatch()
    require.NoError(t, wb.Put("default", []byte("k1"), []byte("v1")))
    require.NoError(t, wb.Put("default", []byte("k2"), []byte("v2")))
    require.NoError(t, wb.Put("default", []byte("k3"), []byte("v3")))

    // Before commit: keys should not exist.
    _, err := eng.Get("default", []byte("k1"))
    assert.ErrorIs(t, err, traits.ErrNotFound)

    // Commit.
    require.NoError(t, wb.Commit())

    // After commit: all keys exist.
    v1, err := eng.Get("default", []byte("k1"))
    require.NoError(t, err)
    assert.Equal(t, []byte("v1"), v1)

    v2, err := eng.Get("default", []byte("k2"))
    require.NoError(t, err)
    assert.Equal(t, []byte("v2"), v2)

    v3, err := eng.Get("default", []byte("k3"))
    require.NoError(t, err)
    assert.Equal(t, []byte("v3"), v3)
}
```

#### 4.4.2 SavePoint / Rollback

Verifies the copy-on-savepoint behavior implemented in `internal/engine/rocks/engine.go` (lines 398-441):

```go
func testWriteBatchSavePoint(t *testing.T, factory EngineFactory) {
    eng, cleanup := factory(t)
    defer cleanup()

    wb := eng.NewWriteBatch()
    require.NoError(t, wb.Put("default", []byte("a"), []byte("1")))

    wb.SetSavePoint()
    require.NoError(t, wb.Put("default", []byte("b"), []byte("2")))
    require.NoError(t, wb.Delete("default", []byte("a")))

    // Rollback: "b" put and "a" delete are undone.
    require.NoError(t, wb.RollbackToSavePoint())

    // Commit: only "a"="1" should exist.
    require.NoError(t, wb.Commit())

    v, err := eng.Get("default", []byte("a"))
    require.NoError(t, err)
    assert.Equal(t, []byte("1"), v)

    _, err = eng.Get("default", []byte("b"))
    assert.ErrorIs(t, err, traits.ErrNotFound)
}
```

Additional sub-cases:
- **Nested save points**: Set two save points, rollback to the inner one, verify outer operations are preserved.
- **PopSavePoint**: Set a save point, add operations, pop the save point (without rollback), commit -- all operations apply.
- **Rollback with no save point**: Returns error.

#### 4.4.3 Snapshot Isolation

Verifies that a snapshot provides a consistent point-in-time view:

```go
func testSnapshotIsolation(t *testing.T, factory EngineFactory) {
    eng, cleanup := factory(t)
    defer cleanup()

    // Write initial data.
    require.NoError(t, eng.Put("default", []byte("k1"), []byte("before")))

    // Take snapshot.
    snap := eng.NewSnapshot()
    defer snap.Close()

    // Write new data after snapshot.
    require.NoError(t, eng.Put("default", []byte("k1"), []byte("after")))
    require.NoError(t, eng.Put("default", []byte("k2"), []byte("new")))

    // Snapshot should see "before" for k1 and not see k2.
    v, err := snap.Get("default", []byte("k1"))
    require.NoError(t, err)
    assert.Equal(t, []byte("before"), v)

    _, err = snap.Get("default", []byte("k2"))
    assert.ErrorIs(t, err, traits.ErrNotFound)

    // Engine sees latest values.
    v, err = eng.Get("default", []byte("k1"))
    require.NoError(t, err)
    assert.Equal(t, []byte("after"), v)
}
```

#### 4.4.4 Iterator Boundaries

Verifies `IterOptions.LowerBound` and `UpperBound` are respected:

```go
func testIteratorBoundaries(t *testing.T, factory EngineFactory) {
    eng, cleanup := factory(t)
    defer cleanup()

    // Write keys a, b, c, d, e.
    for _, k := range []string{"a", "b", "c", "d", "e"} {
        require.NoError(t, eng.Put("default", []byte(k), []byte(k+"-val")))
    }

    // Iterate [b, d).
    iter := eng.NewIterator("default", traits.IterOptions{
        LowerBound: []byte("b"),
        UpperBound: []byte("d"),
    })
    defer iter.Close()

    var keys []string
    for iter.SeekToFirst(); iter.Valid(); iter.Next() {
        keys = append(keys, string(iter.Key()))
    }
    assert.NoError(t, iter.Error())
    assert.Equal(t, []string{"b", "c"}, keys)
}
```

#### 4.4.5 Cross-CF Isolation

Verifies that data written to one column family is not visible in another:

```go
func testCrossCFIsolation(t *testing.T, factory EngineFactory) {
    eng, cleanup := factory(t)
    defer cleanup()

    require.NoError(t, eng.Put("default", []byte("shared-key"), []byte("default-val")))
    require.NoError(t, eng.Put("write", []byte("shared-key"), []byte("write-val")))

    // Each CF returns its own value.
    v1, err := eng.Get("default", []byte("shared-key"))
    require.NoError(t, err)
    assert.Equal(t, []byte("default-val"), v1)

    v2, err := eng.Get("write", []byte("shared-key"))
    require.NoError(t, err)
    assert.Equal(t, []byte("write-val"), v2)

    // Delete from one CF does not affect the other.
    require.NoError(t, eng.Delete("default", []byte("shared-key")))
    _, err = eng.Get("default", []byte("shared-key"))
    assert.ErrorIs(t, err, traits.ErrNotFound)

    v2, err = eng.Get("write", []byte("shared-key"))
    require.NoError(t, err)
    assert.Equal(t, []byte("write-val"), v2)
}
```

#### 4.4.6 DeleteRange

Verifies that `DeleteRange(cf, start, end)` removes keys in `[start, end)`:

```go
func testDeleteRange(t *testing.T, factory EngineFactory) {
    eng, cleanup := factory(t)
    defer cleanup()

    for _, k := range []string{"a", "b", "c", "d", "e"} {
        require.NoError(t, eng.Put("default", []byte(k), []byte("val")))
    }

    // Delete [b, d).
    require.NoError(t, eng.DeleteRange("default", []byte("b"), []byte("d")))

    // "a", "d", "e" should remain; "b", "c" should be gone.
    _, err := eng.Get("default", []byte("a"))
    assert.NoError(t, err)
    _, err = eng.Get("default", []byte("b"))
    assert.ErrorIs(t, err, traits.ErrNotFound)
    _, err = eng.Get("default", []byte("c"))
    assert.ErrorIs(t, err, traits.ErrNotFound)
    _, err = eng.Get("default", []byte("d"))
    assert.NoError(t, err)
    _, err = eng.Get("default", []byte("e"))
    assert.NoError(t, err)
}
```

#### 4.4.7 Concurrent Access

Verifies that concurrent reads and writes do not panic or corrupt data:

```go
func testConcurrentAccess(t *testing.T, factory EngineFactory) {
    eng, cleanup := factory(t)
    defer cleanup()

    const goroutines = 10
    const opsPerGoroutine = 100

    var wg sync.WaitGroup
    wg.Add(goroutines)

    for g := 0; g < goroutines; g++ {
        go func(id int) {
            defer wg.Done()
            for i := 0; i < opsPerGoroutine; i++ {
                key := []byte(fmt.Sprintf("g%d-k%d", id, i))
                val := []byte(fmt.Sprintf("v%d", i))
                _ = eng.Put("default", key, val)
                _, _ = eng.Get("default", key)
            }
        }(g)
    }
    wg.Wait()
}
```

### 4.5 Full Test Case Matrix

| Suite | Test Case | Behavior Verified |
|-------|-----------|-------------------|
| WriteBatch | Atomicity | Uncommitted batch invisible; committed batch fully visible |
| WriteBatch | SavePointRollback | Single rollback discards ops after save point |
| WriteBatch | NestedSavePoints | Two-level rollback works correctly |
| WriteBatch | PopSavePoint | Pop without rollback retains operations |
| WriteBatch | RollbackNoSavePoint | Returns error |
| WriteBatch | Clear | Resets count, dataSize, and save points |
| WriteBatch | CountAndDataSize | Accurate after put, delete, rollback |
| Snapshot | Isolation | Writes after snapshot are invisible to snapshot |
| Snapshot | IteratorConsistency | Snapshot iterator sees point-in-time data |
| Iterator | Boundaries | LowerBound/UpperBound respected |
| Iterator | SeekForPrev | Positions at last key <= target |
| Iterator | EmptyCF | No panic on empty column family |
| Iterator | ReverseIteration | Prev() works correctly from SeekToLast() |
| CrossCF | Isolation | Same key in different CFs are independent |
| DeleteRange | Basic | Direct engine DeleteRange removes [start, end) |
| DeleteRange | ViaWriteBatch | WriteBatch DeleteRange + Commit removes range |
| Concurrent | ReadWriteAccess | No panics or data corruption under contention |

### 4.6 Files to Create/Modify

| File | Change |
|------|--------|
| `internal/engine/conformance/conformance.go` | New: `EngineFactory` type, `RunAll` function |
| `internal/engine/conformance/writebatch_test.go` | New: WriteBatch test functions |
| `internal/engine/conformance/snapshot_test.go` | New: Snapshot test functions |
| `internal/engine/conformance/iterator_test.go` | New: Iterator test functions |
| `internal/engine/conformance/crosscf_test.go` | New: Cross-CF and DeleteRange test functions |
| `internal/engine/conformance/concurrent_test.go` | New: Concurrent access test functions |
| `internal/engine/rocks/engine_test.go` | Add `TestConformanceSuite` registration |

**Alternative layout**: All conformance test logic in a single package `internal/engine/conformance` with the `RunAll` entry point. Tests are not `_test.go` files themselves -- they are exported functions called from each engine's test file.

## 5. Item 12: Codec Fuzz Tests

### 5.1 Purpose

The `pkg/codec` package provides memcomparable encoding (`EncodeBytes`/`DecodeBytes`) and number encoding (`EncodeUint64`/`DecodeUint64`, `EncodeInt64`/`DecodeInt64`, `EncodeFloat64`/`DecodeFloat64`). These are foundational to key encoding correctness throughout gookv.

The existing tests (`pkg/codec/bytes_test.go`, `pkg/codec/number_test.go`) use fixed test vectors ported from TiKV. Fuzz testing adds coverage for edge cases and arbitrary inputs that hand-written test vectors may miss.

### 5.2 Go Native Fuzzing

Go 1.18+ provides built-in fuzz testing via `testing.F`. Fuzz targets are placed in `_test.go` files and run with `go test -fuzz=FuzzName`.

### 5.3 Fuzz Target: Bytes Codec

File: `pkg/codec/bytes_fuzz_test.go`

```go
package codec

import (
    "bytes"
    "testing"
)

func FuzzEncodeDecodeBytes(f *testing.F) {
    // Seed corpus: edge cases.
    f.Add([]byte{})
    f.Add([]byte{0x00})
    f.Add([]byte{0xFF})
    f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})      // exact group size
    f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})      // all 0xFF
    f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})                           // group + 1
    f.Add(bytes.Repeat([]byte{0xAB}, 256))                               // large input

    f.Fuzz(func(t *testing.T, data []byte) {
        // Property 1: Round-trip (ascending).
        encoded := EncodeBytes(nil, data)
        decoded, remaining, err := DecodeBytes(encoded)
        if err != nil {
            t.Fatalf("DecodeBytes failed: %v", err)
        }
        if !bytes.Equal(data, decoded) {
            t.Fatalf("round-trip mismatch: got %v, want %v", decoded, data)
        }
        if len(remaining) != 0 {
            t.Fatalf("unexpected remaining bytes: %v", remaining)
        }

        // Property 2: Round-trip (descending).
        encodedDesc := EncodeBytesDesc(nil, data)
        decodedDesc, remainingDesc, err := DecodeBytesDesc(encodedDesc)
        if err != nil {
            t.Fatalf("DecodeBytesDesc failed: %v", err)
        }
        if !bytes.Equal(data, decodedDesc) {
            t.Fatalf("desc round-trip mismatch: got %v, want %v", decodedDesc, data)
        }
        if len(remainingDesc) != 0 {
            t.Fatalf("unexpected remaining bytes (desc): %v", remainingDesc)
        }

        // Property 3: Encoded length matches formula.
        expectedLen := EncodedBytesLength(len(data))
        if len(encoded) != expectedLen {
            t.Fatalf("encoded length %d != expected %d for input length %d",
                len(encoded), expectedLen, len(data))
        }
    })
}

func FuzzEncodeBytesOrdering(f *testing.F) {
    // Seed with pairs of byte slices.
    f.Add([]byte{}, []byte{0x00})
    f.Add([]byte{0x01}, []byte{0x02})
    f.Add([]byte{0xFF}, []byte{0xFF, 0x00})
    f.Add([]byte("hello"), []byte("world"))

    f.Fuzz(func(t *testing.T, a, b []byte) {
        encA := EncodeBytes(nil, a)
        encB := EncodeBytes(nil, b)
        rawCmp := bytes.Compare(a, b)
        encCmp := bytes.Compare(encA, encB)

        // Property: encoding preserves ordering.
        if (rawCmp < 0 && encCmp >= 0) ||
           (rawCmp > 0 && encCmp <= 0) ||
           (rawCmp == 0 && encCmp != 0) {
            t.Fatalf("ordering violated: Compare(%v,%v)=%d but Compare(enc,enc)=%d",
                a, b, rawCmp, encCmp)
        }

        // Property: descending encoding reverses ordering.
        encADesc := EncodeBytesDesc(nil, a)
        encBDesc := EncodeBytesDesc(nil, b)
        encCmpDesc := bytes.Compare(encADesc, encBDesc)

        if (rawCmp < 0 && encCmpDesc <= 0) ||
           (rawCmp > 0 && encCmpDesc >= 0) ||
           (rawCmp == 0 && encCmpDesc != 0) {
            t.Fatalf("desc ordering violated: Compare(%v,%v)=%d but Compare(encDesc,encDesc)=%d",
                a, b, rawCmp, encCmpDesc)
        }
    })
}
```

### 5.4 Fuzz Target: Number Codec

File: `pkg/codec/number_fuzz_test.go`

```go
package codec

import (
    "bytes"
    "math"
    "testing"
)

func FuzzEncodeDecodeUint64(f *testing.F) {
    f.Add(uint64(0))
    f.Add(uint64(1))
    f.Add(uint64(math.MaxUint32))
    f.Add(uint64(math.MaxUint64))
    f.Add(uint64(math.MaxUint64 / 2))

    f.Fuzz(func(t *testing.T, v uint64) {
        // Round-trip ascending.
        enc := EncodeUint64(nil, v)
        dec, rem, err := DecodeUint64(enc)
        if err != nil {
            t.Fatalf("DecodeUint64 failed: %v", err)
        }
        if dec != v {
            t.Fatalf("round-trip: got %d, want %d", dec, v)
        }
        if len(rem) != 0 {
            t.Fatalf("unexpected remaining: %v", rem)
        }

        // Round-trip descending.
        encDesc := EncodeUint64Desc(nil, v)
        decDesc, remDesc, err := DecodeUint64Desc(encDesc)
        if err != nil {
            t.Fatalf("DecodeUint64Desc failed: %v", err)
        }
        if decDesc != v {
            t.Fatalf("desc round-trip: got %d, want %d", decDesc, v)
        }
        if len(remDesc) != 0 {
            t.Fatalf("unexpected remaining (desc): %v", remDesc)
        }

        // Fixed length.
        if len(enc) != 8 {
            t.Fatalf("encoded length %d != 8", len(enc))
        }
    })
}

func FuzzUint64Ordering(f *testing.F) {
    f.Add(uint64(0), uint64(1))
    f.Add(uint64(100), uint64(math.MaxUint64))
    f.Add(uint64(math.MaxUint32), uint64(math.MaxUint32+1))

    f.Fuzz(func(t *testing.T, a, b uint64) {
        encA := EncodeUint64(nil, a)
        encB := EncodeUint64(nil, b)

        if a < b && bytes.Compare(encA, encB) >= 0 {
            t.Fatalf("ordering: %d < %d but enc(a) >= enc(b)", a, b)
        }
        if a > b && bytes.Compare(encA, encB) <= 0 {
            t.Fatalf("ordering: %d > %d but enc(a) <= enc(b)", a, b)
        }
        if a == b && !bytes.Equal(encA, encB) {
            t.Fatalf("ordering: %d == %d but enc(a) != enc(b)", a, b)
        }

        // Descending: reversed.
        encADesc := EncodeUint64Desc(nil, a)
        encBDesc := EncodeUint64Desc(nil, b)

        if a < b && bytes.Compare(encADesc, encBDesc) <= 0 {
            t.Fatalf("desc ordering: %d < %d but encDesc(a) <= encDesc(b)", a, b)
        }
        if a > b && bytes.Compare(encADesc, encBDesc) >= 0 {
            t.Fatalf("desc ordering: %d > %d but encDesc(a) >= encDesc(b)", a, b)
        }
    })
}

func FuzzEncodeDecodeInt64(f *testing.F) {
    f.Add(int64(0))
    f.Add(int64(-1))
    f.Add(int64(math.MinInt64))
    f.Add(int64(math.MaxInt64))

    f.Fuzz(func(t *testing.T, v int64) {
        enc := EncodeInt64(nil, v)
        dec, rem, err := DecodeInt64(enc)
        if err != nil {
            t.Fatalf("DecodeInt64 failed: %v", err)
        }
        if dec != v {
            t.Fatalf("round-trip: got %d, want %d", dec, v)
        }
        if len(rem) != 0 {
            t.Fatalf("unexpected remaining: %v", rem)
        }
    })
}

func FuzzInt64Ordering(f *testing.F) {
    f.Add(int64(-1000), int64(1000))
    f.Add(int64(math.MinInt64), int64(0))
    f.Add(int64(0), int64(math.MaxInt64))

    f.Fuzz(func(t *testing.T, a, b int64) {
        encA := EncodeInt64(nil, a)
        encB := EncodeInt64(nil, b)

        if a < b && bytes.Compare(encA, encB) >= 0 {
            t.Fatalf("ordering: %d < %d but enc(a) >= enc(b)", a, b)
        }
        if a > b && bytes.Compare(encA, encB) <= 0 {
            t.Fatalf("ordering: %d > %d but enc(a) <= enc(b)", a, b)
        }
        if a == b && !bytes.Equal(encA, encB) {
            t.Fatalf("ordering: %d == %d but enc(a) != enc(b)", a, b)
        }
    })
}

func FuzzEncodeDecodeFloat64(f *testing.F) {
    f.Add(0.0)
    f.Add(-0.0)
    f.Add(1.0)
    f.Add(-1.0)
    f.Add(math.MaxFloat64)
    f.Add(math.SmallestNonzeroFloat64)
    f.Add(math.Inf(1))
    f.Add(math.Inf(-1))

    f.Fuzz(func(t *testing.T, v float64) {
        if math.IsNaN(v) {
            t.Skip("NaN is not comparable")
        }

        enc := EncodeFloat64(nil, v)
        dec, rem, err := DecodeFloat64(enc)
        if err != nil {
            t.Fatalf("DecodeFloat64 failed: %v", err)
        }
        if dec != v {
            t.Fatalf("round-trip: got %v, want %v", dec, v)
        }
        if len(rem) != 0 {
            t.Fatalf("unexpected remaining: %v", rem)
        }
    })
}

func FuzzEncodeDecodeVarint(f *testing.F) {
    f.Add(uint64(0))
    f.Add(uint64(127))
    f.Add(uint64(128))
    f.Add(uint64(math.MaxUint64))

    f.Fuzz(func(t *testing.T, v uint64) {
        enc := EncodeVarint(nil, v)
        dec, rem, err := DecodeVarint(enc)
        if err != nil {
            t.Fatalf("DecodeVarint failed: %v", err)
        }
        if dec != v {
            t.Fatalf("round-trip: got %d, want %d", dec, v)
        }
        if len(rem) != 0 {
            t.Fatalf("unexpected remaining: %v", rem)
        }
    })
}
```

### 5.5 Seed Corpus Design

Each fuzz target includes explicit seed values covering:

| Category | Values | Rationale |
|----------|--------|-----------|
| Zero/empty | `[]byte{}`, `0`, `0.0` | Boundary condition |
| Single byte | `0x00`, `0xFF` | Min/max byte values |
| Group boundary | 8-byte slice, 9-byte slice | Exactly one group, group + 1 byte |
| Numeric extremes | `MaxUint64`, `MinInt64`, `MaxFloat64`, `Inf` | Overflow and special value handling |
| Sign boundaries | `-1`, `0`, `1` for signed types | Sign bit flip correctness |
| Special floats | `+0.0`, `-0.0`, `+Inf`, `-Inf` | IEEE 754 special cases |
| Large input | 256-byte repeated pattern | Multi-group encoding |

### 5.6 Properties Tested

| Property | Applicable To | Description |
|----------|---------------|-------------|
| Round-trip | All codecs | `Decode(Encode(x)) == x` |
| Order preservation (ascending) | `EncodeBytes`, `EncodeUint64`, `EncodeInt64`, `EncodeFloat64` | `a < b` implies `Encode(a) < Encode(b)` in byte comparison |
| Order reversal (descending) | `EncodeBytesDesc`, `EncodeUint64Desc` | `a < b` implies `Encode(a) > Encode(b)` |
| Fixed output length | `EncodeUint64`, `EncodeInt64`, `EncodeFloat64` | Always 8 bytes |
| Predictable output length | `EncodeBytes` | `EncodedBytesLength(len(input))` matches actual |
| No remaining bytes | All codecs (single value) | Decode consumes the entire encoded buffer |

### 5.7 Running Fuzz Tests

```bash
# Run a specific fuzz target for 30 seconds:
go test -fuzz=FuzzEncodeDecodeBytes -fuzztime=30s ./pkg/codec/

# Run all fuzz targets briefly (CI-friendly):
go test -fuzz=Fuzz -fuzztime=5s ./pkg/codec/

# Run standard tests (including fuzz seed corpus):
go test ./pkg/codec/
```

Note: `go test` without `-fuzz` runs fuzz targets as regular tests using only the seed corpus. This provides deterministic CI coverage.

### 5.8 Files to Create

| File | Contents |
|------|----------|
| `pkg/codec/bytes_fuzz_test.go` | `FuzzEncodeDecodeBytes`, `FuzzEncodeBytesOrdering` |
| `pkg/codec/number_fuzz_test.go` | `FuzzEncodeDecodeUint64`, `FuzzUint64Ordering`, `FuzzEncodeDecodeInt64`, `FuzzInt64Ordering`, `FuzzEncodeDecodeFloat64`, `FuzzEncodeDecodeVarint` |

## 6. Implementation Priority

| Item | Effort | Dependencies | Priority |
|------|--------|-------------|----------|
| 5 (compact) | Done | None | N/A |
| 12 (codec fuzz) | Small (2 new test files) | None | High -- low effort, high value |
| 11 (conformance) | Medium (new package + test functions) | None | Medium -- requires designing the conformance package |
| 6 (SST dump) | Medium (Pebble sstable API) | Verify sstable API availability | Medium -- useful for debugging |

## 7. Risks and Mitigations

| Risk | Item | Likelihood | Impact | Mitigation |
|------|------|-----------|--------|------------|
| `sstable.NewReader` API changes between Pebble versions | 6 | Medium | Blocks SST dump | Pin to current Pebble version; wrap in build-tagged file if needed |
| Fuzz tests find bugs in codec | 12 | Low-Medium | Requires fixing foundational code | Good -- that is the purpose of fuzz testing. Fix immediately. |
| Conformance suite too tightly coupled to Pebble behavior | 11 | Low | Tests pass vacuously on other engines | Test only documented interface contracts, not implementation details |
| SST file format incompatible with current Pebble options | 6 | Low | Cannot read SST files | Use same `pebble.Options` used by `rocks.Open()` when creating the reader |
