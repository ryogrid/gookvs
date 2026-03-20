# Raw KV Extensions: TTL, BatchScan, CAS, Checksum

This document covers the design for the remaining Raw KV partial RPCs (Item 7):

- **RawBatchScan** -- multi-range scan
- **RawGetKeyTTL** -- retrieve remaining TTL for a key
- **RawCompareAndSwap** -- atomic compare-and-swap
- **RawChecksum** -- CRC64-XOR checksum over key-value pairs

All four RPCs depend on a TTL value encoding scheme that must be integrated into the existing `RawStorage` layer.

---

## 1. Overview

The existing Raw KV implementation in `internal/server/raw_storage.go` provides:

| Method | Status |
|--------|--------|
| `Get(cf, key)` | Implemented |
| `Put(cf, key, value)` | Implemented |
| `Delete(cf, key)` | Implemented |
| `Scan(cf, start, end, limit, keyOnly, reverse)` | Implemented |
| `BatchGet(cf, keys)` | Implemented |
| `BatchPut(cf, pairs)` | Implemented |
| `BatchDelete(cf, keys)` | Implemented |
| `DeleteRange(cf, start, end)` | Implemented |
| `PutModify(cf, key, value)` / `DeleteModify(cf, key)` | Implemented (for Raft cluster mode) |

The `RawStorage` struct wraps `traits.KvEngine` and operates directly on the engine, bypassing MVCC. All existing methods store raw bytes without any value encoding.

The four unimplemented RPCs are defined in `proto/kvrpcpb.proto` and `proto/tikvpb.proto`, with handler stubs falling through to `UnimplementedTikvServer` in the gRPC service.

---

## 2. TTL Value Encoding Design

### 2.1 Encoding Format

TiKV's Raw API V1 TTL mode appends TTL metadata to the stored value. We adopt a compatible suffix-based encoding:

```
Non-TTL value: [user_value_bytes]
                (no suffix; stored as-is)

TTL value:     [user_value_bytes][expire_timestamp_u64_be][0x01]
                                 ^^^^^^^^^^^^^^^^^^^^^^^^  ^^^^
                                 8 bytes, big-endian       1 byte marker
```

The trailing marker byte `0x01` distinguishes TTL-encoded values from plain values. Since the marker is the last byte:
- If the last byte is `0x01` and the value is at least 9 bytes long, it is a TTL-encoded value.
- Otherwise, it is a plain value (no TTL).

The `expire_timestamp_u64_be` is an absolute Unix timestamp in seconds. A value of `0` means no expiration (infinite TTL).

### 2.2 Encoding/Decoding Functions

Add to `internal/server/raw_storage.go` (or a new file `internal/server/raw_ttl.go`):

```go
const (
    ttlMarker    byte = 0x01
    ttlSuffixLen      = 9 // 8 bytes timestamp + 1 byte marker
)

// EncodeTTLValue appends TTL expiration metadata to a user value.
// If ttlSeconds == 0, the value is stored without TTL (plain bytes).
func EncodeTTLValue(value []byte, ttlSeconds uint64) []byte {
    if ttlSeconds == 0 {
        return value
    }
    expireTS := uint64(time.Now().Unix()) + ttlSeconds
    buf := make([]byte, len(value)+ttlSuffixLen)
    copy(buf, value)
    binary.BigEndian.PutUint64(buf[len(value):], expireTS)
    buf[len(buf)-1] = ttlMarker
    return buf
}

// DecodeTTLValue extracts the user value and expiration timestamp.
// Returns (userValue, expireTS, hasTTL).
// If expireTS == 0 or hasTTL == false, the value has no TTL.
func DecodeTTLValue(raw []byte) (value []byte, expireTS uint64, hasTTL bool) {
    if len(raw) < ttlSuffixLen || raw[len(raw)-1] != ttlMarker {
        return raw, 0, false
    }
    expireTS = binary.BigEndian.Uint64(raw[len(raw)-ttlSuffixLen : len(raw)-1])
    value = raw[:len(raw)-ttlSuffixLen]
    return value, expireTS, true
}

// IsExpired checks whether a TTL-encoded value has expired.
func IsExpired(expireTS uint64) bool {
    if expireTS == 0 {
        return false
    }
    return uint64(time.Now().Unix()) >= expireTS
}

// RemainingTTLSeconds returns the remaining TTL in seconds.
// Returns 0 if the key has no TTL or has already expired.
func RemainingTTLSeconds(expireTS uint64) uint64 {
    if expireTS == 0 {
        return 0
    }
    now := uint64(time.Now().Unix())
    if now >= expireTS {
        return 0
    }
    return expireTS - now
}
```

### 2.3 Lazy Expiry Check on Read

We implement **lazy expiry** -- expired keys are filtered out at read time, not by a background worker. This simplifies the implementation significantly and avoids the complexity of a background goroutine that scans and deletes expired keys.

Trade-off: expired keys occupy disk space until they are overwritten, scanned past, or garbage-collected by a separate mechanism. This is acceptable for gookv's scope.

### 2.4 Impact on Existing Methods

The TTL encoding is **opt-in per request**. Existing `RawPut` and `RawBatchPut` requests that do not set a `ttl` field store values as-is (no encoding change, fully backward-compatible).

However, read methods must be updated to handle potentially TTL-encoded values:

**`Get(cf, key)`**: After reading the raw bytes, call `DecodeTTLValue()`. If expired, return `nil` (as if not found). Otherwise return the user value portion.

**`Scan(cf, ...)`**: After reading each key-value pair, decode and check TTL. Skip expired entries. Adjust the loop to continue scanning past expired entries to fill the limit.

**`BatchGet(cf, keys)`**: Same decode-and-filter logic per key.

The methods that take a `ttl` parameter are only the new TTL-aware write paths: `PutWithTTL()`, `CompareAndSwap()`.

To maintain backward compatibility, we add TTL-aware variants rather than changing existing method signatures:

```go
func (rs *RawStorage) GetWithTTL(cf string, key []byte) (value []byte, ttl uint64, err error)
func (rs *RawStorage) PutWithTTL(cf string, key, value []byte, ttlSeconds uint64) error
```

The existing `Get()` and `Put()` methods continue to work unchanged -- they store/retrieve raw bytes. The gRPC handlers for `RawPut` already have access to `req.GetTtl()` and can choose to call `PutWithTTL()` when `ttl > 0`.

The existing `Get()` should also be updated to decode TTL and filter expired values, so that even plain `RawGet` correctly handles keys that were written with a TTL:

```go
func (rs *RawStorage) Get(cf string, key []byte) ([]byte, error) {
    cf = rs.resolveCF(cf)
    raw, err := rs.engine.Get(cf, key)
    if err != nil {
        if errors.Is(err, traits.ErrNotFound) {
            return nil, nil
        }
        return nil, err
    }
    value, expireTS, hasTTL := DecodeTTLValue(raw)
    if hasTTL && IsExpired(expireTS) {
        return nil, nil  // treat as not found
    }
    return value, nil
}
```

Similarly, `Scan()` and `BatchGet()` should filter expired values.

---

## 3. RawBatchScan

### 3.1 Proto Definition

```protobuf
message RawBatchScanRequest {
  Context context = 1;
  repeated KeyRange ranges = 2;
  uint32 each_limit = 3;
  bool key_only = 4;
  string cf = 5;
  bool reverse = 6;
}

message RawBatchScanResponse {
  errorpb.Error region_error = 1;
  repeated KvPair kvs = 2;
}
```

### 3.2 Storage Method

```go
func (rs *RawStorage) BatchScan(
    cf string,
    ranges []KeyRange,
    eachLimit uint32,
    keyOnly bool,
    reverse bool,
) ([]KvPair, error)
```

Where `KeyRange` is:
```go
type KeyRange struct {
    StartKey []byte
    EndKey   []byte
}
```

Implementation:
1. For each range in `ranges`, call the existing `rs.Scan(cf, range.StartKey, range.EndKey, eachLimit, keyOnly, reverse)`.
2. Concatenate results from all ranges into a single `[]KvPair` slice.
3. Each range contributes at most `eachLimit` pairs.
4. Filter expired TTL values during scan (handled by the updated `Scan()` method).

This is straightforward because the existing `Scan()` method already handles forward/reverse iteration, bounds, limits, and snapshot isolation.

### 3.3 Handler

```go
func (svc *tikvService) RawBatchScan(ctx context.Context, req *kvrpcpb.RawBatchScanRequest) (*kvrpcpb.RawBatchScanResponse, error) {
    resp := &kvrpcpb.RawBatchScanResponse{}

    ranges := make([]KeyRange, len(req.GetRanges()))
    for i, r := range req.GetRanges() {
        ranges[i] = KeyRange{StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
    }

    pairs, err := svc.server.rawStorage.BatchScan(
        req.GetCf(),
        ranges,
        req.GetEachLimit(),
        req.GetKeyOnly(),
        req.GetReverse(),
    )
    if err != nil {
        return nil, status.Errorf(codes.Internal, "raw batch scan failed: %v", err)
    }

    for _, p := range pairs {
        resp.Kvs = append(resp.Kvs, &kvrpcpb.KvPair{Key: p.Key, Value: p.Value})
    }
    return resp, nil
}
```

### 3.4 BatchCommands Support

Add to the `handleBatchCmd` switch:
```go
case *tikvpb.BatchCommandsRequest_Request_RawBatchScan:
    r, _ := svc.RawBatchScan(ctx, cmd.RawBatchScan)
    resp.Cmd = &tikvpb.BatchCommandsResponse_Response_RawBatchScan{RawBatchScan: r}
```

---

## 4. RawGetKeyTTL

### 4.1 Proto Definition

```protobuf
message RawGetKeyTTLRequest {
  Context context = 1;
  bytes key = 2;
  string cf = 3;
}

message RawGetKeyTTLResponse {
  errorpb.Error region_error = 1;
  string error = 2;
  uint64 ttl = 3;
  bool not_found = 4;
}
```

### 4.2 Storage Method

```go
func (rs *RawStorage) GetKeyTTL(cf string, key []byte) (ttlSeconds uint64, notFound bool, err error)
```

Implementation:
1. Read raw bytes from engine: `raw, err := rs.engine.Get(cf, key)`.
2. If not found, return `notFound = true`.
3. Call `DecodeTTLValue(raw)` to extract `expireTS`.
4. If expired, return `notFound = true` (lazy expiry).
5. If `hasTTL == false`, return `ttlSeconds = 0` (key exists but has no TTL).
6. Otherwise, return `ttlSeconds = RemainingTTLSeconds(expireTS)`.

### 4.3 Handler

```go
func (svc *tikvService) RawGetKeyTTL(ctx context.Context, req *kvrpcpb.RawGetKeyTTLRequest) (*kvrpcpb.RawGetKeyTTLResponse, error) {
    resp := &kvrpcpb.RawGetKeyTTLResponse{}

    ttl, notFound, err := svc.server.rawStorage.GetKeyTTL(req.GetCf(), req.GetKey())
    if err != nil {
        resp.Error = err.Error()
        return resp, nil
    }
    if notFound {
        resp.NotFound = true
    } else {
        resp.Ttl = ttl
    }

    return resp, nil
}
```

---

## 5. RawCompareAndSwap

### 5.1 Proto Definition

```protobuf
message RawCASRequest {
  Context context = 1;
  bytes key = 2;
  bytes value = 3;
  bool previous_not_exist = 4;
  bytes previous_value = 5;
  string cf = 6;
  uint64 ttl = 7;
  bool delete = 8;
}

message RawCASResponse {
  errorpb.Error region_error = 1;
  string error = 2;
  bool succeed = 3;
  bool previous_not_exist = 4;
  bytes previous_value = 5;
}
```

### 5.2 Semantics

CAS atomically:
1. Reads the current value of `key`.
2. Compares it against the expected previous state:
   - If `req.previous_not_exist == true`: expects the key to NOT exist.
   - Otherwise: expects the current value to equal `req.previous_value`.
3. If comparison succeeds:
   - If `req.delete == true`: deletes the key.
   - Otherwise: writes `req.value` (with optional TTL from `req.ttl`).
4. Returns the actual previous value regardless of success.

The comparison must be TTL-aware: the "current value" used for comparison is the **decoded user value** (not the raw TTL-encoded bytes). An expired key is treated as non-existent.

### 5.3 Per-Key Mutex Hashing

CAS requires atomicity at the per-key level. Since the engine's `Get` + `Put` are not atomic, we need external synchronization.

We use a fixed-size array of mutexes with key hashing (similar to the latch mechanism in the transaction layer, but simpler since CAS is a single-key operation):

```go
const casLockSlots = 256

type RawStorage struct {
    engine   traits.KvEngine
    casLocks [casLockSlots]sync.Mutex
}

func (rs *RawStorage) casLock(key []byte) *sync.Mutex {
    h := fnv.New64a()
    h.Write(key)
    return &rs.casLocks[h.Sum64()%casLockSlots]
}
```

### 5.4 Storage Method

```go
func (rs *RawStorage) CompareAndSwap(
    cf string,
    key, newValue []byte,
    previousNotExist bool,
    previousValue []byte,
    ttlSeconds uint64,
    doDelete bool,
) (succeed bool, prevNotExist bool, prevValue []byte, err error)
```

Implementation:
1. Lock the per-key mutex: `mu := rs.casLock(key); mu.Lock(); defer mu.Unlock()`.
2. Read current raw bytes: `raw, err := rs.engine.Get(cf, key)`.
3. Decode TTL: `currentValue, expireTS, hasTTL := DecodeTTLValue(raw)`.
4. Check expiry: if `hasTTL && IsExpired(expireTS)`, treat as not found (`raw = nil`).
5. Compare:
   ```go
   if previousNotExist {
       // Expect key to not exist
       if raw != nil && !expired {
           return false, false, currentValue, nil
       }
   } else {
       // Expect specific value
       if raw == nil || expired {
           return false, true, nil, nil
       }
       if !bytes.Equal(currentValue, previousValue) {
           return false, false, currentValue, nil
       }
   }
   ```
6. If comparison succeeds:
   - If `doDelete`: `rs.engine.Delete(cf, key)`
   - Else: `rs.PutWithTTL(cf, key, newValue, ttlSeconds)`
7. Return success and previous state.

### 5.5 Cluster Mode (Raft Proposal for Writes)

In cluster mode, CAS writes must go through Raft for replication. The approach:

```go
func (rs *RawStorage) CompareAndSwapModify(
    cf string, key, newValue []byte,
    previousNotExist bool, previousValue []byte,
    ttlSeconds uint64, doDelete bool,
) (succeed bool, prevNotExist bool, prevValue []byte, modify *mvcc.Modify, err error)
```

1. Acquire the per-key CAS mutex.
2. Perform the read and comparison (same as standalone).
3. If comparison succeeds, construct the `mvcc.Modify` but do NOT apply it.
4. Return the modify to the handler, which proposes it via Raft.

The handler:
```go
func (svc *tikvService) RawCompareAndSwap(ctx context.Context, req *kvrpcpb.RawCASRequest) (*kvrpcpb.RawCASResponse, error) {
    resp := &kvrpcpb.RawCASResponse{}

    if coord := svc.server.coordinator; coord != nil {
        succeed, prevNotExist, prevValue, modify, err := svc.server.rawStorage.CompareAndSwapModify(
            req.GetCf(), req.GetKey(), req.GetValue(),
            req.GetPreviousNotExist(), req.GetPreviousValue(),
            req.GetTtl(), req.GetDelete(),
        )
        if err != nil {
            resp.Error = err.Error()
            return resp, nil
        }
        if succeed && modify != nil {
            if err := coord.ProposeModifies(1, []mvcc.Modify{*modify}, 10*time.Second); err != nil {
                return nil, status.Errorf(codes.Unavailable, "raft propose failed: %v", err)
            }
        }
        resp.Succeed = succeed
        resp.PreviousNotExist = prevNotExist
        resp.PreviousValue = prevValue
    } else {
        succeed, prevNotExist, prevValue, err := svc.server.rawStorage.CompareAndSwap(
            req.GetCf(), req.GetKey(), req.GetValue(),
            req.GetPreviousNotExist(), req.GetPreviousValue(),
            req.GetTtl(), req.GetDelete(),
        )
        if err != nil {
            resp.Error = err.Error()
            return resp, nil
        }
        resp.Succeed = succeed
        resp.PreviousNotExist = prevNotExist
        resp.PreviousValue = prevValue
    }

    return resp, nil
}
```

Note: In a multi-node cluster, the CAS mutex only provides local atomicity on the leader. Since all writes go through Raft and are linearized by the Raft log, this is sufficient -- the leader is the only node that processes write proposals, and the mutex prevents concurrent CAS operations on the same key from interleaving their read-compare-write sequences.

---

## 6. RawChecksum

### 6.1 Proto Definition

```protobuf
enum ChecksumAlgorithm {
  Crc64_Xor = 0;
}

message RawChecksumRequest {
  Context context = 1;
  ChecksumAlgorithm algorithm = 2;
  repeated KeyRange ranges = 3;
}

message RawChecksumResponse {
  errorpb.Error region_error = 1;
  string error = 2;
  uint64 checksum = 3;
  uint64 total_kvs = 4;
  uint64 total_bytes = 5;
}
```

### 6.2 Algorithm: CRC64-XOR

For each key-value pair in the specified ranges:
1. Compute `crc64(key + value)` using the ECMA polynomial (`crc64.MakeTable(crc64.ECMA)`).
2. XOR the result into a running accumulator.
3. Count total key-value pairs and total bytes (sum of `len(key) + len(value)`).

The XOR accumulation makes the checksum order-independent within a range, which is useful for comparing data across replicas that might iterate in slightly different orders.

TTL-encoded values should be decoded before checksumming -- the checksum is over the **user value**, not the internal TTL-encoded representation. Expired keys should be skipped.

### 6.3 Storage Method

```go
func (rs *RawStorage) Checksum(
    cf string,
    ranges []KeyRange,
) (checksum uint64, totalKvs uint64, totalBytes uint64, err error)
```

Implementation:
```go
import "hash/crc64"

func (rs *RawStorage) Checksum(cf string, ranges []KeyRange) (uint64, uint64, uint64, error) {
    cf = rs.resolveCF(cf)
    table := crc64.MakeTable(crc64.ECMA)

    var checksum, totalKvs, totalBytes uint64

    snap := rs.engine.NewSnapshot()
    defer snap.Close()

    for _, r := range ranges {
        opts := traits.IterOptions{
            LowerBound: r.StartKey,
        }
        if len(r.EndKey) > 0 {
            opts.UpperBound = r.EndKey
        }

        iter := snap.NewIterator(cf, opts)
        for iter.SeekToFirst(); iter.Valid(); iter.Next() {
            key := iter.Key()
            rawValue := iter.Value()

            // Decode TTL and skip expired entries.
            value, expireTS, hasTTL := DecodeTTLValue(rawValue)
            if hasTTL && IsExpired(expireTS) {
                continue
            }

            // CRC64 of key + user value.
            digest := crc64.New(table)
            digest.Write(key)
            digest.Write(value)
            checksum ^= digest.Sum64()

            totalKvs++
            totalBytes += uint64(len(key) + len(value))
        }
        iter.Close()
    }

    return checksum, totalKvs, totalBytes, nil
}
```

### 6.4 Handler

```go
func (svc *tikvService) RawChecksum(ctx context.Context, req *kvrpcpb.RawChecksumRequest) (*kvrpcpb.RawChecksumResponse, error) {
    resp := &kvrpcpb.RawChecksumResponse{}

    ranges := make([]KeyRange, len(req.GetRanges()))
    for i, r := range req.GetRanges() {
        ranges[i] = KeyRange{StartKey: r.GetStartKey(), EndKey: r.GetEndKey()}
    }

    // Only CRC64-XOR is supported.
    checksum, totalKvs, totalBytes, err := svc.server.rawStorage.Checksum(
        "",  // default CF; RawChecksumRequest has no cf field
        ranges,
    )
    if err != nil {
        resp.Error = err.Error()
        return resp, nil
    }

    resp.Checksum = checksum
    resp.TotalKvs = totalKvs
    resp.TotalBytes = totalBytes

    return resp, nil
}
```

---

## 7. Cluster Mode Considerations

| RPC | Read/Write | Cluster Handling |
|-----|-----------|-----------------|
| RawBatchScan | Read | Direct engine read (no Raft). Same as RawScan. |
| RawGetKeyTTL | Read | Direct engine read (no Raft). Same as RawGet. |
| RawCompareAndSwap | Read+Write | Read locally, propose write via `coord.ProposeModifies()`. Per-key mutex on leader. |
| RawChecksum | Read | Direct engine read (no Raft). Snapshot-consistent. |

For `RawBatchScan`, `RawGetKeyTTL`, and `RawChecksum`, no Raft interaction is needed because they are read-only operations that read from a local engine snapshot.

For `RawCompareAndSwap`, the write portion must be replicated. The handler follows the same pattern as `RawPut`:
1. Compute the comparison result and the intended modification locally.
2. Propose the modification via Raft.
3. Return the comparison result to the client.

The TTL-aware `RawPut` (when `req.GetTtl() > 0`) also needs the cluster-mode treatment. The handler should encode the TTL into the value before constructing the `PutModify`:

```go
func (svc *tikvService) RawPut(ctx context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
    resp := &kvrpcpb.RawPutResponse{}
    value := req.GetValue()
    if req.GetTtl() > 0 {
        value = EncodeTTLValue(value, req.GetTtl())
    }
    if coord := svc.server.coordinator; coord != nil {
        modify := svc.server.rawStorage.PutModify(req.GetCf(), req.GetKey(), value)
        // ... propose via Raft
    } else {
        // ... direct write
    }
    return resp, nil
}
```

---

## 8. E2E Test Plan

### 8.1 Test File

`e2e/raw_kv_extended_test.go`

### 8.2 Test Cases

#### 8.2.1 TTL Basic Put and Get

1. `RawPut` key `"ttl_key"` with value `"hello"` and `ttl=5` (seconds).
2. Immediately `RawGet` -- expect value `"hello"`.
3. `RawGetKeyTTL` -- expect `ttl` in range `[3, 5]` (accounting for test execution time).
4. Sleep 6 seconds.
5. `RawGet` -- expect `not_found = true` (expired).
6. `RawGetKeyTTL` -- expect `not_found = true`.

#### 8.2.2 TTL Zero Means No Expiry

1. `RawPut` key `"no_ttl"` with value `"forever"` and `ttl=0`.
2. `RawGetKeyTTL` -- expect `ttl = 0`, `not_found = false`.
3. Value persists indefinitely (no expiry check).

#### 8.2.3 Non-TTL Values Are Unaffected

1. `RawPut` key `"plain"` with value `"data"` (no TTL field in request).
2. `RawGet` -- expect `"data"`.
3. `RawGetKeyTTL` -- expect `ttl = 0` (no TTL), `not_found = false`.

#### 8.2.4 RawBatchScan -- Multiple Ranges

1. Put keys: `a/1`, `a/2`, `a/3`, `b/1`, `b/2`, `c/1`.
2. `RawBatchScan` with ranges `[a/, a0)` and `[b/, b0)`, `each_limit=2`.
3. Expect 4 results: `a/1`, `a/2`, `b/1`, `b/2`.

#### 8.2.5 RawBatchScan -- With TTL Filtering

1. Put keys with TTL: `x/1` (ttl=1), `x/2` (no ttl), `x/3` (ttl=1).
2. Sleep 2 seconds.
3. `RawBatchScan` range `[x/, x0)`.
4. Expect only `x/2` (others expired).

#### 8.2.6 RawBatchScan -- Reverse

1. Put keys `r/a`, `r/b`, `r/c`.
2. `RawBatchScan` with range `[r/, r0)`, `reverse=true`, `each_limit=2`.
3. Expect `r/c`, `r/b`.

#### 8.2.7 RawCompareAndSwap -- Successful Swap

1. `RawPut` key `"cas"` with value `"v1"`.
2. `RawCompareAndSwap` with `previous_value="v1"`, `value="v2"`.
3. Expect `succeed=true`, `previous_value="v1"`.
4. `RawGet` -- expect `"v2"`.

#### 8.2.8 RawCompareAndSwap -- Failed Swap (Value Mismatch)

1. `RawPut` key `"cas2"` with value `"v1"`.
2. `RawCompareAndSwap` with `previous_value="wrong"`, `value="v2"`.
3. Expect `succeed=false`, `previous_value="v1"`.
4. `RawGet` -- still `"v1"`.

#### 8.2.9 RawCompareAndSwap -- Create If Not Exists

1. Ensure key `"cas3"` does not exist.
2. `RawCompareAndSwap` with `previous_not_exist=true`, `value="new"`.
3. Expect `succeed=true`, `previous_not_exist=true`.
4. `RawGet` -- expect `"new"`.

#### 8.2.10 RawCompareAndSwap -- Create Fails If Exists

1. `RawPut` key `"cas4"` with value `"existing"`.
2. `RawCompareAndSwap` with `previous_not_exist=true`, `value="new"`.
3. Expect `succeed=false`, `previous_not_exist=false`, `previous_value="existing"`.

#### 8.2.11 RawCompareAndSwap -- Delete On Match

1. `RawPut` key `"cas5"` with value `"v1"`.
2. `RawCompareAndSwap` with `previous_value="v1"`, `delete=true`.
3. Expect `succeed=true`.
4. `RawGet` -- expect `not_found`.

#### 8.2.12 RawCompareAndSwap -- With TTL

1. `RawCompareAndSwap` to create `"cas6"` with `previous_not_exist=true`, `value="ttl_val"`, `ttl=2`.
2. `RawGet` -- expect `"ttl_val"`.
3. `RawGetKeyTTL` -- expect `ttl > 0`.
4. Sleep 3 seconds.
5. `RawGet` -- expect `not_found`.

#### 8.2.13 RawCompareAndSwap -- Expired Key Treated as Non-Existent

1. `RawPut` key `"cas7"` with value `"temp"`, `ttl=1`.
2. Sleep 2 seconds.
3. `RawCompareAndSwap` with `previous_not_exist=true`, `value="replaced"`.
4. Expect `succeed=true` (expired key treated as absent).

#### 8.2.14 RawChecksum -- Basic Checksum

1. Put keys `chk/a=val_a`, `chk/b=val_b`, `chk/c=val_c`.
2. `RawChecksum` with range `[chk/, chk0)`.
3. Expect `total_kvs=3`, `total_bytes=sum(len(key)+len(value))`.
4. Compute expected CRC64-XOR locally in the test and compare with `checksum`.

#### 8.2.15 RawChecksum -- Multiple Ranges

1. Put keys in ranges `[a/, b/)` and `[x/, y/)`.
2. `RawChecksum` with both ranges.
3. Verify `total_kvs` and `checksum` match local computation.

#### 8.2.16 RawChecksum -- TTL Filtering

1. Put `chk2/a` with ttl=1, `chk2/b` without ttl.
2. Sleep 2 seconds.
3. `RawChecksum` range `[chk2/, chk20)`.
4. Expect `total_kvs=1` (expired key excluded from checksum).

#### 8.2.17 Cluster Mode -- CAS Through Raft

1. Start a 3-node cluster.
2. `RawPut` key `"raft_cas"` with value `"v1"` on leader.
3. `RawCompareAndSwap` with `previous_value="v1"`, `value="v2"` on leader.
4. Verify replication: read `"raft_cas"` from a follower after leader step-down, expect `"v2"`.

---

## 9. Implementation Order

1. **TTL encoding functions** (`EncodeTTLValue`, `DecodeTTLValue`, `IsExpired`, `RemainingTTLSeconds`) in `internal/server/raw_ttl.go`.
2. **Update `RawStorage.Get()`** to decode TTL and filter expired values.
3. **Update `RawStorage.Scan()`** to filter expired values and fill limit correctly.
4. **Update `RawStorage.BatchGet()`** to filter expired values.
5. **Add `PutWithTTL()`** and `GetWithTTL()` methods to `RawStorage`.
6. **Add CAS mutex array** to `RawStorage` struct and `NewRawStorage()`.
7. **Implement `RawStorage.BatchScan()`** and `RawBatchScan` handler.
8. **Implement `RawStorage.GetKeyTTL()`** and `RawGetKeyTTL` handler.
9. **Implement `RawStorage.CompareAndSwap()`** / `CompareAndSwapModify()` and `RawCompareAndSwap` handler.
10. **Implement `RawStorage.Checksum()`** and `RawChecksum` handler.
11. **Update `RawPut` handler** to encode TTL when `req.GetTtl() > 0`.
12. **Add BatchCommands routing** for `RawBatchScan`.
13. **Write E2E tests**.
