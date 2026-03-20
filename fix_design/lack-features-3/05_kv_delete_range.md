# Item 5: KvDeleteRange RPC (Transactional)

## Impact: Low

## Current State

### What exists
- `RawDeleteRange` RPC handler: delegates to `rawStorage.DeleteRange()` which calls `engine.DeleteRange()` directly (`server.go:1012-1023`)
- `RawStorage.DeleteRange()` operates on a single CF (`raw_storage.go:175-178`)
- `Storage.ApplyModifies()` supports `ModifyTypePut` and `ModifyTypeDelete` but not `DeleteRange` (`storage.go:47-67`)
- `traits.KvEngine.DeleteRange(cf, startKey, endKey)` exists at the engine level

### What is missing
- No `KvDeleteRange` handler in `tikvService` (no function found — not even a stub)
- No transactional `DeleteRange` method in `Storage`
- `mvcc.Modify` does not have a `ModifyTypeDeleteRange` variant

## Design

### 1. Add ModifyTypeDeleteRange to mvcc.Modify

**File**: `internal/storage/mvcc/txn.go`

Add new modify type:

```go
const (
    ModifyTypePut         ModifyType = iota
    ModifyTypeDelete
    ModifyTypeDeleteRange  // New
)
```

For `ModifyTypeDeleteRange`, `Key` holds the start key and a new `EndKey` field holds the end key:

```go
type Modify struct {
    Type   ModifyType
    CF     string
    Key    []byte
    Value  []byte
    EndKey []byte  // Used only for DeleteRange
}
```

Use Serena to verify the current Modify struct:
```
find_symbol("Modify", relative_path="internal/storage/mvcc", include_body=True, depth=1)
```

### 2. Update Storage.ApplyModifies

**File**: `internal/server/storage.go`

Add `ModifyTypeDeleteRange` case:

```go
case mvcc.ModifyTypeDeleteRange:
    if err := wb.DeleteRange(m.CF, m.Key, m.EndKey); err != nil {
        return err
    }
```

Note: `WriteBatch.DeleteRange()` already exists in the engine traits.

### 3. Add Storage.DeleteRange method

**File**: `internal/server/storage.go`

```go
// DeleteRange performs a transactional range deletion across data CFs.
// It deletes all MVCC data (CF_DEFAULT, CF_WRITE) in [startKey, endKey)
// and cleans up any locks in CF_LOCK within the range.
func (s *Storage) DeleteRange(startKey, endKey []byte) ([]mvcc.Modify, error) {
    var modifies []mvcc.Modify

    // 1. Scan and release existing locks in the range
    snap := s.engine.NewSnapshot()
    defer snap.Close()

    reader := mvcc.NewMvccReader(snap)
    lockIter := snap.NewIterator(cfnames.CFLock, traits.IterOptions{
        LowerBound: mvcc.EncodeLockKey(startKey),
        UpperBound: mvcc.EncodeLockKey(endKey),
    })
    defer lockIter.Close()

    for lockIter.SeekToFirst(); lockIter.Valid(); lockIter.Next() {
        modifies = append(modifies, mvcc.Modify{
            Type: mvcc.ModifyTypeDelete,
            CF:   cfnames.CFLock,
            Key:  append([]byte{}, lockIter.Key()...),
        })
    }
    reader.Close()

    // 2. DeleteRange on CF_DEFAULT and CF_WRITE
    for _, cf := range []string{cfnames.CFDefault, cfnames.CFWrite} {
        encodedStart := mvcc.EncodeKey(startKey, txntypes.TSMax)
        encodedEnd := mvcc.EncodeKey(endKey, txntypes.TSMax)
        modifies = append(modifies, mvcc.Modify{
            Type:   mvcc.ModifyTypeDeleteRange,
            CF:     cf,
            Key:    encodedStart,
            EndKey: encodedEnd,
        })
    }

    return modifies, nil
}
```

Use Serena to verify MVCC key encoding:
```
find_symbol("EncodeKey", relative_path="internal/storage/mvcc", include_body=True)
```

### 4. Implement KvDeleteRange handler

**File**: `internal/server/server.go`

```go
func (svc *tikvService) KvDeleteRange(ctx context.Context, req *kvrpcpb.DeleteRangeRequest) (*kvrpcpb.DeleteRangeResponse, error) {
    resp := &kvrpcpb.DeleteRangeResponse{}

    startKey := req.GetStartKey()
    endKey := req.GetEndKey()

    if len(startKey) == 0 || len(endKey) == 0 {
        resp.Error = "start_key and end_key are required"
        return resp, nil
    }

    modifies, err := svc.server.storage.DeleteRange(startKey, endKey)
    if err != nil {
        resp.Error = err.Error()
        return resp, nil
    }

    coord := svc.server.coordinator
    if coord != nil {
        // Cluster mode: propose via Raft
        if err := coord.ProposeModifies(1, modifies, 30*time.Second); err != nil {
            resp.Error = err.Error()
        }
    } else {
        // Standalone mode: apply directly
        if err := svc.server.storage.ApplyModifies(modifies); err != nil {
            resp.Error = err.Error()
        }
    }

    return resp, nil
}
```

### 5. Update ModifiesToRequests / RequestsToModifies

**File**: `internal/server/raftcmd.go`

Use Serena to check the current implementation:
```
find_symbol("ModifiesToRequests", relative_path="internal/server", include_body=True)
```

Add handling for `ModifyTypeDeleteRange` so that it can be serialized to/from `raft_cmdpb.Request` for Raft proposal and apply.

## File Changes

| File | Changes |
|------|---------|
| `internal/storage/mvcc/txn.go` | Add `ModifyTypeDeleteRange`, `EndKey` field to `Modify` |
| `internal/server/storage.go` | Add `DeleteRange()` method; update `ApplyModifies()` |
| `internal/server/server.go` | Add `KvDeleteRange()` handler |
| `internal/server/raftcmd.go` | Add `ModifyTypeDeleteRange` serialization |

## Tests

### Unit Tests

**File**: `internal/server/storage_delete_range_test.go`

```
TestDeleteRangeProducesCorrectModifies
  - Create engine with MVCC data (locks, writes, defaults) in range [a, z)
  - Call Storage.DeleteRange([]byte("a"), []byte("z"))
  - Verify modifies include: lock deletes for each locked key,
    DeleteRange for CF_DEFAULT and CF_WRITE

TestApplyModifiesHandlesDeleteRange
  - Create engine, write data to CF_DEFAULT in range [a, z)
  - Apply a ModifyTypeDeleteRange modify
  - Verify all keys in range are deleted
```

### E2E Tests

**File**: `e2e/kv_delete_range_test.go`

```
TestKvDeleteRangeStandalone
  1. Start standalone server
  2. Prewrite+Commit 100 keys in range [key000, key099]
  3. Call KvDeleteRange for [key010, key050)
  4. KvGet each key — keys 010-049 should be gone, others remain

TestKvDeleteRangeWithLockedKeys
  1. Start standalone server
  2. Prewrite keys but do NOT commit (locks exist)
  3. Call KvDeleteRange for the locked range
  4. Verify locks are removed
  5. Verify subsequent Prewrite on same keys succeeds
```

## Risks

- **MVCC key encoding**: DeleteRange on CF_WRITE and CF_DEFAULT must use encoded keys (with timestamp suffix), not raw user keys. The encoding must cover all timestamps. Using `EncodeKey(key, TSMax)` as boundaries ensures all versions are covered.
- **Concurrent transactions**: DeleteRange does NOT check for write conflicts. This matches TiKV's behavior — `KvDeleteRange` is a privileged operation (used for DDL, not normal transactions). The caller is responsible for ensuring no concurrent transactions exist on the range.
- **Large ranges**: A DeleteRange that spans many regions would need to be split per-region in cluster mode. For the initial implementation, targeting single-region or standalone mode is acceptable. Multi-region DeleteRange can be added later via the client library.
