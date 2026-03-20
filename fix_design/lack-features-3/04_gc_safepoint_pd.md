# Item 4: GC Safe Point PD Centralization

## Impact: Medium

## Current State

### What exists
- **PDServer**: `GetGCSafePoint` and `UpdateGCSafePoint` RPCs fully implemented (`internal/pd/server.go`)
- **GCSafePointManager**: In-memory safe point tracking with forward-only updates (`internal/pd/server.go`)
- **GCWorker**: `SafePointProvider` interface defined (`internal/storage/gc/gc.go:304-307`)
- **GCTask**: Receives safe point via `GCTask.SafePoint` field at task submission
- **KvGC handler**: Already implemented, submits GCTask with safe point from request

### What is missing
- `pdclient.Client` interface lacks `GetGCSafePoint()` and `UpdateGCSafePoint()` methods
- `grpcClient` has no corresponding gRPC call implementations
- `MockClient` has no GC safe point fields or methods
- No `PDSafePointProvider` that implements `SafePointProvider` via PD
- `KvGC` handler does not call `UpdateGCSafePoint()` on PD after processing

## Design

### 1. Add GC safe point methods to Client interface

**File**: `pkg/pdclient/client.go`

Add two methods to the `Client` interface:

```go
// GetGCSafePoint returns the cluster-wide GC safe point from PD.
GetGCSafePoint(ctx context.Context) (uint64, error)

// UpdateGCSafePoint advances the cluster-wide GC safe point on PD.
// PD ensures the safe point only moves forward.
UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error)
```

### 2. Implement in grpcClient

**File**: `pkg/pdclient/client.go`

```go
func (c *grpcClient) GetGCSafePoint(ctx context.Context) (uint64, error) {
    resp, err := c.withRetry(func() (interface{}, error) {
        c.mu.RLock()
        client := c.client
        c.mu.RUnlock()
        return client.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{
            Header: c.requestHeader(),
        })
    })
    if err != nil {
        return 0, err
    }
    return resp.(*pdpb.GetGCSafePointResponse).GetSafePoint(), nil
}

func (c *grpcClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
    resp, err := c.withRetry(func() (interface{}, error) {
        c.mu.RLock()
        client := c.client
        c.mu.RUnlock()
        return client.UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{
            Header:    c.requestHeader(),
            SafePoint: safePoint,
        })
    })
    if err != nil {
        return 0, err
    }
    return resp.(*pdpb.UpdateGCSafePointResponse).GetNewSafePoint(), nil
}
```

Use Serena to verify the `withRetry` pattern and `requestHeader()` usage:
```
find_symbol("withRetry", relative_path="pkg/pdclient", include_body=True)
```

### 3. Add to MockClient

**File**: `pkg/pdclient/mock.go`

Add field and methods:

```go
// In MockClient struct:
gcSafePoint atomic.Uint64

func (m *MockClient) GetGCSafePoint(ctx context.Context) (uint64, error) {
    return m.gcSafePoint.Load(), nil
}

func (m *MockClient) UpdateGCSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
    for {
        old := m.gcSafePoint.Load()
        if safePoint <= old {
            return old, nil  // PD only moves forward
        }
        if m.gcSafePoint.CompareAndSwap(old, safePoint) {
            return safePoint, nil
        }
    }
}
```

### 4. Create PDSafePointProvider

**File**: `internal/storage/gc/pd_safe_point.go` (new)

```go
package gc

import (
    "context"
    "github.com/ryogrid/gookv/pkg/pdclient"
    "github.com/ryogrid/gookv/pkg/txntypes"
)

// PDSafePointProvider retrieves the GC safe point from PD.
type PDSafePointProvider struct {
    pdClient pdclient.Client
}

func NewPDSafePointProvider(pdClient pdclient.Client) *PDSafePointProvider {
    return &PDSafePointProvider{pdClient: pdClient}
}

func (p *PDSafePointProvider) GetGCSafePoint(ctx context.Context) (txntypes.TimeStamp, error) {
    sp, err := p.pdClient.GetGCSafePoint(ctx)
    if err != nil {
        return 0, err
    }
    return txntypes.TimeStamp(sp), nil
}
```

### 5. Integrate with KvGC handler

**File**: `internal/server/server.go`

In `KvGC()`, after the GC task completes successfully, update PD safe point:

```go
func (svc *tikvService) KvGC(ctx context.Context, req *kvrpcpb.GCRequest) (*kvrpcpb.GCResponse, error) {
    // ... existing task scheduling and waiting ...

    // After successful GC, update PD safe point
    if svc.server.pdClient != nil && resp.Error == nil {
        safePoint := req.GetSafePoint()
        if _, err := svc.server.pdClient.UpdateGCSafePoint(ctx, safePoint); err != nil {
            slog.Warn("failed to update PD GC safe point", "err", err)
            // Non-fatal: GC already completed locally
        }
    }

    return resp, nil
}
```

### 6. Wire PDSafePointProvider in server startup

**File**: `cmd/gookv-server/main.go`

When PD client is available, create `PDSafePointProvider` and pass it to GCWorker:

Use Serena to find how GCWorker is currently created:
```
search_for_pattern("NewGCWorker", relative_path="cmd/gookv-server")
```

```go
var safePointProvider gc.SafePointProvider
if pdClient != nil {
    safePointProvider = gc.NewPDSafePointProvider(pdClient)
}
gcWorker := gc.NewGCWorker(engine, gcConfig)
// If needed, add a method: gcWorker.SetSafePointProvider(safePointProvider)
```

Note: The current GCWorker does not use SafePointProvider internally (safe points come via GCTask). The provider is used for future automatic GC scheduling where the worker polls PD. For now, the primary value is `KvGC` updating PD on completion.

## File Changes

| File | Changes |
|------|---------|
| `pkg/pdclient/client.go` | Add `GetGCSafePoint`, `UpdateGCSafePoint` to interface; implement in `grpcClient` |
| `pkg/pdclient/mock.go` | Add `gcSafePoint` field; implement both methods |
| `internal/storage/gc/pd_safe_point.go` | New file: `PDSafePointProvider` |
| `internal/server/server.go` | Add PD safe point update to `KvGC()` handler |
| `cmd/gookv-server/main.go` | Wire `PDSafePointProvider` if PD client available |

## Tests

### Unit Tests

**File**: `pkg/pdclient/client_gc_test.go`

```
TestMockClientGCSafePointForwardOnly
  - Call UpdateGCSafePoint(100) → returns 100
  - Call UpdateGCSafePoint(50) → returns 100 (no backward)
  - Call GetGCSafePoint() → returns 100

TestMockClientGCSafePointInitialZero
  - Call GetGCSafePoint() on fresh mock → returns 0
```

**File**: `internal/storage/gc/pd_safe_point_test.go`

```
TestPDSafePointProviderDelegates
  - Create PDSafePointProvider with MockClient
  - Set mock safe point to 42
  - Call GetGCSafePoint() → returns 42
```

### E2E Tests

**File**: `e2e/gc_pd_safepoint_test.go`

```
TestKvGCUpdatesPDSafePoint
  1. Start PD + standalone server with PD connection
  2. Write data at timestamp 100
  3. Call KvGC RPC with safe_point=50
  4. Query PD's GetGCSafePoint → should return 50
  5. Call KvGC RPC with safe_point=30 (lower)
  6. Query PD's GetGCSafePoint → should still return 50

TestGCSafePointConsistencyAcrossNodes
  1. Start PD + 2-node cluster
  2. KvGC on node 1 with safe_point=100
  3. Verify PD safe point is 100
  4. Verify node 2 can query PD and sees safe_point=100
```

## Risks

- **Backward compatibility**: Adding methods to the `Client` interface is a breaking change for any external implementors. Since `pkg/pdclient` is a public package, the `MockClient` must be updated simultaneously. No known external consumers exist.
- **PD unavailable during KvGC**: If PD is down, the local GC still succeeds. Only the PD safe point update fails (logged as warning). This is acceptable.
