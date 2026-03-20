# 03. PD Resilience: TSO Integration and Leader Failover/Retry

## 1. Overview

This document covers two related improvements to PD client reliability and timestamp management:

- **Item 9 -- PD Leader Failover / Retry**: The PD client (`pkg/pdclient/client.go`) currently connects to the first available endpoint at startup and never reconnects. If the PD server restarts or becomes unreachable, all RPCs fail permanently. The `Config` struct already defines `RetryInterval`, `RetryMaxCount`, and `UpdateInterval` fields, but `grpcClient` never reads them. This design adds endpoint rotation, automatic reconnection, a `withRetry()` wrapper with exponential backoff, and a background leader refresh goroutine.

- **Item 8 -- TSO Integration**: All timestamps in gookv are currently provided by the client in RPC requests (e.g., `GetRequest.Version`, `PrewriteRequest.StartVersion`, `CommitRequest.CommitVersion`). The server never allocates timestamps itself. This design introduces optional server-side timestamp allocation via `pdclient.Client.GetTS()` for two specific paths: **1PC commit timestamps** and **async commit maxCommitTS**. Normal 2PC continues to use client-provided timestamps.

### Current State Summary

| Component | File | Status |
|-----------|------|--------|
| `pdclient.Config.RetryInterval` | `pkg/pdclient/client.go:89` | Defined (300ms default), never read |
| `pdclient.Config.RetryMaxCount` | `pkg/pdclient/client.go:90` | Defined (10 default), never read |
| `pdclient.Config.UpdateInterval` | `pkg/pdclient/client.go:91` | Defined (10min default), never read |
| `grpcClient.conn` / `grpcClient.client` | `pkg/pdclient/client.go:109-110` | Set once in `NewClient()`, never refreshed |
| TSO allocation (`grpcClient.GetTS()`) | `pkg/pdclient/client.go:165-193` | Fully implemented, never called by server |
| `PDServer.Tso()` RPC | `internal/pd/server.go:141-170` | Streaming TSO allocator, fully working |
| Timestamp in `KvCommit` | `internal/server/server.go:291` | `commitTS` comes from `req.GetCommitVersion()` |
| `pdClient` in `cmd/gookv-server/main.go` | `cmd/gookv-server/main.go:118-119` | Created, used for bootstrap/heartbeats only |
| `server.Server` struct | `internal/server/server.go:34-46` | No `pdClient` field |

---

## 2. Item 9: PD Leader Failover / Retry

### 2.1 Problem

`NewClient()` (line 120) iterates `cfg.Endpoints` and connects to the first one that responds. The resulting `*grpc.ClientConn` is stored in `grpcClient.conn` and never replaced. If the PD process restarts or the network partitions, every subsequent RPC returns a transport error. The caller (e.g., `PDWorker.sendStoreHeartbeat()`) logs a warning and drops the heartbeat, but never attempts recovery.

### 2.2 Design: Endpoint Rotation and Reconnection

Add a `reconnect()` method to `grpcClient` that cycles through all configured endpoints:

```go
// reconnect closes the current connection and tries each endpoint in order.
// Returns nil on success or the last error if all endpoints fail.
func (c *grpcClient) reconnect(ctx context.Context) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    if c.conn != nil {
        c.conn.Close()
        c.conn = nil
        c.client = nil
    }

    var lastErr error
    for _, ep := range c.cfg.Endpoints {
        dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
        conn, err := grpc.DialContext(dialCtx, ep,
            grpc.WithTransportCredentials(insecure.NewCredentials()),
            grpc.WithBlock(),
        )
        cancel()
        if err != nil {
            lastErr = err
            continue
        }
        c.conn = conn
        c.client = pdpb.NewPDClient(conn)
        return nil
    }
    return fmt.Errorf("pdclient: reconnect failed: %w", lastErr)
}
```

### 2.3 Design: `withRetry()` Wrapper

Wrap every RPC call with exponential backoff using the existing config fields:

```go
// withRetry executes fn with exponential backoff.
// It reconnects on transport errors and retries up to cfg.RetryMaxCount times.
func (c *grpcClient) withRetry(ctx context.Context, fn func(pdpb.PDClient) error) error {
    backoff := c.cfg.RetryInterval
    for attempt := 0; ; attempt++ {
        c.mu.RLock()
        client := c.client
        c.mu.RUnlock()

        if client == nil {
            if err := c.reconnect(ctx); err != nil {
                if c.cfg.RetryMaxCount >= 0 && attempt >= c.cfg.RetryMaxCount {
                    return err
                }
                select {
                case <-ctx.Done():
                    return ctx.Err()
                case <-time.After(backoff):
                    backoff = min(backoff*2, 10*time.Second)
                    continue
                }
            }
            c.mu.RLock()
            client = c.client
            c.mu.RUnlock()
        }

        err := fn(client)
        if err == nil {
            return nil
        }

        // If it's a transport/unavailable error, reconnect and retry.
        if isRetryableError(err) {
            _ = c.reconnect(ctx)
            if c.cfg.RetryMaxCount >= 0 && attempt >= c.cfg.RetryMaxCount {
                return err
            }
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(backoff):
                backoff = min(backoff*2, 10*time.Second)
                continue
            }
        }

        // Non-retryable error (e.g., application-level PD error).
        return err
    }
}
```

`isRetryableError()` checks for gRPC status codes `Unavailable`, `DeadlineExceeded`, and `Internal` (connection reset). All non-streaming RPCs (`GetRegion`, `GetStore`, `Bootstrap`, `PutStore`, `StoreHeartbeat`, `AskBatchSplit`, `ReportBatchSplit`, `AllocID`, `IsBootstrapped`) are wrapped with `withRetry()`.

**Streaming RPCs** (`GetTS`, `ReportRegionHeartbeat`) handle errors differently: on stream failure, they call `reconnect()` once and re-establish the stream before returning the error to the caller.

### 2.4 Design: Background Leader Refresh Goroutine

Use `Config.UpdateInterval` (default 10 minutes) to periodically verify the PD leader is still reachable and discover leader changes:

```go
// startLeaderRefresh starts a goroutine that periodically calls GetMembers
// to verify connectivity and discover leader changes.
func (c *grpcClient) startLeaderRefresh(ctx context.Context) {
    go func() {
        ticker := time.NewTicker(c.cfg.UpdateInterval)
        defer ticker.Stop()
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                c.mu.RLock()
                client := c.client
                c.mu.RUnlock()
                if client == nil {
                    _ = c.reconnect(ctx)
                    continue
                }
                rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
                _, err := client.GetMembers(rctx, &pdpb.GetMembersRequest{})
                cancel()
                if err != nil {
                    _ = c.reconnect(ctx)
                }
            }
        }
    }()
}
```

This goroutine is started at the end of `NewClient()`. On `Close()`, the client's context is cancelled, stopping the goroutine.

### 2.5 Changes to `grpcClient`

The struct gains two new fields:

```go
type grpcClient struct {
    // ... existing fields ...

    ctx    context.Context    // Client-scoped context, cancelled on Close()
    cancel context.CancelFunc // Cancels ctx
}
```

`NewClient()` creates this context and calls `startLeaderRefresh()`. `Close()` calls `c.cancel()` before closing the connection.

### 2.6 Config Usage

After this change, all three config fields are consumed:

| Field | Used By | Purpose |
|-------|---------|---------|
| `RetryInterval` | `withRetry()` | Initial backoff between retries |
| `RetryMaxCount` | `withRetry()` | Max retry attempts (-1 = infinite) |
| `UpdateInterval` | `startLeaderRefresh()` | Leader health check interval |

---

## 3. Item 8: TSO Integration

### 3.1 Problem

Currently, every timestamp used in transactions originates from the client side. The server receives them as fields in protobuf requests:

- `GetRequest.Version` -- snapshot read timestamp
- `PrewriteRequest.StartVersion` -- transaction start timestamp
- `CommitRequest.CommitVersion` -- transaction commit timestamp

This works for normal 2PC where a client obtains timestamps from PD before issuing RPCs. However, two optimization paths need server-allocated timestamps:

1. **1PC optimization** (`internal/storage/txn/async_commit.go:139`): `PrewriteAndCommit1PC()` takes `OnePCProps.CommitTS` which must be greater than `StartTS`. Currently the client must supply this, but server-side allocation via PD TSO is more correct and avoids clock skew.

2. **Async commit** (`internal/storage/txn/async_commit.go:28`): `PrewriteAsyncCommit()` uses `AsyncCommitPrewriteProps.MaxCommitTS` to compute `MinCommitTS`. Getting this from PD TSO instead of a client-provided value ensures globally consistent ordering.

### 3.2 Design: Optional `pdClient` in Server

Add an optional PD client to `server.Server`:

```go
// Server struct changes in internal/server/server.go
type Server struct {
    cfg         ServerConfig
    grpcServer  *grpc.Server
    storage     *Storage
    rawStorage  *RawStorage
    gcWorker    *gc.GCWorker
    coordinator *StoreCoordinator
    pdClient    pdclient.Client   // NEW: optional PD client for TSO
    listener    net.Listener

    ctx    context.Context
    cancel context.CancelFunc
    wg     sync.WaitGroup
}
```

Add a setter method (same pattern as `SetCoordinator`):

```go
// SetPDClient sets the PD client for server-side timestamp allocation.
// Must be called before Start() if TSO integration is needed.
func (s *Server) SetPDClient(client pdclient.Client) {
    s.pdClient = client
}
```

Add a helper that allocates a timestamp or falls back:

```go
// GetTS allocates a timestamp from PD if available, otherwise returns 0
// to indicate the caller should use the client-provided timestamp.
func (s *Server) GetTS(ctx context.Context) (uint64, error) {
    if s.pdClient == nil {
        return 0, nil // No PD -- use client-provided TS
    }
    ts, err := s.pdClient.GetTS(ctx)
    if err != nil {
        return 0, fmt.Errorf("server: get ts: %w", err)
    }
    return ts.ToUint64(), nil
}
```

### 3.3 Integration Points

#### 3.3.1 1PC CommitTS

In `KvPrewrite` handler, when 1PC is detected (indicated by `req.GetTryOnePc()`), the server allocates a commitTS:

```go
// In tikvService.KvPrewrite, after building mutations:
if req.GetTryOnePc() && s.server.pdClient != nil {
    commitTS, err := s.server.GetTS(ctx)
    if err == nil && commitTS > 0 {
        // Use server-allocated TS for 1PC commit
        resp.OnePcCommitTs = commitTS
    }
}
```

When `pdClient` is nil (standalone mode), 1PC falls back to the client-provided commit timestamp as it does today.

#### 3.3.2 Async Commit MaxCommitTS

In async commit prewrite, the server queries PD for a fresh timestamp to use as `MaxCommitTS`:

```go
// In tikvService.KvPrewrite, when UseAsyncCommit is true:
if req.GetUseAsyncCommit() && s.server.pdClient != nil {
    ts, err := s.server.GetTS(ctx)
    if err == nil && ts > 0 {
        // Use PD-allocated TS as maxCommitTS
        maxCommitTS = txntypes.TimeStamp(ts)
    }
}
```

### 3.4 Wiring in `cmd/gookv-server/main.go`

The PD client is already created in main.go at line 119. After creating the server, pass it through:

```go
// After line 92: srv := server.NewServer(srvCfg, storage)
// Inside the PD connection block (after pdClient is created):
srv.SetPDClient(pdClient)
```

This means TSO is only available in cluster mode when `--pd-endpoints` is configured and the PD connection succeeds. In standalone mode, `pdClient` remains nil and all timestamps come from the client, preserving backward compatibility.

### 3.5 TSO Caching (Future Optimization)

The current `GetTS()` makes a streaming RPC for every call. A future optimization would batch TSO requests using `grpcClient.tsoBatchSize` (already defined as 64 at line 146 of `pkg/pdclient/client.go`). This would maintain a long-lived stream and pre-allocate a batch of timestamps, serving requests from a local cache. This optimization is deferred to a separate design.

---

## 4. E2E Test Plan

### 4.1 PD Failover Tests

**Test: `TestPDClientReconnectOnServerRestart`**
1. Start a PD server on a known port.
2. Create a PD client with that endpoint.
3. Verify `GetTS()` succeeds.
4. Stop the PD server.
5. Verify `GetTS()` returns an error.
6. Restart the PD server on the same port.
7. Verify `GetTS()` succeeds after automatic reconnection.

**Test: `TestPDClientEndpointRotation`**
1. Start two PD servers on different ports.
2. Create a PD client with both endpoints.
3. Stop the first PD server.
4. Verify RPCs succeed (rotated to second endpoint).

**Test: `TestPDClientRetryExhaustion`**
1. Create a PD client with `RetryMaxCount=2` and no reachable endpoints.
2. Call `GetRegion()`.
3. Verify it returns an error after exactly 2 retries (measure elapsed time).

**Test: `TestPDClientBackgroundLeaderRefresh`**
1. Start a PD server.
2. Create a PD client with `UpdateInterval=1s`.
3. Stop the PD server.
4. Wait 2 seconds (background refresh should detect failure).
5. Restart PD server.
6. Wait 2 seconds (background refresh should reconnect).
7. Verify `GetTS()` succeeds without explicit retry from the caller.

### 4.2 TSO Integration Tests

**Test: `TestServerGetTSWithPD`**
1. Start PD server and gookv-server with `--pd-endpoints`.
2. Call `GetTS()` via the server's internal method.
3. Verify returned timestamp is non-zero and monotonically increasing.

**Test: `TestServerGetTSWithoutPD`**
1. Start gookv-server without `--pd-endpoints` (standalone mode).
2. Verify `GetTS()` returns `(0, nil)` indicating fallback to client timestamps.

**Test: `TestOnePCWithServerTSO`**
1. Start PD server and gookv-server in cluster mode.
2. Issue a small, single-region `KvPrewrite` with `try_one_pc=true`.
3. Verify the response contains a non-zero `one_pc_commit_ts`.
4. Verify the committed data is readable at that timestamp.

**Test: `TestAsyncCommitWithServerTSO`**
1. Start PD server and gookv-server in cluster mode.
2. Issue `KvPrewrite` with `use_async_commit=true`.
3. Verify the lock's `MinCommitTS` is greater than `StartTS`.
4. Verify the transaction resolves correctly.
