# Detailed Design: TxnKV Scan API

## 1. Client Library: `TxnHandle.Scan`

### 1.1 Method Signature

**File**: `pkg/client/txn.go`

```go
func (t *TxnHandle) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]KvPair, error)
```

Returns key-value pairs in `[startKey, endKey)` range, up to `limit` results. Merges local membuf writes with remote storage reads. Empty `endKey` means scan to end of keyspace.

### 1.2 State Guards

Same pattern as `Get` (txn.go:62-71):

```go
t.mu.Lock()
if t.committed { t.mu.Unlock(); return nil, ErrTxnCommitted }
if t.rolledBack { t.mu.Unlock(); return nil, ErrTxnRolledBack }
t.mu.Unlock()
```

### 1.3 Multi-Region Loop with Per-Region Lock Retry

Two nested loops: outer for region advancement, inner for lock retry. Follows `RawKVClient.Scan` (`pkg/client/rawkv.go:273-337`) for the region loop, and `TxnHandle.Get` (`pkg/client/txn.go:84-106`) for the lock retry.

```go
var rpcResults []KvPair
currentKey := startKey
const maxLockRetries = 20

for { // outer: region advancement loop
    remaining := limit - len(rpcResults)
    if remaining <= 0 { break }

    var regionPairs []KvPair
    var regionEnd []byte
    var lastLockErr error

    for retry := 0; retry <= maxLockRetries; retry++ { // inner: per-region lock retry
        var pairs []KvPair
        var lockInfo *kvrpcpb.LockInfo
        regionEnd = nil

        err := t.client.sender.SendToRegion(ctx, currentKey,
            func(cli tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
                regionEnd = info.Region.GetEndKey()
                scanEnd := endKey
                if len(regionEnd) > 0 && (len(scanEnd) == 0 || bytes.Compare(regionEnd, scanEnd) < 0) {
                    scanEnd = regionEnd
                }
                resp, err := cli.KvScan(ctx, &kvrpcpb.ScanRequest{
                    Context:  buildContext(info),
                    StartKey: currentKey,
                    EndKey:   scanEnd,
                    Limit:    uint32(remaining),
                    Version:  uint64(t.startTS),
                })
                if err != nil { return nil, err }
                if resp.GetRegionError() != nil { return resp.GetRegionError(), nil }
                if locked := resp.GetError().GetLocked(); locked != nil {
                    lockInfo = locked
                    return nil, nil
                }
                pairs = nil
                for _, kv := range resp.GetPairs() {
                    pairs = append(pairs, KvPair{Key: kv.GetKey(), Value: kv.GetValue()})
                }
                return nil, nil
            })
        if err != nil { return nil, err }

        if lockInfo == nil {
            regionPairs = pairs
            lastLockErr = nil
            break // success — proceed to next region
        }

        // Lock encountered — resolve and retry this region
        lastLockErr = fmt.Errorf("lock on key %x after %d retries", lockInfo.GetKey(), retry+1)
        if err := t.client.resolver.ResolveLocks(ctx, []*kvrpcpb.LockInfo{lockInfo}); err != nil {
            return nil, fmt.Errorf("resolve lock: %w", err)
        }
        time.Sleep(200 * time.Millisecond)
    }

    if lastLockErr != nil {
        return nil, fmt.Errorf("scan: lock resolution retries exhausted: %w", lastLockErr)
    }

    rpcResults = append(rpcResults, regionPairs...)

    // Region termination conditions
    if len(rpcResults) >= limit { break }
    if len(regionEnd) == 0 { break }
    if len(endKey) > 0 && bytes.Compare(regionEnd, endKey) >= 0 { break }
    currentKey = regionEnd
}

if len(rpcResults) > limit {
    rpcResults = rpcResults[:limit]
}
```

### 1.4 Membuf Merge with Tombstone-Aware Algorithm

After the RPC loop, merge results with local membuf. The merge must handle:
- **Puts**: membuf `Op_Put` entries override or add to RPC results
- **Deletes**: membuf `Op_Del` entries suppress RPC results for the same key

#### `collectMembuf` helper

Returns both puts and deletes from membuf within the key range, **under mutex**:

```go
type membufEntry struct {
    key     string
    value   []byte
    deleted bool // true if Op_Del
}

func (t *TxnHandle) collectMembuf(startKey, endKey []byte) []membufEntry {
    t.mu.Lock()
    defer t.mu.Unlock()

    var entries []membufEntry
    for k, entry := range t.membuf {
        if k < string(startKey) { continue }
        if len(endKey) > 0 && k >= string(endKey) { continue }
        entries = append(entries, membufEntry{
            key:     k,
            value:   entry.value,
            deleted: entry.op == kvrpcpb.Op_Del,
        })
    }
    sort.Slice(entries, func(i, j int) bool { return entries[i].key < entries[j].key })
    return entries
}
```

#### Merge algorithm

Two-pointer merge on sorted `rpcResults` and sorted `membufEntries`:

```go
func mergeResults(rpcResults []KvPair, membuf []membufEntry, limit int) []KvPair {
    var merged []KvPair
    i, j := 0, 0
    for len(merged) < limit && (i < len(rpcResults) || j < len(membuf)) {
        if i >= len(rpcResults) {
            // Only membuf entries left
            if !membuf[j].deleted {
                merged = append(merged, KvPair{Key: []byte(membuf[j].key), Value: membuf[j].value})
            }
            j++
        } else if j >= len(membuf) {
            // Only RPC entries left — but check if any remaining membuf deletes apply
            merged = append(merged, rpcResults[i])
            i++
        } else {
            rpcKey := string(rpcResults[i].Key)
            bufKey := membuf[j].key
            if rpcKey < bufKey {
                merged = append(merged, rpcResults[i])
                i++
            } else if rpcKey > bufKey {
                if !membuf[j].deleted {
                    merged = append(merged, KvPair{Key: []byte(bufKey), Value: membuf[j].value})
                }
                j++
            } else {
                // Same key — membuf wins
                if !membuf[j].deleted {
                    merged = append(merged, KvPair{Key: []byte(bufKey), Value: membuf[j].value})
                }
                // else: deleted — skip both
                i++
                j++
            }
        }
    }
    return merged
}
```

**Known limitation**: The RPC loop caps at `limit` results. If membuf deletes reduce the count below `limit`, the final merged result may have fewer than `limit` entries even though more server-side entries exist. This matches TiKV Go client behavior for simplicity. A streaming approach with re-fetch is deferred as a future optimization.

### 1.5 Imports

Add `"bytes"` and `"sort"` to `txn.go` imports.

## 2. CLI: Context-Aware SCAN

### 2.1 Parser Changes

**File**: `cmd/gookv-cli/parser.go`

**Add constant** (after `CmdTxnDelete`, line 305):
```go
CmdTxnScan     // SCAN (inside txn)
```

**Update `parseScan` signature** (line 566):
```go
func parseScan(args []Token, inTxn bool) (Command, error)
```

When `inTxn` is true, return `CmdTxnScan` instead of `CmdScan`. Same pattern as `parseGet` (line 489-497).

**Update `ParseCommand` switch** (line 385-386) — BOTH the signature AND call site:
```go
case "SCAN":
    return parseScan(args, inTxn)  // was: parseScan(args)
```

### 2.2 Executor Changes

**File**: `cmd/gookv-cli/executor.go`

**Add to `txnHandleAPI` interface** (line 47-55). Note: `activeTxn` is typed as `*client.TxnHandle` (line 88), not `txnHandleAPI`. The interface addition is for documentation and testability consistency. `execTxnScan` calls `e.activeTxn.Scan(...)` on the concrete type.

```go
Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]client.KvPair, error)
```

**Add to `Exec` switch** (after `CmdTxnDelete` case):
```go
case CmdTxnScan:
    return e.execTxnScan(ctx, cmd)
```

**New handler**:
```go
func (e *Executor) execTxnScan(ctx context.Context, cmd Command) (*Result, error) {
    if e.activeTxn == nil {
        return nil, fmt.Errorf("no active transaction")
    }
    startKey, endKey := cmd.Args[0], cmd.Args[1]
    limit := int(cmd.IntArg)
    if limit <= 0 {
        limit = e.defaultScanLimit
    }
    pairs, err := e.activeTxn.Scan(ctx, startKey, endKey, limit)
    if err != nil {
        return nil, err
    }
    rows := make([]KvPairResult, len(pairs))
    for i, p := range pairs {
        rows[i] = KvPairResult{Key: p.Key, Value: p.Value}
    }
    return &Result{Type: ResultRows, Rows: rows}, nil
}
```

## 3. e2elib Helper

**File**: `pkg/e2elib/cli.go`

```go
// CLITxnScan performs a transactional SCAN via CLI.
// Executes BEGIN; SCAN startKey endKey [LIMIT n]; ROLLBACK as a batch.
func CLITxnScan(t *testing.T, pdAddr string, startKey, endKey string, limit int) string {
    t.Helper()
    limitClause := ""
    if limit > 0 {
        limitClause = fmt.Sprintf(" LIMIT %d", limit)
    }
    stmt := fmt.Sprintf("BEGIN; SCAN %s %s%s; ROLLBACK", startKey, endKey, limitClause)
    return CLIExec(t, pdAddr, stmt)
}

// CLITxnScanRaw is the non-fatal variant that returns (stdout, stderr, error).
func CLITxnScanRaw(t *testing.T, pdAddr string, startKey, endKey string, limit int) (string, string, error) {
    t.Helper()
    limitClause := ""
    if limit > 0 {
        limitClause = fmt.Sprintf(" LIMIT %d", limit)
    }
    stmt := fmt.Sprintf("BEGIN; SCAN %s %s%s; ROLLBACK", startKey, endKey, limitClause)
    return CLIExecRaw(t, pdAddr, stmt)
}
```

## 4. Demo: Replace readAllBalances

**File**: `scripts/txn-integrity-demo-verify/main.go`

**Current** (lines 733-753): loops `txn.Get(ctx, acctKey(i))` for `i = 0..numAccounts-1`.

**Replacement**:
```go
func readAllBalances(ctx context.Context, txnClient *client.TxnKVClient) (int, []int, error) {
    txn, err := txnClient.Begin(ctx)
    if err != nil { return 0, nil, err }

    pairs, err := txn.Scan(ctx, []byte("acct:"), []byte("acct;"), numAccounts+1)
    if err != nil {
        _ = txn.Rollback(ctx)
        return 0, nil, fmt.Errorf("scan: %w", err)
    }
    _ = txn.Rollback(ctx)

    if len(pairs) != numAccounts {
        return 0, nil, fmt.Errorf("expected %d accounts, got %d", numAccounts, len(pairs))
    }

    total := 0
    balances := make([]int, 0, numAccounts)
    for _, p := range pairs {
        bal := parseBalance(p.Value)
        balances = append(balances, bal)
        total += bal
    }
    return total, balances, nil
}
```

Key range: `acctKey(i)` produces `"acct:0000"` to `"acct:0999"`. Scan `["acct:", "acct;")` captures all (`';'` = `':'` + 1).

## 5. Fuzz Test: Replace doAudit

**File**: `e2e_external/fuzz_cluster_test.go`

**Current** (lines 314-358): loops `txn.Get(ctx, accountKey(i))` for `i = 0..fuzzNumAccounts-1`.

**Replacement**: Use `txn.Scan` but retain per-key diagnostic reporting for invariant violations:

```go
func (c *fuzzClient) doAudit() (int, error) {
    c.fc.stats.audits.Add(1)
    ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
    defer cancel()

    txn, err := c.fc.cluster.TxnKV().Begin(ctx)
    if err != nil {
        c.fc.stats.auditSkips.Add(1)
        return 0, fmt.Errorf("begin: %w", err)
    }

    pairs, err := txn.Scan(ctx, []byte("account-"), []byte("account."), fuzzNumAccounts+1)
    if err != nil {
        _ = txn.Rollback(ctx)
        c.fc.stats.auditSkips.Add(1)
        return 0, fmt.Errorf("scan: %w", err)
    }

    _ = txn.Commit(ctx)

    // Build map for per-key diagnostics
    found := make(map[string]string, len(pairs))
    for _, p := range pairs {
        found[string(p.Key)] = string(p.Value)
    }

    total := 0
    for i := 0; i < fuzzNumAccounts; i++ {
        key := string(accountKey(i))
        valStr, ok := found[key]
        if !ok {
            return 0, fmt.Errorf("%s not found", key)
        }
        b, err := strconv.Atoi(valStr)
        if err != nil {
            return 0, fmt.Errorf("parse %s balance %q: %w", key, valStr, err)
        }
        if b < 0 {
            return total + b, fmt.Errorf("%s has negative balance: %d", key, b)
        }
        total += b
    }

    if total != fuzzExpectedTotal {
        return total, fmt.Errorf("balance mismatch: got %d, want %d", total, fuzzExpectedTotal)
    }

    c.fc.stats.auditPasses.Add(1)
    return total, nil
}
```

This retains per-key diagnostic messages (which account is missing/negative) while using a single Scan RPC instead of 5000 individual Gets.

`doBatchRead` — **no change** (random-access pattern, `BatchGet` is correct).

## 6. Review Findings Addressed

| # | Finding | Resolution |
|---|---------|------------|
| 1 | Lock retry has no error exit | §1.3: explicit nested loops with `lastLockErr` and exhaustion error |
| 2 | membuf accessed without mutex | §1.4: `collectMembuf` holds `t.mu` during iteration |
| 3 | txnHandleAPI/activeTxn type mismatch | §2.2: documented as interface-for-consistency; executor test uses concrete type |
| 4 | Delete suppression broken | §1.4: `collectMembuf` returns `membufEntry` with `deleted` flag; merge uses it |
| 5 | Under-fetching with membuf deletes | §1.4: documented as known limitation |
| 6 | CLITxnScanRaw undefined | §3: full implementation provided |
| 7 | doAudit diagnostic regression | §5: retained per-key diagnostics via map lookup |
