# Bug 13 Fix: Apply-Level Key Range Validation

## Production Code

- [x] 1.1 Change `applyEntries` signature to accept peer via closure
- [x] 1.2 Update `BootstrapRegion` closure to pass peer to `applyEntriesForPeer`
- [x] 1.3 Update `handleSplitCheckResult` closure (via `BootstrapRegion`)
- [x] 1.4 Implement key range filtering in `applyEntriesForPeer` (CF-aware decode, per-key filter)
- [x] 1.5 Add per-key validation to KvPrewrite handler
- [x] 1.6 Fix KvPrewrite region routing: use mutations[0].Key instead of primary for resolveRegionID
- [x] 1.7 Fix KvPrewrite async commit path: same routing fix

## Verification

- [x] 3.1 go vet clean
- [x] 3.2 make test — pass
- [x] 3.3 make test-e2e — pass
- [ ] 3.4 Transaction integrity demo — 3 consecutive PASSes
- [ ] 3.5 Final: TODO.md all checked, no stale TODO comments in codebase
