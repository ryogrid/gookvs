# Bug 13 Fix: Apply-Level Key Range Validation

## Production Code

- [x] 1.1 Change `applyEntries` signature to accept `regionID`
- [x] 1.2 Update `BootstrapRegion` closure to pass `regionID` to `applyEntries`
- [x] 1.3 Update `handleSplitCheckResult` closure (via `BootstrapRegion`) — already passes regionID
- [x] 1.4 Implement key range validation in `applyEntries` (CF-aware decode, atomic reject)
- [x] 1.5 Add per-key validation to KvPrewrite handler

## Unit Tests

- [ ] 2.1 TestApplyEntriesSkipsOutOfRangeEntry
- [ ] 2.2 TestApplyEntriesAcceptsInRangeEntry
- [ ] 2.3 TestApplyEntriesHandlesMixedCFs
- [ ] 2.4 TestApplyEntriesSkipsSplitBoundaryEntry
- [ ] 2.5 TestKvPrewriteRejectsKeyOutOfRegion

## Verification

- [ ] 3.1 go vet clean
- [ ] 3.2 make test — pass
- [ ] 3.3 make test-e2e — pass
- [ ] 3.4 Final: TODO.md all checked, no stale TODO comments in codebase
