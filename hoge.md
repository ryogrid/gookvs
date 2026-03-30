Implement the raft-stability-fix described in `design_doc/raft-stability-fix/`. This includes all code changes, unit tests, e2e tests, and verification that all tests pass. Fix any failures encountered during testing.

## Primary Reference

Read these design documents FIRST — they contain the complete specification:
- `design_doc/raft-stability-fix/01_problem_analysis.md` — Root cause analysis
- `design_doc/raft-stability-fix/02_design.md` — Detailed design with file-level changes
- `design_doc/raft-stability-fix/03_test_plan.md` — Test design
- `design_doc/raft-stability-fix/04_implementation_checklist.md` — Step-by-step checklist (follow this ordering)

## Additional Source References

- `tikv_impl_docs/` — TiKV implementation docs (for understanding TiKV's bootstrap architecture)
- `tikv/` — TiKV Rust source (for comparing bootstrap behavior)
- `current_impl/` (00-08) — gookv implementation docs
- `README.md`, `USAGE.md`
- Use Serena's symbolic tools (`get_symbols_overview`, `find_symbol`, `replace_symbol_body`) for Go source code navigation and editing — prefer symbol-level inspection over reading entire files.

## Files NOT to Read

Do NOT read markdown or JSON files other than those listed above (no `.claude/`, no `tasks/`, no `hoge.md`, no `package.json`, etc.).

## TODO Tracking

At the start of implementation, create (or overwrite) `TODO.md` in the project root with a checklist of all tasks derived from `04_implementation_checklist.md`. Mark items complete as you progress. Example:

```markdown
# Raft Stability Fix

## Phase 1: KVS Server Changes
- [x] Add `sort` import and `--max-peer-count` flag
- [ ] Extract `bootstrapStoreIDs` helper function
...
```

## Implementation Requirements

### Code Quality
- Run `go vet ./...` before testing. Fix any issues that constitute bugs.
- Run `make -f Makefile build` to verify compilation after each phase.
- Ensure all unit tests pass: `make -f Makefile test`
- Ensure all e2e tests pass: `make -f Makefile test-e2e-external`
- Ensure the fuzz test passes: `FUZZ_ITERATIONS=50 FUZZ_CLIENTS=4 make -f Makefile test-fuzz-cluster`

### Testing
- Add unit tests for `bootstrapStoreIDs` as specified in `03_test_plan.md` §1.1.
- Verify the fuzz test passes with `fuzzNodeCount=5` and default `MaxPeerCount=3` (no workaround).
- If any test fails, diagnose the root cause and fix it — do not disable the test.

### Subagent Delegation
- Delegate tasks to subagents when it would improve efficiency or output quality. For example: use an Explore agent for codebase research, a code-reviewer agent for reviewing changes, or parallel agents for independent tasks.

### Verification Checklist (Before Completion)

1. **TODO.md reconciliation**: Cross-check every item in `TODO.md` against the codebase. No unchecked items should remain.
2. **No stale TODO/FIXME comments**: Search the codebase for `TODO`, `FIXME`, `HACK`, `XXX` comments in files you modified. Remove or resolve them.
3. **No deferred items without documentation**: If any design doc item was intentionally deferred (out of scope), note it clearly in `TODO.md` and report it to me at the end.

## Deferred Items

If during implementation you determine that certain items should be deferred (e.g., a change that requires deeper refactoring not covered by the design docs), do the following:
1. Skip the item and continue with the rest.
2. Add a note in `TODO.md` marking it as "Deferred: <reason>".
3. At the end of the task, report all deferred items to me with the reason for deferral.
