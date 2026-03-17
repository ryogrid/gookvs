# Task Completion Checklist

When a coding task is completed, the following should be done:

1. **Format code**: `make format`
2. **Run clippy**: `make clippy` (NOT `cargo clippy` directly - the Makefile sets proper features/flags)
3. **Run tests**: `make test` or specific tests with `./scripts/test $TESTNAME -- --nocapture`
4. **Full dev check**: `make dev` (runs format + clippy + tests) - must pass before PR submission

Note: The Makefile enables additional features not on by default (see ENABLE_FEATURES variable) that are part of the release build. Always use `make` commands rather than raw `cargo` for quality checks.
