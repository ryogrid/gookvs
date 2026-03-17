# Suggested Commands

## Building
- `make build` - Development build (unoptimized)
- `make release` - Release build (optimized)
- `cargo check --all` - Quick check without full compilation

## Testing
- `make test` - Full test suite
- `./scripts/test $TESTNAME -- --nocapture` - Run specific test
- `env EXTRA_CARGO_ARGS=$TESTNAME make test` - Specific test via make
- `env EXTRA_CARGO_ARGS=$TESTNAME make test_with_nextest` - Faster testing with nextest

## Code Quality
- `make format` - Run rustfmt
- `make clippy` - Run clippy (use this, NOT cargo clippy directly)
- `make dev` - Full dev checks (format + clippy + tests) - must pass before PR

## Running
- `make run` - Run development build

## Git
- `git commit -s -m "msg"` - Commits must be signed off (DCO)

## System Utils
- `git`, `ls`, `cd`, `grep`, `find` - Standard Linux commands
