# Code Style and Conventions

## Rust Formatting (rustfmt.toml)
- Style edition: 2024
- Comment width: 80 chars, wrap comments enabled
- Unix newlines
- Use field init shorthand and try shorthand (`?`)
- Imports: crate-level granularity, grouped as Std/External/Crate
- Format code in doc comments and macro bodies

## Clippy Rules (clippy.toml)
- Disallowed methods: `std::thread::Builder::spawn` (use tikv_util wrapper), `tokio::runtime::builder::Builder::on_thread_start/stop` (use ThreadBuildWrapper), various unsound openssl methods
- Disallowed types: Several openssl types due to RUSTSEC advisories
- No holding dashmap Ref across await points

## PR Requirements
- Title format: `module: what's changed` or `*: what's changed`
- Must link issues with `close #xxx` or `ref #xxx`
- All commits must be signed off (`git commit -s`)
- Follow `.github/pull_request_template.md`
