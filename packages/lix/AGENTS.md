## Lix Engine

- Engine behavior tests use the canonical in-memory storage implementation.
- During development, `cargo nextest run -p lix` runs the fast base simulation
  without compiling external storage adapters.
- Before committing, run
  `RUST_MIN_STACK=16777216 cargo nextest run -p lix --features all-simulations`
  to exercise both the base and tracked-state-rebuild simulations. The
  unoptimized async test poll chains exceed Rust's default 2 MiB test-thread
  stack in existing file and sync paths; CI uses the same setting. Run
  `cargo test -p lix --doc` separately because nextest does not execute doctests.
- Storage adapters own their conformance tests. Run the relevant adapter package
  with nextest when its implementation or the engine storage contract changes:
  `cargo nextest run -p lix_storage_rocksdb` or
  `cargo nextest run -p lix_storage_slatedb`.
- Engine benchmarks live in `lix_e2e`, whose default backend is
  RocksDB. Use `--all-features` for the complete backend benchmark build.
