## Lix Engine

- Engine behavior tests use the canonical in-memory storage implementation.
- During development, `cargo test -p lix` runs the fast base simulation
  without compiling external storage adapters.
- Before committing, run `cargo test -p lix --features all-simulations`
  to exercise both the base and tracked-state-rebuild simulations.
- Storage adapters own their conformance tests. Run the relevant adapter package
  when its implementation or the engine storage contract changes:
  `cargo test -p lix_storage_rocksdb` or
  `cargo test -p lix_storage_slatedb`.
- Engine benchmarks live in `lix_e2e`, whose default backend is
  RocksDB. Use `--all-features` for the complete backend benchmark build.
