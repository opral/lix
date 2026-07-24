## Lix Engine

- Engine behavior tests use the canonical in-memory storage implementation.
- During development, `cargo test -p lix_engine` runs the fast base simulation
  without compiling external storage adapters.
- Before committing, run `cargo test -p lix_engine --features all-simulations`
  to exercise both the base and tracked-state-rebuild simulations.
- Storage adapters own their conformance tests. Run the relevant adapter package
  when its implementation or the engine storage contract changes:
  `cargo test -p lix_rocksdb_storage`, `cargo test -p lix_sqlite_storage`, or
  `cargo test -p lix_slatedb_storage`.
- Engine benchmarks live in `lix_engine_benchmarks`, whose default backend is
  RocksDB. Use `--all-features` for the complete backend benchmark build.
