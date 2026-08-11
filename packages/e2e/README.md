# `lix_e2e`

End-to-end tests and benchmarks for lix. `publish = false`; nothing here ships.

## What belongs here

**Needs a real storage adapter or a wasm plugin → here.**
**Needs only `lix` → `packages/lix/benches/`.**

That is the whole rule. A bench that opens an in-memory `lix::Memory` engine and
measures the SQL or version-control surface does not belong in this crate: it
would drag RocksDB, SlateDB, `object_store` and their transitive trees into a
build that never touches them. `diff_commands`, `undo_redo`,
`row_default_values`, `row_pk_layout`, `registered_row_returning` and
`commit_graph_scale` live in `packages/lix/benches/` for exactly that reason.

Conversely, anything that needs `lix-storage-rocksdb`, `lix-storage-slatedb`,
`lix-storage-filesystem`, a plugin component, or a crash/reopen cycle against a
real backend belongs here and only here.

## Features

| feature | what it is for |
|---|---|
| `rocksdb`, `slatedb`, `storage-benches` | Select the storage adapters. They look redundant for a crate that exists to need real storage, but **116 `#[cfg(feature = ...)]` sites across 12 source files here read them**, including a `StorageProfile::SlateDB` enum variant. Removing the features compiles that code out silently — measured: 160 `unexpected cfg condition value` warnings and 2 hard errors. Un-gating those sites is a separate change. |
| `sdk-tests` | Turns on `lix/default_wasm_runtime`. With it enabled, every Lix opened without an explicit runtime constructs a Wasmtime plugin runtime (`handle.rs::new_engine`), so making it unconditional would change what every benchmark here measures. The benches build without it; the SDK tests that drive plugins declare it. |
| `tpch` | Pulls `duckdb` with the `bundled` C++ build — minutes of compile for one bench. |
| `root-replay-trace` | Instrumentation sources in `lix`, off by default. |
| `system-allocation-profiler` | Replaces the byte-accounting allocator with libc so heap profilers can attribute allocation churn. |

## Running things

```
# what CI runs
cargo test --profile test -p lix_e2e \
  --features sdk-tests,storage-benches,slatedb,root-replay-trace --no-fail-fast

# a single bench, filtered
cargo bench -p lix_e2e --features storage-benches,slatedb --bench tracked_state_crud -- 'read_'
```

Benches are `harness = false`; cargo appends `--bench` to your argv, so targets
that take positional arguments need their full positional list or they will
silently print nothing.
