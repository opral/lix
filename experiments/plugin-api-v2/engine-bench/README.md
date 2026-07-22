# Full-engine CSV baseline

This harness measures the current v1 Wasm plugin API through the real Lix SQL
file path on RocksDB and cached SlateDB. It complements the isolated mechanism
benchmark one directory above: the measured operation includes engine state
resolution, storage reads, Component Model lifting/lowering, real CSV plugin
execution, and transactional writes.

SQLite is intentionally excluded from every performance result.

## Fixture and session semantics

The deterministic fixture contains 220,000 CSV rows and 10,680,000 bytes. A
timed edit changes one field in the middle row and alternates between the
original and edited file so every round is a real one-row transition.

Before timing a write, the harness performs exactly:

```sql
SELECT data FROM lix_file WHERE path = $1
```

That point read grants the session deletion authority for the bytes it actually
received. Plugin compilation, the point read, and construction of the edited
blob stay outside the write timer. The timed write is the ordinary SQL upsert
used by clients.

The two reported backends are:

- `rocksdb`: the direct RocksDB engine storage adapter.
- `slatedb-cached`: SlateDB over a local object store with the Lixray
  per-workspace policy: 64 MiB disk cache, 4 MiB block cache, and 1 MiB metadata
  cache.

Latency runs do not use the I/O-counting wrapper. Logical I/O is collected in
separate single-round runs with `LIX_PROFILE_IO_STATS=1`.

## Production cap and diagnostic timing

The production per-component linear-memory ceiling remains 64 MiB. The v1 CSV
plugin cannot create this fixture under that ceiling. The benchmark feature
therefore exposes the SDK runtime only to this profiling target, whose wrapper
can replace the incoming memory limit when
`LIX_PROFILE_WASM_MEMORY_MIB` is set. This is a diagnostic capacity knob, not a
production configuration or recommendation.

Every timing collected with that knob prints both the diagnostic ceiling and
the 64 MiB production default. Omitting the variable exercises the production
policy.

## Build and run

From the repository root:

```sh
cargo build --release -p lix_sdk \
  --bench profile_merge_10k \
  --features default_wasm_runtime,local_filesystem,profile_wasm_memory
```

Resolve the executable printed under `target/release/deps`, then create a fresh
case directory. The commands used for the recorded 220k runs have this shape:

```sh
env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  <profile-binary> rocksdb setup <rocks-case>

env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  LIX_PROFILE_ROUNDS=12 \
  <profile-binary> rocksdb edit <rocks-case>

env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  LIX_PROFILE_ROUNDS=11 \
  <profile-binary> rocksdb render <rocks-case>

env LIX_PROFILE_INITIAL_ROWS=220000 \
  LIX_PROFILE_WASM_MEMORY_MIB=256 \
  LIX_PROFILE_IO_STATS=1 \
  <profile-binary> rocksdb edit <rocks-case>
```

Replace `rocksdb` with `slatedb-cached` and use a separate fresh case for the
cached SlateDB lane. Wrap a command in `/usr/bin/time -l` on macOS to collect
maximum resident set size without changing the timed interval printed by the
harness.

The harness also prints recursive backend bytes. For cached SlateDB it reports
the object-store and cache directories separately so cache duplication is not
mistaken for durable storage amplification.

See `results-latest-main.md` for the immutable commit, toolchain, raw commands,
measurements, and interpretation.
