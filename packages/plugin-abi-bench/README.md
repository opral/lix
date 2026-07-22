# Plugin Component ABI benchmark

This benchmark isolates the Wasmtime Component Model canonical ABI cost of the
current nested plugin values from a proposed versioned packed byte arena. Both
guests are Wasm components; the guest functions intentionally do almost no
semantic work.

Run the release matrix:

```sh
cargo run --release -p lix_plugin_abi_bench
```

Useful environment variables:

- `LIX_ABI_BENCH_SIZES=102400,1048576` filters logical file/state sizes.
- `LIX_ABI_BENCH_DENSITIES=48,1024` filters snapshot bytes per entity.
- `LIX_ABI_BENCH_MIN_SAMPLES=9` and `LIX_ABI_BENCH_MAX_SAMPLES=31` tune sampling.
- `LIX_ABI_BENCH_TARGET_MS=1500` tunes time spent on each median.
- `LIX_ABI_BENCH_PROFILE=rich-detect-empty` runs one operation repeatedly for a
  profiler. Valid prefixes are `rich-` and `packed-`; operation suffixes are
  `detect-empty`, `entity-round-trip`, and `file-round-trip`.
- `LIX_ABI_BENCH_PROFILE_ITERATIONS=1000` controls profiler repetitions.

The normal matrix times calls under a generous 512 MiB ceiling so both ABIs can
be compared, reports peak aggregate guest linear memory, and separately probes whether
the same call succeeds under Lix's production 64 MiB ceiling. The `memcpy`
rows are one preallocated native host copy, providing a lower-bound copy floor.

The packed format is deliberately small and versioned, not a production wire
format. Its 32-byte header contains magic (`LPK1`), version, packet kind, entity
count, record offset, file offset/length, and total encoded length. Each fixed
32-byte entity record contains offset/length spans for the primary-key item
table, schema key, snapshot, and optional metadata. Primary-key item spans and
UTF-8 field bytes live in the same arena, followed by file bytes.
