# fossil-bench

Head-to-head benchmark harness for:

- `js-sdk` (Lix engine) using `@lix-js/better-sqlite3-backend`
- Fossil repository storage and history commands

The runner executes the same deterministic file workload against both backends and writes a single JSON report.

Lix ingest and update use batched SQL writes (`INSERT` and `UPDATE ... CASE`) so one benchmark batch maps to one Lix commit-like write pass, matching Fossil's batched commit model more closely.

## Run

```bash
pnpm -C packages/fossil-bench run bench
```

Quick smoke run:

```bash
pnpm -C packages/fossil-bench run bench:quick
```

Only one target:

```bash
pnpm -C packages/fossil-bench run bench:lix-only
pnpm -C packages/fossil-bench run bench:lix-only:full
pnpm -C packages/fossil-bench run bench:fossil-only
```

`bench:lix-only` uses a smaller profile so it finishes faster by default.
Use `bench:lix-only:full` for the full default profile.

## Requirements

- Node `>=22`
- `fossil` binary on `PATH` (required for `BENCH_TARGET=fossil` or `BENCH_TARGET=both`)

## Output

- `packages/fossil-bench/results/fossil-vs-lix.bench.json`

## Useful env vars

- `BENCH_TARGET` = `both` | `lix` | `fossil` (default `both`)
- `BENCH_FILES_PER_CLASS` (default `12`)
- `BENCH_UPDATE_ROUNDS` (default `3`)
- `BENCH_HISTORY_READS` (default `24`)
- `BENCH_MAX_BLOB_BYTES` (default `1048576`)
- `BENCH_RESULTS_PATH` custom output path
- `BENCH_KEEP_ARTIFACTS` = `1` to keep `.cache/` artifacts between runs
- `BENCH_PROGRESS` = `1|0` enable/disable phase and loop progress logs (default `1`)
- `BENCH_PROGRESS_SLICES` progress checkpoints per long loop (default `8`)
