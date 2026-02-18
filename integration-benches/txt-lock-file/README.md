# txt-lock-file bench

Minimal reproduction benchmark for text-lines plugin overhead on large `lix_file` updates.

By default it benchmarks the `yarn.lock` update from commit `29c226771ce8...` in cached Next.js history.

## Run

```bash
pnpm -C integration-benches/txt-lock-file bench
```

## Useful environment variables

- `BENCH_REPO_PATH` - path to git repo (default: `packages/nextjs-replay-bench/.cache/nextjs-replay/next.js`)
- `BENCH_TARGET_COMMIT` - target commit SHA (default: `29c226771ce8b5b26632c8e7753e69f7407933b4`)
- `BENCH_FILE_PATH` - file path inside commit (default: `yarn.lock`)
- `BENCH_ITERATIONS` - measured update count per scenario (default: `20`)
- `BENCH_WARMUP` - warmup updates per scenario (default: `4`)
- `BENCH_PLUGIN_MODE` - `on`, `off`, or `both` (default: `both`)
- `BENCH_OUTPUT_PATH` - output JSON path (default: `results/text-lines-repro.bench.json`)

## Output

The script prints a compact console summary and writes a JSON report with:

- per-scenario timing stats (`mean`, `p50`, `p95`, `max`)
- file size / line count of before and after blobs
- resulting `text_line` / `text_document` row counts for the benchmark file
