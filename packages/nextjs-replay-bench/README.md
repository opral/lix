# nextjs-replay-bench

One-off benchmark package to replay linear git history into `js-sdk`/engine.

## Run

```bash
pnpm --filter nextjs-replay-bench run bench:1000
```

Warm vs cold quick commands:

```bash
pnpm -C packages/nextjs-replay-bench bench:25        # warm (5 warmup + 25 measured)
pnpm -C packages/nextjs-replay-bench bench:25:cold   # cold (0 warmup + 25 measured)
pnpm -C packages/nextjs-replay-bench bench:git-files:100   # git file-write + git commit baseline
```

## Useful env vars

- `BENCH_REPLAY_REPO_PATH` path to an existing git clone (skip clone)
- `BENCH_REPLAY_REPO_URL` remote clone URL (default: `https://github.com/vercel/next.js.git`)
- `BENCH_REPLAY_REF` git ref to replay from (default: `HEAD`)
- `BENCH_REPLAY_COMMITS` number of measured commits to replay (default: `1000`)
- `BENCH_REPLAY_WARMUP_COMMITS` number of warmup commits to replay first (excluded from measured stats, default: `5`)
- `BENCH_REPLAY_FETCH` set to `1` to fetch remote updates (default: `0`)
- `BENCH_REPLAY_INSTALL_TEXT_LINES_PLUGIN` set to `0` to disable plugin install
- `BENCH_REPLAY_EXPORT_SNAPSHOT` set to `1` to export a sqlite snapshot artifact
- `BENCH_REPLAY_SNAPSHOT_PATH` custom output path for snapshot artifact (`.lix` recommended)
- `BENCH_REPLAY_PROGRESS_EVERY` progress cadence (default: `25`)
- `BENCH_GIT_FILES_REPORT_PATH` custom output path for git file-replay report
- `BENCH_GIT_FILES_DISABLE_MAINTENANCE` disable git auto maintenance/gc in target replay repo (default: `1`)
- `BENCH_GIT_TRACE2_PERF` set to `1` to enable git Trace2 performance logging for the git baseline replay
- `BENCH_GIT_TRACE2_PERF_PATH` output path for trace2 log (default: `packages/nextjs-replay-bench/results/nextjs-replay.git-files.trace2.perf.log`)

Determinism:

- replay always opens `openLix({ keyValues: [{ key: "lix_deterministic_mode", value: { enabled: true }, lixcol_version_id: "global" }] })`
- for fully reproducible commit sets across machines, pin `BENCH_REPLAY_REF` to a commit SHA

Output report:

- `packages/nextjs-replay-bench/results/nextjs-replay.bench.json`
- `packages/nextjs-replay-bench/results/nextjs-replay.git-files.bench.json`
- `packages/nextjs-replay-bench/results/nextjs-replay.snapshot.lix` (when snapshot export enabled)
