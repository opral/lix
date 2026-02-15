# nextjs-replay-bench

One-off benchmark package to replay linear git history into `js-sdk`/engine.

## Run

```bash
pnpm --filter nextjs-replay-bench run bench:1000
```

## Useful env vars

- `BENCH_REPLAY_REPO_PATH` path to an existing git clone (skip clone)
- `BENCH_REPLAY_REPO_URL` remote clone URL (default: `https://github.com/vercel/next.js.git`)
- `BENCH_REPLAY_COMMITS` number of commits to replay (default: `1000`)
- `BENCH_REPLAY_FETCH` set to `0` to skip `git fetch`
- `BENCH_REPLAY_INSTALL_TEXT_LINES_PLUGIN` set to `0` to disable plugin install
- `BENCH_REPLAY_PROGRESS_EVERY` progress cadence (default: `25`)

Output report:

- `packages/nextjs-replay-bench/results/nextjs-replay.bench.json`
