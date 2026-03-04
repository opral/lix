# vscode-docs-replay

Replay the first 100 commits from `microsoft/vscode-docs` into a `.lix` SQLite artifact using:

- `js-sdk`
- `@lix-js/better-sqlite3-backend`
- no plugins required for `bench`

## CLI

```bash
pnpm --filter vscode-docs-replay run cli -- replay --commits 100
pnpm --filter vscode-docs-replay run cli -- analyze
pnpm --filter vscode-docs-replay run cli -- analyze-file-types
pnpm --filter vscode-docs-replay run cli -- bench --commits 100 --query-runs 10 --query-warmup 2 --verify-state
pnpm --filter vscode-docs-replay run cli -- reset
```

Shorthand scripts:

```bash
pnpm --filter vscode-docs-replay run replay -- --commits 100
pnpm --filter vscode-docs-replay run analyze
pnpm --filter vscode-docs-replay run analyze-file-types
pnpm --filter vscode-docs-replay run bench -- --commits 100 --query-runs 10 --query-warmup 2 --verify-state
pnpm --filter vscode-docs-replay run reset
```

## Usage (manual)

```bash
pnpm --filter vscode-docs-replay run bootstrap
pnpm --filter vscode-docs-replay run replay:raw
```

Or run both:

```bash
pnpm --filter vscode-docs-replay run run
```

## Determinism

`bootstrap` writes an anchor commit SHA to:

- `packages/vscode-docs-replay/.cache/vscode-docs.anchor`

`replay` uses this anchored SHA and replays the first commits from repository start up to that exact anchor.

Set `VSCODE_REPLAY_RESET_ANCHOR=1` to refresh the anchor.

## Git LFS Replay

Replay resolves Git LFS pointer blobs to local `.git/lfs/objects` content by default.
If objects are missing locally, replay also runs one-time `git lfs fetch --all origin`
automatically.

- `VSCODE_REPLAY_RESOLVE_LFS_POINTERS=0` disables pointer resolution.
- `VSCODE_REPLAY_FETCH_MISSING_LFS_OBJECTS=0` disables automatic missing-object fetch.
- `VSCODE_REPLAY_INSTALL_TEXT_PLUGIN=0` disables text plugin install.
- `VSCODE_REPLAY_INSTALL_MD_PLUGIN=0` disables markdown plugin install.

## Output

Default replay output:

- `packages/vscode-docs-replay/results/vscode-docs-first-100.lix`

Default bench output:

- `packages/vscode-docs-replay/results/vscode-docs.bench.json`
- `packages/vscode-docs-replay/results/vscode-docs.git-replay/` (git replay baseline repo)

Override with `VSCODE_REPLAY_OUTPUT_PATH`.
Override report path with `VSCODE_BENCH_REPORT_PATH`.
Override git baseline path with `VSCODE_BENCH_GIT_REPLAY_PATH`.

## Bench Metrics

`bench` runs:

1. replay into `.lix`
2. build a git replay baseline from the same commit slice
3. compare storage size (`lix` vs git replay, plus source clone context)
4. run paired query timings (`lix` SQL vs git commands) with warmup + measured runs

`bench` replays commits into Lix without installing plugins.

Useful env vars:

- `VSCODE_BENCH_QUERY_RUNS` measured iterations per query (default `10`)
- `VSCODE_BENCH_QUERY_WARMUP` warmup iterations per query (default `2`)
- `VSCODE_BENCH_INSERT_BATCH_ROWS` insert rows per replay statement (default `100`)
- `VSCODE_BENCH_VERIFY_STATE=1` verify git and lix file-path state after each replayed commit
- `VSCODE_BENCH_INCLUDE_COUNT_QUERY=1` include heavy `SELECT COUNT(*) FROM lix_file` paired query
- `VSCODE_BENCH_SKIP_REPLAY=1` currently ignored (replay is required for ingestion/query metrics)
