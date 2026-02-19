# vscode-docs-replay

Replay the first 100 commits from `microsoft/vscode-docs` into a `.lix` SQLite artifact using:

- `js-sdk`
- `@lix-js/better-sqlite3-backend`
- installed plugins: `text_plugin`, `plugin_md_v2`

## CLI

```bash
pnpm --filter vscode-docs-replay run cli -- replay --commits 100
pnpm --filter vscode-docs-replay run cli -- analyze
pnpm --filter vscode-docs-replay run cli -- analyze-file-types
pnpm --filter vscode-docs-replay run cli -- reset
```

Shorthand scripts:

```bash
pnpm --filter vscode-docs-replay run replay -- --commits 100
pnpm --filter vscode-docs-replay run analyze
pnpm --filter vscode-docs-replay run analyze-file-types
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

## Output

Default replay output:

- `packages/vscode-docs-replay/results/vscode-docs-first-100.lix`

Override with `VSCODE_REPLAY_OUTPUT_PATH`.
