# Lix Skill

Use this skill when working with `.lix` repositories.

## Goal

Read and write data in a Lix repo safely through the Lix CLI.

## Concepts

- Files:
  - Files are exposed through `lix_file` (active version) and `lix_file_by_version` (explicit version).
  - `data` is bytes; use `lix_text_encode('...')` for text payloads.
- Entities:
  - Entities are schema-scoped records in `lix_state` / `lix_state_by_version`.
  - They are keyed by `schema_key` + `entity_id` + `file_id`, with schemas discoverable via `lix_stored_schema`.
- Checkpoints:
  - A checkpoint is a committed history boundary (a saved change set) used to anchor history/diffs.
  - History views (`*_history`) are read-only projections over these committed changes.
- Working changes:
  - Uncheckpointed changes are exposed via `lix_working_changes`.
  - This is the primary surface for “what changed since last checkpoint”.
- Versions:
  - Versions are the name for "branches". Version is used because non technical users dont know what a branch is.
  - `lix_active_version` selects the current one; `lix_version` lists available versions.

## Rules (non-negotiable)

1. Never use `sqlite3` (or any direct SQLite client) on `.lix` files.
2. Always use the `lix` CLI.
3. Always pass `--path` to avoid operating on the wrong repo.
4. For `lix_file.data`, write bytes only:
   - text: `lix_text_encode('...')`
   - hex blob: `X'...'`
   - blob parameter

## CLI quickstart

Build/run from source:

```sh
cd /Users/samuel/git-repos/flashtype/submodule/lix/packages/cli
cargo run --bin lix -- --help
```

## Canonical commands

Read:

```sh
lix --path /path/to/repo.lix sql execute "SELECT id, path FROM lix_file ORDER BY path;"
```

Write text file data:

```sh
lix --path /path/to/repo.lix sql execute \
  "INSERT INTO lix_file (path, data) VALUES ('/hello.md', lix_text_encode('hello'));"
```

Read query via stdin:

```sh
cat <<'SQL' | lix --path /path/to/repo.lix sql execute -
SELECT path, hidden
FROM lix_file
ORDER BY path;
SQL
```

## Common gotchas

- `.lix` is the repository. There is no checked-out working directory.
- `lix_file` uses `id` (not `file_id`).
- Some views are read-only (`*_history`).
- Unknown table/column errors should be fixed by checking `lix_*` table/column names first.
