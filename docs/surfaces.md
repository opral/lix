---
description: The SQL surfaces in Lix at a glance. State surfaces are JSON-shaped and schema-agnostic; per-entity, file, and directory surfaces are typed sugar over the same data. One grid, eleven tables.
---

# SQL Surfaces

Lix exposes the same underlying state through several SQL surfaces so you can query it the way that fits the question you're asking.

Two ergonomic axes:

- **Grain.** Typed columns for one schema vs. raw JSON across all schemas vs. file bytes.
- **Scope.** The active version, all versions side-by-side, or history walked through commits.

A third surface, `lix_change`, sits outside the grid as the immutable global change journal: every write across every schema and every version, ordered by `created_at`.

## The grid

|                              | Active (current state)         | Cross-version (side-by-side)              | History (time-travel)                  |
| :--------------------------- | :----------------------------- | :---------------------------------------- | :------------------------------------- |
| **Per-entity, typed**        | `<schema>`                     | `<schema>_by_version`                     | `<schema>_history`                     |
| **State, raw JSON, all schemas** | `lix_state`                | `lix_state_by_version`                    | `lix_state_history`                    |
| **Files (bytes)**            | `lix_file`                     | `lix_file_by_version`                     | `lix_file_history`                     |
| **Directories**              | `lix_directory`                | `lix_directory_by_version`                | `lix_directory_history`                |

Plus: `lix_change`, the global change journal (no version filter).

Pick the row by what you're querying; pick the column by which version(s) and which time. Same data underneath, different ergonomics.

## State surfaces

Schema-agnostic, JSON-shaped reads across every registered schema.

| Surface | Use for |
| :-- | :-- |
| `lix_state` | Current state of every entity in the active version. |
| `lix_state_by_version` | Same, but with a `version_id` column so you can read across versions. |
| `lix_state_history` | State walked back through the commit graph from a given commit. |

Common columns (`lix_state` and `lix_state_by_version`): `entity_pk` (JSON array of primary-key values), `schema_key`, `file_id`, `snapshot_content` (JSON), `metadata` (JSON), `schema_version`, `change_id`, `commit_id`. `lix_state_by_version` adds `version_id`.

`lix_state_history` shares `entity_pk` (JSON array of primary-key values), `schema_key`, `file_id`, `snapshot_content`, `metadata`, `schema_version`, `change_id`, and instead of `commit_id` exposes `start_commit_id`, `observed_commit_id`, `commit_created_at`, and `depth` (commit-graph distance from `start_commit_id`; `0` is the freshest observation, higher values walk back, and intermediate commits that didn't touch the entity are skipped).

> **History queries require a literal filter on `start_commit_id`.** A correlated subquery against `lix_version` is rejected by the planner. Use `lix_active_version_commit_id()` for the active version, or resolve the commit id with one query and pass it as a parameter. See [`lix_active_version_commit_id()`](./sql-functions.md#lix_active_version_commit_id).

```sql
-- Every entity in the active version, raw JSON
SELECT entity_pk, schema_key, snapshot_content FROM lix_state;

-- Same entity in two versions, side by side
SELECT version_id, snapshot_content
FROM lix_state_by_version
WHERE schema_key = 'task'
  AND lix_json_get_text(entity_pk, 0) = 't1'
  AND version_id IN ($a, $b);

-- Walk history of one entity from a version's tip
SELECT depth, observed_commit_id, snapshot_content
FROM lix_state_history
WHERE schema_key = 'task'
  AND lix_json_get_text(entity_pk, 0) = 't1'
  AND start_commit_id = lix_active_version_commit_id()
ORDER BY depth;
```

## Per-entity sugar

For each registered schema `X`, Lix generates three typed surfaces named after `x-lix-key`:

| Surface | Use for |
| :-- | :-- |
| `<schema>` | `INSERT` / `SELECT` / `UPDATE` / `DELETE` against the active version with typed columns. |
| `<schema>_by_version` | Read or write across versions; INSERTs require `lixcol_version_id`. |
| `<schema>_history` | Time-travel through one schema's history with typed columns. |

Per-entity surfaces project user columns directly (`id`, `title`, `done`, …) plus `lixcol_*`-prefixed system columns. The set varies by scope:

- `<schema>` (active): `lixcol_change_id`, `lixcol_commit_id`, `lixcol_created_at`, `lixcol_updated_at`, plus bookkeeping. **No `lixcol_version_id`**; the active surface is implicitly the active version.
- `<schema>_by_version`: adds `lixcol_version_id`. INSERT/UPDATE require it.
- `<schema>_history`: `lixcol_start_commit_id`, `lixcol_observed_commit_id`, `lixcol_depth`, `lixcol_snapshot_content`, `lixcol_change_id` (no `lixcol_commit_id` here; commits in history are addressed via `lixcol_observed_commit_id`).

Note the prefix asymmetry between grains: state surfaces use **bare** column names (`start_commit_id`, `depth`, `observed_commit_id`); per-entity, file, and directory surfaces wear `lixcol_` on the same columns.

```sql
-- Current rows of one schema, typed columns
SELECT id, title, done FROM task;

-- Compare one entity across two versions, typed
SELECT lixcol_version_id, title, done
FROM task_by_version
WHERE id = 't1' AND lixcol_version_id IN ($a, $b);

-- History of one entity, typed
SELECT lixcol_depth, title, done
FROM task_history
WHERE id = 't1'
  AND lixcol_start_commit_id = lix_active_version_commit_id()
ORDER BY lixcol_depth;
```

When you need the typed columns, reach for the per-entity sugar. When you're querying across schemas, drop down to `lix_state*`. Same data either way.

## Files

`lix_file` versions byte content alongside path metadata. Each file gets the same three views as a registered schema, plus a `data BLOB` column for bytes.

| Surface | Use for |
| :-- | :-- |
| `lix_file` | Current files in the active version. Read bytes via `data`. |
| `lix_file_by_version` | Read or write files across versions. |
| `lix_file_history` | Walk previous versions of a file's bytes through the commit graph. |

User columns: `id`, `path`, `directory_id`, `name`, `hidden`, `data`. System columns are `lixcol_*` (`lixcol_version_id` on `_by_version`; `lixcol_start_commit_id`, `lixcol_depth`, `lixcol_observed_commit_id` on `_history`).

```sql
-- Write a file into the active version
INSERT INTO lix_file (id, path, data, hidden)
VALUES ('orders-file', '/orders.xlsx', $1, false);

-- Current bytes of a file
SELECT data FROM lix_file WHERE path = '/orders.xlsx';

-- Bytes of the same file in two versions
SELECT lixcol_version_id, data
FROM lix_file_by_version
WHERE path = '/orders.xlsx' AND lixcol_version_id IN ($a, $b);

-- Every previous version of a file's bytes
SELECT lixcol_depth, lixcol_observed_commit_id, data
FROM lix_file_history
WHERE path = '/orders.xlsx'
  AND lixcol_start_commit_id = lix_active_version_commit_id()
ORDER BY lixcol_depth;
```

In JavaScript, pass a `Uint8Array` or `ArrayBuffer` for `$1`. Read `data` with `row.value("data").asBytes()`.

## Directories

Same shape as files, minus the `data` column.

| Surface | Use for |
| :-- | :-- |
| `lix_directory` | Current directories in the active version. |
| `lix_directory_by_version` | Cross-version directory reads/writes. |
| `lix_directory_history` | Directory history walked through commits. |

User columns: `id`, `path`, `parent_id`, `name`, `hidden`. Same `lixcol_*` system columns as files. Directory paths must end with a trailing slash (`/data/`, not `/data`).

Inserting a `lix_file` at `/a/b/c.txt` auto-creates `lix_directory` rows for `/a/` and `/a/b/` if they don't already exist; you only need to insert directories explicitly when you want them to exist before any file does.

```sql
-- List children of a directory
SELECT name, path FROM lix_directory WHERE parent_id = (
  SELECT id FROM lix_directory WHERE path = '/data/'
);
```

## `lix_change`: the global journal

Outside the grid because it isn't scoped to a version: every write across every schema, every version, every file, in commit order.

Columns: `id`, `entity_pk`, `schema_key`, `file_id`, `metadata`, `snapshot_content`, `created_at`.

Use `lix_change` for cross-cutting questions where neither version nor schema scopes the answer:

```sql
-- Last 20 application-level changes across the entire repo
SELECT created_at, schema_key, entity_pk, snapshot_content
FROM lix_change
WHERE schema_key NOT LIKE 'lix_%'
ORDER BY created_at DESC
LIMIT 20;
```

Without the `schema_key NOT LIKE 'lix_%'` filter the feed is dominated by Lix's own bookkeeping entities (`lix_commit`, `lix_binary_blob_ref`, `lix_file_descriptor`).

Per-version history goes through the commit graph, not `lix_change` directly. See [Change History](./history.md).

## Naming conventions

| Surface family | System column prefix | Version column |
| :-- | :-- | :-- |
| `lix_state*` | bare (no prefix) | `version_id` |
| `<schema>*`, `lix_file*`, `lix_directory*` | `lixcol_*` | `lixcol_version_id` |
| `lix_change` | bare | (none, global) |

State surfaces are projection-friendly raw views. Per-entity, file, and directory surfaces wear `lixcol_*` to keep your user columns (`id`, `title`, `path`, …) cleanly separated from Lix bookkeeping.

## Composition recap

- One row in **`lix_change`** per write, ever. Global, version-blind, immutable.
- **State surfaces** (`lix_state*`) project that journal as JSON snapshots, scoped by version (`_by_version`) or walked through commits (`_history`).
- **Per-entity surfaces** (`<schema>*`) and **file/directory surfaces** are typed projections of the same state, with user columns extracted into native SQL types.

Reach for typed surfaces when you know the schema. Drop to `lix_state*` for cross-schema reads. Drop to `lix_change` for raw activity feeds.
