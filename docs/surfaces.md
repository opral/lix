---
description: The SQL surfaces in Lix at a glance. State surfaces are JSON-shaped and schema-agnostic; per-entity, file, and directory surfaces are typed sugar over the same data. One grid, eleven tables.
---

# SQL Surfaces

Lix exposes the same underlying state through several SQL surfaces so you can query it the way that fits the question you're asking.

Two ergonomic axes:

- **Grain.** Typed columns for one schema vs. raw JSON across all schemas vs. file bytes.
- **Scope.** The active version, all versions side-by-side, or history walked through commits.

A third surface, `lix_change`, sits outside the grid as the global change ledger: retained tracked history plus the latest compactable untracked change for each current identity, ordered by `created_at`.

## The grid

|                              | Active (current state)         | Cross-version (side-by-side)              | History (time-travel)                  |
| :--------------------------- | :----------------------------- | :---------------------------------------- | :------------------------------------- |
| **Per-entity, typed**        | `<schema>`                     | `<schema>_by_version`                     | `<schema>_history`                     |
| **State, raw JSON, all schemas** | `lix_state`                | `lix_state_by_version`                    | `lix_state_history`                    |
| **Files (bytes)**            | `lix_file`                     | `lix_file_by_version`                     | `lix_file_history`                     |
| **Directories**              | `lix_directory`                | `lix_directory_by_version`                | `lix_directory_history`                |

Plus: `lix_change`, the global change ledger (no version filter).

Pick the row by what you're querying; pick the column by which version(s) and which time. Same data underneath, different ergonomics.

## State surfaces

Schema-agnostic, JSON-shaped reads across every registered schema.

| Surface | Use for |
| :-- | :-- |
| `lix_state` | Current state of every entity in the active version. |
| `lix_state_by_version` | Same, but with a `version_id` column so you can read across versions. |
| `lix_state_history` | State walked back through the commit graph from a given commit. |

Common columns (`lix_state` and `lix_state_by_version`): `entity_pk` (JSON array of primary-key values), `schema_key`, `file_id`, `snapshot_content` (JSON), `metadata` (JSON), `schema_version`, `change_id`, `commit_id`. `lix_state_by_version` adds `version_id`.

Every canonical current row has a `change_id`, including rows written with `lixcol_untracked = true`. Untracked current rows have no `commit_id`; tracked rows reference the commit that retained their change. Deleting an untracked row physically removes its flat current-index entry and standalone ChangeRecord; no untracked tombstone is retained. Valid writes reject tracked and untracked rows with the same canonical identity.

`lix_state_history` uses the same prefixed history vocabulary as every other history surface: `lixcol_entity_pk`, `lixcol_observed_commit_id`, `lixcol_commit_created_at`, `lixcol_as_of_commit_id`, `lixcol_depth`, and `lixcol_is_deleted`. Because it exposes one canonical change per row, it also provides singular provenance through `lixcol_change_id`, `lixcol_change_created_at`, and `lixcol_origin_key`, plus `lixcol_schema_key`, `lixcol_file_id`, `lixcol_snapshot_content`, and `lixcol_metadata`.

> **History queries require a literal filter on `lixcol_as_of_commit_id`.** A correlated subquery against `lix_branch` is rejected by the planner. Use `lix_active_branch_commit_id()` for the active branch, or resolve the commit id with one query and pass it as a parameter. See [`lix_active_branch_commit_id()`](./sql-functions.md#lix_active_branch_commit_id).

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
SELECT lixcol_depth, lixcol_observed_commit_id, lixcol_snapshot_content
FROM lix_state_history
WHERE lixcol_schema_key = 'task'
  AND lix_json_get_text(lixcol_entity_pk, 0) = 't1'
  AND lixcol_as_of_commit_id = lix_active_branch_commit_id()
ORDER BY lixcol_depth;
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
- `<schema>_history`: the common history columns plus `lixcol_snapshot_content`, `lixcol_change_id`, `lixcol_change_created_at`, and `lixcol_origin_key` (no `lixcol_commit_id`; revisions are anchored by `lixcol_as_of_commit_id` and observed at `lixcol_observed_commit_id`).

There are no bare history aliases. The `lixcol_` prefix and `lixcol_as_of_commit_id` spelling are identical across raw, typed, file, and directory history.

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
  AND lixcol_as_of_commit_id = lix_active_branch_commit_id()
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

User columns: `id`, `path`, `directory_id`, `name`, `data`. File history adds the common history columns and `lixcol_source_changes`, a deterministic JSON array of the canonical changes that caused the composed file revision. It deliberately has no singular `lixcol_change_id`, `lixcol_schema_key`, or `lixcol_origin_key`.

`lix_file_history` records changes to the composed file projection, not only
changes to the file descriptor or bytes. Renaming, moving, deleting, or
restoring any ancestor directory creates a revision for every affected
descendant file. The file `id` remains stable while `path` reflects the exact
observed commit. `lixcol_source_changes` contains the descriptor, blob, plugin,
and ancestor-directory changes combined into that logical revision.

```sql
-- Write a file into the active version
INSERT INTO lix_file (path, data)
VALUES ('/orders.xlsx', ?);

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
  AND lixcol_as_of_commit_id = lix_active_branch_commit_id()
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

User columns: `id`, `path`, `parent_id`, `name`. Directory history uses the same common history columns and structured `lixcol_source_changes` provenance as file history. Directory paths must end with a trailing slash (`/data/`, not `/data`).

Directory history uses the same composed-projection rule: an ancestor rename,
move, deletion, or restoration creates a revision for each descendant
directory whose `path` changed. Recursive deletion provenance includes both a
descendant's own tombstone and the tombstones of deleted ancestors.

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

| Surface family | System column prefix | Branch column |
| :-- | :-- | :-- |
| `lix_state`, `lix_state_by_branch` | bare (no prefix) | `branch_id` |
| `lix_state_history` | `lixcol_*` | (none; anchor with `lixcol_as_of_commit_id`) |
| `<schema>*`, `lix_file*`, `lix_directory*` | `lixcol_*` | `lixcol_branch_id` |
| `lix_change` | bare | (none, global) |

Current and cross-branch state surfaces are projection-friendly raw views with
bare bookkeeping names. History, per-entity, file, and directory surfaces wear
`lixcol_*` to keep user columns (`id`, `title`, `path`, …) cleanly separated
from Lix bookkeeping.

## Composition recap

- One row in **`lix_change`** per write, ever. Global, version-blind, immutable.
- **State surfaces** (`lix_state*`) project that journal as JSON snapshots, scoped by version (`_by_version`) or walked through commits (`_history`).
- **Per-entity surfaces** (`<schema>*`) and **file/directory surfaces** are typed projections of the same state, with user columns extracted into native SQL types.

Reach for typed surfaces when you know the schema. Drop to `lix_state*` for cross-schema reads. Drop to `lix_change` for raw activity feeds.
