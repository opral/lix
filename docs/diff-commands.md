---
description: Compare typed Lix relations across commits and select rows atomically for revert, apply, or checkpoint commands.
---

# Diff commands

`lix_diff(relation, from_commit_id, to_commit_id)` compares one relation across
two explicitly selected commits. A file is one row of `lix_file`; a registered
schema row is one row of its schema relation. Commands consume that same
`(relation, row_pk)` identity, so selecting a file also selects its underlying
tracked content.

## Review changed files

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
const changedFiles = await lix.execute(
  `SELECT lixcol_row_pk, diff_type, from_path, to_path, row_count
   FROM lix_diff(
     'lix_file',
     lix_latest_checkpoint_commit_id(),
     lix_active_branch_commit_id()
   )
   ORDER BY coalesce(to_path, from_path)`,
);

for (const row of changedFiles.rows) {
  console.log(row.diff_type, row.to_path ?? row.from_path);
}

const reverted = await lix.execute(
  `INSERT INTO lix_revert (relation, row_pk)
   SELECT 'lix_file', lixcol_row_pk
   FROM lix_diff(
     'lix_file',
     lix_latest_checkpoint_commit_id(),
     lix_active_branch_commit_id()
   )
   WHERE lixcol_row_pk = $1
   RETURNING commit_id`,
  [changedFiles.rows[0].lixcol_row_pk],
);

if (reverted.rows.length > 0) {
  console.log("reverted in commit", reverted.rows[0].commit_id);
}

await lix.close();
```

`lix_latest_checkpoint_commit_id()` uses the active branch's checkpoint, not
the newest repository-global checkpoint marker. Before the branch has a
checkpoint, it returns `lix_root_commit_id()`, so the same query also works
for a new branch.

## Diff rows

Every relation diff exposes `lixcol_row_pk`, `diff_type`,
`row_count`, and a
`from_<column>` / `to_<column>` pair for each column of the compared relation.
`diff_type` is `added`, `modified`, or `removed`. Added rows have empty `from_`
values; removed rows have empty `to_` values. Use
`coalesce(to_path, from_path)` when displaying a path that also covers removed
or renamed files.

Projecting `from_content` or `to_content` for `lix_file` is unsupported because
reconstructing historical file bytes would turn lightweight diff reads into blob
materialization. Query `lix_history('lix_file', commit_id)` when file bytes are
required.

`row_count` is `1` for a changed schema row. For a file it counts the underlying
descriptor and content rows contributing to the aggregate:

```sql
SELECT count(*) AS changed_files, sum(row_count) AS changed_rows
FROM lix_diff(
  'lix_file',
  lix_latest_checkpoint_commit_id(),
  lix_active_branch_commit_id()
);
```

`lix_diff('lix_directory', ...)` supports changes to directory descriptors,
including directory creation, removal, and renaming. Rolling changed files or
their content up into an otherwise unchanged parent directory is unsupported;
query `lix_diff('lix_file', ...)` for file-level changes and aggregate those
rows by path when directory summaries are needed.

Bulk deletion of an entire schema collection is represented by an internal
collection-generation marker, not individual row changes. Expanding that marker
into per-row `lix_diff` results is unsupported; delete selected rows individually
when row-level history or diff visibility is required.

Both commit IDs are required. `lix_root_commit_id()` returns the repository
root; comparing it with another commit reports files present in that commit as
added rows. Genesis comparisons of internal bootstrap schema rows are
unsupported because those metadata rows already exist in the bootstrap root.

```sql
SELECT lixcol_row_pk, to_path
FROM lix_diff('lix_file', lix_root_commit_id(), $commit_id);
```

## Commands

The insert-only command sinks consume queries selecting `(relation, row_pk)`:

```sql
INSERT INTO lix_revert (relation, row_pk)
SELECT 'lix_file', lixcol_row_pk
FROM lix_diff(
  'lix_file',
  lix_latest_checkpoint_commit_id(),
  lix_active_branch_commit_id()
)
WHERE coalesce(to_path, from_path) LIKE '/docs/%';

INSERT INTO lix_apply (relation, row_pk)
SELECT 'acme_task', lixcol_row_pk
FROM lix_diff('acme_task', $from_commit_id, $to_commit_id)
WHERE to_done = true;

INSERT INTO lix_create_checkpoint (relation, row_pk)
SELECT 'lix_file', lixcol_row_pk
FROM lix_diff(
  'lix_file',
  lix_latest_checkpoint_commit_id(),
  lix_active_branch_commit_id()
)
WHERE to_path LIKE '/docs/%'
RETURNING commit_id;

INSERT INTO lix_create_checkpoint DEFAULT VALUES RETURNING commit_id;
```

Selecting a file includes the tracked rows composing that file. Partial file
checkpoints also include required ancestor directory descriptors. Selections
outside the supported dependency-closure paths fail with a descriptive error
instead of producing an invalid checkpoint. Selecting `lix_directory` rows
directly is unsupported; select the affected `lix_file` rows instead.

Each statement is atomic. An empty selection succeeds without creating a
commit, duplicate selected identities are rejected, and every command supports
`RETURNING commit_id`. A command that creates a commit returns exactly one row,
regardless of how many relation-row identities it consumes. For relation-row
selection commands, `rowsAffected` continues to report the number of selected
identities. Full checkpoints report one affected row, use `DEFAULT VALUES`, and
structurally reuse the branch state without copying application rows.

Rows written with `lixcol_untracked` are absent from every diff. Untracked
state belongs to the local repository replica and is not transported through
commit-based synchronization; use a separate service for state that needs
synchronization without version history.
