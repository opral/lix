---
description: Compare typed Lix relations across commits and select rows atomically for revert, apply, or checkpoint commands.
---

# Diff commands

`lix_diff(relation, from_commit_id, to_commit_id)` compares one relation across two explicitly selected commits. With only the relation argument it defaults to the active branch's working baseline → current head, pinned for the statement. A write's own span is on its result: `execute` returns `commit: { before, after }`, and `lix_diff('lix_file', before, after)` is exactly what that write changed. A file is one row of `lix_file`; a registered schema row is one row of its schema relation. Commands consume the diff's `row_ref` identity, so selecting a file also selects its underlying tracked content.

## Review changed files

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
const changedFiles = await lix.execute(
  `SELECT row_ref, id, diff_type, from_path, to_path
   FROM lix_diff('lix_file')
   ORDER BY coalesce(to_path, from_path)`,
);

for (const row of changedFiles.rows) {
  console.log(row.diff_type, row.to_path ?? row.from_path);
}

const reverted = await lix.execute(
  `SELECT commit_id
   FROM lix_restore(
     (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()),
     ARRAY(
       SELECT row_ref
       FROM lix_diff('lix_file')
       WHERE id = $1
     )
   )`,
  [changedFiles.rows[0].id],
);

console.log("restored in commit", reverted.rows[0].commit_id);

await lix.close();
```

The working baseline is `lix_branch.working_base_commit_id`. It can be an ordinary commit after a fork, so use the one-argument diff for working changes.

## Diff rows

Every relation diff exposes `row_ref`, the relation's typed primary-key columns, `diff_type`, and a `from_<column>` / `to_<column>` pair for each non-primary-key column of the compared relation. `diff_type` is `added`, `modified`, or `removed`. Added rows have SQL `NULL` `from_` values; removed rows have SQL `NULL` `to_` values. A present row can also have nullable columns, so use `diff_type` to identify an absent side. Use `coalesce(to_path, from_path)` when displaying a path that also covers removed or renamed files.

Project `from_content` and `to_content` to reconstruct historical file bytes. For each present side, these bytes equal `content` from `lix_as_of('lix_file', endpoint_commit_id)` for the same file. Absent sides are SQL `NULL`; an existing empty file is an empty `BYTEA`. A snapshot of an absent file returns no row. Read or reconstruction failures are errors, not absent rows or empty bytes. Content is only materialized when projected or used by a predicate. File metadata predicates, including `id`, `from_path`, and `to_path`, select rows before content materialization. A content predicate itself requires reading bytes. On a partial replica, projecting `from_content` or `to_content` may fetch data from the server. If the data cannot be fetched, the query errors; it never returns NULL for a file that exists.

```sql
SELECT id, diff_type, from_content, to_content
FROM lix_diff('lix_file', $1, $2)
WHERE id = $3;
```

Count the rows of the relation being displayed. For example, count file diff rows for changed files, or query a plugin relation and count its diff rows for changed entities.

```sql
SELECT count(*) AS changed_files FROM lix_diff('lix_file');
```

`lix_diff('lix_directory', ...)` supports changes to directory descriptors, including directory creation, removal, and renaming. Rolling changed files or their content up into an otherwise unchanged parent directory is unsupported; query `lix_diff('lix_file', ...)` for file-level changes and aggregate those rows by path when directory summaries are needed.

Bulk deletion of an entire schema collection is represented by an internal collection-generation marker, not individual row changes. Expanding that marker into per-row `lix_diff` results is unsupported; delete selected rows individually when row-level history or diff visibility is required.

`lix_root_commit_id()` returns the repository root; comparing it with another commit reports files present in that commit as added rows. Genesis comparisons of internal bootstrap schema rows are unsupported because those metadata rows already exist in the bootstrap root.

```sql
SELECT row_ref, id, to_path
FROM lix_diff('lix_file', lix_root_commit_id(), $1);
```

## Revert a selected historical change

To undo a historical span, use `lix_revert_range` with the original endpoint order:

```sql
SELECT commit_id FROM lix_revert_range(
  $1,
  $2,
  ARRAY(
    SELECT row_ref
    FROM lix_diff('acme_task', $1, $2)
  )
);
```

To undo one commit, use `lix_revert`, which resolves that commit's actual first parent:

```sql
SELECT commit_id FROM lix_revert(
  $1,
  ARRAY[lix_row_ref('acme_task', $2, $3)]
);
```

Use the `commit.before` and `commit.after` returned by the original write or transaction. Add a `WHERE` clause to undo only selected rows. The reversed diff removes rows the write added, restores rows it deleted, and restores the previous values of rows it modified. Undo creates a new commit; it does not erase history or move the branch back to the original commit.

Later changes to unrelated rows are preserved. Each affected row must still have the version expected by the reversed diff (or be absent when absence is expected). A later edit to an affected row rejects the entire statement with `LIX_CONSTRAINT_VIOLATION`, even if that edit changed a different column. No subset of the undo is committed. Related rows required for a valid apply can also be included by dependency planning; constraints and their version checks still apply.

`lix_restore` copies selected rows from a source commit into a new commit on the active branch. Pass `(SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id())` as the source when discarding current working edits. Use `lix_revert_range(before, after[, rows])` for a historical span, or `lix_revert(commit[, rows])` for a single commit. Use `lix_apply(before, after[, rows])` when replaying a forward difference. These commands are explicit content operations; they do not alter checkpoint designation or undo/redo receipt bookkeeping.

## Undo and redo

SQL undo/redo navigates the durable logical editor stack while keeping every
commit in immutable history. The one-argument undo target is the original
ordinary commit or checkpoint `C`; redo takes the undo receipt `U` returned by
that call:

```sql
SELECT commit_id FROM lix_undo($1);
SELECT commit_id FROM lix_redo($1); -- $1 is U, not C

SELECT commit_id
FROM lix_undo(
  $1,
  ARRAY(
    SELECT row_ref
    FROM lix_diff('acme_task', $2, $3)
    WHERE done = false
  )
);

SELECT commit_id
FROM lix_redo(
  $1,
  ARRAY[lix_row_ref('acme_task', $2, $3)]
);
```

Use `lix_undo()` and `lix_redo()` for the current stack cursor. Generated undo
and redo commits are skipped by that cursor. A later ordinary edit clears the
convenience redo path; retained ordinary receipts remain explicitly replayable
when their exact effects are still pending and current-row preconditions hold.
Undoing a restore or revert commit is ordinary stack navigation, while issuing
restore/revert/apply itself does not silently become checkpoint navigation.

`row_ref` selects row identities; the target commit or undo receipt determines
the exact historical effects. Omitted scope includes all content dependencies and
checkpoint metadata. `ARRAY[]` is a successful no-op and returns `commit_id =
NULL`; NULL scope is an error. Every non-empty call commits atomically. A
selected conflict rejects content, receipt consumption, checkpoint metadata,
baseline, and stack movement together.

For a checkpoint, partial undo keeps its baseline. Final causal undo retires
the checkpoint and moves the working baseline to its predecessor; complete redo
reactivates it. A checkpoint with no content rows still has a durable metadata
effect, so its undo and redo each return a non-NULL commit. A newer checkpoint
stales all explicit receipts for the older checkpoint, including filtered redo.
On a partial replica, the command requires authoritative complete effect
coverage, before-images, dependencies, and checkpoint metadata. Event arrival
alone does not move the working baseline.

## Commands

The recovery and apply functions are top-level mutating `SELECT` commands. Their outer statement must select the one `commit_id` receipt column:

```sql
SELECT commit_id
FROM lix_restore(
  (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()),
  ARRAY(
    SELECT row_ref
    FROM lix_diff('lix_file')
    WHERE coalesce(to_path, from_path) LIKE '/docs/%'
  )
);

SELECT commit_id
FROM lix_apply(
  $1,
  $2,
  ARRAY(
    SELECT row_ref
    FROM lix_diff('acme_task', $1, $2)
    WHERE to_done = true
  )
);

SELECT commit_id
FROM lix_create_checkpoint($1, $2, ARRAY(
  SELECT row_ref
  FROM lix_diff('lix_file')
  WHERE to_path LIKE '/docs/%'
));

SELECT commit_id FROM lix_create_checkpoint($1, $2);
```

Selecting a file includes the tracked rows composing that file. Partial file checkpoints also include required ancestor directory descriptors. Directory rows and mixed-relation selections use the same dependency planner. A scope that cannot be closed into a valid checkpoint fails before commit.

For checkpoint calls, `$1` is a nullable title and `$2` is a nullable Zettel JSONB comment. An empty checkpoint selection is rejected. Each statement is atomic. An empty recovery/apply selection succeeds without creating a content commit and returns one receipt row with `commit_id = NULL`; duplicate selected identities are rejected. A non-empty command returns one new commit ID. Full checkpoints retain their intentional empty milestone behavior. Undo/redo metadata-only transitions are the exception: they intentionally create a non-NULL commit even when no content row changes. The command result is a receipt, so callers should inspect its row rather than infer the commit from selected-row counts. Full checkpoints structurally reuse the branch state without copying application rows.

Rows written with `lixcol_untracked` are absent from every diff. They still have a `lixcol_change_id` for their current-state write, but no commit ID or retained `lix_change` history record. Untracked state belongs to the local repository replica and is not transported through commit-based synchronization; use a separate service for state that needs synchronization without version history.
