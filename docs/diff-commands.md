---
description: Select native Lix diffs with SQL and feed them atomically into revert, apply, and checkpoint commands.
---

# Diff commands

Diff commands act on a chosen subset of changes: revert selected changes, apply
changes from history, or checkpoint a subset of your work while the rest stays
in progress. You select diffs with a SQL query and pipe the resulting
`diff_id` rows into a command.

## Complete example

Revert every working change in one file:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();

// Inspect what would be reverted.
const diffs = await lix.execute(
  "SELECT diff_id, diff_type FROM lix_working_diff WHERE file_id = $1",
  ["file_1"],
);

for (const row of diffs.rows) {
  console.log(row.get("diff_type"), row.get("diff_id"));
}

// Revert them in one atomic statement.
const reverted = await lix.execute(
  `INSERT INTO lix_revert (diff_id)
   SELECT diff_id FROM lix_working_diff WHERE file_id = $1
   RETURNING commit_id`,
  ["file_1"],
);

if (reverted.rows.length > 0) {
  console.log("reverted in commit", reverted.rows[0].get("commit_id"));
}

await lix.close();
```

`result.rows` contains row objects. Read a column with `row.get("name")`.

## Diff sources

`lix_working_diff` compares the active branch head with its latest checkpoint.
`lix_working_diff_by_branch` adds `lixcol_branch_id` for cross-branch
inspection. Their columns are listed in the
[checkpoint SQL surfaces table](./checkpoints.md#sql-surfaces).

`lix_diff` compares any two commits. Both commit arguments are required:

```sql
SELECT diff_id, entity_pk, schema_key, file_id, diff_type,
       before_change_id, after_change_id
FROM lix_diff($1, $2);
```

## Commands

Three insert-only command sinks consume a one-column query of `diff_id` rows:

```sql
-- Revert selected changes from their after side to their before side.
INSERT INTO lix_revert (diff_id)
SELECT diff_id
FROM lix_working_diff
WHERE file_id = $1;

-- Apply selected changes from their before side to their after side.
INSERT INTO lix_apply (diff_id)
SELECT diff_id
FROM lix_diff($1, $2)
WHERE schema_key = 'acme_task';

-- Move only the selected working diffs behind a new checkpoint.
INSERT INTO lix_create_checkpoint (diff_id)
SELECT diff_id
FROM lix_working_diff
WHERE file_id <> $1;
```

The source is a normal SQL query, so filters, joins, ordering, and limits stay
inside the database. To submit a `diff_id` your application already holds, use
a parameterized `VALUES` relation:

```sql
INSERT INTO lix_revert (diff_id)
SELECT diff_id FROM (VALUES ($1)) AS selected(diff_id);
```

Direct `INSERT ... VALUES` is intentionally rejected. Every command must be
`INSERT ... SELECT`; the query may read a Lix relation, a `VALUES` relation, a
CTE, or another valid query.

## Semantics

Each statement is atomic: it either fully succeeds or changes nothing.

Revert and apply check that every selected entity is still on the side the
command starts from. If someone changed an entity after you selected its diff,
the whole statement fails and nothing changes — re-query and retry.

An empty selection succeeds, does nothing, and creates no commit. You do not
need a preflight query.

A partial checkpoint commits the selected state as a checkpoint and preserves
the unselected working state in a child commit. Both commits and the
branch-head move publish atomically. Checkpoint naming is not part of this API.

Every command supports `RETURNING commit_id`:

```sql
INSERT INTO lix_apply (diff_id)
SELECT diff_id
FROM lix_diff($1, $2)
WHERE file_id = $3
RETURNING commit_id;
```

For a non-empty selection, every returned row carries the same engine-created
commit ID, and both the affected-row count and the returned-row count equal
the number of consumed diffs. An empty selection returns zero rows.

## Note on `diff_id`

Treat `diff_id` as opaque: select it, pipe it into a command. It is a
deterministic, versioned encoding of the native before/after change pair (its
current textual form starts with `d1.`), but clients must not construct or
decode it. Use `before_change_id` and `after_change_id` for joins to
`lix_change` and for debugging.

A `diff_id` is only valid while the entity is still on the side the command
starts from, so it is a short-lived selection token rather than a durable
handle — do not persist one and expect it to resolve later. It also describes a
change, not an entity's history: an entity that was deleted in an earlier
checkpoint and later re-added encodes the same way as one that was never
present, even though `before_change_id` still reports the underlying row.

These command names describe state transformations. They are not a session
undo/redo stack.
