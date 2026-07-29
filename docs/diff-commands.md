---
description: Select native Lix diffs with SQL and feed them atomically into revert, apply, and checkpoint commands.
---

# Diff commands

Lix represents a diff as rows. `lix_working_diff` compares the active branch
head with its latest checkpoint. `lix_diff` compares any two commits:

```sql
SELECT diff_id, entity_pk, schema_key, file_id, change_kind,
       before_change_id, after_change_id
FROM lix_diff($1, $2);
```

Both commit arguments are required.
`lix_working_diff_by_branch` adds `lixcol_branch_id` for cross-branch
inspection.

`diff_id` is a deterministic, versioned, opaque encoding of the native
before/after change pair. Its current textual form starts with `d1.`, but
clients must not construct or decode it. The visible `before_change_id` and
`after_change_id` columns remain available for joins to `lix_change` and for
debugging.

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

-- Move only the selected working changes behind a new checkpoint.
INSERT INTO lix_create_checkpoint (diff_id)
SELECT diff_id
FROM lix_working_diff
WHERE file_id <> $1;
```

The source is a normal SQL query, so filters, joins, ordering, and limits stay
inside the database. `INSERT ... VALUES` is intentionally rejected: a client
must select diff rows rather than copy opaque IDs out and send them back.
Selecting zero rows is an error.

Each statement is atomic. Revert and apply use strict compare-and-set
semantics: every selected entity must still be on the side from which the
command starts. If any entity has moved, the entire statement fails without
changing the branch.

A partial checkpoint commits the selected state as a checkpoint and preserves
the unselected working state in a child commit. Both commits and the branch-head
move publish atomically. Checkpoint naming is not part of this API.

These command names describe state transformations. They are not a session
undo/redo stack.
