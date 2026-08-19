---
description: Create restore points and query changes since the latest checkpoint.
---

# Checkpoints

Lix automatically commits tracked changes. A checkpoint marks one of those
states as a user-meaningful restore point. The changes after the newest
checkpoint are the branch's working diffs.

Create a checkpoint:

```ts
const checkpoint = await lix.createCheckpoint();
console.log("created checkpoint", checkpoint.commitId);
```

`createCheckpoint()` checkpoints every working diff on the active branch and
returns the new checkpoint commit ID.

## Complete example

This example writes a tracked row and inspects its working diff. It then
creates a checkpoint and verifies that no working diffs remain:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();

await lix.execute("INSERT INTO lix_key_value (key, value) VALUES ($1, $2)", [
  "checkpoint-demo",
  "draft",
]);

const working = await lix.execute(
  "SELECT row_pk, schema_key, diff_type FROM lix_working_diff()",
);

for (const row of working.rows) {
  console.log(row.get("diff_type"), row.get("schema_key"), row.get("row_pk"));
}

const checkpoint = await lix.createCheckpoint();

const checkpoints = await lix.execute(
  "SELECT id, commit_id FROM lix_checkpoint WHERE commit_id = $1",
  [checkpoint.commitId],
);

console.assert(checkpoints.rows.length === 1);

const remaining = await lix.execute(
  "SELECT COUNT(*) AS count FROM lix_working_diff()",
);
console.assert(remaining.rows[0].get("count") === 0);

await lix.close();
```

`result.rows` contains row objects. Read a column with `row.get("name")`.

A runnable Rust version lives at
[`checkpoints.rs`](https://github.com/opral/lix/blob/main/packages/lix/examples/checkpoints.rs).

## SQL surfaces

Checkpointing has three read-only SQL surfaces:

| Surface | Scope | Columns |
| :-- | :-- | :-- |
| `lix_working_diff()` | Active branch | `diff_id`, `row_pk`, `schema_key`, `file_id`, `diff_type`, `before_change_id`, `after_change_id` |
| `lix_checkpoint` | Repository-global | `id`, `commit_id`, plus the standard `lixcol_*` row columns |
| `lix_history('lix_checkpoint')` | Revisions reachable from a commit | `id`, `commit_id`, plus the standard history `lixcol_*` columns including `lixcol_depth` |

Working-diff relations are scoped to the current session. Open another session
on another branch to inspect that branch without switching the primary session.

`diff_type` is `added`, `modified`, or `removed`. Working diffs compare the
current branch head with that branch's newest checkpoint. Creating a checkpoint
makes the current head the new baseline, so `lix_working_diff()` is empty until
another tracked change is committed.

`lix_working_diff()` reports one row per underlying schema change, the same
granularity as `lix_change`. Its heterogeneous envelope exposes source-schema
identity and change IDs; applications can join the affected `file_id` or row
identity to typed current-state relations when presenting a composed review.

`lix_checkpoint` holds the checkpoint rows themselves. It carries no ordering
column. Use `lix_history('lix_checkpoint')` when you need order, because it exposes
`lixcol_depth`.

Depth is commit distance from the anchor commit. A checkpoint has
`lixcol_depth = 0` only while it is the head; three later auto-commits put that
checkpoint at depth `3`. Ascending depth is therefore newest-first, but depths
are not checkpoint ordinals and may have gaps. SQL row order is not implicit, so
request it explicitly:

```sql
SELECT id, commit_id, lixcol_depth
FROM lix_history('lix_checkpoint')
ORDER BY lixcol_depth;
```

All three surfaces are read-only. Create a checkpoint for every working diff
through `lix.createCheckpoint()`, or checkpoint a SQL-selected subset
through `lix_create_checkpoint`. See [Diff commands](./diff-commands.md).
