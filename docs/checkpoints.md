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

Checkpointing and commit reachability use these read-only SQL surfaces:

| Surface | Scope | Columns |
| :-- | :-- | :-- |
| `lix_working_diff()` | Active branch | `diff_id`, `row_pk`, `schema_key`, `file_id`, `diff_type`, `before_change_id`, `after_change_id` |
| `lix_checkpoint` | Repository-global | `id`, `commit_id`, plus the standard `lixcol_*` row columns |
| `lix_history('lix_checkpoint'[, commit_id])` | Global row-authorship history | `id`, `commit_id`, plus the standard history `lixcol_*` columns |
| `lix_commit_ancestry()` | Active branch head | `commit_id`, `depth` |
| `lix_commit_ancestry(commit_id)` | Explicit commit | `commit_id`, `depth` |

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

`lix_checkpoint` is a normal immutable global schema. Its current table retains
checkpoint markers even when a branch restore abandons their commits. Join the
table with `lix_commit_ancestry()` when you need checkpoints reachable from the
active branch head:

`lix_history('lix_checkpoint'[, commit_id])` remains the normal history of
those global rows. It follows where checkpoint-row changes were authored; it
does not interpret a row's `commit_id` as membership in another commit graph.

```sql
SELECT checkpoint.id, checkpoint.commit_id, ancestry.depth
FROM lix_checkpoint AS checkpoint
JOIN lix_commit_ancestry() AS ancestry
  ON ancestry.commit_id = checkpoint.commit_id
ORDER BY ancestry.depth, checkpoint.commit_id;
```

The anchor itself has `depth = 0`; parents have depth `1`. A commit reachable
through multiple merge paths appears once at its shortest depth. SQL row order
is not implicit, so request it explicitly.

These surfaces are read-only. Create a checkpoint for every working diff
through `lix.createCheckpoint()`, or checkpoint a SQL-selected subset
through `lix_create_checkpoint`. See [Diff commands](./diff-commands.md).
