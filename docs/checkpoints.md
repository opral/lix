---
description: Create restore points and query relation-specific changes between checkpoints and branch heads.
---

# Checkpoints

Lix automatically commits tracked changes. A checkpoint marks one of those
states as a user-meaningful restore point. Compare its commit with the active
branch head to inspect subsequent changes at the relation level your interface
uses.

```ts
const checkpoint = await lix.createCheckpoint();
console.log("created checkpoint", checkpoint.commitId);
```

`createCheckpoint()` checkpoints the active branch and returns the new checkpoint
commit ID. The equivalent full SQL checkpoint is a metadata-only operation:

```sql
INSERT INTO lix_create_checkpoint DEFAULT VALUES RETURNING commit_id;
```

## Complete example

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
const initial = await lix.createCheckpoint();

await lix.execute("INSERT INTO lix_key_value (key, value) VALUES ($1, $2)", [
  "checkpoint-demo",
  "draft",
]);

const working = await lix.execute(
  `SELECT row_pk, diff_type, from_value, to_value
   FROM lix_diff('lix_key_value', $1, lix_active_branch_commit_id())`,
  [initial.commitId],
);

for (const row of working.rows) {
  console.log(row.get("diff_type"), row.get("row_pk"));
}

const checkpoint = await lix.createCheckpoint();
const remaining = await lix.execute(
  `SELECT count(*) AS count
   FROM lix_diff('lix_key_value', $1, lix_active_branch_commit_id())`,
  [checkpoint.commitId],
);
console.assert(remaining.rows[0].get("count") === 0);

await lix.close();
```

A runnable Rust version lives at
[`checkpoints.rs`](https://github.com/opral/lix/blob/main/packages/lix/examples/checkpoints.rs).

## SQL surfaces

| Surface | Scope | Columns |
| :-- | :-- | :-- |
| `lix_diff(relation, from_commit_id, to_commit_id)` | One relation across two explicit commits | `row_pk`, `diff_type`, `row_count`, and paired `from_<column>` / `to_<column>` relation columns |
| `lix_checkpoint` | Repository-global checkpoint markers | `id`, `commit_id`, and standard `lixcol_*` columns |
| `lix_commit` | Repository-global commit graph | `id`, `parent_commit_ids`, and standard `lixcol_*` columns |
| `lix_history('lix_checkpoint'[, commit_id])` | Global checkpoint-row authorship history | Checkpoint columns and standard history `lixcol_*` columns |
| `lix_commit_ancestry([commit_id])` | Active head or an explicit anchor | `commit_id`, `depth` |
| `lix_root_commit_id()` | Repository root | Scalar ID of the repository bootstrap root |

`parent_commit_ids` is an ordered JSONB array: element zero is the mainline
parent, and later elements are merge parents:

```sql
SELECT parent_commit_ids ->> 0 AS parent_commit_id
FROM lix_commit
WHERE id = $checkpoint_commit_id;
```

The newest checkpoint remains a normal query:

```sql
SELECT commit_id
FROM lix_checkpoint
ORDER BY lixcol_created_at DESC
LIMIT 1;
```

`lix_checkpoint` retains checkpoint markers even when a branch restore abandons
their commits. Join it with `lix_commit_ancestry()` when you need only checkpoints
reachable from the active branch head:

```sql
SELECT checkpoint.id, checkpoint.commit_id, ancestry.depth
FROM lix_checkpoint AS checkpoint
JOIN lix_commit_ancestry() AS ancestry
  ON ancestry.commit_id = checkpoint.commit_id
ORDER BY ancestry.depth, checkpoint.commit_id;
```

The anchor has `depth = 0`; direct parents have depth `1`. A commit reachable
through several merge paths appears once at its shortest depth.

Create a checkpoint for every tracked change through `lix.createCheckpoint()`
or `INSERT INTO lix_create_checkpoint DEFAULT VALUES`. Select a subset through
the `(relation, row_pk)` command form described in
[Diff commands](./diff-commands.md).
