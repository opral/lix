---
description: Create restore points and query relation-specific changes between checkpoints and branch heads.
---

# Checkpoints

Every tracked write becomes a commit automatically. You never run a commit command. A checkpoint marks one of those commits as a restore point. Compare its commit with the active branch head to inspect subsequent changes at the relation level your interface uses.

```ts
const checkpoint = await lix.execute(
  "SELECT commit_id FROM lix_create_checkpoint()",
);
console.log("created checkpoint", checkpoint.rows[0].commit_id);
```

`lix_create_checkpoint()` checkpoints the active branch and returns the new checkpoint commit ID. A full checkpoint is a metadata-only operation:

```sql
SELECT commit_id FROM lix_create_checkpoint();
```

## Complete example

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
await lix.execute("SELECT commit_id FROM lix_create_checkpoint()");

await lix.execute("INSERT INTO lix_key_value (key, value) VALUES ($1, $2)", [
  "checkpoint-demo",
  "draft",
]);

const working = await lix.execute(
  `SELECT row_ref, key, diff_type, from_value, to_value
   FROM lix_diff('lix_key_value')`,
);

for (const row of working.rows) {
  console.log(row.diff_type, row.key, row.row_ref);
}

await lix.execute("SELECT commit_id FROM lix_create_checkpoint()");
const remaining = await lix.execute(
  `SELECT count(*) AS count
   FROM lix_diff('lix_key_value')`,
);
console.assert(remaining.rows[0].count === 0);

await lix.close();
```

A runnable Rust version lives at [`checkpoints.rs`](https://github.com/opral/lix/blob/main/packages/lix/examples/checkpoints.rs).

## Commit membership and queries

A checkpoint is a new immutable commit with `is_checkpoint = true`. Automatic commits have the flag set to false. Empty checkpoints are retained log entries. A selected checkpoint creates a marked commit and an ordinary child containing remaining working changes. The flag cannot be updated through SQL.

```sql
SELECT commit_id, parent_commit_id, created_at
FROM lix_log()
WHERE is_checkpoint
ORDER BY position
LIMIT 20;

SELECT id, created_at
FROM lix_commit
WHERE is_checkpoint
ORDER BY created_at DESC, id DESC;
```

The first query is branch-relative; the second includes repository-global, off-branch checkpoints. There is no separate `lix_checkpoint` relation or marker write. Commit creation time is the single public checkpoint timestamp.

Use `lix_diff('lix_file')` for working changes. Its baseline is exposed as `lix_branch.working_base_commit_id` and can be an ordinary commit after a fork. The latest marked commit is not necessarily the working baseline.

A full checkpoint creates a new metadata-only commit. It copies no rows. Storage is reclaimed in the background.

## Undo and redo a checkpoint

Use SQL undo/redo to navigate a checkpoint cycle while retaining immutable
history:

```sql
SELECT commit_id FROM lix_undo($checkpoint_id);
SELECT commit_id FROM lix_redo($undo_receipt_id);
```

Undoing selected effects keeps the checkpoint as the working baseline. The
final causal undo applies the recorded checkpoint interval from C to its
pre-checkpoint baseline B; a complete redo restores C. The endpoints are the
checkpoint cycle's durable baseline metadata. Locally authored checkpoints set
B as C's first parent; the recovered head used to compact the interval is a
separate alias. A metadata-only checkpoint still produces a non-NULL undo or
redo receipt. Explicit receipts for an old checkpoint become stale after a
newer checkpoint is created.

See [History](./history.md) for log, endpoint history, snapshots, and paged previews, and [Diff commands](./diff-commands.md) for scoped checkpoints.

See [Undo and redo](./undo-redo.md) for the full receipt contract and
checkpoint-cycle behavior.
