---
description: Create restore points and query relation-specific changes between checkpoints and branch heads.
---

# Checkpoints

Every tracked write becomes a commit automatically. You never run a commit command. A checkpoint marks one of those commits as a restore point. Compare its commit with the active branch head to inspect subsequent changes at the relation level your interface uses.

```ts
const checkpoint = await lix.execute(
  "SELECT commit_id FROM lix_create_checkpoint($1, $2)",
  ["Validate imports", { _type: "zettel_doc", blocks: [
    { _type: "zettel_block", _key: "context", style: "normal", markDefs: [], children: [
      { _type: "zettel_span", _key: "summary", text: "Malformed rows are rejected before insertion; retry behavior is unchanged.", marks: [] }
    ] }
  ] }],
);
console.log("created checkpoint", checkpoint.rows[0].commit_id);
```

`lix_create_checkpoint(title, comment)` checkpoints the active branch and returns the new checkpoint commit ID. Both arguments accept `NULL`: `title` is one-line text when present, and `comment` is a Zettel JSONB document when present. Use the comment to describe what changed, why, and caveats; see the [Zettel schema](../packages/lix/vendor/zettel/schema.json). A non-null title or comment creates a canonical global conversation in the same transaction. A non-null comment creates its opening comment. If both are null, no conversation is created. A full checkpoint is a metadata-only branch commit:

```sql
SELECT commit_id FROM lix_create_checkpoint($1, $2);
-- Pass NULL for either value when it is not supplied.
```

## Complete example

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
await lix.execute("SELECT commit_id FROM lix_create_checkpoint($1, $2)", [
  "Start draft", { _type: "zettel_doc", blocks: [] }
]);

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

await lix.execute("SELECT commit_id FROM lix_create_checkpoint($1, $2)", [
  "Finish draft", { _type: "zettel_doc", blocks: [] }
]);
const remaining = await lix.execute(
  `SELECT count(*) AS count
   FROM lix_diff('lix_key_value')`,
);
console.assert(remaining.rows[0].count === 0);

await lix.close();
```

A runnable Rust version lives at [`checkpoints.rs`](https://github.com/opral/lix/blob/main/packages/lix/examples/checkpoints.rs).

## Commit membership and queries

A checkpoint is a new immutable commit with internal checkpoint identity. `lix_log().is_checkpoint` exposes whether it is active at the query anchor. Empty checkpoints are retained log entries. A selected checkpoint creates a marked commit and an ordinary child containing remaining working changes. Checkpoint status changes only through checkpoint creation and undo/redo.

```sql
SELECT commit_id, parent_commit_id, created_at, conversation_id
FROM lix_log()
WHERE is_checkpoint
ORDER BY position
LIMIT 20;
```

`lix_log().conversation_id` is `NULL` for ordinary commits and checkpoints created with two null values. Otherwise it identifies the checkpoint's canonical conversation. Join to read titles and comments without repeating log rows for other conversations that may target the same commit:

```sql
SELECT l.commit_id, l.created_at, c.title
FROM lix_log() AS l
JOIN lix_conversation AS c ON c.id = l.conversation_id
WHERE l.is_checkpoint
ORDER BY l.position;

SELECT m.id, m.body, m.lixcol_created_at
FROM lix_comment AS m
WHERE m.conversation_id = $1
ORDER BY m.lixcol_created_at, m.id;
```

The query follows the anchor’s first-parent history. There is no public checkpoint relation or checkpoint column on `lix_commit`. Commit creation time is the single public checkpoint timestamp.

Use `lix_diff('lix_file')` for working changes. Its baseline is exposed as `lix_branch.working_base_commit_id` and can be an ordinary commit after a fork. The latest marked commit is not necessarily the working baseline.

A full checkpoint creates a metadata-only branch commit. When it has a title or comment, it also creates a small global commit for the conversation and any opening comment. It copies no application rows. Storage is reclaimed in the background.

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

## SQL surface upgrade

Checkpoint status is exposed only by `lix_log().is_checkpoint`. The commit
inventory has no checkpoint column, and row history has no checkpoint flag.
Join log and history on `commit_id = lixcol_to_commit_id` at the same anchor.
The removed columns have no compatibility aliases.

This SQL change retains the internal commit encoding and checkpoint inventory.
Current-format repositories keep their commit IDs, content, working baselines,
and undo receipts. Older supported repository formats continue through Lix's
registered migration chain; changing the public catalog does not rewrite
historical commits or their historical schema registrations.
