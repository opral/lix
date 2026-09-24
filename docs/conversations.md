---
description: "SQL conversations and comments on rows and commits, with same-scope references and cascades."
---

# Conversations and comments

`lix_conversation` and `lix_comment` are built-in writable SQL relations. They are available without schema registration in both branch-local and global scopes, including existing databases when opened by this engine version. Discovery through `information_schema` includes these built-ins even when an older database has no persisted `lix_registered_schema` rows for them.

| Relation | Payload columns |
| --- | --- |
| `lix_conversation` | `id UUID PRIMARY KEY`, `target TEXT NULL`, `detached_target TEXT NULL`, `title TEXT NULL`, `resolved BOOLEAN NOT NULL DEFAULT false` |
| `lix_comment` | `id UUID PRIMARY KEY`, `conversation_id UUID NOT NULL`, `body JSONB NOT NULL` |

`target` is a canonical `lix_row_ref` constrained to an existing row in the conversation's scope. It is nullable for standalone conversations. Deleting the target cascades its conversations; deleting a conversation cascades its comments. Scope and durability follow the ordinary FK and row-reference rules. Global rows remain visible through the normal branch read overlay, but visibility does not allow cross-scope references.

The [vendored Zettel JSON Schema](../packages/lix/vendor/zettel/schema.json) defines the document structure.

`body` stores a Zettel document, for example `{"_type":"zettel_doc","blocks":[]}`. The SQL column enforces JSONB and non-nullability, not the complete Zettel document grammar. Applications must validate imported or authored documents before writing. No editor, renderer, or Markdown converter is part of this SQL API.

`resolved` marks a conversation as resolved. It defaults to `false`, including for conversations written before the column existed: an existing repository opens without migration and reads them as unresolved.

There are no separate author, timestamp, or ordering columns, and no `resolved_by` or `resolved_at`. Use Lix row/change metadata for attribution and chronology. For live display, `ORDER BY lixcol_created_at, id` supplies a timestamp order with an ID tie-breaker; this is not a causal ordering guarantee for distributed writers.

## Comment on a row in the current branch

Use one transaction for a conversation and its opening comment. The following uses parameter binding: `$1` is the conversation UUID, `$2` the comment UUID, `$3` the file UUID, `$4` the paragraph row ID, and `$5` a Zettel JSON document. The Markdown plugin must have already materialized the target row.

```sql
BEGIN;

INSERT INTO lix_conversation (id, target)
VALUES ($1, lix_row_ref('markdown_node', $3, $4));

INSERT INTO lix_comment (id, conversation_id, body)
VALUES ($2, $1, $5::jsonb);

COMMIT;
```

For a CSV row use `lix_row_ref('csv_row', file_id, row_id)`. For a fileless application row pass `NULL` as the file argument. File-qualified row identity prevents equal row keys in different files from selecting the wrong target.

A standalone conversation omits the target:

```sql
INSERT INTO lix_conversation (id) VALUES (uuidv7()) RETURNING id;
```

## Discuss a commit globally

A repository-wide commit discussion stores both conversation and comments globally. `$1` and `$2` are fresh conversation/comment UUIDs; `$3` is an existing commit UUID; `$4` is the Zettel body.

```sql
BEGIN;

INSERT INTO lix_conversation (id, target, title, lixcol_global)
VALUES ($1, lix_row_ref('lix_commit', NULL, $3), 'Commit discussion', true);

INSERT INTO lix_comment (id, conversation_id, body, lixcol_global)
VALUES ($2, $1, $4::jsonb, true);

COMMIT;
```

`lixcol_global` selects storage scope on insert. SQL does not infer write scope from `target` or `conversation_id`, and it does not automatically route invalid local inserts globally. A branch-local paragraph discussion stays with that branch when it is forked or merged. A global commit discussion remains shared across branches.

## Reply using the selected conversation's scope

Copy scope from the conversation rather than guessing from its ID. `$1` is a fresh comment UUID, `$2` is its Zettel body, and `$3` is the conversation UUID.

```sql
INSERT INTO lix_comment (id, conversation_id, body, lixcol_global)
SELECT $1, id, $2::jsonb, lixcol_global
FROM lix_conversation
WHERE id = $3
RETURNING id, conversation_id, lixcol_global;
```

This example uses ordinary tracked, fileless conversations. Check the returned row: if no conversation is visible, no comment is inserted. The read overlay selects the visible conversation; an identically keyed local conversation can shadow a global one. To address the other identity, select the appropriate session scope rather than assuming UUID lookup bypasses the overlay. If using untracked conversations, explicitly copy `lixcol_untracked` as well; tracked comments must not depend on untracked-only targets.

Read a selected conversation's comments with its known scope to avoid mixing distinct same-ID conversations:

```sql
SELECT id, body, lixcol_created_at, lixcol_global
FROM lix_comment
WHERE conversation_id = $1 AND lixcol_global = $2
ORDER BY lixcol_created_at, id;
```

## Resolve and reopen

Resolving and reopening are ordinary updates:

```sql
UPDATE lix_conversation SET resolved = true WHERE id = $1;   -- resolve
UPDATE lix_conversation SET resolved = false WHERE id = $1;  -- reopen
```

To resolve with a closing note, post a comment in the same transaction. Both rows land in one commit:

```sql
BEGIN;

UPDATE lix_conversation SET resolved = true WHERE id = $1;

INSERT INTO lix_comment (id, conversation_id, body)
VALUES ($2, $1, $3::jsonb);

COMMIT;
```

Filter open threads with `WHERE resolved = false`. Detached conversations can be resolved and reopened like any other; detaching does not change `resolved`.

### Who resolved this and when

Every write records a `lix_change` with the writing session's `account_id` and `created_at`. A row's `lixcol_change_id` names the change that wrote its current state, so for a conversation whose latest write was the resolution:

```sql
SELECT c.resolved, ch.account_id, a.name, ch.created_at
FROM lix_conversation c
JOIN lix_change ch ON ch.id = c.lixcol_change_id
LEFT JOIN lix_account a ON a.id = ch.account_id
WHERE c.id = $1;
```

A later write, such as a title edit, becomes the current change. To find the most recent resolution regardless of later edits or a reopen, read the transition from history:

```sql
SELECT ch.account_id, a.name, ch.created_at
FROM lix_history('lix_conversation') h
JOIN lix_change ch ON ch.id = h.to_lixcol_change_id
LEFT JOIN lix_account a ON a.id = ch.account_id
WHERE h.id = $1
  AND h.to_resolved
  AND NOT COALESCE(h.from_resolved, false)
ORDER BY h.lixcol_position
LIMIT 1;
```

History compares each retained commit with its first parent. After a checkpoint compacts automatic commits, the resolution and a later edit inside the same compacted range appear as one net change, attributed to the range's final change. `lix_diff('lix_conversation', $from, $to)` reports `from_resolved` / `to_resolved` between two checkpoints.

## Deletion and history

Deleting a local target removes its conversations and comments in that branch. Standalone conversations are unaffected. Cascades also apply when a merge brings in target deletion and when plugin changes remove referenced file rows. Deleting a conversation directly removes its comments.

```sql
DELETE FROM lix_conversation WHERE id = $1 AND lixcol_global = $2;
```

Deleting live rows does not erase tracked history. Use `lix_history('lix_conversation')`, `lix_history('lix_comment')`, or `lix_as_of` at a prior commit. Restoring a target alone does not automatically restore deleted discussions. See [History](./history.md) and [Diff commands](./diff-commands.md).

## Bulk-operation limits

Schema-table `INSERT … SELECT` runs against the current transaction snapshot and streams selected rows through the normal schema write path. It is suitable for scope-copying replies. Whole-table deletion checks live reference-source tables before using the collection-generation optimization. Those overlay checks can scan source rows to reconcile pending changes; their one-row result limit is not an I/O bound.
