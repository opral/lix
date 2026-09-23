---
description: Built-in scalar SQL functions and PostgreSQL JSONB syntax supported by Lix.
---

# SQL Functions

Lix exposes a small set of runtime functions. JSON uses PostgreSQL casts and operators; there are no public `lix_json_*` functions.

| Function                                | Returns     | Purpose                                                                           |
| :-------------------------------------- | :---------- | :-------------------------------------------------------------------------------- |
| `lix_active_account_id()`               | text        | Active SQL-session account.                                                       |
| `lix_active_branch_id()`                | text        | Active branch.                                                                    |
| `lix_active_branch_commit_id()`         | text        | Active branch head pinned for the statement.                                      |
| `lix_root_commit_id()`                  | text        | Repository bootstrap root.                                                        |
| `lix_row_ref(relation, file_id, primary_key...)` | row_ref     | Opaque address of one relation row, including file scope and composite keys.       |
| `lix_order_between(previous, next)`     | text        | Allocate a plugin row order key between exclusive bounds; NULL means an open end. |
| `uuidv7()`                              | uuid        | Generate a UUIDv7 value.                                                          |
| `CURRENT_TIMESTAMP`                     | timestamptz | Transaction-start instant at microsecond precision.                               |

`lix_row_ref` always takes the relation name, its file scope, and the typed
primary-key values in declared order. Pass SQL `NULL` for fileless rows,
including `lix_file` and `lix_directory`; pass the owning file ID for a
file-scoped plugin row:

```sql
SELECT lix_row_ref('json_object_member', $1, $2, $3, $4) AS row_ref;
SELECT lix_row_ref('lix_file', NULL, $1) AS row_ref;
```

For `json_object_member`, the components are `parent_id`, decoded `key`, and `occurrence` (zero for an ordinary unique key).

`NULL` identifies only fileless rows; it never means all files. The reference
does not contain a branch: operations resolve it in their current branch or
candidate state. Construction validates the relation and key types without
requiring the target row to exist. References are opaque; store and pass them
unchanged. The v2 encoding rejects legacy v1 references.

ROW_REF values support identity equality with other ROW_REF values. Cast a
reference to `TEXT` explicitly to compare or order its encoded representation.

To enforce a stored reference, declare a `text` column and a schema-level
`row_refs` constraint:

```json
"row_refs": [{"column": "target", "on_delete": "cascade"}]
```

The referenced row must exist in the current branch, including pending writes.
The target may belong to another file or relation. SQL NULL is allowed when the
column is nullable. Omitting `on_delete` uses `no_action`, which rejects an
invalid final relationship. `cascade` deletes referencing rows as part of the
modifying statement, so subsequent statements see the deletion and rollback
restores both rows. Merge preview and execution apply the same actions to the
candidate state. Merely constructing a reference does not enable enforcement.

Construct references directly in writes as well as queries:

```sql
INSERT INTO acme_link (id, target)
VALUES ($1, lix_row_ref('acme_task', $2, $3));
```

## Row ordering

Use `lix_order_between($1, NULL)` to append after the last key, or pass both neighbors to insert between them. Two NULL bounds allocate the first key. Read rows with `ORDER BY order_key, id`: concurrent allocations may tie, and UUID identity supplies deterministic tie ordering. See [Plugin ordering](./plugin-ordering.md) for batch allocation, validation, and concurrency behavior.

## JSONB

Cast JSON text or a bound JSON value with `::jsonb` and use PostgreSQL operators:

```sql
SELECT
  value -> 'primary_key' AS primary_key,
  value ->> 'key' AS schema_key
FROM lix_registered_schema
WHERE value @> '{"deprecated":false}'::jsonb;
```

Supported syntax includes `->`, `->>`, `#>`, `#>>`, `@>`, `?`, equality, and `'…'::jsonb`. Missing paths return SQL `NULL`; `->` preserves JSONB `null`, while `->>` converts JSONB `null` to SQL `NULL`. Negative array indexes follow PostgreSQL behavior.

JSONB identity is distinct from plain text. Equality, `IN`, `ANY`, `ALL`, and
set operations compare canonical JSONB values; cast text with `::jsonb` before
comparing it. `CONCAT` and `CONCAT_WS` consume Lix's compact canonical JSONB
text and return `TEXT`. Lix does not currently implement PostgreSQL's JSONB
ordering, so range comparisons, `BETWEEN`, `ORDER BY`, `MIN`, and `MAX` reject
JSONB values. Cast to `TEXT` explicitly when DataFusion's lexical text behavior
is intended.

The SDK accepts structured JSON parameters directly. If a parameter contains JSON text, cast it explicitly:

```ts
await lix.execute(
  "INSERT INTO acme_event (id, payload) VALUES ($1, $2::jsonb)",
  [id, JSON.stringify(payload)],
);
```

## Branch and history

`lix_log([anchor])` lists retained first-parent commits. `lix_history(relation [, anchor])` describes each commit's changes against its actual first parent. Both default to the active head pinned for the statement.

```sql
SELECT lixcol_position, diff_type, from_title, to_title
FROM lix_log() l
JOIN lix_history('acme_task') h ON h.lixcol_to_commit_id = l.commit_id
WHERE h.id = 't1' AND l.is_checkpoint
ORDER BY lixcol_position;

SELECT row_ref, id, diff_type
FROM lix_diff('lix_file');
```

The one-argument diff uses the branch working baseline. Read `working_base_commit_id` alongside `commit_id` from `lix_branch` when the comparison context is needed even for an empty diff. See [History](./history.md) for endpoint columns, anchored checkpoint queries, and paged previews.

`lix_as_of(relation, commit_id)` returns the complete tracked state of a relation at one commit. Its columns are identical to the live relation, and entities that did not exist at that commit produce no row:

```sql
SELECT id, path, content
FROM lix_as_of('lix_file', $1)
WHERE id IN ($2, $3);
```

The relation argument must be a text literal. The commit may be a text parameter or `lix_root_commit_id()` / `lix_active_branch_commit_id()`. Primary key `=` and `IN` predicates are pushed into the point-in-time read, so batched entity lookups do not scan unrelated tracked rows. Untracked rows are never included.

A commit on a branch records the `global` commit it depends on in `lix_commit.base_commit_id`. `lix_as_of` returns the branch rows at that commit plus the repository-wide rows from that `global` commit; a branch value wins over a global value. Commits on `global` have a null base. The base is a state dependency, not an ancestor, so it does not appear in `parent_commit_ids` or `lix_commit_ancestry()`.

`lix_commit_ancestry()` returns the active head at depth `0` and every reachable ancestor once at its shortest depth. Pass one commit ID to use an explicit graph anchor:

```sql
SELECT commit_id, depth
FROM lix_commit_ancestry($1)
ORDER BY depth, commit_id;
```

`(SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id())` returns the active branch's actual working-diff baseline. It may be an ordinary commit after a fork and is not necessarily the newest marked checkpoint.

Recovery, undo/redo, and apply are top-level mutating `SELECT` functions. The exact outer shape is `SELECT commit_id FROM ...`; each command returns one receipt row, with `commit_id = NULL` for an empty or unchanged selection:

```sql
SELECT commit_id FROM lix_restore($1);

SELECT commit_id
FROM lix_restore(
  (SELECT working_base_commit_id FROM lix_branch WHERE id = lix_active_branch_id()),
  ARRAY(
    SELECT row_ref
    FROM lix_diff('lix_file')
    WHERE id = $1
  )
);

SELECT commit_id
FROM lix_apply(
  $1,
  $2,
  ARRAY(
    SELECT row_ref
    FROM lix_diff('lix_file', $1, $2)
    WHERE id = $3
  )
);

SELECT commit_id FROM lix_revert($1);

SELECT commit_id
FROM lix_revert_range(
  $1,
  $2,
  ARRAY[lix_row_ref('lix_file', NULL, $3)]
);

SELECT commit_id FROM lix_undo();
SELECT commit_id FROM lix_undo($1); -- original ordinary commit or checkpoint C
SELECT commit_id
FROM lix_undo(
  $1,
  ARRAY(
    SELECT row_ref FROM lix_diff('acme_task', $2, $3) WHERE selected
  )
);

SELECT commit_id FROM lix_redo();
SELECT commit_id FROM lix_redo($1); -- undo receipt U returned by lix_undo
SELECT commit_id
FROM lix_redo($1, ARRAY[lix_row_ref('acme_task', $2, $3)]);
```

Restore makes selected tracked content equal the source commit in a new commit on the current branch. Omitted scope restores the whole tracked repository, including deleting rows absent from the source. Selected restore leaves unrelated content alone and handles required dependencies atomically. It leaves the current working baseline unchanged, preserves branch-local untracked rows, and does not move the branch pointer backward. Revert reverses one commit, including a checkpoint commit, against its actual first parent; `lix_revert_range(before, after [, rows])` reverses the net endpoint difference. Apply replays the forward difference between explicit `before` and `after` endpoints. Later conflicting versions reject the command atomically. Mutating functions execute once as top-level commands and cannot be used as join inputs.

Undo and redo are SQL-only navigation commands. `lix_undo(C [, rows])` targets an eligible ordinary forward commit or checkpoint cycle and returns a new undo receipt commit `U`. `lix_redo(U [, rows])` takes that undo receipt, never the original `C` and never a redo receipt. Receipt effect identities are consumed exactly once, so partial redo leaves the unconsumed effects on the same receipt. An exhausted receipt returns `NULL`; a wrong-role or unknown ID is an error.

See [Undo and redo](./undo-redo.md) for the complete signatures, operation
selection guide, checkpoint-cycle rules, and replication contract.

No-argument undo and redo follow the durable logical editor stack and skip generated undo/redo commits. A later ordinary edit clears the convenience redo cursor but does not erase immutable receipts or history. A restore or revert commit is an ordinary undoable action and does not update undo/redo bookkeeping merely because it reverses content.

For checkpoints, partial undo keeps the checkpoint as the working baseline. The final causal undo retires the checkpoint and changes the baseline to its predecessor; complete redo reactivates it. The transition includes one durable metadata effect even when the checkpoint contains no content rows, so metadata-only undo and redo return non-NULL commits. `ARRAY[]` is an explicit empty selection and returns `NULL`; an omitted scope includes checkpoint metadata. A newer checkpoint makes every explicit receipt for the older checkpoint stale, including filtered redo, without changing state.

Partial replicas may execute these commands only with authoritative complete effect coverage, before-images, dependency closure, and checkpoint metadata. Receiving an event or a row subset alone does not advance the working baseline. Incorporation publishes content, metadata, baseline, and logical stack together; failed hydration or a selected conflict leaves all of them unchanged. Undo/redo do not add a server-protocol endpoint or a typed SDK method; remote callers use `execute` with these SQL statements.

## IDs and time

```sql
INSERT INTO event (id, occurred_at)
VALUES (uuidv7(), CURRENT_TIMESTAMP);
```

Use numbered bound parameters: `$1`, `$2`, and so on.
