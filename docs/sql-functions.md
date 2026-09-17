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
| `lix_row_ref(relation, primary_key...)` | row_ref     | Opaque address of one relation row, including composite keys.                     |
| `lix_order_between(previous, next)`     | text        | Allocate a plugin row order key between exclusive bounds; NULL means an open end. |
| `uuidv7()`                              | uuid        | Generate a UUIDv7 value.                                                          |
| `CURRENT_TIMESTAMP`                     | timestamptz | Transaction-start instant at microsecond precision.                               |

`lix_row_ref` takes the relation's typed primary-key values in declared order:

```sql
SELECT lix_row_ref('json_object_member', $1, $2, $3) AS row_ref;
```

For `json_object_member`, the components are `parent_id`, decoded `key`, and `occurrence` (zero for an ordinary unique key).

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
FROM lix_history('acme_task')
WHERE id = 't1' AND lixcol_commit_is_checkpoint
ORDER BY lixcol_position;

SELECT row_ref, id, diff_type
FROM lix_diff('lix_file');
```

The one-argument diff uses the branch working baseline. Read `working_base_commit_id` alongside `commit_id` from `lix_branch` when the comparison context is needed even for an empty diff. See [History](./history.md) for endpoint columns, global checkpoint metrics, and paged previews.

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

`lix_restore` is an insert-only command sink:

```sql
INSERT INTO lix_restore (commit_id)
VALUES ($1)
RETURNING commit_id;
```

The commit must exist and be an ancestor of the active branch head. The command returns the restored commit ID. It creates no commit, leaves other branches untouched, preserves branch-local untracked rows, and starts a fresh undo interval. A restore cannot be combined with another write in the same transaction and must be the final statement before commit or rollback. Orphaned commits may remain stored until ordinary reachability-based garbage collection reclaims them. Checkpoint commits remain stored even when they are no longer on the branch.

Use `execute` for remote callers as well; restore does not add a server-protocol endpoint or a typed SDK method.

## IDs and time

```sql
INSERT INTO event (id, occurred_at)
VALUES (uuidv7(), CURRENT_TIMESTAMP);
```

Bound parameters may use `?` or `$1`, `$2`, and so on, but a statement cannot mix the two styles.
