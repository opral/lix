---
description: Built-in scalar SQL functions and PostgreSQL JSONB syntax supported by Lix.
---

# SQL Functions

Lix exposes a small set of runtime functions. JSON uses PostgreSQL casts and
operators; there are no public `lix_json_*` functions.

| Function | Returns | Purpose |
| :-- | :-- | :-- |
| `lix_active_account_id()` | text | Active SQL-session account. |
| `lix_active_branch_id()` | text | Active branch. |
| `lix_active_branch_commit_id()` | text | Active branch head pinned for the statement. |
| `lix_restore(commit_id)` | text | Move the active branch head to an ancestor commit. |
| `uuidv7()` | uuid | Generate a UUIDv7 value. |
| `CURRENT_TIMESTAMP` | timestamptz | Transaction-start instant at microsecond precision. |

## JSONB

Cast JSON text or a bound JSON value with `::jsonb` and use PostgreSQL
operators:

```sql
SELECT
  value -> 'primary_key' AS primary_key,
  value ->> 'key' AS schema_key
FROM lix_registered_schema
WHERE value @> '{"deprecated":false}'::jsonb;
```

Supported syntax includes `->`, `->>`, `#>`, `#>>`, `@>`, `?`, equality, and
`'…'::jsonb`. Missing paths return SQL `NULL`; `->` preserves JSONB `null`,
while `->>` converts JSONB `null` to SQL `NULL`. Negative array indexes follow
PostgreSQL behavior.

The SDK accepts structured JSON parameters directly. If a parameter contains
JSON text, cast it explicitly:

```ts
await lix.execute(
  "INSERT INTO acme_event (id, payload) VALUES ($1, $2::jsonb)",
  [id, JSON.stringify(payload)],
);
```

## Branch and history

`lix_active_branch_commit_id()` resolves the same pinned head used by history
functions called without an explicit commit:

```sql
SELECT lixcol_depth, title
FROM lix_history('acme_task')
WHERE id = 't1'
ORDER BY lixcol_depth;
```

`lix_commit_ancestry()` returns the active head at depth `0` and every
reachable ancestor once at its shortest depth. Pass one commit ID to use an
explicit graph anchor:

```sql
SELECT commit_id, depth
FROM lix_commit_ancestry($1)
ORDER BY depth, commit_id;
```

`lix_restore` is a command-shaped mutation and must be the statement's only
projection:

```sql
SELECT lix_restore($1);
```

The commit must exist and be an ancestor of the active branch head. The
function returns the restored commit ID. It creates no commit, leaves other
branches untouched, preserves branch-local untracked rows, and starts a fresh
undo interval. A restore cannot be combined with another write in the same
transaction and must be the final statement before commit or rollback.
Orphaned commits may remain stored until ordinary
reachability-based garbage collection reclaims them. Checkpoint rows remain
stored even when their commits are no longer on the branch.

Use `execute` for remote callers as well; restore does not add a server-protocol
endpoint or a typed SDK method.

## IDs and time

```sql
INSERT INTO event (id, occurred_at)
VALUES (uuidv7(), CURRENT_TIMESTAMP);
```

Bound parameters may use `?` or `$1`, `$2`, and so on, but a statement cannot
mix the two styles.
