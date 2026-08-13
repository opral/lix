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
| `uuidv7()` | uuid | Generate a UUIDv7 value using PostgreSQL 18 syntax. |
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
FROM acme_task_history()
WHERE id = 't1'
ORDER BY lixcol_depth;
```

## IDs and time

```sql
INSERT INTO event (id, occurred_at)
VALUES (uuidv7(), CURRENT_TIMESTAMP);
```

Bound parameters may use `?` or `$1`, `$2`, and so on, but a statement cannot
mix the two styles.
