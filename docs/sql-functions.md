---
description: Built-in scalar SQL functions provided by the Lix engine. Covers JSON parsing and projection, ID and timestamp generation, and helpers for the active branch.
---

# SQL Functions

Lix accepts a PostgreSQL-dialect SQL subset executed by its DataFusion-backed engine. It registers a small set of scalar functions for use inside `lix.execute()`. They cover the gaps between PostgreSQL grammar and Lix's own conventions: parsing JSON parameters, producing IDs and timestamps, and resolving the active branch and its commit id.

## At a glance

| Function | Returns | Use for |
| :-- | :-- | :-- |
| `lix_active_account_id()` | text | Reading the current SQL session's active account id. |
| `lix_active_branch_id()` | text | Reading the current SQL session's active branch id. |
| `lix_active_branch_commit_id()` | text | Reading the active branch head pinned for this SQL statement. |
| `lix_json(text)` | JSON | Parse a JSON string parameter into a JSON-typed value. |
| `lix_json_get(json, path...)` | JSON | Project a value out of a JSON column, preserving JSON type. |
| `lix_json_get_text(json, path...)` | text | Project a value out of a JSON column as plain text. |
| `lix_uuid_v7()` | text | Generate a UUIDv7 string. |
| `lix_timestamp()` | text | Current ISO-8601 timestamp string. |

All functions are scalar; call them anywhere a SQL expression is allowed.

## Branch & history

### `lix_active_account_id()`

Returns the active account id of the current SQL session. Each session has its own active account, so concurrent sessions on the same Lix can act as different accounts.

### `lix_active_branch_id()`

Returns the active branch id of the current SQL session. Branch-pinned clients therefore get their own branch id even when multiple sessions query the same Lix.

### `lix_active_branch_commit_id()`

Returns the commit id at the tip of the **currently active** branch, as resolved when the SQL statement was planned.

History table functions (`<schema>_history`, `lix_file_history`, and
`lix_directory_history`) use that same pinned active-branch head when called
without an argument:

```sql
-- Walk one entity's history from the active branch's tip
SELECT lixcol_depth, lixcol_observed_commit_id, title
FROM acme_task_history()
WHERE id = 't1'
ORDER BY lixcol_depth;
```

For time travel and querying other branches, see [History](./history.md).

## JSON

### `lix_json(text)`

Parses a JSON string into a JSON-typed value. Use this when binding a JSON parameter, since DataFusion otherwise treats the bound value as plain text:

```ts
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [JSON.stringify(schema)],
);
```

### `lix_json_get(json, path...)`

Returns the value at a JSON path, **preserving JSON type** (objects, arrays, numbers, booleans, strings stay as JSON). Variadic path: pass each segment as a separate argument.

```sql
SELECT lix_json_get(value, 'x-lix-primary-key')
FROM lix_registered_schema
WHERE lix_json_get_text(value, 'x-lix-key') = 'acme_task';
-- returns ["/id"] as JSON
```

### `lix_json_get_text(json, path...)`

Same as `lix_json_get` but returns the value as plain text. Useful for filtering or display:

```sql
SELECT lix_json_get_text(value, 'x-lix-key') AS schema_key
FROM lix_registered_schema
WHERE lix_json_get_text(value, 'type') = 'object';
```

Both return `NULL` if the path is missing or the underlying value is `null`.

## IDs & time

### `lix_uuid_v7()`

Generates a fresh RFC 9562 UUIDv7 string. Useful in `INSERT` defaults and CEL `x-lix-default` expressions in JSON Schema:

```sql
INSERT INTO task (id, title, done)
VALUES (lix_uuid_v7(), 'New task', false);
```

### `lix_timestamp()`

Returns the current time as an ISO-8601 string.

```sql
INSERT INTO event (id, occurred_at) VALUES (lix_uuid_v7(), lix_timestamp());
```

## Text & bytes

For the column types Lix accepts in `CAST` expressions (for example `TEXT` and `BYTEA`), see [the executable column contract](./surfaces.md#the-executable-column-contract).

## Notes

- Functions are pure scalars; they do not consume rows or take aggregates.
- Bound parameters use PostgreSQL-style `$1`, `$2`, … placeholders.
- `lix_active_branch_id()`, `lix_active_branch_commit_id()`, `lix_uuid_v7()`, and `lix_timestamp()` reflect the engine's current view at planning/execution time and are stable across the rows of a single statement.
