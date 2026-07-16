---
description: Built-in scalar SQL functions provided by the Lix engine. Covers JSON parsing and projection, ID and timestamp generation, text/blob coercion, and the active-version commit id helper used to scope history queries.
---

# SQL Functions

Lix's DataFusion-backed engine registers a small set of scalar functions for use inside `lix.execute()`. They cover the gaps between standard SQL and Lix's own conventions: parsing JSON parameters, producing IDs and timestamps, coercing between text and bytes, and resolving the active version's commit id for history queries.

## At a glance

| Function | Returns | Use for |
| :-- | :-- | :-- |
| `lix_active_version_commit_id()` | text | Scoping `_history` queries to the active version. |
| `lix_json(text)` | JSON | Parse a JSON string parameter into a JSON-typed value. |
| `lix_json_get(json, path...)` | JSON | Project a value out of a JSON column, preserving JSON type. |
| `lix_json_get_text(json, path...)` | text | Project a value out of a JSON column as plain text. |
| `lix_uuid_v7()` | text | Generate a UUIDv7 string. |
| `lix_timestamp()` | text | Current ISO-8601 timestamp string. |
| `lix_text_decode(blob[, encoding])` | text | Decode a `BLOB` to text (default `utf-8`). |
| `lix_text_encode(text[, encoding])` | blob | Encode text into a `BLOB` (default `utf-8`). |
| `lix_empty_blob()` | blob | Zero-byte `BLOB` literal. |

All functions are scalar; call them anywhere a SQL expression is allowed.

## Version & history

### `lix_active_version_commit_id()`

Returns the commit id at the tip of the **currently active** version, as resolved when the SQL statement was planned.

History surfaces (`lix_state_history`, `<schema>_history`, `lix_file_history`, `lix_directory_history`) require a literal or bound-parameter equality on `start_commit_id` (or `lixcol_start_commit_id`). A correlated subquery against `lix_version` is rejected by the planner. `lix_active_version_commit_id()` is the canonical way to scope history to the active version in a single statement:

```sql
-- Walk one entity's history from the active version's tip
SELECT depth, observed_commit_id, snapshot_content
FROM lix_state_history
WHERE schema_key = 'task'
  AND lix_json_get_text(entity_pk, 0) = 't1'
  AND start_commit_id = lix_active_version_commit_id()
ORDER BY depth;
```

For an arbitrary version, resolve the commit id with one query and pass it as a parameter:

```ts
const { rows } = await lix.execute(
  "SELECT commit_id FROM lix_version WHERE id = $1",
  [versionId],
);
const commitId = rows[0].value("commit_id").asText();

await lix.execute(
  `SELECT depth, snapshot_content
     FROM lix_state_history
    WHERE start_commit_id = $1
      AND schema_key = $2
      AND lix_json_get_text(entity_pk, 0) = $3
    ORDER BY depth`,
  [commitId, "task", "t1"],
);
```

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
SELECT lix_json_get(snapshot_content, 'tags') FROM lix_state WHERE schema_key = 'task';
-- returns ["urgent","draft"] as JSON
```

### `lix_json_get_text(json, path...)`

Same as `lix_json_get` but returns the value as plain text. Useful for filtering or display:

```sql
SELECT entity_pk
FROM lix_state
WHERE schema_key = 'task'
  AND lix_json_get_text(snapshot_content, 'priority') = 'high';
```

Both return `NULL` if the path is missing or the underlying value is `null`.

## IDs & time

### `lix_uuid_v7()`

Generates a fresh RFC 9562 UUIDv7 string. Useful in `INSERT` defaults and CEL `default` expressions in JSON Schema:

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

### `lix_text_decode(blob[, encoding])`

Decodes a `BLOB` to text. The optional second argument is the encoding name (`"utf-8"` is the default and currently the only supported encoding):

```sql
SELECT lix_text_decode(data) FROM lix_file WHERE path = '/notes/readme.md';
```

### `lix_text_encode(text[, encoding])`

Inverse of `lix_text_decode`. Encodes text into a `BLOB`:

```sql
INSERT INTO lix_file (path, data)
VALUES ('/notes/hello.txt', lix_text_encode('hello world'));
```

Standard SQL binary casts are also supported when an explicit cast is more convenient:

```sql
INSERT INTO lix_file (path, data)
VALUES ($1, CAST($2 AS BINARY));
```

### `lix_empty_blob()`

Returns a zero-length `BLOB`. Handy for creating an empty file:

```sql
INSERT INTO lix_file (path, data)
VALUES ('/empty.bin', lix_empty_blob());
```

## Notes

- Functions are pure scalars; they do not consume rows or take aggregates.
- Bound parameters can use `?` or `$1`, `$2`, …
- `lix_active_version_commit_id()`, `lix_uuid_v7()`, and `lix_timestamp()` reflect the engine's current view at planning/execution time and are stable across the rows of a single statement.
