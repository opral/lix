---
description: Lix tracks semantic changes at the entity level, so apps and agents can review what changed in structured data instead of raw lines or bytes.
---

# Semantic Changes

Semantic changes are changes to the things your app understands: rows, cells, paragraphs, clauses, nodes, tasks, symbols, or any other entity described by a Lix schema.

Git sees text lines and file bytes. Lix can see structured entities.

That means Lix can represent:

- `price: 10 -> 12`
- `cell B4: pending -> shipped`
- `property theme: light -> dark`
- `paragraph intro: updated`

instead of only "line 4 changed" or "binary files differ".

## Why semantic changes matter

Semantic changes make version control usable inside applications and agent workflows.

- **Review the thing that changed.** Show a row, field, paragraph, or symbol instead of a patch hunk.
- **Ask precise questions.** Query changes by schema, file, entity, branch, or time.
- **Burn fewer tokens.** Agents can inspect structured change rows instead of rereading whole files.
- **Rollback with intent.** Revert an entity or field using the model your app already understands.

## How Lix stores them

Lix stores changes as rows in its version-control model. Each row carries the schema, entity pk, optional file id, and the entity snapshot after the change.

The global journal is `lix_change`:

```sql
SELECT created_at, schema_key, entity_pk, snapshot_content
FROM lix_change
ORDER BY created_at DESC
LIMIT 20;
```

For version-scoped reads, use the state and history surfaces documented in [SQL Surfaces](./surfaces.md) and [Change History](./history.md).

## JSON example

Given a JSON file:

**Before:**

```json
{"theme":"light","notifications":true,"language":"en"}
```

**After:**

```json
{"theme":"dark","notifications":true,"language":"en"}
```

Git sees:

```diff
-{"theme":"light","notifications":true,"language":"en"}
+{"theme":"dark","notifications":true,"language":"en"}
```

Lix can see:

```diff
property theme:
- light
+ dark
```

## Excel example

The same idea applies to binary formats. With an XLSX plugin, Lix can expose cell or row level changes.

> **v0.6 status:** entity-level change tracking and the physical storage layout are stable. The file plugin API for writing custom plugins (XLSX, DOCX, PDF, code) is being finalized. See the [roadmap](https://github.com/opral/lix#roadmap).

**Before:**

```text
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | pending |
```

**After:**

```text
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | shipped |
```

Git sees:

```diff
-Binary files differ
```

Lix can see:

```diff
order_id 1002 status:

- pending
+ shipped
```

## Query semantic changes

Because semantic changes are rows, agents can answer version-control questions with SQL.

For example:

> Which orders changed status in this branch?

```sql
SELECT
  f.path,
  lix_json_get_text(c.entity_pk, 0) AS row_id,
  c.snapshot_content AS change
FROM lix_change AS c
JOIN lix_file AS f
  ON f.id = c.file_id
WHERE c.schema_key = 'xlsx_row'
  AND f.path = '/orders.xlsx'
ORDER BY c.created_at DESC;
```

The result is scoped to the file and entity type the agent asked about. No spreadsheet reread required.
