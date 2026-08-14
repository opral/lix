---
description: A file is not a byte stream. It has rows. Lix records each change as a SQL row that says which row changed and how.
---

# Semantic Changes

A file is not a byte stream. It has rows: a paragraph, a cell, a property, a record. A semantic change says which row changed and how. Lix stores it as a row you can query with SQL.

That means Lix can represent:

- `price: 10 -> 12`
- `cell B4: pending -> shipped`
- `property theme: light -> dark`
- `paragraph intro: updated`

instead of "line 4 changed" or "binary files differ".

## From file edit to change row

An agent edits `/orders.csv` and sets order 1002 to shipped:

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

A text diff sees the whole line and no meaning:

```diff
-1002,Widget B,pending
+1002,Widget B,shipped
```

Lix stores the change as a row in `lix_change`:

| schema_key | row_pk  | snapshot_content                                                   |
| ---------- | ---------- | ------------------------------------------------------------------ |
| `csv_row`  | `["1002"]` | `{"order_id": "1002", "product": "Widget B", "status": "shipped"}` |

That row is the semantic change: record 1002 changed, and its new state is `shipped`. An app renders it as a diff of the field:

```diff
order_id 1002 status:

- pending
+ shipped
```

## Where rows come from

Plugins define the rows of a file format. The Markdown plugin defines blocks. The CSV plugin defines records. Both ship with the JavaScript SDK. Other formats, such as JSON, XLSX, or DOCX, need a plugin.

<img src="../website/public/assets/file-to-rows.svg" alt="A plugin maps /orders.csv to SQL rows with row, field, and value columns" width="760" />

App data that does not come from a file uses the same model. Register a schema, and Lix tracks changes to its rows the same way.

## What change rows buy you

- **Review the thing that changed.** Show a record, field, or paragraph instead of a patch hunk.
- **Ask precise questions.** Query changes by schema, file, row, branch, or time.
- **Revert one field, not the whole file.**

## Query changes with SQL

> Which orders changed status in this branch?

```sql
SELECT
  f.path,
  c.row_pk ->> 0 AS row_id,
  c.snapshot_content AS change
FROM lix_change AS c
JOIN lix_file AS f
  ON f.id = c.file_id
WHERE c.schema_key = 'csv_row'
  AND f.path = '/orders.csv'
ORDER BY c.created_at DESC;
```

## Learn more

- [SQL Surfaces](./surfaces.md) and [Change History](./history.md): branch-scoped state and history queries.
- [Schemas](./schemas.md): define rows for app data.
