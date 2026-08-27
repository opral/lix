---
description: Lix diffs at the row level. Plugins parse files into rows, so a diff names the record, cell, or paragraph that changed.
---

# Diffs

Lix diffs rows, not lines.

Plugins parse a file into rows: a CSV record, a spreadsheet cell, a Markdown
block. Lix stores every change as a row in `lix_change`. A diff is therefore a
set of changed rows. It names the row that changed and gives its new value.

That is the whole idea.

## Example

An agent edits `/orders.csv` and sets order 1002 to shipped.

Before:

```text
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | pending |
```

After:

```text
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | shipped |
```

A text diff sees a line:

```diff
-1002,Widget B,pending
+1002,Widget B,shipped
```

Lix sees a row:

| row_ref | schema_key | snapshot_content |
| --- | --- | --- |
| `lix_row_ref:v1:…` | `csv_row` | `{"id": "0192f3a1-…-7b2c", "order_key": "…", "cells": ["1002", "Widget B", "shipped"]}` |

The CSV plugin gives each record a stable `id`, so the row keeps its identity
even when the file is reordered. `cells` holds the decoded record. One cell
changed, and your app renders that:

```diff
order_id 1002 status:

- pending
+ shipped
```

## Granularity comes from plugins

A plugin decides what a row is for its format. The Markdown plugin emits blocks,
the CSV plugin emits records. Both ship with the JavaScript SDK. Other formats
need a plugin you install yourself. Without one, Lix still tracks the file but
does not split it into rows, so its diff is the whole file.

<img src="../website/public/assets/file-to-rows.svg" alt="A plugin maps /orders.csv to SQL rows with row, field, and value columns" width="760" />

Application data that never touches a file works the same way. Register a
[schema](./schemas.md) and its rows diff like any other row.

## What this buys you

- Review the record, field, or paragraph that changed, not a patch hunk.
- Query diffs by schema, file, row, branch, or time.
- Revert one field without reverting the file.
- Merge concurrent edits to different rows of the same file without a conflict.

## Query it

Which rows of `/orders.csv` changed, most recent first?

```sql
SELECT
  f.path,
  c.created_at,
  c.snapshot_content
FROM lix_change AS c
JOIN lix_file AS f
  ON f.id = c.file_id
WHERE c.schema_key = 'csv_row'
  AND f.path = '/orders.csv'
ORDER BY c.created_at DESC;
```

## Learn more

- [Diff Commands](./diff-commands.md): compare two commits.
- [History](./history.md) and [SQL Surfaces](./surfaces.md): branch-scoped state
  and history queries.
- [Schemas](./schemas.md): define rows for application data.
