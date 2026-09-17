---
description: Git is a CLI for source code. Lix is a library that versions any file format and stores files and app data in one SQL database.
---

# How Lix compares to Git

Git is a CLI designed for source code. It assumes a local POSIX filesystem, tracks whole files, and diffs text lines. Lix is a library you embed in a product. It versions any file format with row-level diffs, runs on pluggable storage, and stores files, app tables, and history as rows in one SQL database.

## When to use which

- **Use Git** for software engineering. Your source code needs GitHub, CI, code review, and the tooling every developer knows.
- **Use Lix** when your product stores files and data for its users: documents, spreadsheets, media, app records, and the scripts that work on them. Those scripts do not need the Git ecosystem. They need history next to the data they touch.
- **Use both.** Your source code lives in Git. Your product's repositories live in Lix. The two stay independent.

## What Lix adds over Git

| Capability                              | Git                        | Lix                                                                                                  |
| --------------------------------------- | -------------------------- | ---------------------------------------------------------------------------------------------------- |
| Diffs by cell, clause, or row           | Text lines only            | Yes, via plugins. Markdown and CSV ship with the JS SDK; JSON, text, and Excalidraw are installable. |
| SQL over content and history            | No                         | Yes                                                                                                  |
| ACID transactions across files and rows | No                         | Yes                                                                                                  |
| Runs embedded in your app               | CLI-first; libraries exist | Library-first                                                                                        |
| Pluggable storage (memory, disk, S3)    | Assumes a POSIX filesystem | Yes                                                                                                  |

An agent updates one field in an orders CSV. Git shows a changed text line. Lix shows the row that changed:

```diff
order_id 1002 status:

- pending
+ shipped
```

You query that history with SQL:

```sql
SELECT created_at, schema_key, row_pk, snapshot_content
FROM lix_change
ORDER BY created_at DESC
LIMIT 20;
```

## What Git has that Lix doesn't

- A mature ecosystem: GitHub, CI, code review, hosting, and two decades of tooling.
- A universal CLI workflow every developer already knows.
- Battle-tested stability. Lix is in alpha.

## Deeper reading

- [Diffs](./diffs.md): how plugins split files into rows.
- [History](./history.md): query what changed with SQL.
- [Storage](./persistence.md): storage adapters from memory to S3.
- [Plugins](./plugins.md): install plugins for more file formats.
