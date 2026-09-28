---
description: Git versions files. Lix is an embeddable library that versions files of any format and SQL tables in one repository.
---

# How Lix compares to Git

Git versions files, but an app's tables usually live in a separate database. You can commit a SQLite database file to Git, but Git sees its bytes rather than queryable rows. Database changes are hard to review or merge, and successive versions grow the repository. Lix versions files and SQL tables in one repository. File plugins provide structured diffs for supported formats; other files have whole-file history.

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
