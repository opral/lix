---
description: Lix is not a Git replacement. Keep code in Git; use Lix to version, query, and merge the files and data your app works with.
---

# How Lix compares to Git

Lix is not a Git replacement. Git versions source code as line-diffed text. Lix is an SDK you embed in an app to version files and data, with row-level diffs and SQL over content and history.

Keep your code in Git. Use Lix for the documents and app data your product needs to diff, merge, and roll back.

## When to use which

- **Use Git** for source code repositories and developer workflows.
- **Use Lix** when non-developers and agents change files and data: documents, spreadsheets, and app records. Lix is designed for their workflows: real-time collaboration, automatic change tracking, review, and restore — no commits, no CLI.
- **Use both.** Code lives in a Git repository. Your product's files and data live in a Lix repository. They do not conflict.

## What Lix adds over Git

| Capability                                | Git                          | Lix                                     |
| ----------------------------------------- | ---------------------------- | --------------------------------------- |
| Diffs by cell, clause, or row             | Text lines only              | Yes, via plugins (Markdown, CSV today)  |
| SQL over content and history              | No                           | Yes                                     |
| ACID transactions across files and rows   | No                           | Yes                                     |
| Runs embedded in your app                 | CLI-first; libraries exist   | SDK-first                               |
| Pluggable storage (memory, disk, S3)      | Assumes a POSIX filesystem   | Yes                                     |

An agent updates one field in an orders CSV. Git shows a changed text line or a binary blob. Lix shows the row that changed:

```diff
order_id 1002 status:

- pending
+ shipped
```

And the history is queryable:

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

- [Semantic Changes](./semantic-changes.md): how plugins split files into rows.
- [Change History](./history.md): query what changed with SQL.
- [Persistence and Storage](./persistence.md): storage adapters from memory to S3.
