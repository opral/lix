---
description: Compare Git, databases, and Lix across normal files, SQL, branches, and semantic diffs.
---

# How Lix compares to Git

Git versions files but cannot query them. PostgreSQL and SQLite query rows but have no files and no history. Lix does both.

## The map

```text
                     database
                         │
   PostgreSQL / SQLite   │        ★ Lix
   rows, no history      │     rows + files,
                         │       versioned
                         │
   ──────────────────────┼──────────────────▶  version control
                         │
                         │          Git
                         │    text files only
                         │
```

## Comparison

| Capability                    | Git             | PostgreSQL / SQLite | Lix              |
| ----------------------------- | --------------- | ------------------- | ---------------- |
| Normal files                  | Yes             | No                  | Yes              |
| SQL and transactions          | No              | Yes                 | Yes              |
| Branches and merging          | Yes             | No                  | Yes              |
| Diffs by cell, clause, or row | Text lines only | No                  | Yes, via plugins |

Use Git for source-code repositories and developer workflows. Use Lix when a product or agent must work with normal files while the app queries their contents and history with SQL.

Git and Lix can work together. Keep source code in Git. Use Lix for the files and app data your product needs to query, review, merge, and roll back.

## The unit of change

Git stores file snapshots and usually shows line diffs. This works well for source code.

Lix stores changes as data. Plugins define the parts inside a file, such as Markdown blocks or CSV rows. Lix can query and review those parts directly.

For example, an agent updates one field in an orders CSV:

```diff
order_id 1002 status:

- pending
+ shipped
```

The Markdown and CSV plugins ship with the JavaScript SDK. Other formats need a plugin. For example, an XLSX plugin could define cells or rows, and a JSON plugin could define properties.

## SQL history

Lix exposes changes as rows:

```sql
SELECT created_at, schema_key, entity_pk, snapshot_content
FROM lix_change
ORDER BY created_at DESC
LIMIT 20;
```

Apps and agents can ask which entities changed, which files they came from, and whether branches touch the same entity. See [Change History](./history.md).
