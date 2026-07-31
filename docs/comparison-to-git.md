---
description: Compare Git, databases, and Lix across file access, SQL, transactions, history, branches, and semantic review.
---

# How Lix compares to Git

Git gives source code history, branches, and merge. Databases give apps SQL and transactions. Lix combines both with a filesystem that normal tools and agents can use.

## The map: interoperability × database semantics

```text
                           database semantics
                       (query · transact · track)
                                  ▲
                                  │
       PostgreSQL / SQLite        │                 ★ Lix
       SQL and transactions,      │          normal files and SQL rows,
       but data lives in tables   │          with every change tracked
                                  │
   ───────────────────────────────┼──────────────────────────────────▶
                                  │              file interoperability
                                  │       (any agent or tool can read/write)
                                  │
                                  │             Filesystem / Git
                                  │             normal files,
                                  │         but no queryable rows
```

## Comparison

| Capability                                 | Filesystem / Git                     | PostgreSQL / SQLite    | Lix                |
| ------------------------------------------ | ------------------------------------ | ---------------------- | ------------------ |
| Works with normal workspace files          | Yes                                  | No                     | Yes                |
| Queries file content with SQL              | No                                   | Only after import      | Yes, with a plugin |
| ACID transactions                          | No                                   | Yes                    | Yes                |
| Change history                             | Git: blobs and lines; filesystem: no | You build audit tables | Files and rows     |
| Branches and merging                       | Git: yes; filesystem: no             | You build them         | Yes                |
| Reviews changes by paragraph, cell, or row | Text lines only                      | You build it           | Yes, with a plugin |

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
