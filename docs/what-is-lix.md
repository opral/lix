---
description: Lix combines a database, filesystem, and version control. Tools work with normal files, apps use SQL, and Lix tracks every change.
---

# What is Lix?

Lix is a **database + filesystem + version control system in one**.

Tools and agents work with normal files. Apps query and update SQL rows. Lix tracks every change with branches, history, review, rollback, and merge.

## Files become queryable rows

File plugins map parts of a file to rows. A row can represent a Markdown block, CSV record, spreadsheet cell, JSON property, document clause, or another entity defined by a plugin.

```text
what tools see                 what your app can query

                               entity      field      value
/orders.csv   ── plugin ──▶   row 1001    status     shipped
                               row 1002    status     pending
```

Apps read and write these rows with SQL. Lix records their history. In filesystem mode, plugin changes are written back to normal files on disk.

The JavaScript SDK includes Markdown and CSV plugins. Other formats, including JSON, XLSX, DOCX, and PDF, need a plugin.

## What Lix provides

```text
filesystem      normal files for existing tools and agents
database        SQL queries, schemas, and ACID transactions
version control branches, history, review, rollback, and merge
```

The same model also works for app data that does not come from a file. Register a schema and Lix creates a SQL table for it. Rows in that table get the same history and branch behavior as file entities.

## Prime use cases

### Safe workspaces for agents

Give each agent task its own branch. The agent can edit files and SQL rows without changing the main branch. Preview the result, then merge or discard it.

See [Lix for AI Agents](./lix-for-ai-agents.md).

### File-based apps with SQL and version control

Build editors, knowledge bases, document workflows, and other file-based apps. Existing tools keep using files while your app uses SQL for queries and transactions. Lix adds history, rollback, branches, merging, and review.

## Run Lix locally or remotely

Run Lix inside your app with memory, `LocalFilesystem`, or SQLite:

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new LocalFilesystem({ path: "./workspace", syncAllFiles: true }),
});
```

Or connect to a Lix server:

```ts
const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/workspaces/acme",
  },
});
```

Remote clients use the same file, SQL, branch, and observation APIs supported by the server protocol.

## Next

- [Getting Started](./getting-started.md): open Lix, write data, create a branch, and merge it.
- [How Lix compares to Git](./comparison-to-git.md): files, databases, and version control side by side.
- [Schemas](./schemas.md): define app rows and plugin entities.
- [Semantic Changes](./semantic-changes.md): track changes inside files.
- [Persistence](./persistence.md): choose a local or remote setup.
