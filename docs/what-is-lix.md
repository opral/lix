---
description: Lix is a version control system beyond code. Tools work with normal files, apps use SQL, and Lix tracks every change.
---

# What is Lix?

Lix is a **version control system beyond code**. It combines files, a database, and version control in one system.

Tools and agents work with normal files. Apps query and update SQL rows. Lix tracks every change with branches, history, review, rollback, and merge.

## Files become queryable rows

File plugins map parts of a file to rows. A row can represent a Markdown block, CSV record, spreadsheet cell, JSON property, document clause, or another entity defined by a plugin.

<img src="../website/public/assets/file-to-rows.svg" alt="A plugin maps /orders.csv to SQL rows with entity, field, and value columns" width="760" />

Apps read and write these rows with SQL. Lix records their history. In filesystem mode, plugin changes are written back to normal files on disk.

Diffs are semantic: review the clause, cell, or row that changed, not lines of bytes. See [Semantic Changes](./semantic-changes.md).

The JavaScript SDK includes Markdown and CSV plugins. Other formats, including JSON, XLSX, DOCX, and PDF, need a plugin.

## What Lix provides

```text
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│    FILESYSTEM    │     │     DATABASE     │     │  VERSION CONTROL │
│                  │  ×  │                  │  ×  │                  │
│ normal files for │     │ SQL queries,     │     │ branches, review │
│ existing tools   │     │ schemas, and     │     │ history, merge,  │
│ and agents       │     │ ACID transactions│     │ and rollback     │
└──────────────────┘     └──────────────────┘     └──────────────────┘
```

The same model also works for app data that does not come from a file. Register a schema and Lix creates a SQL table for it. Rows in that table get the same history and branch behavior as file entities.

## Real-time collaboration

People and agents share a repository and see changes live. Apps observe SQL queries with `lix.observe()` and receive updates when rows change. Clients connected to the same Lix server see each other's changes as they happen.

## Prime use cases

### Safe workspaces for agents

Give each agent task its own branch. The agent can edit files and SQL rows without changing the main branch. Preview the result, then merge or discard it.

See [Lix for AI Agents](./lix-for-ai-agents.md).

### File-based apps with SQL and version control

Build editors, knowledge bases, document workflows, and other file-based apps. Existing tools keep using files while your app uses SQL for queries and transactions. Lix adds history, rollback, branches, merging, and review.

## Pluggable storage

Lix has pluggable storage, which makes it easy to embed and scale: run it in memory, on the local filesystem, or against a server backed by S3. See [Persistence and Storage](./persistence.md).

Run Lix inside your app, for example with `LocalFilesystem`:

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

## Permissions (in development)

Permissions are on the roadmap. They will live inside the repository: per file, per group, and versioned like any other change. A policy change can then be proposed, reviewed, and merged on a branch.

## Next

- [Getting Started](./getting-started.md): open Lix, write data, create a branch, and merge it.
- [How Lix compares to Git](./comparison-to-git.md): files, databases, and version control side by side.
- [Schemas](./schemas.md): define app rows and plugin entities.
- [Semantic Changes](./semantic-changes.md): track changes inside files.
- [Persistence](./persistence.md): choose a local or remote setup.
