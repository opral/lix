---
description: Lix gives your product a repository with files, SQL, and version control in one place.
---

# What is Lix?

Lix gives your product a repository: **files, SQL, and version control in one place**. You embed it in your product.

Agents and tools read and write normal files. Your product queries and updates SQL rows. Both work on the same repository. Lix versions everything they write, with branches, history, review, rollback, and merge.

Unlike Git, Lix tracks the rows inside files, not lines of text. See [How Lix compares to Git](./comparison-to-git.md).

<img src="../website/public/assets/filesystem-database-version-control.svg" alt="Lix combines a filesystem, a database, and version control" width="760" />

## Use cases

### Safe repositories for agents

Give each agent task its own branch. The agent can edit files and SQL rows without changing the main branch. Preview the result, then merge or discard it.

<img src="../website/public/assets/agent-branch.svg" alt="An agent works on its own branch while main stays stable; the branch is merged or discarded" width="760" />

See [Lix for AI Agents](./lix-for-ai-agents.md).

### Sync files

Sync the files that coding agents and applications work on, between machines and with a server.

<img src="../website/public/assets/file-sync.svg" alt="Client A and client B each hold the same project files on their own filesystem and synchronize them with a Lix server" width="760" />

```ts
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

// ./project stays a normal directory. Lix syncs it through the server.
const lix = await openLix({
  storage: new FilesystemStorage({ path: "./project" }),
  server: {
    mode: "sync",
    url: "https://lixray.com/@acme/project",
  },
});
```

See [Collaboration and Sync](./collaboration-and-sync.md).

### File-based apps with SQL and version control

Build editors, knowledge bases, document workflows, and other file-based apps. Existing tools keep using files while your app uses SQL for queries and transactions. Lix versions everything both sides write.

<img src="../website/public/assets/app-and-tools-on-lix.svg" alt="Your app uses SQL and existing tools use files; both work on the same Lix repository" width="760" />

## Files become queryable rows

File plugins map parts of a file to rows. A row can represent a Markdown block, CSV record, spreadsheet cell, JSON property, or document clause.

<img src="../website/public/assets/file-to-rows.svg" alt="A plugin maps /orders.csv to SQL rows with row, field, and value columns" width="760" />

Apps read and write these rows with SQL. Lix records their history. With
`FilesystemStorage`, it also writes changes back to normal files on disk.

Diffs are row-level: review the clause, cell, or row that changed, not lines of text. See [Diffs](./diffs.md).

## Pluggable storage

Run Lix in memory, on the local filesystem, or against a server backed by S3. See [Persistence and Storage](./persistence.md).

<img src="../website/public/assets/pluggable-storage.svg" alt="Lix runs in your app on a storage adapter: in memory, local filesystem, or S3 bucket" width="760" />

## Local, remote, and sync

Lix supports local repositories, direct remote clients, and synchronized local
replicas with the same API. See [Persistence and Storage](./persistence.md) for
setup examples.

Clients can execute directly on a server or use a synchronized local replica.
Both modes use the same files, SQL, and branches. Clients on the same server
see each other's changes through `lix.observe()`. See
[Collaboration and Sync](./collaboration-and-sync.md).

## Permissions (planned)

Permissions will live inside the repository: per file, per group, and versioned like any other change. A policy change can then be proposed, reviewed, and merged on a branch.

## Next

- [Getting Started](./getting-started.md): choose the JavaScript or Rust quickstart.
- [How Lix compares to Git](./comparison-to-git.md): files, databases, and version control side by side.
- [Schemas](./schemas.md): define app rows and plugin rows.
- [Diffs](./diffs.md): track changes inside files.
- [Files and Media](./files-and-media.md): store text, binary files, and large media.
- [Collaboration and Sync](./collaboration-and-sync.md): connect clients directly or through local replicas.
- [Persistence](./persistence.md): choose a local or remote setup.
