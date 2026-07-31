<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/website/public/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">Database + filesystem + version control in one</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

Agents and tools work with files. Applications need a database. Teams need version control.

Lix combines all three: normal files for tools, SQL rows for apps, and version control for every change.

- 📁 **Keep normal files.** Existing tools and agents can keep reading and writing files on disk.
- 🧠 **Query everything with SQL.** Query file content, app data, and change history without rereading whole files.
- 🔍 **Track semantic changes.** Review the paragraph, CSV record, property, or app row that changed.
- 🔀 **Branch and merge safely.** Give every user or agent an isolated workspace, then review and merge its work.
- ✅ **Use ACID transactions.** Update files and rows together while Lix records their history.
- 🤝 **Run locally or remotely.** Embed Lix in an app or connect to a shared workspace through the server protocol.

## Try a demo app

[Flashtype](https://flashtype.ai) is a Markdown editor for Claude and Codex built on Lix. Open local Markdown files, let agents edit them, review changes as diffs, and restore previous versions from history.

[![Flashtype app preview](https://flashtype.ai/og.png)](https://flashtype.ai)

## Getting started

<p>
  <img src="https://cdn.simpleicons.org/javascript/F7DF1E" alt="JavaScript" width="18" height="18" /> JavaScript ·
  <a href="https://github.com/opral/lix/issues/373"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="18" height="18" /> Python</a> ·
  <a href="https://github.com/opral/lix/issues/371"><img src="https://cdn.simpleicons.org/rust/CE422B" alt="Rust" width="18" height="18" /> Rust</a> ·
  <a href="https://github.com/opral/lix/issues/370"><img src="https://cdn.simpleicons.org/go/00ADD8" alt="Go" width="18" height="18" /> Go</a>
</p>

```bash
npm install @lix-js/sdk
```

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new LocalFilesystem({ path: "./workspace", syncAllFiles: true }),
});

await lix.execute("INSERT INTO lix_file (path, data) VALUES ($1, $2)", [
  "/notes/status.txt",
  new TextEncoder().encode("ready"),
]);
```

Connect to a shared Lix server with the same API:

```ts
const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/workspaces/acme",
  },
});
```

## Why Lix?

### Files × database

Plugins map file parts such as paragraphs, cells, and properties to SQL rows. Your app can also define its own rows.

```text
what tools see                 what your app can query

                               entity      field      value
/orders.csv   ── plugin ──▶   row 1001    status     shipped
                               row 1002    status     pending
```

Apps read and write the rows with SQL. Lix tracks every change. With `LocalFilesystem`, other tools and agents keep working with the files on disk.

The SDK includes plugins for Markdown and CSV. Add a plugin for other formats, such as JSON, XLSX, DOCX, or PDF.

### The map: interoperability × database semantics

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

### Comparison

| Capability                                 | Filesystem / Git                     | PostgreSQL / SQLite    | Lix                |
| ------------------------------------------ | ------------------------------------ | ---------------------- | ------------------ |
| Works with normal workspace files          | Yes                                  | No                     | Yes                |
| Queries file content with SQL              | No                                   | Only after import      | Yes, with a plugin |
| ACID transactions                          | No                                   | Yes                    | Yes                |
| Change history                             | Git: blobs and lines; filesystem: no | You build audit tables | Files and rows     |
| Branches and merging                       | Git: yes; filesystem: no             | You build them         | Yes                |
| Reviews changes by paragraph, cell, or row | Text lines only                      | You build it           | Yes, with a plugin |

### Prime use cases

#### Give agents safe, isolated workspaces

Give each agent its own branch. The agent works with normal files while the main branch stays stable. Preview its work, then merge or discard it.

```ts
const main = await lix.activeBranchId();
const task = await lix.createBranch({ name: "Agent task" });

await lix.switchBranch({ branchId: task.id });
// Run the agent. Its file and SQL writes are isolated to this branch.

await lix.switchBranch({ branchId: main });
const preview = await lix.mergeBranchPreview({ sourceBranchId: task.id });

if (preview.conflicts.length === 0) {
  await lix.mergeBranch({ sourceBranchId: task.id });
}
```

#### Build file-based apps with SQL and version control

Build editors, knowledge bases, and document workflows on normal files. Use SQL for app logic and Lix for history, rollback, branches, merging, and review.

Plugins define the parts Lix tracks. You can review a change to a paragraph, cell, property, or row instead of only bytes and lines.

For example, when an agent updates an orders CSV, Lix can show the row field that changed:

```diff
order_id 1002 status:

- pending
+ shipped
```

Query changes without reading every file:

```ts
const changes = await lix.execute(`
  SELECT created_at, schema_key, entity_pk, snapshot_content
  FROM lix_change
  ORDER BY created_at DESC
  LIMIT 20
`);
```

Update Lix files and rows in one ACID transaction. Lix records the history automatically.

[Read more about semantic changes →](https://lix.dev/docs/semantic-changes)

## Where this is going

Git makes code available to every developer tool. Lix aims to do the same for documents, spreadsheets, app data, and agent output while keeping SQL and version control.

```text
 contracts.docx    pricing.xlsx      app data     agent output
       │                │              │              │
       └────────────────┴───────┬──────┴──────────────┘
                                ▼
┌─────────────────── ONE LIX REPOSITORY ───────────────────┐
│                                                          │
│  ┌──────────────┐     ┌──────────────┐     ┌────────────┐ │
│  │  FILESYSTEM  │  ×  │   DATABASE   │  ×  │  VERSION   │ │
│  │              │     │              │     │  CONTROL   │ │
│  │ tools and    │     │ SQL queries  │     │review/merge│ │
│  │ agents       │     │ transactions │     │ rollback   │ │
│  └──────────────┘     └──────────────┘     └────────────┘ │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

## Learn more

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## License

[MIT](./LICENSE)
