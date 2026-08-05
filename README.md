<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/website/public/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">A version control system beyond code</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

Code lives in version control. The documents, spreadsheets, and data a company runs on do not.

Lix is a version control system for work beyond code: any file format, SQL over content and history, and review for every change.

- 📄 **Works with any file format.** Plugins map DOCX, CSV, Markdown, or your own format to versioned entities.
- 🔍 **Semantic changes.** Review the clause, cell, or row that changed, not lines of bytes.
- 🗄️ **SQL and transactions.** Query file content, app data, and history; update files and rows in one ACID transaction.
- 👥 **Real-time collaboration.** People and agents share a repository and see changes live.
- 🏁 **Checkpoints instead of commits.** Lix records every change automatically; a checkpoint marks a state you want to return to.

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

Run locally with `LocalFilesystem`:

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new LocalFilesystem({ path: "./workspace", syncAllFiles: true }),
});

await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/notes/status.txt",
  new TextEncoder().encode("ready"),
]);
```

Or against a server:

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

Plugins map files to SQL rows. A paragraph, cell, or property becomes a row Lix can version.

<img src="./website/public/assets/file-to-rows.svg" alt="A plugin maps /orders.csv to SQL rows with entity, field, and value columns" width="760" />

The file stays a normal file on disk. The rows are queryable with SQL. Lix tracks every change to both.

The SDK includes plugins for Markdown and CSV. Add a plugin for other formats, such as JSON, XLSX, DOCX, or PDF.

### Comparison

Git versions files but cannot query them. PostgreSQL and SQLite query rows but have no files and no history. Lix does both.

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

| Capability                    | Git             | PostgreSQL / SQLite | Lix                 |
| ----------------------------- | --------------- | ------------------- | ------------------- |
| Normal files                  | Yes             | No                  | Yes                 |
| SQL and transactions          | No              | Yes                 | Yes                 |
| Branches and merging          | Yes             | No                  | Yes                 |
| Diffs by cell, clause, or row | Text lines only | No                  | Yes, via plugins    |

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

#### Put your company's files under agent operation

Agents already know how to read and write files. Put contracts, pricing sheets, and order data in a repository, and agents can work on them without custom integrations.

Lix records every change automatically. When an agent updates an orders CSV, reviewers see the row field that changed before the change merges:

```diff
order_id 1002 status:

- pending
+ shipped
```

Merge good changes, discard bad ones, and restore any earlier state of the company.

#### Build file-based apps with SQL and version control

Build editors, knowledge bases, and document workflows on normal files. Use SQL for app logic and Lix for history, rollback, branches, merging, and review.

Plugins define the parts Lix tracks. You can review a change to a paragraph, cell, property, or row instead of only bytes and lines.

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

The goal is one repository for everything a company produces. Code lives in git. Contracts, spreadsheets, app data, and agent output get the same foundation with Lix: one history, queryable with SQL, safe to branch and merge.

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
