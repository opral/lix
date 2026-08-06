<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/website/public/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">A version control system for files and data beyond code</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

Agents and tools work with files. Applications need a database. Teams need version control. Lix combines all three: normal files for tools, SQL rows for apps, and version control for every change.

<img src="./website/public/assets/lix-triad.svg" alt="Lix is a filesystem, a database, and version control in one system" width="760" />

- 📄 **Works with any file format.** Plugins map DOCX, CSV, Markdown, or your own format to versioned entities.
- 🔍 **Semantic changes.** Review the clause, cell, or row that changed, not lines of bytes.
- 🗄️ **SQL and transactions.** Query file content, app data, and history; update files and rows in one ACID transaction.
- 👥 **Real-time collaboration.** People and agents share a repository and see changes live.
- 🔌 **Pluggable storage.** An S3 bucket, the local filesystem, or OPFS in the browser: Lix is easy to embed and scale, in contrast to existing VCS like Git that assume a local POSIX filesystem.
- 🔐 **Permissions (soon).** Finance, legal, and contractors need different access. Permissions will live inside the repository: per file, per group, and versioned like any other change.

## Try a demo app

[Flashtype](https://flashtype.ai) is a Markdown editor for Claude and Codex built on Lix. Open local Markdown files, let agents edit them, review changes as diffs, and restore previous versions from history.

[![Flashtype app preview](https://flashtype.ai/og.png)](https://flashtype.ai)

## Getting started

<p>
  <img src="https://cdn.simpleicons.org/javascript/F7DF1E" alt="JavaScript" width="18" height="18" /> JavaScript ·
  <a href="https://github.com/opral/lix/issues/373" title="The Python SDK is planned. Upvote the issue on GitHub."><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="18" height="18" /> Python</a> ·
  <a href="https://github.com/opral/lix/issues/371" title="The Rust SDK is planned. Upvote the issue on GitHub."><img src="https://cdn.simpleicons.org/rust/CE422B" alt="Rust" width="18" height="18" /> Rust</a> ·
  <a href="https://github.com/opral/lix/issues/370" title="The Go SDK is planned. Upvote the issue on GitHub."><img src="https://cdn.simpleicons.org/go/00ADD8" alt="Go" width="18" height="18" /> Go</a>
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

### Pluggable storage

Lix embeds in your product and runs on storage adapters: in memory, on the local filesystem, or on an S3 bucket.

<img src="./website/public/assets/pluggable-storage.svg" alt="Lix embeds in your product and runs on a storage adapter: in memory, local filesystem, or S3 bucket" width="760" />

Existing VCS like Git assume a local POSIX filesystem, which makes them hard to embed and scale. See the [Persistence and Storage](https://lix.dev/docs/persistence) docs.

### Comparison

Git versions files but cannot query them. PostgreSQL and SQLite query rows but have no files and no history. Lix does both.

<img src="./website/public/assets/comparison-quadrant.svg" alt="Quadrant chart: database capability on the vertical axis, version control on the horizontal axis. PostgreSQL and SQLite sit top left, Git bottom right, Lix top right." width="760" />

| Capability                    | Git             | PostgreSQL / SQLite | Lix                 |
| ----------------------------- | --------------- | ------------------- | ------------------- |
| Normal files                  | Yes             | No                  | Yes                 |
| SQL and transactions          | No              | Yes                 | Yes                 |
| Branches and merging          | Yes             | No                  | Yes                 |
| Diffs by cell, clause, or row | Text lines only | No                  | Yes, via plugins    |
| Pluggable storage             | No              | No                  | Yes                 |

### Prime use cases

#### Automation repository for your customers

LLMs let your product write automations for customers. Automations are code, so every customer needs a repository. Lix is simpler than git here: it embeds in your product, and your customers review and undo changes without branch, merge, or pull request vocabulary.

```ts
// One Lix repository per customer, hosted on your server (for example backed by S3).
const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://lix.example.com/customers/acme",
  },
});

// The agent writes an automation. Lix records the change, no commit needed.
await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/automations/booking.ts",
  code,
]);

// Your UI shows the diff. The customer clicks accept or undo.
```

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

The goal is one repository for everything a company produces. Contracts, spreadsheets, app data, agent output, and the automations non-engineers now write with LLMs get the same foundation: one history, queryable with SQL, safe to branch and merge.

<img src="./website/public/assets/one-repository.svg" alt="Automation product, agents, analytics, and knowledge search all build on one Lix repository holding automations, skills, sales data, and contracts" width="760" />

## Learn more

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## License

[MIT](./LICENSE)
