<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/website/public/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">Embeddable repository backend</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

Lix is a repository you embed in your product. Everything inside is versioned data: file content, app tables, reviews, comments. There is no external control plane to sync. Agents read and write normal files. Your product queries SQL. You branch, diff, merge, and roll back all of it together:

<img src="./website/public/assets/lix-repo.svg" alt="A Lix repo holds files, a SQL database, and version control in one system" width="760" />

- 📄 **Files, in any format.** Store text and binary files. Plugins make supported formats queryable as versioned rows.
- 🗄️ **SQL database.** File content, app data, and history live in an ACID OLTP database. Query millions of rows with SQL.
- 🔀 **Version control.** Diffs name the clause, cell, or row that changed, not a byte blob. Review, merge, and roll back.
- ⚡ **Real-time collaboration.** People and agents share a repository and see changes as they happen.
- 🧩 **Pluggable storage.** Local filesystem, SQLite on browser OPFS, or S3 behind a Lix server.
- 🔒 **Permissions (soon).** Finance, legal, and contractors need different access. Permissions will live inside the repository: per file, per group, and versioned like any other change.

## Getting started

<p>
  <a href="https://lix.dev/docs/javascript-quickstart"><img src="https://cdn.simpleicons.org/javascript/F7DF1E" alt="JavaScript" width="18" height="18" /> JavaScript</a> ·
  <a href="https://lix.dev/docs/rust-quickstart"><img src="https://cdn.simpleicons.org/rust/CE422B" alt="Rust" width="18" height="18" /> Rust</a> ·
  <a href="https://github.com/opral/lix/issues/373" title="The Python SDK is planned. Upvote the issue on GitHub."><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="18" height="18" /> Python</a> ·
  <a href="https://github.com/opral/lix/issues/370" title="The Go SDK is planned. Upvote the issue on GitHub."><img src="https://cdn.simpleicons.org/go/00ADD8" alt="Go" width="18" height="18" /> Go</a>
</p>

```bash
npm install @lix-js/sdk @lix-js/storage-filesystem
```

Run locally with `FilesystemStorage`:

```ts
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

const lix = await openLix({
  storage: new FilesystemStorage({ path: "./repository" }),
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
    url: "https://example.com/repositories/acme",
  },
});
```

## Try a demo

Try out [lixray.com](https://lixray.com):

<a href="https://lixray.com"><img src="./website/public/assets/lixray-og.png" alt="LixRay: a repository for your entire company. Works with Claude, OpenAI, and Gemini." width="760" /></a>

## Prime use cases

### Give each customer a repository

Your product gives every customer their own repository: their files, their data, and the automations LLMs now write for them.

Lix handles any file format, collaborates in real time, and embeds in your product. Non-technical users get accept and undo, not branches and pull requests. Permissions are coming.

<img src="./website/public/assets/customer-repositories.svg" alt="Your product creates one Lix repository per customer, each holding a different mix of automations, handbooks, pricing, and knowledge files" width="760" />

```ts
// One hosted repository per customer.
const lix = await openLix({
  server: {
    mode: "remote",
    url: `https://example.com/repositories/${customer.id}`,
  },
});

// The agent writes an automation. Lix records the change, no commit needed.
await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/automations/booking.ts",
  code,
]);

// Your UI shows the diff. The customer clicks accept or undo.
```

### Sync files

Sync the files that coding agents and applications work on, between machines and with a server.

<img src="./website/public/assets/file-sync.svg" alt="Client A and client B each hold the same project files on their own filesystem and synchronize them with a Lix server" width="760" />

```ts
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

// ./project stays a normal directory. Lix syncs it through the server.
const lix = await openLix({
  storage: new FilesystemStorage({ path: "./project" }),
  server: {
    mode: "remote",
    url: "https://lixray.com/@acme/project",
  },
});
```

Use [LixRay](https://lixray.com) or [run your own server](./docs/hosting.md).

### Apps with version control

Your app reads and writes SQL rows and normal files. Lix records every change with its author, so history, blame, branching, and rollback are queries instead of features you build.

<img src="./website/public/assets/app-with-history.svg" alt="An app window with a document diff, an accept and undo control, and a history sidebar with checkpoints, all provided by Lix" width="760" />

```ts
// A normal app write. Lix records the change automatically.
await lix.execute("UPDATE orders SET status = 'shipped' WHERE id = 1002");

// The history sidebar, diff view, and undo button are queries:
const changes = await lix.execute(`
  SELECT created_at, account_id, schema_key, row_pk, snapshot_content
  FROM lix_change
  ORDER BY created_at DESC
`);
```

Files and rows update in one ACID transaction.

[Read more about diffs →](https://lix.dev/docs/diffs)

## How Lix works

### Files × database

Plugins map files to SQL rows. A paragraph, cell, or property becomes a row Lix can version.

With `FilesystemStorage`, the file stays available on disk. Its rows are queryable with SQL. Lix tracks changes to both.

<img src="./website/public/assets/file-to-rows.svg" alt="A plugin maps /orders.csv to SQL rows with row, field, and value columns" width="760" />

### Runs in-process as part of your infrastructure

Lix runs in-process with pluggable storage: in memory, on the local filesystem, or in a server backed by S3.

<img src="./website/public/assets/pluggable-storage.svg" alt="Lix runs in-process inside your product, with an arrow to pluggable storage: memory, filesystem, or S3" width="760" />

Existing VCS like Git assume a local POSIX filesystem, which makes them hard to embed and scale. See the [Persistence and Storage](https://lix.dev/docs/persistence) docs.

## Comparison

Git tracks files but has no SQL. PostgreSQL/SQLite have SQL but no files and no change history. Lix has all three.

<img src="./website/public/assets/comparison-quadrant.svg" alt="Quadrant chart: database capability on the vertical axis, version control on the horizontal axis. PostgreSQL and SQLite sit top left, Git bottom right, Lix top right." width="760" />

| Capability                    | Lix            | Git                | PostgreSQL / SQLite |
| ----------------------------- | -------------- | ------------------ | ------------------- |
| Normal files                  | ✅             | ✅                 | ❌                  |
| SQL and transactions          | ✅             | ❌                 | ✅                  |
| Branches and merging          | ✅             | ✅                 | ❌                  |
| Diffs by cell, clause, or row | ✅ via plugins | ❌ text lines only | ❌                  |
| Pluggable storage             | ✅             | ❌                 | ❌                  |

## Learn more

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## License

[MIT](./LICENSE)
