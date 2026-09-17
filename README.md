<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/website/public/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">Universal version control system</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

Lix is a version control system for any file format: code, documents, spreadsheets, design files, video, datasets. It runs in-process on pluggable storage. Files, app tables, and history are rows in one ACID database. Agents read and write normal files. Your product queries SQL. You branch, diff, merge, and roll back all of it together:

<img src="./website/public/assets/one-lix-repo.svg" alt="One Lix repository holding files of every format and the application's own database tables" width="760" />

- 📄 **Any format.** Text, binaries, large blobs. Plugins make formats like DOCX, XLSX, and JSON diffable and mergeable by clause, cell, or row.
- 🧩 **Embeddable.** Runs in-process as a library. Storage is pluggable: memory, filesystem, browser OPFS, or S3.
- 🗄️ **Designed as a database.** File content, app tables, and history are rows in one ACID OLTP database. Query millions of rows with SQL. Every write becomes a commit automatically. You never run a commit command.
- ⚡ **Real-time collaboration.** People and agents share a repository and see changes as they happen.
- 🔒 **Permissions (planned).** Per file, per group, stored in the repository and versioned like any other change.

## Why not Git?

Git is a CLI designed for source code. It assumes a local POSIX filesystem, tracks whole files, and diffs text lines. Keep software engineering in Git. Use Lix when your product stores files and data for its users.

|                   | Git                  | Lix                             |
| ----------------- | -------------------- | ------------------------------- |
| Process model     | Separate CLI process | Library in your process         |
| Storage           | Local disk           | Memory · filesystem · OPFS · S3 |
| Application data  | Separate database    | SQL rows, ACID, with the files  |
| Recording changes | Manual commits       | Every write, automatically      |
| Formats           | Text lines           | Any, by clause, cell, or row    |
| Collaboration     | Push and pull        | Real time                       |

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
    url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
  },
});
```

Lix is in alpha.

## Prime use cases

### Co-locate code, documents, and app state

Code lives in Git. Documents, design files, and media live in Drive, Figma, and S3. App state lives in Postgres. No system versions all of them together. Lix stores them in one repository with one history.

<img src="./website/public/assets/one-lix-repo.svg" alt="One Lix repository holding files of every format and the application's own database tables" width="760" />

```ts
// A script, a 4.8 GB video, and an app table in one transaction.
await lix.executeBatch([
  {
    sql: "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
    params: ["/automations/weekly-report.js", source],
  },
  {
    sql: "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
    params: ["/media/launch.mp4", video],
  },
  {
    sql: "UPDATE orders SET status = 'shipped' WHERE id = $1",
    params: [1002],
  },
]);

// Branch, diff, and roll back all of it together.
```

### Give each customer a repository

Your customers want agents that write automations and edit their documents, with a way to review and undo. Drive has no version control. Your customers do not have GitHub repos. Embed Lix and give each customer a repository that holds their code, documents, spreadsheets, and media.

<img src="./website/public/assets/customer-repositories.svg" alt="Your product creates one Lix repository per customer, each holding a different mix of automations, handbooks, pricing, and knowledge files" width="760" />

```ts
// One hosted repository per customer.
const lix = await openLix({
  server: {
    url: `https://example.com/lix/${customer.repositoryId}`,
  },
});

// The agent writes an automation. Lix commits the change automatically.
await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/automations/booking.ts",
  code,
]);

// Your UI shows the diff. The customer clicks accept or undo.
```

### Apps with version control

Your app reads and writes SQL rows and normal files. Lix records every change with its author, so history, blame, branching, and rollback are queries instead of features you build.

<img src="./website/public/assets/app-with-history.svg" alt="An app window with a document diff, an accept and undo control, and a history sidebar with checkpoints, all provided by Lix" width="760" />

```ts
// A normal app write. "orders" is a table you registered with a Lix schema.
await lix.execute("UPDATE orders SET status = 'shipped' WHERE id = 1002");

// The history sidebar, diff view, and undo button are queries:
const changes = await lix.execute(`
  SELECT created_at, account_id, schema_key, row_pk, snapshot_content
  FROM lix_change
  ORDER BY created_at DESC
`);
```

[Read more about diffs →](https://lix.dev/docs/diffs)

## How Lix works

### Files × database

Plugins map files to SQL rows. A paragraph, cell, or property becomes a row Lix can version.

With `FilesystemStorage`, the file stays available on disk. Its rows are queryable with SQL. Lix tracks changes to both.

<img src="./website/public/assets/file-to-rows.svg" alt="A plugin maps /orders.csv to SQL rows with row, field, and value columns" width="760" />

### Runs in-process as part of your infrastructure

Lix runs in-process with pluggable storage: memory, filesystem, browser OPFS, or S3. See the [Storage](https://lix.dev/docs/persistence) docs.

<img src="./website/public/assets/pluggable-storage.svg" alt="Lix runs in-process inside your product, with an arrow to pluggable storage: memory, filesystem, or S3" width="760" />

## Try a hosted repository

Try out [lixray.com](https://lixray.com):

<a href="https://lixray.com"><img src="./website/public/assets/lixray-og.png" alt="LixRay: a repository for your entire company. Works with Claude, OpenAI, and Gemini." width="760" /></a>

## Learn more

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## License

[MIT](./LICENSE)
