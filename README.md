<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/website/public/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">Database + version control system in one</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

Lix stores files as rows. Every row is version controlled. That gives you files any tool can open, a database you can query, and version control across all of it, from one mechanism.

Agents want files. Your product wants a database. Lix is both in one system.

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

await lix.execute(
	"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
	["/notes/status.txt", new TextEncoder().encode("draft")],
);

const main = await lix.activeBranchId();

const draft = await lix.createBranch({ name: "Explore" });

await lix.switchBranch({ branchId: draft.id });

await lix.execute(
	"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
	["/notes/status.txt", new TextEncoder().encode("ready for review")],
);

await lix.switchBranch({ branchId: main });

const changes = await lix.execute(
	"SELECT schema_key, count(*) AS count FROM lix_change GROUP BY schema_key",
);
```

## Files × database

Lix does not store files as opaque blobs. A plugin parses each file format into SQL rows, one row per entity: a paragraph, a cell, a JSON property.

```
what you see               what lix stores
──────────────             ──────────────────────────────
                           entity      property   value
/orders.xlsx  ◂─plugin─▸   row 1001    status     shipped
                           row 1002    status     pending
```

Every row is version controlled. Everything else follows from this. A query is SQL over rows. A diff is a row comparison: "row 1002: pending → shipped". A merge is a row merge. History is rows over time. And because the file is materialized back from the rows, it stays an ordinary file that any tool can open.

## Comparison

You are building a product that revolves around files. Your options:

1. **Git.** Version control: yes. Database: no. Files are blobs. You cannot query them.
2. **Postgres.** Database: yes. Version control: no. You build revision tables and audit logs yourself.
3. **Lix.** Both. Files stay files, content is queryable rows, every change is version controlled.

Options 1 and 2 make you hand-build the missing half.

|                                    | Git                     | Postgres        | Lix                                                        |
| ---------------------------------- | ----------------------- | --------------- | ---------------------------------------------------------- |
| Files                              | ✅                      | ❌ import/export | ✅ files stay files                                         |
| Full database (SQL, ACID, queries) | ❌                      | ✅               | ✅                                                          |
| Version control                    | ⚠️ text files only      | ❌               | ✅ semantic, any file format                                |
| Embeddable                         | ❌ CLI, not a library   | ❌ server        | ✅ in-process, storage is an adapter (filesystem, SQLite, Postgres, S3) |

```
                          database semantics
                      (query · transact · track)
                                 ▲
                                 │
       Postgres / Dolt           │           ★ Lix
       semantics, but your       │      files stay files,
       data lives inside         │      entities get SQL,
       their tables              │      every change tracked
                                 │
   ──────────────────────────────┼──────────────────────────────▶
                                 │       file interoperability
                                 │  (any agent or tool can read/write)
                                 │
                                 │        Filesystem / Git
                                 │      files, but opaque blobs —
                                 │      no queries, no entities
```

[How does Lix compare to Git? →](https://lix.dev/docs/comparison-to-git)

## When to use Lix

Does your product use files? Use Lix.

Editors, agent workspaces, document workflows, spreadsheet tools, config, CAD: if users or agents change files and your product needs to store, query, review, or roll back those changes, Lix is the layer you would otherwise build by hand.

## When not to use Lix

- **No files.** Plain relational app data: use Postgres.
- **Source code.** Use Git. Lix is for every other file your product touches.
- **Blobs you never look inside.** Video archives, backups: use S3.

## What you build with it

### Agents work in files

Agents want a workspace made of files. Any agent or tool can read and write them. Give each agent its own branch and review its changes.

```ts
const main = await lix.activeBranchId();

const copy = await lix.createBranch({ name: "Copy draft" });
const pricing = await lix.createBranch({ name: "Pricing draft" });
const qa = await lix.createBranch({ name: "QA draft" });

await lix.switchBranch({ branchId: copy.id });
await lix.execute(
	"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
	["/landing.md", copyDraft],
);
```

[Flashtype](https://flashtype.ai), a Markdown editor for Claude and Codex, is built on Lix. Agents edit local Markdown files, every edit can be reviewed as a diff and restored from history.

[![Flashtype app preview](https://flashtype.ai/og.png)](https://flashtype.ai)

### Version control for any file format

History, branches, review, rollback. Not just for code. Conflicts happen per entity, not per file: edits to different cells of the same XLSX merge cleanly.

A workflow edits an orders spreadsheet:

```text
Before:
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1002     | Widget B | pending |

After:
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1002     | Widget B | shipped |
```

Git can only tell you the file changed:

```diff
-Binary files differ
```

Lix tells you what changed inside the file:

```diff
order_id 1002 status:

- pending
+ shipped
```

[Read more about semantic changes →](https://lix.dev/docs/semantic-changes)

### A full database

Query changes with SQL. Write files and history in one ACID transaction.

```ts
const rows = await lix.execute(`
  SELECT created_at, schema_key, entity_pk, snapshot_content
  FROM lix_change
  ORDER BY created_at DESC
  LIMIT 20
`);
```

Every change, across every file and every branch, is a row in `lix_change`. Filter by branch, file, schema, or time without rereading whole files.

```ts
const tx = await lix.beginTransaction();

try {
	await tx.execute(
		"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
		["/spec.docx", body],
	);
	await tx.execute(
		"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
		["/spec.png", image],
	);
	await tx.commit();
} catch (error) {
	await tx.rollback();
	throw error;
}
```

### Embeddable, with pluggable storage

Lix runs in-process. No daemon, no protocol. Local-first in the browser or desktop app, or on a server with the client-server protocol. Storage is an adapter:

<p><img src="https://cdn.simpleicons.org/sqlite/003B57" alt="SQLite" width="18" height="18" /> SQLite · <img src="https://cdn.simpleicons.org/postgresql/4169E1" alt="Postgres" width="18" height="18" /> Postgres · <img src="https://api.iconify.design/logos:aws-s3.svg" alt="S3" width="18" height="18" /> S3 · <img src="https://cdn.simpleicons.org/cloudflareworkers/F38020" alt="Cloudflare Workers" width="18" height="18" /> Cloudflare Workers · <img src="https://cdn.simpleicons.org/supabase/3FCF8E" alt="Supabase" width="18" height="18" /> Supabase</p>

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
	storage: new LocalFilesystem({ path: "./workspace", syncAllFiles: true }),
});
```

## How Lix works

Lix runs in-process. It owns the version control model: files, branches, history, transactions, and semantic changes. SQL is the query interface on top. Storage is an adapter underneath. Run it in two shapes:

**Embedded, local-first.** Lix runs inside your app.

```
┌─────────────────────────────────────────┐
│                Your app                 │
│    browser · desktop · CLI · worker     │
│                                         │
│   ┌─────────────────────────────────┐   │
│   │               Lix               │   │
│   │ Filesystem · Versions · History │   │
│   └────────────────┬────────────────┘   │
└────────────────────┼────────────────────┘
                     ▼
       storage: filesystem · SQLite
```

**Client-server.** Lix runs on your server. Apps, agents, and tools connect through the client-server protocol.

```
┌────────┐    ┌────────┐    ┌────────┐
│  app   │    │ agent  │    │  CLI   │
└───┬────┘    └───┬────┘    └───┬────┘
    └─────────────┼─────────────┘
                  │  client-server protocol
         ┌────────▼────────┐
         │   Server + Lix  │
         └────────┬────────┘
                  ▼
     storage: Postgres · S3 · SQLite
```

[Read more about Lix architecture →](https://lix.dev/docs/architecture)

## Where this is going

Code lives in Git because files are simple: every tool can work with them. The rest of a company's work belongs in the same kind of place. One repository. Files, so every tool and agent can work with them. A database, so apps can build on top. Version control, so every change can be reviewed and rolled back.

```
                 ┌────────────────────────────────┐
                 │       one lix repository       │
 contracts.docx  │                                │
 pricing.xlsx    │  files: docs · sheets · data   │  ← any tool reads/writes
 app records     │  SQL: entities · history       │  ← apps & agents query
 agent output    │  branches · review · merge     │  ← every change tracked
                 └────────────────────────────────┘
```

## Learn More

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## License

[MIT](./LICENSE)
