<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/website/public/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">An embeddable version control system for AI agents</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

Lix is an **embeddable version control system** you import as a library. Give agents versions, checkpoints, semantic change history, rollback, immutable history, and SQL-queryable context without wrapping Git or managing repo internals.

- **Runs in-process.** Import it as a library and run it inside your app. No daemon, no protocol.
- **ACID transactions.** One transaction can cover state, blobs, and history.
- **Semantic changes.** Track XLSX rows, DOCX clauses, JSON properties, and more as entities.
- **SQL interface.** Agents can query history and changes without rereading whole files.
- **Bring your own backend.** Start in memory, then plug into SQLite, Postgres, S3, Cloudflare, or your own adapter.

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
import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
  backend: new SqliteBackend({ path: "app.lix" }),
});

await lix.fs.writeFile("/notes/status.txt", new TextEncoder().encode("draft"));

const main = await lix.activeBranchId();

const draft = await lix.createBranch({ name: "Explore" });

await lix.switchBranch({ branchId: draft.id });

await lix.fs.writeFile(
  "/notes/status.txt",
  new TextEncoder().encode("ready for review"),
);

await lix.switchBranch({ branchId: main });

const changes = await lix.execute(
  "SELECT schema_key, count(*) AS count FROM lix_change GROUP BY schema_key",
);
```

## Why Lix?

### Git was not designed to be embedded

AI agents are creating explosive demand for version control: isolated workspaces, checkpoints, versions, reviewable changes, and rollback.

Teams reach for Git, but wrapping it means managing repository directories, worktrees, locks, packfiles, garbage collection, LFS, process calls, protocol servers, and transaction coordination around a tool that expects to live outside the app.

Lix is built the other way around: version control runs in-process inside your app.

[How does Lix compare to Git? →](https://lix.dev/docs/comparison-to-git)

### What Lix provides

#### Import as a library

Import Lix and open it inside your app. No daemon, no protocol.

```ts
import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
  backend: new SqliteBackend({ path: "app.lix" }),
});
```

#### ACID transactions

Write files, blobs, and history in one transaction.

```ts
const tx = await lix.beginTransaction();

try {
  await tx.fs.writeFile("/spec.docx", body);
  await tx.fs.writeFile("/spec.png", image);
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

#### Parallel branches. No worktrees.

Give every agent its own isolated branch without creating Git-style multi-checkout worktrees.

```ts
const main = await lix.activeBranchId();

const copy = await lix.createBranch({ name: "Copy draft" });
const pricing = await lix.createBranch({ name: "Pricing draft" });
const qa = await lix.createBranch({ name: "QA draft" });

await lix.switchBranch({ branchId: copy.id });
await lix.fs.writeFile("/landing.md", copyDraft);

await lix.switchBranch({ branchId: pricing.id });
await lix.fs.writeFile("/plans.json", priceModel);

await lix.switchBranch({ branchId: qa.id });
await lix.fs.writeFile("/checks/report.json", testRun);

await lix.switchBranch({ branchId: main });
```

#### Semantic changes

Unlike Git's line-based diffs, Lix can track structured entities: XLSX rows, DOCX clauses, JSON properties, app records, and more.

```ts
const changes = await lix.execute(`
  SELECT created_at, schema_key, entity_pk, snapshot_content
  FROM lix_change
  ORDER BY created_at DESC
  LIMIT 20
`);
```

For example, an agent edits an orders spreadsheet:

```text
Before:
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | pending |

After:
| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | shipped |
```

Git can only tell you the file changed:

```diff
-Binary files differ
```

Lix can expose the row field that changed:

```diff
order_id 1002 status:

- pending
+ shipped
```

[Read more about semantic changes →](https://lix.dev/docs/semantic-changes)

#### SQL interface

Agents burn fewer tokens and keep cleaner context when version-control questions are answered with SQL instead of whole-file rereads.

<img src="./website/public/assets/claude-sql-question.svg" alt="Claude Code asks: Which orders changed status in this branch? Executing SQL" width="460" />

```ts
const rows = await lix.execute(`
  SELECT created_at, schema_key, entity_pk, snapshot_content
  FROM lix_change
  ORDER BY created_at DESC
  LIMIT 20
`);
```

Every change, across every file and every branch, is a row in `lix_change`. Filter by branch, file, schema, or time without re-reading whole files.

#### Bring your own backend

Start in memory, then plug Lix into the infrastructure your app already runs.

<p><img src="https://cdn.simpleicons.org/sqlite/003B57" alt="SQLite" width="18" height="18" /> SQLite · <img src="https://cdn.simpleicons.org/postgresql/4169E1" alt="Postgres" width="18" height="18" /> Postgres · <img src="https://api.iconify.design/logos:aws-s3.svg" alt="S3" width="18" height="18" /> S3 · <img src="https://cdn.simpleicons.org/cloudflareworkers/F38020" alt="Cloudflare Workers" width="18" height="18" /> Cloudflare Workers · <img src="https://cdn.simpleicons.org/supabase/3FCF8E" alt="Supabase" width="18" height="18" /> Supabase</p>

```ts
const lix = await openLix({
  backend: new SqliteBackend({ path: "app.lix" }),
});
```

## How Lix works

Lix runs in-process inside your app.

It owns the version-control model: files, blobs, versions, history, transactions, and semantic changes. You plug it into whatever backend you need: in-memory, SQLite, Postgres, S3, Cloudflare, or your own adapter.

SQL is the query interface on top. Agents can ask what changed without rereading whole files.

```
┌─────────────────────────────────────────────────┐
│                  Your runtime                   │
│        agent worker · server · CLI · app         │
│                                                 │
│   ┌─────────────────────────────────────────┐   │
│   │                  Lix                    │   │
│   │  Filesystem · Versions · History · SQL  │   │
│   └────────────────────┬────────────────────┘   │
│                        │                        │
└────────────────────────┼────────────────────────┘
                         ▼
┌─────────────────────────────────────────────────┐
│                    Backend                      │
│      SQLite, Postgres, S3, Cloudflare, custom   │
└─────────────────────────────────────────────────┘
```

[Read more about Lix architecture →](https://lix.dev/docs/architecture)

## What you can build with Lix

- **AI agent filesystems** - isolated workspaces, versioned explore steps, semantic change history, and rollback when a run goes sideways.
- **Version control for Postgres & SQLite** - time-travel and versioned schemas on top of an existing database. Reviewable migrations. Diffable rows.
- **Apps with version control** - add versions, review, rollback, and history to editors, CMSs, design tools, internal ops apps, and AI-native products.
- **Review for AI-generated changes** - surface what an agent actually changed at the entity level. Approve, request edits, or revert by symbol instead of patch.

## Roadmap

**v0.6: Lix SDK (current)**

- [x] Importable SDK
- [x] ACID transactions across state, blobs, and history
- [x] Parallel sessions and versions
- [x] Entity-level change tracking, queryable via SQL
- [x] Stable physical storage layout
- [x] Pluggable backend interface

**v0.7: Lix CLI**

- [ ] CLI for creating, inspecting, and scripting Lix repositories

**v0.8: file plugin API**

- [ ] Finalized file plugin API for DOCX, XLSX, CAD, PDF, and code

**v0.9: merge conflicts**

- [ ] Merge conflicts as first-class citizens

**v0.10: working changes**

- [ ] Working changes and checkpointing

## Learn More

- **[Getting Started Guide](https://lix.dev/docs/getting-started)** - Build your first app with Lix
- **[Documentation](https://lix.dev/docs)** - Full API reference and guides
- **[Discord](https://discord.gg/gdMPPWy57R)** - Get help and join the community
- **[GitHub](https://github.com/opral/lix)** - Report issues and contribute

## Blog posts

- [Introducing Lix: An embeddable version control system](https://lix.dev/blog/introducing-lix)
- [What if a Git SDK to build apps exists?](https://samuelstroschein.com/blog/what-if-a-git-sdk-exists)
- [Git is unsuited for applications](https://samuelstroschein.com/blog/git-limitations)
- [Does a git-based architecture make sense?](https://samuelstroschein.com/blog/git-based-architecture)

## License

[MIT](https://github.com/opral/lix/blob/main/packages/lix-sdk/LICENSE)
