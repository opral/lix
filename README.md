<p align="center">
  <img src="https://raw.githubusercontent.com/opral/lix/main/assets/logo.svg" alt="Lix" height="60">
</p>

<h3 align="center">Embeddable version control system</h3>

<p align="center">
  <a href="https://www.npmjs.com/package/@lix-js/sdk"><img src="https://img.shields.io/npm/dw/%40lix-js%2Fsdk?logo=npm&logoColor=red&label=npm%20downloads" alt="weekly downloads on NPM"></a>
  <a href="https://discord.gg/gdMPPWy57R"><img src="https://img.shields.io/discord/897438559458430986?style=flat&logo=discord&labelColor=white" alt="Discord"></a>
  <a href="https://github.com/opral/lix"><img src="https://img.shields.io/github/stars/opral/lix?style=flat&logo=github&color=brightgreen" alt="GitHub Stars"></a>
  <a href="https://x.com/lixCCS"><img src="https://img.shields.io/badge/Follow-@lixCCS-black?logo=x&logoColor=white" alt="X (Twitter)"></a>
</p>

> [!NOTE]
>
> **Lix is in alpha** · [Follow progress to v1.0 →](https://github.com/opral/lix/issues/374)

---

Lix is an **embeddable version control system for files of any format** (DOCX, XLSX, CAD, PDF, JSON) with semantic, per-entity diffs. Branches, merge, and an immutable change history, exposed as SQL, all in-process.

Use it inside a contract editor, a feature-flag service, an artifact registry, an AI-agent platform, a versioned filesystem, or a domain-specific CLI.

> Lix is to version control what DuckDB is to analytics: an embeddable engine with pluggable support for file formats.

- **It's just a library.** `npm install`, import, run. No daemon, no protocol, no remote.
- **Semantic per-entity diffs.** XLSX cells, DOCX clauses, CAD parts. Not line-by-line text.
- **History is SQL.** Diffs, blame, and audit are direct queries against `lix_change`.

The entity foundation ships today. A plugin API is on the [roadmap](#roadmap); once it lands, anyone can author a plugin that turns a file format (DOCX, XLSX, CAD, PDF, anything else) into entities.

[How does Lix compare to Git? →](https://lix.dev/docs/comparison-to-git)

## Getting started

<p>
  <img src="https://cdn.simpleicons.org/javascript/F7DF1E" alt="JavaScript" width="18" height="18" /> JavaScript ·
  <a href="https://github.com/opral/lix/issues/370"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="18" height="18" /> Python</a> ·
  <a href="https://github.com/opral/lix/issues/371"><img src="https://cdn.simpleicons.org/rust/CE422B" alt="Rust" width="18" height="18" /> Rust</a> ·
  <a href="https://github.com/opral/lix/issues/373"><img src="https://cdn.simpleicons.org/go/00ADD8" alt="Go" width="18" height="18" /> Go</a>
</p>

```bash
npm install @lix-js/sdk
```

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix(); // in-memory by default; pass a backend for persistence

// Register a schema for a tracked entity
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [
    JSON.stringify({
      "x-lix-key": "task",
      "x-lix-version": "1",
      "x-lix-primary-key": ["/id"],
      type: "object",
      required: ["id", "title"],
      properties: {
        id: { type: "string" },
        title: { type: "string" },
      },
      additionalProperties: false,
    }),
  ],
);

// Write rows like any SQL table
await lix.execute(
  "INSERT INTO task (id, title) VALUES ($1, $2)",
  ["t1", "Ship v1"],
);

// Every change is journaled; query it with SQL
const changes = await lix.execute(
  "SELECT entity_id, schema_key, snapshot_content FROM lix_change",
);
```

## Semantic change (delta) tracking

Unlike Git's line-based diffs, Lix understands file structure through plugins. Lix sees `price: 10 → 12` or `cell B4: pending → shipped`, not "line 4 changed" or "binary files differ".

### JSON file example

**Before:**
```json
{"theme":"light","notifications":true,"language":"en"}
```

**After:**
```json
{"theme":"dark","notifications":true,"language":"en"}
```

**Git sees:**
```diff
-{"theme":"light","notifications":true,"language":"en"}
+{"theme":"dark","notifications":true,"language":"en"}
```

**Lix sees:**

```diff
property theme:
- light
+ dark
```

### Excel file example

The same approach works for binary formats. With an XLSX plugin, Lix shows cell-level changes:

**Before:**
```diff
  | order_id | product  | status   |
  | -------- | -------- | -------- |
  | 1001     | Widget A | shipped  |
  | 1002     | Widget B | pending |
```

**After:**
```diff
  | order_id | product  | status   |
  | -------- | -------- | -------- |
  | 1001     | Widget A | shipped  |
  | 1002     | Widget B | shipped |
```

**Git sees:**

```diff
-Binary files differ
```

**Lix sees:**

```diff
order_id 1002 status:

- pending
+ shipped
```

## How Lix Works

Lix uses SQL databases as query engine and persistence layer. Virtual tables like `file` and `file_history` are exposed on top:

```sql
SELECT * FROM file_history
WHERE path = '/orders.xlsx'
ORDER BY created_at DESC;
```

When a file is written, a plugin parses it and detects entity-level changes. These changes (deltas) are stored in the database, enabling branching, merging, and audit trails.

```
┌─────────────────────────────────────────────────┐
│                      Lix                        │
│                                                 │
│ ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│ │ Filesystem │ │ Branches │ │ History │ │ ... │ │
│ └────────────┘ └──────────┘ └─────────┘ └─────┘ │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│                  SQL database                   │
│            (SQLite, Postgres, etc.)             │
└─────────────────────────────────────────────────┘
```

[Read more about Lix architecture →](https://lix.dev/docs/architecture)

## Roadmap

- [x] Core API (<v0.5)
- [x] ACID transactions (v0.6)
- [x] Branching, diffing, merging (v0.6)
- [x] SQL API (v0.6)
- [x] Stable physical storage layout (v0.6)
- [ ] Plugin API for file formats (community-authored plugins for DOCX, XLSX, CAD, PDF, …)
- [ ] Merge conflict semantics and resolution
- [ ] Working changes & checkpointing
- [ ] Real-time sync

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
