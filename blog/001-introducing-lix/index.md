---
date: "2026-01-20"
og:description: "Lix is a version control system you import as a library. It records semantic changes to enable diffs, reviews, rollback, and querying of edits."
---

# Introducing Lix: An embeddable version control system

Lix is an **embeddable version control system** that can be imported as a library. Use lix, for example, to enable human-in-the-loop workflows for AI agents like diffs and reviews.

- **It's just a library** — Lix is a library you import. Get branching, diff, rollback in your existing stack
- **Tracks semantic changes** — diffs, blame, and history are queryable via SQL
- **Approval workflows for agents** — agents propose changes in isolated versions, humans review and merge

![AI agent changes need to be visible and controllable](./ai-agents-guardrails.png)

> [!TIP]
> Lix does not replace Git. [Read how Lix compares to Git →](https://lix.dev/docs/comparison-to-git)

## Semantic change tracking

Lix doesn't track line-by-line text changes. It tracks **semantic changes** at the entity level via plugins.

A plugin parses a format (or a piece of app state) into structured entities. Then Lix stores **what changed** — not just which bytes differ.

**Before:**
```json
{"theme":"light","notifications":true,"language":"en"}
```

**After:**
```json
{"theme":"dark","notifications":true,"language":"en"}
```

**Git tracks:**
```diff
-{"theme":"light","notifications":true,"language":"en"}
+{"theme":"dark","notifications":true,"language":"en"}
```

**Lix tracks:**
```diff
property theme:
- light
+ dark
```

### Excel file example

With an XLSX plugin (not shipped yet), Lix can show a cell-level diff like:
This is exactly the kind of semantic surface plugins define: cells vs formulas vs styling.

**Before:**

| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | pending |

**After:**

| order_id | product  | status  |
| -------- | -------- | ------- |
| 1001     | Widget A | shipped |
| 1002     | Widget B | shipped |

**Git tracks:**
```diff
-Binary files differ
```

**Lix tracks:**
```diff
order_id 1002 status:
- pending
+ shipped
```

The same approach extends to any other format your product cares about — **as long as there’s a plugin** that can interpret it.

## How does Lix work?

Lix is **change-first**: it stores semantic changes as queryable data, not snapshots.

That means audit trails, rollbacks, and “blame” become simple queries:

```sql
SELECT *
FROM state_history
WHERE entity_id = 'settings.theme'
ORDER BY depth ASC;
```

Lix uses existing SQL databases as both **query engine** and **persistence layer**.

Plugins parse files (including binary formats) into "meaningful changes" e.g. cells, properties, whitespace, etc. Lix stores those changes as rows in virtual tables like `file`, `file_history`, and `state_history`.

Why this matters:

- **Doesn't reinvent databases** — durability, ACID, and recovery come from proven SQL engines.
- **SQL API for changes** — query diffs, history, and audit trails directly.
- **Portable** — runs on SQLite, Postgres, or other SQL databases.

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

This means: no separate infrastructure to manage, and no “special” datastore just for version control.

## Plugins (format support)

Lix’s format support depends on plugins. Here’s the current status:

| Format | Plugin | Status |
| ------ | ------ | ------ |
| JSON | `@lix-js/plugin-json` | Stable |
| CSV | `@lix-js/plugin-csv` | Stable |
| Markdown | `@lix-js/plugin-md` | Beta |
| ProseMirror | `@lix-js/plugin-prosemirror` | Stable |

**Building your own plugin:** take an off-the-shelf parser for your format, map it to Lix’s entity/change schema, and you get semantic diffs + history for that format. [Plugin documentation →](https://lix.dev/docs/plugins)

## Why did we build Lix?

Lix was developed alongside [inlang](https://inlang.com), open-source localization infrastructure.

We needed version control **as a library**, not as an external tool. Git's architecture didn't fit: we needed database semantics (transactions, ACID), queryable history, and semantic diffing. [Read more →](https://samuelstroschein.com/blog/git-limitations)

The result is Lix, now at over [90k weekly downloads on NPM](https://www.npmjs.com/package/@lix-js/sdk).

![Weekly npm downloads](./npm-downloads.png)


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
import { openLix, selectWorkingDiff } from "@lix-js/sdk";

const lix = await openLix({
  environment: new InMemorySQLite()
});

await lix.db.insertInto("file").values({ path: "/hello.txt", data: ... }).execute();

const diff = await selectWorkingDiff({ lix }).selectAll().execute();
```

## What's next

The next version of Lix will be a refactor to be purely "preprocessor" based. This makes Lix easier to embed anywhere and enables:

- **Fast writes** ([RFC 001](/rfc/001-preprocess-writes))
- **Any SQL database** (SQLite, Postgres, Turso, MySQL)
- **SDKs for Python, Rust, Go** ([RFC 002](/rfc/002-rewrite-in-rust))

```
                      ┌────────────────┐
  SELECT * FROM ...   │  Lix Engine    │   SELECT * FROM ...
 ───────────────────▶ │    (Rust)      │ ───────────────────▶  Database
                      └────────────────┘
```

### Join the community

- ⭐ [Star the lix repo on GitHub](https://github.com/opral/lix)
- 💬 [Chat on Discord](https://discord.gg/gdMPPWy57R)
