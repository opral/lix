---
date: "2026-01-16"
og:description: "Lix is a universal version control system for any file format. Unlike Git's line-based diffs, Lix understands file structure, showing 'price: 10 → 12' instead of 'line 4 changed'."
---

# Announcing Lix: A universal version control system

## Introduction

Lix is a **universal version control system** that can track changes in any file format.

Unlike Git's line-based diffs, Lix understands file structure. Lix sees `price: 10 → 12` or `cell B4: pending → shipped`, not "line 4 changed" or "binary files differ". This makes Lix an ideal version control layer for AI agents operating on non-code formats.

### JSON example

An agent changes `theme` in `settings.json`.

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

**Lix sees (illustrative):**
```
settings.json
  property "theme": "light" → "dark"
```

### Excel example

An agent updates an order status in `orders.xlsx`.

**Before:**
```
| order_id | product    | status  |
|----------|------------|---------|
| 1001     | Widget A   | shipped |
| 1002     | Widget B   | pending |
```

**After:**
```
| order_id | product    | status  |
|----------|------------|---------|
| 1001     | Widget A   | shipped |
| 1002     | Widget B   | shipped |
```

**Git sees:** `Binary files differ`

**Lix sees:**
```
orders.xlsx
  order_id 1002 → status: "pending" → "shipped"
```

## AI agents need version control

AI agents modifying files need guardrails.

Git provides agents with guardrails for text files: review the diff, reject bad changes, roll back mistakes. 

Lix brings the same guardrails to any file format and agent outside of software engineering:

- **See what changed**: Agent edits to spreadsheets, JSON, or other structured files are reviewable.
- **Humans stay in control**: Agents propose changes; people decide what ships.
- **Safe experimentation**: Agents work in isolated branches, not on production data.

![AI agent changes need to be visible and controllable](./ai-agents-guardrails.png)


[Learn more about using Lix with agents →](/docs/lix-for-ai-agents/)


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

const lix = await openLix({
  environment: new InMemorySQLite()
});

await lix.db.insertInto("file").values({ path: "/hello.txt", data: ... }).execute();

const diff = selectWorkingDiff({ lix })
```


## How does Lix work?

Lix adds a version control system on top of SQL databases.

The Lix SDK exposes virtual tables like `file`, `file_history` that are queryable with plain SQL. 

**Why this matters:**

- **Lix doesn't reinvent databases** — durability, ACID, and corruption recovery are handled by battle-tested SQL databases.
- **Full SQL support** — query your version control system with the same SQL.
- **Can runs in your existing database** — no separate storage layer to manage. 



```
┌─────────────────────────────────────────────────┐
│                      Lix                        │
│           (version control system)              │
│                                                 │
│ ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│ │ Filesystem │ │ Branches │ │ History │ │ ... │ │
│ └────────────┘ └──────────┘ └─────────┘ └─────┘ │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│                  SQL database                   │
└─────────────────────────────────────────────────┘
```

> [!NOTE]
> Lix targets SQLite at the moment. [Upvote issue #372 for Postgres support →](https://github.com/opral/lix/issues/372)


### How does lix detect changes?

Inserts and updates to virtual tables like `file` are forwarded to plugins. Plugins parse the file and emit structured changes.

Each plugin defines one or more **entities**, the smallest piece of data that can be independently created, updated, or deleted:

- JSON → property
- CSV → row
- Excel → cell, row, columns

```
File:                                       Lix:
┌────────────────────┐                      ┌────────────────────┐
│ { "price": 10 }    │    ┌──────────┐      │ property "price"   │
│        ↓           │───▶│   JSON   │───▶  │ changed: 10 → 12   │
│ { "price": 12 }    │    │  Plugin  │      └────────────────────┘
└────────────────────┘    └──────────┘
```

## Background

Lix was developed alongside [inlang](https://inlang.com), open-source localization infrastructure.

Solving localization requires Git's collaboration model (branches, diffs, merges) but Git only handles text files, in addition to other issues (see ["Git is unsuited for applications"](https://samuelstroschein.com/blog/git-limitations)). We even explored [designing git compatible file formats](https://github.com/opral/inlang/issues/3965) just to make Git's merge work. 

We had to develop a new version control system that addressed git's limitations inlang ran into. The result is Lix, now at over [90k weekly downloads on NPM](https://www.npmjs.com/package/@lix-js/sdk). 

![90k weekly npm downloads](./npm-downloads.png)

## What’s next

- **Faster writes**: Move write handling fully into the SQL preprocessor ([RFC 001](https://lix.dev/rfc/001-preprocess-writes)).
- **More robust engine + multi-language bindings**: Rewrite the core in Rust for better parsing, validation, and bindings beyond JS ([RFC 002](https://lix.dev/rfc/002-rewrite-in-rust)).
- **Broader backends**: The preprocessor-first design unlocks future Postgres support ([tracking issue #372](https://github.com/opral/lix/issues/372)).

## Join the community

- ⭐ [Star the lix repo on GitHub](https://github.com/opral/lix)
- 💬 [Chat on Discord](https://discord.gg/gdMPPWy57R)