---
date: "2026-01-16"
og:description: "Lix is a universal version control system for any file format. Unlike Git's line-based diffs, Lix understands file structure, showing 'price: 10 → 12' instead of 'line 4 changed'."
---

# Announcing Lix: A universal version control system

## Introduction

Lix is a **universal version control system** that can track changes in any file format.

Unlike Git's line-based diffs, Lix understands file structure. You see `price: 10 → 12` or `cell B4: pending → shipped`, not "line 4 changed" or "binary files differ". This makes Lix an ideal version control layer for AI agents operating on non-code formats.

<p>
  <img src="https://cdn.simpleicons.org/javascript/F7DF1E" alt="JavaScript" width="18" height="18" /> JavaScript ·
  <a href="https://github.com/opral/lix/issues/370"><img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="18" height="18" /> Python</a> ·
  <a href="https://github.com/opral/lix/issues/371"><img src="https://cdn.simpleicons.org/rust/CE422B" alt="Rust" width="18" height="18" /> Rust</a> ·
  <a href="https://github.com/opral/lix/issues/373"><img src="https://cdn.simpleicons.org/go/00ADD8" alt="Go" width="18" height="18" /> Go</a>
</p>

```bash
npm install @lix-js/sdk
```

> [!NOTE]
> The API is work in progress. Expect breaking changes before v1.0.

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({});

await lix.db.insertInto("file").values({ path: "/hello.txt", data: ... }).execute();

const diff = selectWorkingDiff({ lix })
```

## Example

### JSON: structure-aware diffs

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

**Lix sees:**
```
settings.json
  property "theme": "light" → "dark"
```

### Excel: cell-level changes

An agent updates an order status in `orders.xlsx`.

**Git sees:** `Binary files differ`

**Lix sees:**
```
orders.xlsx
  cell (row: order_id=1002, column: status): "pending" → "shipped"
```

You can review exactly what the agent did, approve it, or roll it back. Just like code review, but for data.

This difference, bytes vs meaning, is exactly the guardrail AI agents need.

## AI agents need version control

![AI agent changes need to be visible and controllable](./ai-agents.svg)

Software engineers trust AI coding assistants because Git provides guardrails: review the diff, reject bad changes, roll back mistakes. Changes made by coding agents stay under control.

Lix brings the same primitives software engineers rely on (branches, diffs, merges) to any file format and agent outside of software engineering:

- **See what changed**: Agent edits to spreadsheets, JSON, or other structured files are reviewable.
- **Humans stay in control**: Agents propose changes; people decide what ships.
- **Safe experimentation**: Agents work in isolated branches, not on production data.

[Learn more about using Lix with agents →](/docs/lix-for-ai-agents/)

## Lix is portable

To work in agent runtimes (browser, sandbox, serverless), version control must be portable by design.

A Lix repository is a single SQLite file that is:

- **Easy to move**: Copy, backup, or transfer like any other file.
- **Storable anywhere**: S3, embedded in an app, or on disk.
- **Runnable anywhere**: Browser, server, serverless, sandbox.

```
                            .____________.
                            |            |
                            |            |
                            |            |
                            | lix.sqlite |
                            |            |
                            |            |
                            |____________|
                                  │
          ┌───────────┬───────────┼───────────┬───────────┐
          ▼           ▼           ▼           ▼           ▼
     ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌────────────┐
     │   S3    │ │ Browser │ │ Sandbox │ │ Filesystem │  ...
     └─────────┘ └─────────┘ └─────────┘ └────────────┘
```

This design is a direct response to Git’s model. Git assumes a local filesystem and exposes a CLI, not an SDK. That model doesn’t embed well in browsers, sandboxes, or as a single portable artifact. We needed a version control system that can run in the browser, locally on a user’s machine, and on the server. SQLite’s embedded design enables Lix to run everywhere.

## How does Lix work?

Under the hood, Lix stores everything in SQLite tables. Most users interact through an SDK; SQL is the underlying interface.

Files, change history, branches, and metadata live in tables. Lix adds version control primitives (filesystem semantics, branching, and history) on top of SQLite.

```
┌─────────────────────────────────────────────────┐
│                      Lix                        │
│           (version control system)              │
│                                                 │
│ ┌────────────┐ ┌──────────┐ ┌─────────┐ ┌─────┐ │
│ │ Filesystem │ │ Branching│ │ History │ │ ... │ │
│ └────────────┘ └──────────┘ └─────────┘ └─────┘ │
└────────────────────────┬────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────┐
│               SQLite database                   │
└─────────────────────────────────────────────────┘
```

You interact with Lix through the SDK. The current version of lix exposes a SQL interface. Future versions might have a direct `lix.fs.write_file()` API. 

> [!NOTE]
> The API is work in progress. Expect breaking changes before v1.0.

```ts
// Create a file
await lix.db.insertInto("file")
  .values({ path: "/settings.json", data: encode('{"price": 10}') })
  .execute();

// Update the file
await lix.db.updateTable("file")
  .set({ data: encode('{"price": 12}') })
  .where("path", "=", "/settings.json")
  .execute();
```

Under the hood, lix maps incoming SQL queries to native tables of the database.

[Upvote issue #372 for Postgres support →](https://github.com/opral/lix/issues/372)

### Detecting changes

Every insert or update is passed to plugins. Plugins parse the file and emit structured changes.

**Plugins** make Lix structure-aware. Each plugin defines a *trackable unit* for a file format, called an **entity**: the smallest piece of data that can be independently created, updated, or deleted.

- JSON → property  
- CSV → row  
- Excel → cell  

This is what powers entity-aware diffs: Lix answers *what changed* at the level that matters for each format.

```
File:                                       Lix:
┌────────────────────┐                      ┌────────────────────┐
│ { "price": 10 }    │    ┌──────────┐      │ property "price"   │
│        ↓           │───▶│   JSON   │───▶  │ changed: 10 → 12   │
│ { "price": 12 }    │    │  Plugin  │      └────────────────────┘
└────────────────────┘    └──────────┘
```

## Background

This architecture didn't start as a research project. It emerged from shipping real systems.

Lix started as part of [inlang](https://inlang.com), open-source localization infrastructure.

To make localization work for translators, designers, and non-developers, we needed version control that worked beyond text files. That led to the idea of a universal, structure-aware version control system, outlined in the RFC for ["Git-based architecture"](https://samuelstroschein.com/blog/git-based-architecture).

### Git was too limiting

The first version of Lix was built on Git, but extending Git to support arbitrary file formats didn’t work:

- Git can store any file, but it's line-based and treats non-text formats as opaque blobs. You can't ask "what changed in cell C45?"
- Making Git structure-aware breaks compatibility, at which point Git stops providing its core benefits

### SQLite to the rescue

Those limitations led to a rewrite from scratch on top of SQLite.

SQLite provided transactions, custom data structures, and a query engine out of the box. Early versions relied heavily on [SQLite’s virtual table](https://www.sqlite.org/vtab.html) mechanism to intercept reads and writes. While this worked, we couldn’t achieve the performance and optimizer behavior we needed at scale.

Because virtual tables were only used to intercept reads and writes, the next iteration of Lix became a SQL preprocessor that rewrites incoming queries to native SQLite tables. See [RFC 001](https://lix.dev/rfc/001-preprocess-writes) for details.

## What’s next

- **Faster writes**: Move write handling fully into the SQL preprocessor ([RFC 001](https://lix.dev/rfc/001-preprocess-writes)).
- **More robust engine + multi-language bindings**: Rewrite the core in Rust for better parsing, validation, and bindings beyond JS ([RFC 002](https://lix.dev/rfc/002-rewrite-in-rust)).
- **Broader backends**: The preprocessor-first design unlocks future Postgres support ([tracking issue #372](https://github.com/opral/lix/issues/372)).

[Get started with Lix →](/docs/getting-started)
