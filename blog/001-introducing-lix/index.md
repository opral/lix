---
date: "2026-01-20"
og:description: "Lix is a universal version control system for any file format. Unlike Git's line-based diffs, Lix understands file structure, showing 'price: 10 → 12' instead of 'line 4 changed'."
---

# Announcing Lix: A universal version control system

## Introduction

Lix is a **universal version control system** that can diff any file format (`.xlsx`, `.pdf`, `.docx`, etc).

Unlike Git's line-based diffs, Lix understands file structure. Lix sees `price: 10 → 12` or `cell B4: pending → shipped`, not "line 4 changed" or "binary files differ". 

This makes Lix the ideal version control layer for AI agents operating on non-code formats.

### Excel file example

An AI agent updates an order status in `orders.xlsx`.


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

### JSON file example

Even for structured text file formats like `.json` lix is tracking semantics rather than line by line diffs. 

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

## AI agents need version control

Changes AI agents make need to be reviewable by humans.

For code, Git solves this: review the diff, reject bad changes, roll back mistakes. 

Lix brings these primitives to any file format, not just text:

- **Reviewable diffs**: See exactly what an agent changed in any file format.
- **Human-in-the-loop**: Agents propose, humans approve.
- **Safe rollback**: Undo mistakes instantly.


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


[Read more about Lix architecture →](https://lix.dev/docs/architecture)

## Why did we built lix? 

Lix was developed alongside [inlang](https://inlang.com), open-source localization infrastructure.

Solving localization requires Git's collaboration model (branches, diffs, merges) but Git only handles text files, in addition to other issues (see ["Git is unsuited for applications"](https://samuelstroschein.com/blog/git-limitations)). 

We had to develop a new version control system that addressed git's limitations inlang ran into. The result is Lix, now at over [90k weekly downloads on NPM](https://www.npmjs.com/package/@lix-js/sdk).

![90k weekly npm downloads](./npm-downloads.png)

## What’s next

- **Faster writes**: Move write handling fully into the SQL preprocessor ([RFC 001](https://lix.dev/rfc/001-preprocess-writes)).
- **Multi-language bindings**: Rewrite the core in Rust for better parsing, validation, and bindings beyond JS ([RFC 002](https://lix.dev/rfc/002-rewrite-in-rust)).

## Join the community

- ⭐ [Star the lix repo on GitHub](https://github.com/opral/lix)
- 💬 [Chat on Discord](https://discord.gg/gdMPPWy57R)
