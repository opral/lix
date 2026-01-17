---
date: "2026-01-16"
og:description: "Lix is a universal version control system for data files. Unlike Git's line-based diffs, Lix understands file structure—showing 'price: 10 → 12' instead of 'line 4 changed'."
---

# Announcing Lix: A universal version control system

## Introduction

Lix is a **universal version control system** that works beyond text files.

Instead of tracking lines of text like Git, Lix understands the _structure_ of each file format. A change in a JSON file shows `price: 10 → 12`, not "line 4 changed". A change in a spreadsheet shows `cell B4: pending → shipped`. Git can store files like `.xlsx`, but diffs are effectively opaque because they're binary.

This makes Lix an ideal version control layer for AI agents that operate on non-code file formats.

[Getting started →](https://lix.dev/docs/getting-started)

## AI agents need version control

Software engineers trust AI coding assistants because Git provides guardrails: review the diff, reject bad changes, roll back mistakes. The code stays under human control.

AI agents are now modifying files beyond code—spreadsheets, documents, PDFs. These agents need the same guardrails, but Git can't provide them.

**Lix brings the same primitives software engineers rely on (branches, diffs, merges) to any file format.**

![AI agent changes need to be visible and controllable](./ai-agents.svg)

- **See what changed**: Agent edits to spreadsheets, JSON, or any structured file are reviewable.
- **Humans stay in control**: Agents propose changes; people decide what ships.
- **Safe experimentation**: Agents work in isolated branches, not on production data.

[Learn more about using Lix with agents →](https://lix.dev/docs/ai-agents)

## One more thing: Lix is portable

Git is tied to the filesystem. Moving repositories between environments, embedding them in apps, or opening them in browsers requires workarounds.

**Lix fixes that.**

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

## How does lix work?

Lix operates on top of a SQL(ite) database.

Files, change history, branches, and metadata are stored in tables. Lix adds version control primitives (filesystem, branching, history) as a layer on top of SQLite.

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
│               SQL(ite) database                 │
└─────────────────────────────────────────────────┘
```

You interact with Lix through SQL queries: insert a file, query the diff, read the history.

```sql
-- Create a file
INSERT INTO file (path, data)
VALUES ('/settings.json', encode('{"price": 10}'));

-- Update the file
UPDATE file
SET data = encode('{"price": 12}')
WHERE path = '/settings.json';
```

[Upvote issue #372 for Postgres support →](https://github.com/opral/lix/issues/372)

### Detecting changes

Every insert or update to a file is passed to plugins. Plugins parse the file and tell lix what changed.

**Plugins** make lix structure-aware. A plugin tells lix how to parse a file format and what counts as a change. The JSON plugin tracks properties. A CSV plugin tracks rows. An Excel plugin tracks cells. Without plugins, lix would only see binary blobs.

```
File:                                       Lix:
┌────────────────────┐                      ┌────────────────────┐
│ { "price": 10 }    │    ┌──────────┐      │ property "price"   │
│        ↓           │───▶│   JSON   │───▶  │ changed: 10 → 12   │
│ { "price": 12 }    │    │  Plugin  │      └────────────────────┘
└────────────────────┘    └──────────┘
```

When you insert or update a file, the plugin parses it and emits changes at the entity level. A change to `{ "price": 10 }` → `{ "price": 12 }` becomes `property "price" changed from 10 to 12`, not `line 1 changed`.

## Background

Lix started as part of [inlang](https://inlang.com), a localization infrastructure. Inlang needed a universal version control system that works for translators, designers, and other non-developers. (Read ["Git-based architecture" →](https://samuelstroschein.com/blog/git-based-architecture))

The first version of lix was built on git, but extending git to support arbitrary file formats didn't work:

- Git only tracks text files line-by-line
- Making git structure-aware breaks git compatibility
- At that point, extending git provides no benefit over building from scratch

This led to a rewrite from scratch on top of SQLite.

## What's next

- **Faster writes**: Move write handling into the SQL preprocessor to avoid vtable overhead ([RFC 001](https://lix.dev/rfc/001-preprocess-writes)).
- **More robust engine + multi-language bindings**: Implement the core engine in Rust for better parsing/validation and bindings beyond JS ([RFC 002](https://lix.dev/rfc/002-rewrite-in-rust)).
- **Broader backends**: Preprocessor-first design unlocks future Postgres support ([tracking issue #372](https://github.com/opral/lix/issues/372)).
