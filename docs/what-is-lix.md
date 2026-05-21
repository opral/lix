---
description: Lix is an embeddable version control system for files of any format. Diffs are semantic and per entity (which cells changed in a spreadsheet, which clauses moved in a contract), exposed as SQL, all in-process.
---

# What is Lix?

Lix is an **embeddable version control system for files of any format**. A spreadsheet diff tells you which cells changed. A contract diff tells you which clauses moved. A CAD diff tells you which parts changed. Lix diffs files **semantically, per entity**, across DOCX, XLSX, CAD, PDF, JSON, and any format with a parser plugin.

Versions, merge, and an immutable change history, exposed as SQL, all running in-process inside your program.

> Lix is to version control what DuckDB is to analytics: an embeddable engine with pluggable support for file formats.

[See what a semantic diff looks like →](./comparison-to-git.md#what-this-looks-like)

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();

await lix.execute(
  "INSERT INTO lix_file (id, path, data, hidden) VALUES ($1, $2, lix_text_encode($3), false)",
  ["hello-file", "/hello.txt", "hello"],
);

const changes = await lix.execute(
  "SELECT created_at, schema_key, entity_pk FROM lix_change",
);
```

## How it works

Each file format is parsed into **entities**: cells in a spreadsheet, clauses in a document, parts in a CAD drawing. Lix versions those entities. Per-row merge and history fall out for free.

**Status:** the entity foundation ships today. Register a JSON Schema, write rows through SQL, version structured data end-to-end. A plugin API for file formats is on the [roadmap](https://github.com/opral/lix#roadmap); once it lands, anyone can author a plugin that turns a format (XLSX, DOCX, CAD, PDF, anything else) into entities, and the same primitives apply.

## Three shapes

The same `openLix()` powers three different shapes:

**A library inside an end-user product.** Lawyers redlining a contract, analysts iterating on a forecast, engineers updating a BOM, designers exploring a layout: give them Git-like drafts, review, and rollback inside your product UI, no terminal in sight.

**A library inside an AI agent platform.** Every agent task gets an isolated workspace; humans or policies review the diff and merge or discard. See [Lix for AI Agents](./lix-for-ai-agents.md).

**The engine of an infrastructure product.** Build a versioned filesystem, an artifact or model registry, a configuration service, a Git-style branchable database, or a domain-specific CLI. Lix is the version-control core; you ship the surface.

## Why embed it

Git's diff model is line-based on text, so it doesn't surface meaningful changes for binary or structured files (DOCX, XLSX, CAD). Git is also CLI-driven and operates outside your process, which makes it awkward for runtime data, programmatic edits, or end-user workflows that aren't a developer at a terminal.

Lix is the opposite shape:

- A **library** you import; call it from an app, a service, a CLI, or another database engine.
- **Pluggable storage.** Run in-memory, persist to a `.lix` SQLite file, or implement the [backend interface](./backend.md) to put Lix on Postgres, S3, Cloudflare, IndexedDB, OPFS, or anything transactional and key-value-shaped.
- **SQL** as the query interface, for application code, AI agents, and tools.
- **ACID** transactions across files and entities.

No daemon, no protocol, no remote.

## The change-first model

Lix stores changes as data, not snapshots. One immutable journal across every entity, every version:

```sql
-- What does this version see right now?
SELECT entity_pk, schema_key, snapshot_content
FROM lix_state_history
WHERE start_commit_id = lix_active_version_commit_id()
  AND depth = 0
ORDER BY schema_key, entity_pk;
```

Whether the entity is a spreadsheet cell, a document clause, a CAD part, or an application row, the surface is the same. Diffs, undo, audit, blame, and attribution are all SQL. See [Change History](./history.md).

## Examples of what Lix versions

Once the plugin API lands and people start writing plugins:

- DOCX contracts, with clause-level diffs and redlines
- XLSX models, with cell-level history and conflict-aware merges
- CAD drawings, with per-part revision tracking
- PDFs and any other format behind a parser plugin

Available today through the entity foundation:

- Application state: tasks, line items, translations, CMS sections, model metadata, config keys
- Anything you can describe with a JSON Schema

## Next

- [Getting Started](./getting-started.md): install, register a schema, version, merge.
- [Comparison to Git](./comparison-to-git.md): when to reach for which.
- [Lix for AI Agents](./lix-for-ai-agents.md): one shape, in depth.
- [Schemas](./schemas.md), [Versions & Merging](./versions.md), [Change History](./history.md), [Persistence](./persistence.md), [SQL Functions](./sql-functions.md).
