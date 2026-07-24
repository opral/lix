---
description: Lix is a version control system for every file format. It tracks semantic changes across Markdown, DOCX, XLSX, JSON, PDFs, and custom formats, exposed as SQL.
---

# What is Lix?

Lix is a **version control system for every file format**. It tracks changes across Markdown, DOCX, XLSX, JSON, PDFs, CAD files, and custom formats as semantic entities: a spreadsheet cell, document clause, JSON property, PDF section, CAD part, or application row.

Versions, branches, merge, rollback, and immutable change history are exposed through SQL, so products and tools can bring version control workflows beyond source code.

> Lix makes version control a runtime primitive for files: products can store, query, review, sync, and merge changes without inventing a custom history system.

[See what a semantic diff looks like →](./comparison-to-git.md#what-this-looks-like)

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();

await lix.execute(
  "INSERT INTO lix_file (path, data) VALUES (?, CAST(? AS BYTEA))",
  ["/hello.txt", "hello"],
);

const changes = await lix.execute(
  "SELECT created_at, schema_key, entity_pk FROM lix_change",
);
```

## How it works

Each file format is parsed into **entities**: cells in a spreadsheet, clauses in a document, parts in a CAD drawing. Lix versions those entities. Per-row merge and history fall out for free.

## Where Lix fits

The same `openLix()` powers three different shapes:

**Inside an end-user product.** Lawyers redlining a contract, analysts iterating on a forecast, engineers updating a BOM, designers exploring a layout: give them drafts, review, rollback, and history inside your product UI.

**Inside an AI workflow.** Every agent task gets an isolated workspace; humans or policies review the diff and merge or discard. See [Lix for AI Agents](./lix-for-ai-agents.md).

**As the version-control core for file-based products.** Build a versioned filesystem, an artifact or model registry, a configuration service, a branchable database, or a domain-specific CLI. Lix is the version-control core; you ship the surface.

## Why this matters

Source-code version control works best when text diffs explain the change. Many products edit files where the useful diff is a domain entity instead: a cell, clause, property, section, part, record, or generated output.

Lix gives those files version-control primitives directly:

- **Any file format** can be represented through parser plugins or custom schemas.
- **Semantic changes** are stored per entity instead of only as whole-file snapshots.
- **SQL** is the query interface for application code, AI agents, and tools.
- **Pluggable storage.** Run in-memory, sync a filesystem workspace with `LocalFilesystem`, use a `.lix` SQLite file as an application file format, or implement the [storage interface](./storage.md) to put Lix on Postgres, S3, Cloudflare, IndexedDB, OPFS, or anything transactional and key-value-shaped.
- **ACID transactions** work across files and entities.

No daemon, no protocol, no remote.

## The change-first model

Lix stores changes as data, not snapshots. Typed history reconstructs the
states reachable from a branch, while the global journal records workspace
activity:

```sql
-- Which revisions of this task are reachable from the active branch?
SELECT id, title, lixcol_depth, lixcol_is_deleted
FROM acme_task_history
WHERE id = 't1'
ORDER BY lixcol_depth;
```

Whether the entity is a spreadsheet cell, a document clause, a CAD part, or an
application row, its registered schema supplies the typed SQL surface. Diffs,
undo, audit, blame, and attribution are all SQL. See
[Change History](./history.md).

## Examples of what Lix versions

With parser plugins, Lix can version:

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
