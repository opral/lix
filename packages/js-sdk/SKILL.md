---
name: lix-js-sdk
description: Use this skill when building examples, demos, tests, or applications with @lix-js/sdk — opening a Lix, registering schemas, writing entities through generated SQL tables, branching state into named versions, merging, and querying the change history.
---

# Lix JS SDK Skill

## What Is Lix

Lix is an embeddable version control system. The primary use case is **versioning files of any kind** — `.docx`, CAD drawings, PDFs, JSON catalogs, source code — by parsing them into typed entities on write. Once parsed, the file's contents become rows in your schema tables, and Lix versions, branches, and merges those rows. A registered file plugin owns the parse-and-serialize round trip; the engine handles versioning, branching, three-way merge, and an immutable change journal.

Apps can also use the entity layer **directly**, without files in the picture. That's the appealing path for app-first products: define a schema, write rows, get versioning for free. Same engine, same primitives.

Every row in every registered schema is a tracked entity, every write happens on a named **version** (Lix's word for "branch"), and `mergeVersion` performs a three-way merge over those entities. The whole engine runs in-process, normally against a local SQLite file.

**Merge granularity is per-entity (per-row), not per-field.** Two versions editing *different rows* (or different entities of the same schema) merge cleanly. Two versions editing the *same row* — even on disjoint fields — surface as a conflict today. Field-level merge is on the roadmap; until it ships, model your data as multiple small entities (sections, blocks, paragraphs, message keys, line items) when you want concurrent reviewers to compose without conflict.

**Lix targets non-developers as the primary end-user.** That shapes the vocabulary: what Git calls a "branch" is called a **version** in Lix, because "you can have multiple versions of this document" reads naturally to non-devs while "branch" does not. Use "version" — not "branch" — in user-facing copy, demo names, and example values.

**Where Lix sits next to neighbors:** *not Yjs* (Lix is entity-grained, not character-CRDT), *not Git* (typed rows + a queryable change journal, not text diffs), *not plain SQLite* (branches and merges are primitives, not something you build on top).

## When To Use Lix

Reach for Lix when an app needs branchable, mergeable, auditable structured state:

- **Document / CMS / no-code editors with drafts and review.** A user clicks "Propose changes" → that's `createVersion({ name: "Marketing edit" })`. Reviewers diff via `lix_change`, accept via `mergeVersion`. Like Google Docs "suggesting mode", but durable, branchable, and over typed entities.
- **AI agent sandboxes.** Spawn an agent on its own version, let it mutate freely, the human reviews the diff and merges or discards. Cheap rollback, no shadow tables.
- **Real-time and local-first multiplayer.** Lix already journals every change with author, timestamp, and entity identity — that is exactly what a sync protocol needs. Each peer is a version; sync is merge. Conflicts surface as exceptions instead of silent last-writer-wins.
- **Scenario / what-if branching** in spreadsheets, budgets, OKR planners, pricing tables.
- **Translation / localization workflows.** Each translator works on a version of the message catalog; merges flow back to main.
- **Auditable records** (compliance, clinical, legal). `lix_change` is an immutable journal queryable as SQL — replaces a hand-rolled audit log.

Anti-fits: high-throughput OLTP (payments, telemetry, ad bidding), pure analytics ingest, and read-only caches. Lix tracks every write — that is dead weight when there is nothing to version.

## API Surface

```ts
import { openLix } from "@lix-js/sdk";
import { createBetterSqlite3Backend } from "@lix-js/sdk/sqlite";

const lix = await openLix({
  backend: createBetterSqlite3Backend({ path: "/path/to/file.lix" }),
});
```

`better-sqlite3` is an optional peer dependency of `@lix-js/sdk`; install it in any project that imports `@lix-js/sdk/sqlite`:

```sh
npm i @lix-js/sdk better-sqlite3
```

Use the version of this skill that ships with the installed `@lix-js/sdk` package; do not copy version-specific snippets from older releases.

The default `openLix()` (no `backend`) is in-memory and dies with the process. For anything that needs to persist — demos, scripts, tests, real apps — pass a `better-sqlite3` backend with a real file path. Each successful `execute()` is durable; `lix.close()` releases the backend handle. Reopening with the same path picks up where you left off.

For tests and demos, use an isolated temp dir per run:

```ts
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const dir = mkdtempSync(path.join(tmpdir(), "lix-"));
const lix = await openLix({
  backend: createBetterSqlite3Backend({ path: path.join(dir, "demo.lix") }),
});
```

The handle is intentionally small:

```ts
type Lix = {
  execute(sql: string, params?: readonly unknown[]): Promise<ExecuteResult>;
  activeVersionId(): Promise<string>;
  createVersion(options: { id?: string; name: string }): Promise<{ versionId: string }>;
  switchVersion(options: { versionId: string }): Promise<{ versionId: string }>;
  mergeVersion(options: { sourceVersionId: string }): Promise<MergeVersionResult>;
  close(): Promise<void>;
};
```

Use the public `@lix-js/sdk` API only. Do not import from `engine-wasm`, do not call `initLix` / `createWasmSqliteBackend`, do not open SQLite directly against a `.lix` file.

If behavior is unclear, read source before guessing:

- [packages/js-sdk/src/open-lix.ts](https://github.com/opral/lix/blob/561f92b5bc3fa68e48a863ed3a02129645a57011/packages/js-sdk/src/open-lix.ts) — current JS API.
- [packages/js-sdk/src/open-lix.test.ts](https://github.com/opral/lix/blob/561f92b5bc3fa68e48a863ed3a02129645a57011/packages/js-sdk/src/open-lix.test.ts) — canonical end-to-end flow.
- [packages/js-sdk/src/sqlite/index.ts](https://github.com/opral/lix/blob/561f92b5bc3fa68e48a863ed3a02129645a57011/packages/js-sdk/src/sqlite/index.ts) — `better-sqlite3` backend factory.
- [packages/engine/src/schema/builtin](https://github.com/opral/lix/tree/561f92b5bc3fa68e48a863ed3a02129645a57011/packages/engine/src/schema/builtin) — built-in entity table shapes.
- [packages/engine/src/sql2/udfs](https://github.com/opral/lix/tree/561f92b5bc3fa68e48a863ed3a02129645a57011/packages/engine/src/sql2/udfs) — registered SQL functions.

## Canonical End-To-End Example

This is the demo to imitate. It shows the four things that make Lix Lix in one script: **isolation** (one SELECT against `<schema>_by_version` returns all versions side by side), **clean parallel merges** (two reviewers editing *different entities* both land on Published), **the audit journal** (`lix_change` is a queryable SQL table), and **conflicts** (two versions edit the *same* entity → `mergeVersion` throws).

The schema models a brochure as a list of **section entities** (headline, body, disclaimer) — not one row with multiple fields. That matches Lix's per-row merge granularity: Marketing edits the headline section, Legal edits the disclaimer section, both merge cleanly because they touched different rows.

Note: every registered schema `X` automatically gets a sibling table `X_by_version` exposing a `lixcol_version_id` column. Use it for cross-version reads and writes — you almost never need `switchVersion` for demos or read paths.

```ts
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { openLix } from "@lix-js/sdk";
import { createBetterSqlite3Backend } from "@lix-js/sdk/sqlite";

const dir = mkdtempSync(path.join(tmpdir(), "lix-"));
const lix = await openLix({
  backend: createBetterSqlite3Backend({ path: path.join(dir, "demo.lix") }),
});
const published = await lix.activeVersionId();

// 1. Register a section schema. A brochure is a *collection* of sections.
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [JSON.stringify({
    $schema: "https://json-schema.org/draft/2020-12/schema",
    "x-lix-key": "acme_section",
    "x-lix-version": "1",
    "x-lix-primary-key": ["/id"],
    type: "object",
    required: ["id", "brochure_id", "kind", "text"],
    properties: {
      id:          { type: "string" },
      brochure_id: { type: "string" },
      kind:        { type: "string" }, // "headline" | "body" | "disclaimer"
      text:        { type: "string" },
    },
    additionalProperties: false,
  })],
);

// 2. Seed three sections of the live brochure on Published.
await lix.execute(
  `INSERT INTO acme_section (id, brochure_id, kind, text) VALUES
     ($1,$2,$3,$4),($5,$6,$7,$8),($9,$10,$11,$12)`,
  [
    "s-headline",   "spring-2026", "headline",   "Meet the Acme X1",
    "s-body",       "spring-2026", "body",       "A fast, friendly bike for everyone.",
    "s-disclaimer", "spring-2026", "disclaimer", "Specs subject to change.",
  ],
);

// 3. Create all three reviewer versions up front from the same Published base.
//    (Forking later — after a merge — would change the merge base and hide the conflict.)
const marketing = await lix.createVersion({ name: "Marketing edit" });
const legal     = await lix.createVersion({ name: "Legal review" });
const competing = await lix.createVersion({ name: "Competing headline" });

// Marketing rewrites the headline section.
await lix.execute(
  `UPDATE acme_section_by_version
      SET text = $1
    WHERE id = $2 AND lixcol_version_id = $3`,
  ["The Acme X1 — built for your weekend", "s-headline", marketing.versionId],
);

// Legal rewrites the disclaimer section. Different entity → won't conflict with Marketing.
await lix.execute(
  `UPDATE acme_section_by_version
      SET text = $1
    WHERE id = $2 AND lixcol_version_id = $3`,
  ["Specifications and pricing subject to change. See acme.com/legal.",
   "s-disclaimer", legal.versionId],
);

// Competing rewrites the *same* headline section as Marketing. This will conflict on merge.
await lix.execute(
  `UPDATE acme_section_by_version
      SET text = $1
    WHERE id = $2 AND lixcol_version_id = $3`,
  ["Acme X1: now with carbon fork", "s-headline", competing.versionId],
);

// 4. THE AHA: one SELECT, four versions side by side.
const sideBySide = await lix.execute(
  `SELECT v.name, s.kind, s.text
     FROM acme_section_by_version s
     JOIN lix_version v ON v.id = s.lixcol_version_id
    WHERE s.brochure_id = $1
      AND s.lixcol_version_id IN ($2, $3, $4, $5)
    ORDER BY v.name, s.kind`,
  ["spring-2026", published, marketing.versionId, legal.versionId, competing.versionId],
);
if (sideBySide.kind === "rows") {
  console.log("Same brochure, four versions:");
  let lastVersion = "";
  for (const row of sideBySide.rows.rows) {
    const v = row[0].asText()!;
    if (v !== lastVersion) { console.log(`  [${v}]`); lastVersion = v; }
    console.log(`    ${row[1].asText()!.padEnd(11)} → ${row[2].asText()}`);
  }
}

// 5. Merge Marketing and Legal into Published. Different entities → both succeed.
const m1 = await lix.mergeVersion({ sourceVersionId: marketing.versionId });
const m2 = await lix.mergeVersion({ sourceVersionId: legal.versionId });
console.log(`\nMarketing merge: ${m1.outcome} (+${m1.appliedChangeCount} changes)`);
console.log(`Legal merge:     ${m2.outcome} (+${m2.appliedChangeCount} changes)`);

const finalState = await lix.execute(
  `SELECT kind, text FROM acme_section
    WHERE brochure_id = $1 ORDER BY kind`,
  ["spring-2026"],
);
if (finalState.kind === "rows") {
  console.log("\nPublished, after both merges (Marketing's headline + Legal's disclaimer):");
  for (const row of finalState.rows.rows) {
    console.log(`  ${row[0].asText()!.padEnd(11)} → ${row[1].asText()}`);
  }
}

// 6. The change journal is just a SQL table. Audit log, blame, undo — all queries.
//    snapshot_content currently comes back as TEXT (a JSON string), so JSON.parse it.
const history = await lix.execute(
  `SELECT created_at, entity_id, snapshot_content
     FROM lix_change
    WHERE schema_key = $1
    ORDER BY created_at`,
  ["acme_section"],
);
if (history.kind === "rows") {
  console.log(`\nAudit trail (${history.rows.rows.length} entries):`);
  for (const row of history.rows.rows) {
    const snap = row[2].asText();
    const parsed = snap ? JSON.parse(snap) : null;
    console.log(`  ${row[0].asText()}  ${row[1].asText()}  ${JSON.stringify(parsed)}`);
  }
}

// 7. Try to merge Competing — it edited the same s-headline that Marketing already merged.
//    The engine detects the entity-level conflict and throws.
try {
  await lix.mergeVersion({ sourceVersionId: competing.versionId });
} catch (err) {
  console.log(`\nConflict surfaced (expected): ${(err as Error).message}`);
}

await lix.close();
```

What gets printed, in order:

- One SELECT shows the same brochure across four versions, each section with its own per-version text — versioning lives *in the query layer*.
- Two `mergeCommitted` outcomes with non-zero `appliedChangeCount`.
- A final Published brochure with **Marketing's headline AND Legal's disclaimer** — clean per-row merge.
- An audit trail straight out of `lix_change`, ordered by `created_at`.
- A caught conflict from the version that re-edited Marketing's section.

That output is the elevator pitch. Imitate this shape when building demos: model the domain as **collections of small entities** (sections, blocks, paragraphs, message keys, line items) so reviewers naturally edit different rows; create all versions up front from the same base; use `<schema>_by_version` for cross-version reads and writes (you almost never need `switchVersion`); name versions in the user's vocabulary (`"Marketing edit"`, `"Legal review"`, `"Editor's pass"`, `"Q3 pricing draft"` — never `"Draft"` or `"branch-1"`); always include a `SELECT FROM lix_change` to surface the audit trail; and a deliberate `try/catch` conflict path.

## Cross-Version Reads And Writes

Every registered schema `X` gets a sibling table `X_by_version` with a `lixcol_version_id` column. Use it whenever a query needs more than one version, or whenever you want to write to a non-active version without `switchVersion`.

- **Reads** filter by `lixcol_version_id` (or omit the filter to scan all versions, joining on `lix_version` for the version name).
- **INSERTs** require `lixcol_version_id` — without it the engine errors with `INSERT into <key>_by_version requires lixcol_version_id`.
- **UPDATEs / DELETEs** must include `lixcol_version_id` in the WHERE clause; DELETEs without it error with `DELETE FROM <key>_by_version requires an explicit lixcol_version_id predicate`.
- The non-suffixed table (`acme_brochure`) is the **active-version view** — convenient for app code that always operates on the current version, but `_by_version` is the right tool for demos, sync, agent inspection, and side-by-side diffs.

The same pattern applies to built-ins: `lix_change_by_version`, `lix_commit_by_version`, `lix_directory_by_version`, etc.

## Files (`lix_file`)

Files are first-class in Lix — and this is a core USP, not a side feature. The built-in `lix_file` table lets you write any file (text, JSON, markdown, binary) into the lix and get versioning, branching, merging, and history over it for free. Parent directories are created automatically.

```ts
// Write a file. `data` is a blob (bytes).
await lix.execute(
  "INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)",
  ["file-readme", "/docs/readme.md", new TextEncoder().encode("# Hello\n")],
);

// Read it back.
const r = await lix.execute(
  "SELECT path, data FROM lix_file WHERE id = $1",
  ["file-readme"],
);
if (r.kind === "rows") {
  const [path, data] = r.rows.rows[0]!;
  console.log(path.asText(), new TextDecoder().decode(data.asBlob()!));
}
```

Columns on `lix_file`:

| Column | Type | What it is |
|--------|------|------------|
| `id` | text | Stable identity for the file (you choose it). |
| `path` | text | Absolute path like `/docs/readme.md`. Parent directories are auto-created in `lix_directory`. |
| `data` | blob | File contents as bytes. |
| `hidden` | bool | UI hint; doesn't affect storage. |
| `lixcol_*` | various | Version metadata (`lixcol_version_id`, `lixcol_global`, `lixcol_untracked`, `lixcol_change_id`, `lixcol_commit_id`, ...). |

`lix_file_by_version` exists for cross-version file reads/writes, exactly like any other entity surface.

**Files-as-entities (upcoming).** A future plugin API will let a registered parser turn a file's contents into rows in your schema tables on write — so `messages.json` becomes per-key entities and two translators editing different keys merge cleanly. Not shipped through `@lix-js/sdk` yet; don't promise it in demos. Today, `lix_file` versions bytes only.

## Registering Schemas

Use stable, namespaced `x-lix-key` and `x-lix-version`. Prefer names that describe a domain entity (`acme_brochure`, `crm_contact`, `cms_page`) — never generic ones (`task`, `item`).

Include `x-lix-primary-key` so the engine can derive entity identity. Each entry is a **JSON Pointer (RFC 6901)** into the entity, leading slash required:

- `["/id"]` — top-level `id` property.
- `["/owner/email"]` — nested property `owner.email`.
- `["/owner", "/slug"]` — composite key over two top-level fields.

Without `x-lix-primary-key`, table-style INSERTs fail with `requires lixcol_entity_id because the schema has no x-lix-primary-key`.

## Reading Results

`lix.execute()` returns a discriminated `ExecuteResult`:

```ts
type ExecuteResult =
  | { kind: "rows"; rows: { columns: string[]; rows: Value[][] } }
  | { kind: "affectedRows"; affectedRows: number };
```

`SELECT` returns `kind: "rows"`. `INSERT` / `UPDATE` / `DELETE` return `kind: "affectedRows"`. Always narrow on `result.kind` before reading `result.rows`.

Cells in `result.rows.rows[i][j]` are `Value` instances (also exported from `@lix-js/sdk`), not raw JS primitives. Use the typed accessors:

```ts
import { openLix, Value } from "@lix-js/sdk";

const r = await lix.execute("SELECT id, headline, price_usd FROM acme_brochure");
if (r.kind === "rows") {
  for (const row of r.rows.rows) {
    const id = row[0].asText();        // string | undefined
    const headline = row[1].asText();  // string | undefined
    const price = row[2].asReal();     // number | undefined
  }
}
```

| Method | Returns | Use for |
|--------|---------|---------|
| `asText()` | `string \| undefined` | `string` (note: `asText`, not `asString`) |
| `asBoolean()` | `boolean \| undefined` | `boolean` |
| `asInteger()` | `number \| undefined` | `integer` |
| `asReal()` | `number \| undefined` | `number` |
| `asJson()` | `JsonValue \| undefined` | `object` / `array` |
| `asBlob()` | `Uint8Array \| undefined` | binary |
| `kindValue()` | `"text" \| "bool" \| "int" \| "float" \| "json" \| "blob" \| "null"` | discriminator |

Each accessor returns `undefined` if the cell's kind doesn't match — branch on `kindValue()` first if you need to handle multiple types. Note the naming mismatch: accessors are `asInteger` / `asReal`, but `kindValue()` returns the short forms `"int"` / `"float"`.

## Versions

Capture the initial active version id rather than hardcoding `"main"`:

```ts
const published = await lix.activeVersionId();
```

Create a version with a name from the user's vocabulary:

```ts
const draft = await lix.createVersion({ name: "Marketing edit" });
```

For demos, agent flows, sync, and any read path that touches more than one version, **write through `<schema>_by_version` with `lixcol_version_id`** (see the canonical demo) — you don't need to switch.

`switchVersion` is for app code that has a "current working version" concept and wants subsequent unqualified writes (`UPDATE acme_section SET …`) to land there. `mergeVersion` always merges *into the active version*, so if you're merging into something other than the currently active version, switch first.

## Merging

`mergeVersion()` merges the source version into the **currently active** version (no `targetVersionId`):

```ts
const merge = await lix.mergeVersion({ sourceVersionId: draft.versionId });
```

The result is a structured receipt:

```ts
type MergeVersionResult = {
  outcome: "alreadyUpToDate" | "mergeCommitted";
  appliedChangeCount: number;
  targetVersionId: string;
  sourceVersionId: string;
  mergeBaseCommitId: string | null;
  targetHeadBeforeCommitId: string;
  sourceHeadBeforeCommitId: string;
  targetHeadAfterCommitId: string;
  createdMergeCommitId: string | null;
};
```

- `outcome: "alreadyUpToDate"` — the source has no commits the target lacks (including self-merge). Nothing applied.
- `outcome: "mergeCommitted"` — a new merge commit was created; `appliedChangeCount > 0` and `createdMergeCommitId` is set.

**Conflicts throw.** If both versions modified the same entity (the same row, identified by `x-lix-key` + primary key) since their merge base, `mergeVersion` raises a `LixError` with a message like `engine2 merge_version found N tracked-state conflict(s)`. Conflicts are detected at row identity, not at field level — disjoint-field edits to the same row still conflict. The current SDK does not expose programmatic conflict resolution — wrap in `try/catch` and surface the error to the user (see the canonical demo above).

## SQL Parameters and UDFs

Use DataFusion-style numbered placeholders. Bare `?` is rejected with `Failed to parse placeholder id`:

```ts
await lix.execute("SELECT * FROM acme_brochure WHERE id = $1", ["spring-2026"]);
```

The only UDF the canonical demo uses is `lix_json($1)` — it parses a TEXT parameter as a JSON value, required when writing JSON-typed columns like `lix_registered_schema.value`. Other UDFs (`lix_json_get`, `lix_json_get_text`, `lix_uuid_v7`, `lix_text_encode`/`_decode`, `lix_empty_blob`, …) live in [packages/engine/src/sql2/udfs](https://github.com/opral/lix/tree/561f92b5bc3fa68e48a863ed3a02129645a57011/packages/engine/src/sql2/udfs) — read source when you need them.

## Built-in Queryable Tables

The four tables demos actually touch:

| Table | What it gives you |
|-------|-------------------|
| `lix_version` | List of versions (`id`, `name`, `hidden`, `commit_id`). Use this instead of hardcoding `"main"`. |
| `lix_change` | The immutable journal — every change with `entity_id`, `schema_key`, `snapshot_content`, `created_at`. See the next section. |
| `lix_file` | Built-in file storage (covered above). |
| `lix_registered_schema` | Your registered schemas (and built-ins). |

The engine ships ~20 more built-ins (commit graph, change sets, key-value, labels, file/directory descriptors, low-level state, etc.) — see [packages/engine/src/schema/builtin](https://github.com/opral/lix/tree/561f92b5bc3fa68e48a863ed3a02129645a57011/packages/engine/src/schema/builtin) when you need them. `lix_account` / `lix_change_author` are declared but **not yet implemented; don't rely on them.**

## The Change Journal (`lix_change`)

`lix_change` is the most reliable feature in the SDK and the most under-marketed one. Every write — INSERT, UPDATE, DELETE, on any registered schema, on any version — appends an immutable row. That row is **just SQL you can query**. Audit logs, blame, undo, "what changed since Tuesday", "show me everything Marketing edited" — none of this is a separate subsystem; it's all `SELECT FROM lix_change`.

Columns that matter: `id`, `entity_id`, `schema_key`, `schema_version`, `snapshot_content` (TEXT JSON — `JSON.parse(asText())`, not `asJson()`), `created_at`, plus `lixcol_*` for version metadata.

```ts
// Audit log: every change to a single entity, oldest to newest.
await lix.execute(
  `SELECT created_at, snapshot_content
     FROM lix_change
    WHERE schema_key = $1 AND entity_id = $2
    ORDER BY created_at`,
  ["acme_section", "s-headline"],
);

// Blame: who/what last touched this entity.
await lix.execute(
  `SELECT created_at, snapshot_content
     FROM lix_change
    WHERE schema_key = $1 AND entity_id = $2
    ORDER BY created_at DESC
    LIMIT 1`,
  ["acme_section", "s-headline"],
);

// Activity feed: latest 20 changes across the whole schema.
await lix.execute(
  `SELECT created_at, entity_id, snapshot_content
     FROM lix_change
    WHERE schema_key = $1
    ORDER BY created_at DESC
    LIMIT 20`,
  ["acme_section"],
);
```

Reach for `lix_change` whenever an app needs history, audit, undo, or activity feeds. It replaces a hand-rolled audit table.

## Gotchas

A short list of things that will burn a fresh agent on the first run.

- **`type: "number"` columns require float literals.** JS numerics bind as Int64 by default, so seeding a `number` column with `899` fails with `expected Float64 but found Int64`. JS has no integer/float distinction, so `899.0` doesn't help either — pass a value with a fractional part (e.g. `899.5`), or model whole-number fields as `type: "integer"` and use `asInteger()` to read.
- **`lix_change.snapshot_content` is currently a TEXT column** (a JSON string), not native JSON. Reading it with `Value.asJson()` returns `undefined`. Use `JSON.parse(cell.asText())` instead.
- **Merge is per-entity, not per-field.** Two versions modifying the same row conflict even if their fields are disjoint. Model concurrent-edit domains as collections of small entities (see the canonical demo).
- **Conflict reproduction is order-dependent.** Fork *all* contending versions from the same base before any merge happens. A version forked *after* a merge has the merged state as its base, so its edit becomes a clean fast-forward instead of a conflict.
- **`mergeVersion` always merges into the active version.** There is no `targetVersionId` parameter. If you need a non-active target, `switchVersion` first.
- **Bare `?` placeholders are rejected** with `Failed to parse placeholder id`. Always use `$1`, `$2`, ...

## Safety Rules

- Never use `sqlite3`, `sql.js`, or raw database access against a `.lix`; use `createBetterSqlite3Backend()` instead of opening the file yourself.
- Never import from `@lix-js/sdk/engine-wasm` or other private internals.
- Use `$1`, `$2`, `$3` placeholders, never bare `?`.
- Schema keys: stable, namespaced, lowercase-snake-case, domain-shaped (`acme_brochure`, not `task`).
- Always include `x-lix-primary-key` and `additionalProperties: false` on registered schemas.
- Name versions in the end-user's vocabulary (`"Marketing edit"`, `"Q3 pricing draft"`), never developer jargon (`"Draft"`, `"branch-1"`).
- Wrap `mergeVersion` in `try/catch` if there is any chance of a conflicting edit.
- Close handles in scripts/tests with `await lix.close()`.

## Reporting SDK Friction

If you encounter an SDK bug, missing API, confusing error, documentation gap, or large implementation friction while using this skill, pause and ask the user whether they want you to open a GitHub issue via the `gh` CLI installed on their computer. Do not file the issue without confirmation. If they approve, include a minimal reproduction, expected behavior, actual behavior, the installed `@lix-js/sdk` package version, runtime/version details, and any relevant error output.
