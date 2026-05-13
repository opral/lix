---
name: lix-js-sdk
description: Use this skill when building examples, demos, tests, or applications with @lix-js/sdk: opening a Lix, registering schemas, writing entities through generated SQL tables, creating named versions, merging, and querying change history.
---

# Lix JS SDK Skill

## What Is Lix

Lix is an embeddable version control system for structured application state. It gives apps named versions, merge, and an immutable SQL-queryable change journal without asking the app to build those systems from scratch.

Current `@lix-js/sdk` capabilities:

- Register JSON schemas as tracked entity tables.
- Read and write entities through generated SQL tables.
- Group related writes in explicit transactions so they commit once.
- Create named versions of state and write/read across versions.
- Merge one version into the active version.
- Query `lix_change` for history, audit, activity feeds, and undo-style features.
- Store files as bytes with `lix_file` and version them like other entities.

Product direction:

- Lix is designed to version files of any kind by parsing them into typed entities on write.
- Parser plugins that turn file contents into app entities are not shipped through the JS SDK yet. Do not promise this behavior in demos. Today, `lix_file` versions bytes, while app entities are modeled directly through registered schemas.

Every row in every registered schema is a tracked entity. Merge granularity is currently per-entity, not per-field: two versions editing different rows merge cleanly; two versions editing the same row conflict, even if the fields are disjoint. Model collaborative domains as many small entities, such as sections, blocks, paragraphs, message keys, or line items.

Use Lix vocabulary in user-facing copy. What Git calls a branch is called a **version** in Lix because that language makes sense to non-developers.

## When To Use This Skill

Use this skill when you need to write or debug consumer code using `@lix-js/sdk`:

- Opening a persistent `.lix` file.
- Registering schemas.
- Writing and reading generated SQL entity tables.
- Grouping imports, migrations, and batch writes into one transaction.
- Reading `execute()` results.
- Creating, switching, previewing, and merging versions.
- Querying history through `lix_change`.
- Building app demos, examples, smoke tests, or product flows around the SDK.

Do not use this skill for raw SQLite access, private engine/wasm internals, SDK publishing, SDK build pipelines, or unreleased file-parser plugin behavior.

## Agent Quick Start

1. Install `@lix-js/sdk` and `better-sqlite3`.
2. Open with `createBetterSqlite3Backend({ path })`; do not open `.lix` with raw SQLite.
3. Register a schema with `x-lix-key`, `x-lix-primary-key`, and `additionalProperties: false`.
4. Write rows through the generated table named by `x-lix-key`.
5. Use `beginTransaction()` for imports, migrations, and multi-row writes that should be one commit.
6. Use `<schema>_by_version` plus `lixcol_version_id` for side-by-side version reads/writes.
7. Query `lix_change` for audit/history instead of hand-rolling audit tables.
8. Wrap `mergeVersion()` in `try/catch` whenever conflicts are possible.

## Core Rules

- Use the public `@lix-js/sdk` API only.
- Use `createBetterSqlite3Backend()` for persistent apps, demos, and tests.
- Use numbered SQL placeholders: `$1`, `$2`, `$3`; bare `?` is rejected.
- Use `lix_json($1)` when inserting JSON text into JSON-typed columns.
- Use scalar SQL functions `SELECT lix_uuid_v7()` and `SELECT lix_timestamp()` when consumer code needs Lix-generated UUID v7 ids or ISO timestamps. Do not call them as table functions with `SELECT * FROM ...`.
- Use `beginTransaction()` for batch writes. One `lix.execute()` write is one transaction and therefore one commit.
- Do not write through the parent `lix` handle while a transaction is active; use the transaction handle until `commit()` or `rollback()`.
- Use stable, namespaced, lowercase schema keys like `acme_section`, not generic names like `task`.
- Always include `x-lix-primary-key` and `additionalProperties: false` on app schemas.
- Use version names from the user's vocabulary, such as `"Marketing edit"` or `"Q3 pricing draft"`.
- Model concurrent-edit domains as collections of small rows because merge is per-row today.
- Prefer `_by_version` tables for demos, sync, agent inspection, and side-by-side diffs.
- Close handles in scripts and tests with `await lix.close()`.

## Install And Open

```sh
npm i @lix-js/sdk better-sqlite3
```

```ts
import { openLix } from "@lix-js/sdk";
import { createBetterSqlite3Backend } from "@lix-js/sdk/sqlite";

const lix = await openLix({
  backend: createBetterSqlite3Backend({ path: "/path/to/app.lix" }),
});
```

`better-sqlite3` is an optional peer dependency. Install it in projects that import `@lix-js/sdk/sqlite`.

`openLix()` without a backend is in-memory and dies with the process. For anything that should persist, pass a real `.lix` path. Reopening the same path picks up existing state.

For tests and demos, use an isolated temp directory per run:

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
```

Use the version of this skill that ships with the installed `@lix-js/sdk` package. If behavior is unclear, inspect the installed package before guessing. The npm package bundles matching engine source under `node_modules/@lix-js/sdk/dist-engine-src/`.

Useful installed-package references:

- `dist-engine-src/src/sql2/entity_provider.rs` - registered schema SQL surfaces.
- `dist-engine-src/src/sql2/change_provider.rs` - `lix_change` projection.
- `dist-engine-src/src/sql2/version_provider.rs` - writable `lix_version` surface.
- `dist-engine-src/src/transaction/validation.rs` - primary-key, unique, foreign-key, and shape validation.
- `dist-engine-src/src/schema/definition.json` - Lix schema-definition meta-schema.
- `dist-engine-src/src/schema/builtin/` - built-in entity table shapes.
- `dist-engine-src/src/sql2/udfs/` - registered SQL functions.

Do not import from `@lix-js/sdk/engine-wasm`, do not call private wasm helpers, and do not open the `.lix` SQLite file directly.

## Minimal Entity Example

This is the smallest useful consumer pattern: open, register a schema, write a row, read it back, and close.

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

await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [
    JSON.stringify({
      $schema: "https://json-schema.org/draft/2020-12/schema",
      "x-lix-key": "acme_note",
      "x-lix-primary-key": ["/id"],
      type: "object",
      required: ["id", "title", "done"],
      properties: {
        id: { type: "string" },
        title: { type: "string" },
        done: { type: "boolean" },
      },
      additionalProperties: false,
    }),
  ],
);

await lix.execute(
  "INSERT INTO acme_note (id, title, done) VALUES ($1, $2, $3)",
  ["n1", "Draft launch copy", false],
);

const result = await lix.execute(
  "SELECT title, done FROM acme_note WHERE id = $1",
  ["n1"],
);

const row = result.rows[0]!;
console.log(row.value("title").asText(), row.value("done").asBoolean());

await lix.close();
```

## Reading Results

`lix.execute()` returns one shape for every statement:

```ts
type ExecuteResult = {
  columns: string[];
  rows: Row[];
  rowsAffected: number;
  notices: LixNotice[];
};
```

There is no `result.kind`. `SELECT` fills `columns` and `rows`; `INSERT`, `UPDATE`, and `DELETE` usually return `rows: []` and set `rowsAffected`.

Each row is a `Row` object. Use `row.value("column")` or `row.valueAt(index)` to get a `Value`, then call typed accessors:

```ts
const r = await lix.execute("SELECT id, title, done FROM acme_note");
for (const row of r.rows) {
  const id = row.value("id").asText();
  const title = row.value("title").asText();
  const done = row.value("done").asBoolean();
}
```

| Method        | Returns                   | Use for                                   |
| ------------- | ------------------------- | ----------------------------------------- |
| `asText()`    | `string \| undefined`     | strings; note `asText`, not `asString`    |
| `asBoolean()` | `boolean \| undefined`    | booleans                                  |
| `asInteger()` | `number \| undefined`     | integer fields                            |
| `asReal()`    | `number \| undefined`     | decimal/real fields                       |
| `asJson()`    | `JsonValue \| undefined`  | objects and arrays                        |
| `asBlob()`    | `Uint8Array \| undefined` | binary data                               |

Accessors return `undefined` when the cell kind does not match. Branch on `value.kind` if a column can hold multiple types. Public kind strings are `"null"`, `"boolean"`, `"integer"`, `"real"`, `"text"`, `"json"`, and `"blob"`.

`Row` also has convenience methods when native JS values are enough: `get(name)`, `tryGet(name)`, `getAt(index)`, `toObject()`, and `toValueMap()`.

## Transactions

Use `beginTransaction()` whenever several writes belong to one logical operation: CSV imports, seed scripts, migrations, bulk updates, or multi-table changes. A write through `lix.execute()` opens and commits its own transaction, so thousands of separate `execute()` writes become thousands of commits. A transaction handle stages those writes and commits them together.

```ts
const tx = await lix.beginTransaction();

try {
  for (const note of notes) {
    await tx.execute(
      "INSERT INTO acme_note (id, title, done) VALUES ($1, $2, $3)",
      [note.id, note.title, false],
    );
  }

  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

Transaction rules:

- `tx.execute()` has the same result shape as `lix.execute()`.
- Writes are visible to later reads on the same transaction before commit.
- `commit()` makes the whole transaction durable as one commit.
- `rollback()` drops the staged writes.
- After `commit()` or `rollback()`, the transaction handle is closed and cannot be reused.
- A Lix handle allows one active transaction at a time. While it is active, keep writes on `tx`; ordinary `lix.execute()` writes are rejected until the transaction closes.
- Do not use a callback-style transaction helper. The JS SDK mirrors the Rust SDK shape: explicitly `beginTransaction()`, then `commit()` or `rollback()`.

## Registering Schemas

Register app schemas by inserting JSON into `lix_registered_schema.value`:

```ts
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [JSON.stringify(schema)],
);
```

Schema basics:

- `x-lix-key` becomes the generated SQL table name.
- Compatible schema amendments are keyed by `x-lix-key`.
- `x-lix-primary-key` tells Lix how to derive entity identity.
- Primary-key entries are JSON Pointers with a leading slash, such as `["/id"]` or `["/owner/email"]`.
- Use `additionalProperties: false` so accidental fields fail fast.

Without `x-lix-primary-key`, table-style INSERTs fail with an error like `requires lixcol_entity_id because the schema has no x-lix-primary-key`.

Uniqueness is not inferred from ordinary JSON Schema fields. If a non-primary-key field must be unique, declare it explicitly:

```ts
const companyDomainSchema = {
  "x-lix-key": "crm_company_domain",
  "x-lix-primary-key": ["/id"],
  "x-lix-unique": [["/domain"]],
  type: "object",
  required: ["id", "domain"],
  properties: {
    id: { type: "string" },
    domain: { type: "string" },
  },
  additionalProperties: false,
};
```

Do not add generic `created_at` or `updated_at` fields by default. Lix already records lifecycle history through `lix_change` and `lixcol_*` metadata. Add timestamp fields only when they are domain data, such as `due_at`, `published_at`, or `occurred_at`.

Discover live schemas before guessing:

```ts
const schemas = await lix.execute(
  "SELECT lixcol_entity_id, value FROM lix_registered_schema ORDER BY lixcol_entity_id",
);

for (const row of schemas.rows) {
  const schema = row.get("value") as { "x-lix-key"?: string };
  console.log(schema["x-lix-key"]);
}
```

## Versions And `_by_version`

Capture the initial active version id instead of hardcoding `"main"`:

```ts
const published = await lix.activeVersionId();
```

Create versions with names from the user's domain:

```ts
const marketing = await lix.createVersion({ name: "Marketing edit" });
const legal = await lix.createVersion({ name: "Legal review" });
```

Every registered schema `X` gets a sibling table `X_by_version` with `lixcol_version_id`. Use it for side-by-side reads and for writes to non-active versions.

```ts
await lix.execute(
  `UPDATE acme_note_by_version
      SET title = $1
    WHERE id = $2 AND lixcol_version_id = $3`,
  ["Sharper launch copy", "n1", marketing.id],
);

const sideBySide = await lix.execute(
  `SELECT v.name, n.title
     FROM acme_note_by_version n
     JOIN lix_version v ON v.id = n.lixcol_version_id
    WHERE n.id = $1
      AND n.lixcol_version_id IN ($2, $3)
    ORDER BY v.name`,
  ["n1", published, marketing.id],
);
```

Rules for `_by_version`:

- Reads filter by `lixcol_version_id`, or omit the filter to scan all versions.
- INSERTs require `lixcol_version_id`.
- UPDATEs and DELETEs must include `lixcol_version_id` in the WHERE clause.
- The non-suffixed table is the active-version view.

`switchVersion()` is for app code with a current working version concept. `mergeVersion()` always merges into the active version, so switch first if you need a different target.

## Merging

`mergeVersion()` merges the source version into the currently active version:

```ts
try {
  const merge = await lix.mergeVersion({ sourceVersionId: marketing.id });
  console.log(merge.outcome, merge.changeStats.total);
} catch (error) {
  console.error("Merge conflict", error);
}
```

Common outcomes:

- `"alreadyUpToDate"` - source has no commits the target lacks.
- `"fastForward"` - target advanced to source without a merge commit.
- `"mergeCommitted"` - a new merge commit was created.

`mergeVersionPreview()` reports the same merge decision without advancing refs, staging changes, or creating commits. Merge conflicts are returned as preview data.

Conflicts throw from `mergeVersion()`. If both versions modified the same entity since their merge base, Lix raises a `LixError`. Conflict detection is row-level today, not field-level. To reproduce a conflict in a demo, fork all contending versions from the same base before merging any of them.

## Demo Pattern To Imitate

For richer demos, show these four things:

1. Isolation: one SELECT against `<schema>_by_version` shows several versions side by side.
2. Clean parallel merges: two reviewers edit different entities and both land.
3. Audit history: `lix_change` is queryable SQL.
4. Conflict handling: two versions edit the same entity and `mergeVersion()` throws.

Shape the domain as a collection of small entities:

- Good: brochure sections, document blocks, paragraph rows, message keys, line items.
- Risky: one huge document row with many editable fields.

Demo recipe:

1. Register a schema such as `acme_section`.
2. Seed several rows in the published version.
3. Create all reviewer versions up front from the same base.
4. Write each reviewer's changes through `acme_section_by_version`.
5. Read side by side by joining `acme_section_by_version` to `lix_version`.
6. Merge non-overlapping row edits successfully.
7. Query `lix_change`.
8. Catch the deliberate same-row conflict.

## Files With `lix_file`

`lix_file` stores files as versioned bytes. Parent directories are created automatically.

```ts
await lix.execute("INSERT INTO lix_file (id, path, data) VALUES ($1, $2, $3)", [
  "file-readme",
  "/docs/readme.md",
  new TextEncoder().encode("# Hello\n"),
]);

const result = await lix.execute(
  "SELECT path, data FROM lix_file WHERE id = $1",
  ["file-readme"],
);

const file = result.rows[0]!;
console.log(
  file.value("path").asText(),
  new TextDecoder().decode(file.value("data").asBlob()!),
);
```

Columns consumers usually need:

| Column     | What it is                                                            |
| ---------- | --------------------------------------------------------------------- |
| `id`       | Stable identity for the file.                                         |
| `path`     | Absolute path like `/docs/readme.md`.                                 |
| `data`     | File contents as bytes.                                               |
| `hidden`   | UI hint; does not affect storage.                                     |
| `lixcol_*` | Version/change metadata, including `lixcol_version_id` where exposed. |

`lix_file_by_version` exists for cross-version file reads and writes. Files-as-parsed-entities are product direction, not current JS SDK behavior.

## The Change Journal

`lix_change` is an immutable SQL table of changes across registered schemas and versions. Use it for audit logs, blame, history, activity feeds, and undo-style UI.

Important columns include `id`, `entity_id`, `schema_key`, `snapshot_content`, `created_at`, and `lixcol_*` metadata.

```ts
// Audit log for one entity, oldest to newest.
await lix.execute(
  `SELECT created_at, snapshot_content
     FROM lix_change
    WHERE schema_key = $1 AND entity_id = $2
    ORDER BY created_at`,
  ["acme_note", "n1"],
);

// Latest activity across a schema.
await lix.execute(
  `SELECT created_at, entity_id, snapshot_content
     FROM lix_change
    WHERE schema_key = $1
    ORDER BY created_at DESC
    LIMIT 20`,
  ["acme_note"],
);
```

`snapshot_content` can be null or absent for tombstones, removals, or rows where content was not materialized. In the JS SDK, read it with `row.value("snapshot_content").asJson()` or `row.get("snapshot_content")`, then handle null. Do not blindly `JSON.parse` it as text.

## Built-In Tables And UDFs

Common tables:

| Table                   | What it gives consumers                                                                                 |
| ----------------------- | ------------------------------------------------------------------------------------------------------- |
| `lix_version`           | Writable version surface: `id`, `name`, `hidden`, `commit_id`.                                          |
| `lix_change`            | Immutable change journal.                                                                               |
| `lix_file`              | Versioned byte storage for files.                                                                       |
| `lix_registered_schema` | Registry of app schemas plus built-ins; also exposes the Lix schema-definition meta-schema at runtime. |

`lix_version` can be updated for admin flows:

```ts
await lix.execute("UPDATE lix_version SET hidden = true WHERE id = $1", [
  marketing.id,
]);
```

There is no documented `deleteVersion()` helper in this preview. If the product wants reversible cleanup, hide the version. If it wants removal, `DELETE FROM lix_version WHERE id = $1` is the SQL surface; the engine rejects deleting the global version and active version.

Use `lix_json($1)` to parse JSON text parameters when writing JSON-typed columns:

```ts
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [JSON.stringify(schema)],
);
```

Other UDFs, such as `lix_json_get`, `lix_uuid_v7`, `lix_text_encode`, and `lix_empty_blob`, live in `dist-engine-src/src/sql2/udfs/` in the installed package.

## Do And Avoid

| Do | Avoid |
| --- | --- |
| Use `createBetterSqlite3Backend({ path })` for persistent state. | Opening `.lix` files with raw SQLite libraries. |
| Use public imports from `@lix-js/sdk` and `@lix-js/sdk/sqlite`. | Importing `engine-wasm` or private internals. |
| Use `$1`, `$2`, `$3` placeholders. | Bare `?` placeholders. |
| Use `lix_json($1)` for JSON parameters. | Inlining stringified JSON directly into SQL. |
| Use `beginTransaction()` for imports and batch writes that should be one commit. | Loops of standalone `lix.execute()` writes for bulk imports. |
| Use the transaction handle for writes until it commits or rolls back. | Mixing parent-handle writes into an active transaction. |
| Use `_by_version` for cross-version reads/writes. | Switching versions just to render a side-by-side view. |
| Name versions in user vocabulary. | User-facing words like branch, branch-1, or generic Draft. |
| Model collaborative data as small rows. | One giant row when multiple reviewers edit different parts. |
| Add `x-lix-unique` for non-primary unique fields. | Assuming JSON Schema property metadata creates uniqueness. |
| Read `snapshot_content` as JSON/native and handle null. | Blindly `JSON.parse(row.value(...).asText())`. |
| Wrap `mergeVersion()` in `try/catch`. | Assuming merges cannot conflict. |

## Reporting SDK Friction

If you encounter an SDK bug, missing API, confusing error, documentation gap, or large implementation friction while using this skill, pause and ask the user whether they want you to open a GitHub issue via the `gh` CLI installed on their computer. Do not file an issue without confirmation.

Before filing, scan existing issues to avoid duplicates. If the user approves a report, include a minimal reproduction, expected behavior, actual behavior, the installed `@lix-js/sdk` version, runtime details, and relevant error output. Do not include private data, customer content, credentials, tokens, local paths, database contents, or proprietary schemas.
