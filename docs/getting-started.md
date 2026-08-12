---
description: Install Lix, open a file workspace, write files and SQL rows, isolate work on a branch, and merge it.
---

# Getting Started

This guide opens a file workspace, writes a normal file, registers an app schema, writes a row, isolates a change on a branch, and merges it.

## Install

```bash
npm install @lix-js/sdk
```

`openLix()` with no arguments opens an in-memory Lix, enough for tests and demos. For persistent local files, use `LocalFilesystem`; see [Persistence](./persistence.md).

## Open Lix

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
```

## Write a normal file

Files are available through SQL and, with `LocalFilesystem`, on disk:

```ts
await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/notes/status.md",
  new TextEncoder().encode("# Status\n\nReady"),
]);

const file = await lix.execute(
  "SELECT path, content FROM lix_file WHERE path = $1",
  ["/notes/status.md"],
);
```

Tools and agents can edit `/notes/status.md` as a normal file. Lix imports those edits and tracks plugin-defined entities for supported formats. The SDK includes Markdown and CSV plugins.

## Register a schema

Lix stores application state as typed entities. Register a schema once, then read and write through the generated SQL table named after `x-lix-key`.

```ts
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [
    JSON.stringify({
      $schema: "https://json-schema.org/draft/2020-12/schema",
      "x-lix-key": "task",
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
```

`lix_json($1)` parses the JSON text into the JSON-typed `value` column. Schema details (the `x-lix-*` fields, primary keys, uniqueness) are covered in [Schemas](./schemas.md).

## Write and read state

```ts
await lix.execute("INSERT INTO task (id, title, done) VALUES ($1, $2, $3)", [
  "task-1",
  "Review agent changes",
  false,
]);

const result = await lix.execute(
  "SELECT id, title, done FROM task WHERE id = $1",
  ["task-1"],
);

const row = result.rows[0]!;
console.log(row.get("title"), row.get("done"));
```

`execute()` returns `{ columns, rows, rowsAffected, notices }`. Use `row.get(name)` or `row.toObject()` for plain JavaScript values, and `row.value(name).asBytes()` for file bytes:

```ts
const bytes = file.rows[0]!.value("content").asBytes();
```

The full Row and Value surface is in the [JS API Reference](./js-api-reference.md).

## Isolate a change on a branch

A branch is an isolated line of state. Create one for the change, switch into it, and edit:

```ts
const main = await lix.activeBranchId();

const draft = await lix.createBranch({ name: "Agent draft" });
await lix.switchBranch({ branchId: draft.id });

await lix.execute("UPDATE task SET done = $1 WHERE id = $2", [true, "task-1"]);

await lix.switchBranch({ branchId: main });
```

The active branch is now `main` again, and `task-1` is still `done = false` here. The draft change is isolated until you merge.

## Preview and merge

```ts
const preview = await lix.mergeBranchPreview({ sourceBranchId: draft.id });
console.log(preview.outcome, preview.changeStats);
// fastForward { total: 1, added: 0, modified: 1, removed: 0 }

if (preview.conflicts.length === 0) {
  await lix.mergeBranch({ sourceBranchId: draft.id });
}
```

`mergeBranchPreview()` reports the same decision as `mergeBranch()` without changing state. See [Branching](./branching.md).

## The loop

1. Open Lix.
2. Register schemas for the entities you want to version.
3. Write and read through generated tables.
4. Create branches for isolated work.
5. Preview, then merge or discard.
6. Query [`lix_change`](./history.md) for audit.
