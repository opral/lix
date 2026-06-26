---
description: Install Lix, open an in-memory repository, register a schema, write rows, and inspect a change in under 30 lines of JavaScript.
---

# Getting Started

This walks through opening Lix, registering a schema, writing a row, isolating a change in a separate version, previewing the merge, and merging.

## Install

```bash
npm install @lix-js/sdk
```

`openLix()` with no arguments opens an in-memory Lix, enough for tests and demos. For persistent local files, use `FsBackend`; see [Persistence](./persistence.md).

## Open Lix

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
```

```ts
import { FsBackend, openLix } from "@lix-js/sdk";

const lix = await openLix({
	backend: new FsBackend({
		path: "./workspace",
		syncAllFiles: true,
	}),
});
```

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
console.log(row.value("title").asText(), row.value("done").asBoolean());
```

`execute()` returns `{ columns, rows, rowsAffected, notices }`.

## Read result values

Each row is a `Row`. Use `row.value(name)` for a typed `Value`, or `row.get(name)` / `row.toObject()` for plain JavaScript values.

| Accessor      | Use for                                                 |
| ------------- | ------------------------------------------------------- |
| `asText()`    | text columns                                            |
| `asBoolean()` | boolean columns                                         |
| `asInteger()` | integer columns                                         |
| `asReal()`    | decimal columns                                         |
| `asJson()`    | JSON columns such as `snapshot_content` and `entity_pk` |
| `asBytes()`   | binary columns such as `lix_file.data`                  |

Use `asBytes()` for byte content:

```ts
const file = await lix.execute("SELECT data FROM lix_file WHERE path = $1", [
	"/orders.xlsx",
]);
const bytes = file.rows[0]!.value("data").asBytes();
```

## Isolate a change in a version

A version is an isolated line of state. Create one for the change, switch into it, and edit:

```ts
const main = await lix.activeVersionId();

const draft = await lix.createVersion({ name: "Agent draft" });
await lix.switchVersion({ versionId: draft.id });

await lix.execute("UPDATE task SET done = $1 WHERE id = $2", [true, "task-1"]);

await lix.switchVersion({ versionId: main });
```

The active version is now `main` again, and `task-1` is still `done = false` here. The draft change is isolated until you merge.

## Preview and merge

```ts
const preview = await lix.mergeVersionPreview({ sourceVersionId: draft.id });
console.log(preview.outcome, preview.changeStats);
// fastForward { total: 1, added: 0, modified: 1, removed: 0 }

if (preview.conflicts.length === 0) {
	await lix.mergeVersion({ sourceVersionId: draft.id });
}
```

`mergeVersionPreview()` reports the same merge decision as `mergeVersion()` without advancing refs. It returns the per-row conflict list when both sides changed the same entity. See [Versions & Merging](./versions.md).

## The loop

1. Open Lix.
2. Register schemas for the entities you want to version.
3. Write and read through generated tables.
4. Create versions for isolated work.
5. Preview, then merge or discard.
6. Query [`lix_change`](./history.md) for audit and undo.
