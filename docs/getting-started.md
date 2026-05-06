---
description: Install Lix, open an in-memory repository, write files, and query semantic changes in a minimal JavaScript quickstart.
---

# Getting Started

This guide opens a Lix instance, registers a small schema, writes data, and makes a change in an isolated version.

## Install

```bash
npm install @lix-js/sdk
```

Lix currently targets JavaScript and TypeScript.

## Open Lix

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
```

By default this opens an in-memory Lix. That is enough for local experiments, tests, and examples.

## Register a schema

Lix stores application state as typed records. Register a JSON schema before writing a new kind of record.

```ts
await lix.execute(
  "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))",
  [
    JSON.stringify({
      $schema: "https://json-schema.org/draft/2020-12/schema",
      "x-lix-key": "task",
      "x-lix-version": "1",
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

After a schema is registered, Lix exposes a table for that state type.

## Write state

```ts
await lix.execute(
  "INSERT INTO task (id, title, done) VALUES ($1, $2, $3)",
  ["task-1", "Review agent changes", false],
);
```

Query it with SQL:

```ts
const result = await lix.execute(
  "SELECT id, title, done FROM task WHERE id = $1",
  ["task-1"],
);

console.log(result.rows[0]?.toObject());
```

## Make an isolated version

Versions let you change state without touching the active main version.

```ts
const mainVersionId = await lix.activeVersionId();

const draft = await lix.createVersion({
  id: "agent-draft",
  name: "Agent draft",
});

await lix.switchVersion({ versionId: draft.id });

await lix.execute("UPDATE task SET done = $1 WHERE id = $2", [
  true,
  "task-1",
]);
```

Switch back to the original version:

```ts
await lix.switchVersion({ versionId: mainVersionId });
```

The draft change is still isolated. Your app can preview it before merging.

```ts
const preview = await lix.mergeVersionPreview({
  sourceVersionId: draft.id,
});

console.log(preview.changeStats);
```

## Merge when ready

```ts
await lix.mergeVersion({
  sourceVersionId: draft.id,
});
```

That is the basic loop:

1. Open Lix.
2. Register the state your app stores.
3. Write and query state.
4. Create versions for isolated work.
5. Preview and merge changes when a human or workflow approves them.
