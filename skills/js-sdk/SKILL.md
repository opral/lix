---
name: lix-js-sdk
description: Use this skill when building examples, demos, tests, or applications with @lix-js/sdk, especially workflows that initialize a Lix, register schemas, write entities through SQL/entity views, create/switch versions, and merge later.
---

# Lix JS SDK Skill

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({});
```

## Current Posture

The JS SDK API is being shaped. Prefer the public SDK exports when they exist. If a high-level helper does not exist yet, use `lix.execute(sql, params)` against the SQL views and keep that usage small and obvious.

Do not reach into WASM internals, private package paths, engine internals, or SQLite directly.

If you hit a dead end or the public SDK behavior is unclear, inspect the engine source code for clarification before guessing. Useful places:

- `packages/engine/src/schema/builtin/*.json` for generated entity table shapes.
- `packages/engine/src/sql2/*` for SQL view/provider behavior.
- `packages/engine/tests/sql/*` for runnable examples of supported SQL.
- `packages/js-sdk/src/open-lix.ts` for the current public JS handle.

## Setup

Use:

```ts
import { openLix } from "@lix-js/sdk";
```

For a new in-memory Lix:

```ts
const lix = await openLix();
```

For caller-owned persistence, initialize first and then reopen:

```ts
import { initLix, openLix, createWasmSqliteBackend } from "@lix-js/sdk";

const backend = await createWasmSqliteBackend();
await initLix({ backend });
const lix = await openLix({ backend });
```

## Register A Schema

Register app schemas before writing entities. Use stable `x-lix-key` and `x-lix-version` values.

Namespace `x-lix-key` to avoid conflicts with other apps or plugins. Prefer names like `crm_task`, `crm_contact`, or `acme_invoice` instead of generic names like `task`.

```ts
const taskSchema = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  "x-lix-key": "crm_task",
  "x-lix-version": "1",
  type: "object",
  required: ["id", "title", "done"],
  properties: {
    id: { type: "string" },
    title: { type: "string" },
    done: { type: "boolean" },
  },
  additionalProperties: false,
} as const;

await lix.execute(
  `INSERT INTO lix_registered_schema (value)
	 VALUES (lix_json(?))`,
  [JSON.stringify(taskSchema)],
);
```

If the SDK exposes a schema helper in the current branch, prefer that helper over raw SQL while preserving the same conceptual shape.

## Write Entities

Registering a schema makes it available as a table named by `x-lix-key`. Write app data through that table.

```ts
await lix.execute(
  `INSERT INTO crm_task (id, title, done)
	 VALUES (?, ?, ?)`,
  ["task-1", "Draft JS SDK skill", false],
);
```

Read from the same active-version table:

```ts
const result = await lix.execute(
  `SELECT id, title, done
	 FROM crm_task
	 ORDER BY id`,
);
```

`lix_state` is the lower-level state table. Use it only for advanced tooling that needs schema/entity metadata directly.

## Versions

Create an isolated version for experiments, agent work, or reviewable edits:

```ts
const draft = await lix.createVersion({
  name: "agent draft",
});

await lix.switchVersion({ versionId: draft.id });
```

Writes after `switchVersion()` go to the active version:

```ts
await lix.execute(
  `UPDATE crm_task
	 SET done = ?
	 WHERE id = ?`,
  [true, "task-1"],
);
```

Switch back to the target version when needed:

```ts
await lix.switchVersion({ versionId: "main" });
```

If the current SDK uses a non-`main` initial version id, ask `await lix.activeVersionId()` or query `lix_version` instead of hardcoding.

## Merge Later

When a merge helper exists, prefer it:

```ts
await lix.mergeVersion({
  sourceVersionId: draft.id,
  targetVersionId: "main",
});
```

If merge is not exposed yet in `@lix-js/sdk`, do not invent a private call. Leave the example with a TODO and explain that the public merge helper is the intended API.

## Recommended Demo Shape

When asked to create a JS SDK demo, build one file that:

1. Opens a fresh Lix.
2. Registers one small schema.
3. Inserts one or two entities on the initial version.
4. Creates and switches to a draft version.
5. Updates an entity in the draft.
6. Reads both the active draft and the target version to show isolation.
7. Calls merge if public API exists, otherwise leaves a TODO marker.

Keep examples runnable with `tsx` or a package script. Avoid framework setup unless the user asks for a UI.

## Safety Rules

- Never use `sqlite3`, `better-sqlite3`, `sql.js`, or raw database access against a `.lix`.
- Never import from `@lix-js/sdk/engine-wasm` or other private internals.
- Prefer parameterized SQL over string interpolation.
- Keep schema keys stable and human-readable.
- Treat versions as user-facing branches; use names that explain intent.
- Close handles in scripts/tests when the API exposes `close()`.
