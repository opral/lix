---
description: Reference for the @lix-js/sdk public API: openLix, execute, version and merge methods, result shapes, and the built-in SQL tables and functions.
---

# API Reference

## `openLix(options?)`

```ts
function openLix(options?: { backend?: LixBackend }): Promise<Lix>;
```

Open a Lix instance. With no `backend`, returns an in-memory Lix. See [Persistence](./persistence.md).

Returns a `Lix` with the following methods.

## `Lix`

### `execute(sql, params?)`

```ts
lix.execute(sql: string, params?: LixRuntimeValue[]): Promise<ExecuteResult>;
```

Run one DataFusion SQL statement. Use anonymous placeholders (`?`) or numbered placeholders (`$1`, `$2`); do not mix both styles in one statement. Use `lix_json(?)` or `lix_json($1)` when binding a JSON-typed parameter.

```ts
type ExecuteResult = {
  columns: string[];
  rows: Row[];
  rowsAffected: number;
  notices: { code: string; message: string; hint?: string }[];
};
```

`SELECT` populates `columns` and `rows`. `INSERT` / `UPDATE` / `DELETE` set `rowsAffected` and usually return `rows: []`.

### `Row`

```ts
class Row {
  columns: string[];
  value(name): Value;            // typed accessor
  tryValue(name): Value | undefined;
  valueAt(index): Value;
  get(name): LixNativeValue;     // plain JS
  tryGet(name): LixNativeValue | undefined;
  getAt(index): LixNativeValue;
  toObject(): Record<string, LixNativeValue>;
  toValueMap(): Record<string, Value>;
}
```

Use `value(name)` for a `Value` with typed accessors:

| Method        | Returns                   | For                     |
| ------------- | ------------------------- | ----------------------- |
| `asText()`    | `string \| undefined`     | text columns            |
| `asBoolean()` | `boolean \| undefined`    | booleans                |
| `asInteger()` | `number \| undefined`     | integers                |
| `asReal()`    | `number \| undefined`     | decimals                |
| `asJson()`    | `JsonValue \| undefined`  | JSON / objects / arrays |
| `asBlob()`    | `Uint8Array \| undefined` | bytes                   |

Accessors return `undefined` when the cell kind doesn't match. Branch on `value.kind` (`"null" | "boolean" | "integer" | "real" | "text" | "json" | "blob"`) for polymorphic columns.

`row.toObject()` is the convenience shortcut to a plain JS object.

### `activeVersionId()`

```ts
lix.activeVersionId(): Promise<string>;
```

Returns the id of the currently active version. Capture this on startup instead of hard-coding `"main"`.

### `createVersion(options)`

```ts
lix.createVersion(options: {
  name: string;
  id?: string;
  fromCommitId?: string;
}): Promise<{ id: string; name: string; hidden: boolean }>;
```

Create a new version. Pass `fromCommitId` to fork from a specific commit; otherwise it forks from the active version's head.

### `switchVersion(options)`

```ts
lix.switchVersion(options: { versionId: string }): Promise<SwitchVersionResult>;
```

Make the given version the active one for this Lix instance. Subsequent SQL goes against it.

### `mergeVersionPreview(options)`

```ts
lix.mergeVersionPreview(options: { sourceVersionId: string }):
  Promise<{
    outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted";
    targetVersionId: string;
    sourceVersionId: string;
    baseCommitId: string;
    targetHeadCommitId: string;
    sourceHeadCommitId: string;
    changeStats: { total: number; added: number; modified: number; removed: number };
    conflicts: MergeConflict[];
  }>;
```

Reports the same merge decision as `mergeVersion()` without touching state. Returns row-level `conflicts`. Always merges into the active version; switch first if you want a different target.

### `mergeVersion(options)`

```ts
lix.mergeVersion(options: { sourceVersionId: string }):
  Promise<{
    outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted";
    targetVersionId: string;
    sourceVersionId: string;
    baseCommitId: string;
    createdMergeCommitId: string | null;
    changeStats: { total; added; modified; removed };
  }>;
```

Throws a `LixError` on conflicts. Wrap in `try/catch` whenever conflicts are possible.

### `close()`

```ts
lix.close(): Promise<void>;
```

Always close in scripts and tests.

## Built-in tables

| Table                                                                  | Purpose                                                                                                                                                                                          |
| ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `lix_registered_schema`                                                | App schemas (and built-ins). Insert into `value` to register. See [Schemas](./schemas.md).                                                                                                       |
| `lix_change`                                                           | Immutable global change journal. Columns: `id`, `entity_id`, `schema_key`, `schema_version`, `file_id`, `metadata`, `snapshot_content`, `created_at`. No version filter; `lix_change` is global. |
| `lix_state` / `lix_state_by_version` / `lix_state_history`             | Schema-agnostic JSON state. Active version, cross-version, and time-travel respectively. See [SQL Surfaces](./surfaces.md).                                                                      |
| `lix_version`                                                          | Writable version surface: `id`, `name`, `hidden`, `commit_id`.                                                                                                                                   |
| `lix_file` / `lix_file_by_version` / `lix_file_history`                | Versioned files (with `data` bytes), cross-version reads/writes, and history.                                                                                                                    |
| `lix_directory` / `lix_directory_by_version` / `lix_directory_history` | Directory tree, cross-version, and history.                                                                                                                                                      |

Every registered schema `X` produces three typed surfaces:

- `X`: the active-version view, used for plain `INSERT`/`SELECT`/`UPDATE`/`DELETE`.
- `X_by_version`: cross-version view with `lixcol_version_id`. See [Versions & Merging](./versions.md).
- `X_history`: typed time-travel through this schema's history with `lixcol_start_commit_id`, `lixcol_depth`, `lixcol_observed_commit_id`.

For the full grid of state / per-entity / file / directory surfaces and how they compose, see [SQL Surfaces](./surfaces.md).

## Built-in SQL functions

| Function                            | What it does                                                                                                                  |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `lix_active_version_commit_id()`    | Commit id at the active version's tip. Use to scope `_history` queries (the planner rejects subqueries on `start_commit_id`). |
| `lix_json(text)`                    | Parse JSON text into a JSON-typed value. Use when binding JSON parameters.                                                    |
| `lix_json_get(json, path...)`       | Project a JSON-typed value out of a JSON column.                                                                              |
| `lix_json_get_text(json, path...)`  | Project a value out of a JSON column as text.                                                                                 |
| `lix_uuid_v7()`                     | Generate a UUIDv7 string.                                                                                                     |
| `lix_timestamp()`                   | Current ISO-8601 timestamp string.                                                                                            |
| `lix_text_decode(blob[, encoding])` | Decode a `BLOB` to text (default `utf-8`).                                                                                    |
| `lix_text_encode(text[, encoding])` | Encode text to a `BLOB`.                                                                                                      |
| `lix_empty_blob()`                  | Zero-byte `BLOB` literal.                                                                                                     |

See [SQL Functions](./sql-functions.md) for examples and signatures.

## Errors

`mergeVersion()` and write paths throw `LixError`. `notices` on `ExecuteResult` carry non-fatal codes with `code`, `message`, and an optional `hint`.

## SQL dialect

Lix runs on a DataFusion-backed engine. SQL is mostly Postgres-compatible. SQLite-specific catalog tables (`sqlite_master`, etc.) are not available; use `lix_registered_schema` and `lix_version` instead.
