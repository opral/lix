---
description: "Reference for the JavaScript SDK Lix instance, transactions, execute results, rows, and values."
---

# JavaScript API Reference

The JavaScript SDK exports `openLix()`, `SqliteBackend`, and `FsBackend` from `@lix-js/sdk`.
`openLix()` returns a `Lix` instance: an in-process handle to one Lix
repository.

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
```

## openLix()

```ts
const lix = await openLix(options?);
```

Options:

| Option    | Type                        | Description                                                          |
| --------- | --------------------------- | -------------------------------------------------------------------- |
| `backend` | `SqliteBackend \| FsBackend` | Optional storage backend. Omit it for the default in-memory backend. |

```ts
import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
  backend: new SqliteBackend({ path: "app.lix" }),
});
```

Use `FsBackend` for a filesystem workspace directory backed by
`<workspace>/.lix/.internal/db.sqlite`:

```ts
import { FsBackend, openLix } from "@lix-js/sdk";

const lix = await openLix({
  backend: new FsBackend({ path: "workspace" }),
});
```

## Lix instance

### execute()

```ts
const result = await lix.execute(sql, params?);
```

Executes one DataFusion SQL statement against the active Lix session.

Parameters:

| Parameter | Type                             | Description                                                        |
| --------- | -------------------------------- | ------------------------------------------------------------------ |
| `sql`     | `string`                         | One SQL statement. Use DataFusion SQL, not SQLite SQL.             |
| `params`  | `ReadonlyArray<LixRuntimeValue>` | Optional positional parameters addressed as `$1`, `$2`, and so on. |

`LixRuntimeValue` accepts JSON values, `Uint8Array`, `ArrayBuffer`, or a `Value`.

Result:

```ts
type ExecuteResult = {
  columns: string[];
  rows: Row[];
  rowsAffected: number;
  notices: LixNotice[];
};
```

| Field          | Description                                                                 |
| -------------- | --------------------------------------------------------------------------- |
| `columns`      | Column names in result order. Empty for statements that do not return rows. |
| `rows`         | Result rows. Each row exposes typed accessors by column name or index.      |
| `rowsAffected` | Number of rows affected by write statements.                                |
| `notices`      | Non-fatal engine notices with `{ code, message, hint? }`.                   |

Example:

```ts
const result = await lix.execute(
  "SELECT path, data FROM lix_file WHERE path = $1",
  ["/hello.txt"],
);

const path = result.rows[0]?.value("path").asText();
const data = result.rows[0]?.value("data").asBytes();
```

### beginTransaction()

```ts
const tx = await lix.beginTransaction();
```

Starts a transaction. While it is open, execute statements on the transaction handle.

```ts
const tx = await lix.beginTransaction();
try {
  await tx.execute("INSERT INTO lix_file (path, data) VALUES (?, ?)", [
    "/hello.txt",
    new TextEncoder().encode("hello"),
  ]);
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

### activeVersionId()

```ts
const versionId = await lix.activeVersionId();
```

Returns the id of the version the Lix handle is currently reading and writing.

### createVersion()

```ts
const version = await lix.createVersion({
  name: "Explore",
});
```

Creates a version.

Options:

| Option         | Type     | Description                       |
| -------------- | -------- | --------------------------------- |
| `name`         | `string` | Version name.                     |
| `id`           | `string` | Optional explicit version id.     |
| `fromCommitId` | `string` | Optional commit id to start from. |

Result:

```ts
type CreateVersionResult = {
  id: string;
  name: string;
  hidden: boolean;
  commitId: string;
};
```

### switchVersion()

```ts
await lix.switchVersion({ versionId });
```

Switches the Lix handle to another version. Plain SQL tables read and write the active version.

### mergeVersionPreview()

```ts
const preview = await lix.mergeVersionPreview({
  sourceVersionId: draft.id,
});
```

Computes the merge result from `sourceVersionId` into the currently active target version without applying it.

Result:

```ts
type MergeVersionPreviewResult = {
  outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted";
  targetVersionId: string;
  sourceVersionId: string;
  baseCommitId: string;
  targetHeadCommitId: string;
  sourceHeadCommitId: string;
  changeStats: MergeChangeStats;
  conflicts: MergeConflict[];
};
```

### mergeVersion()

```ts
const merge = await lix.mergeVersion({
  sourceVersionId: draft.id,
});
```

Merges `sourceVersionId` into the currently active target version.

Result:

```ts
type MergeVersionResult = {
  outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted";
  targetVersionId: string;
  sourceVersionId: string;
  baseCommitId: string;
  targetHeadBeforeCommitId: string;
  sourceHeadBeforeCommitId: string;
  targetHeadAfterCommitId: string;
  createdMergeCommitId: string | null;
  changeStats: MergeChangeStats;
};
```

`MergeChangeStats`:

```ts
type MergeChangeStats = {
  total: number;
  added: number;
  modified: number;
  removed: number;
};
```

`MergeConflict`:

```ts
type MergeConflict = {
  kind: "sameEntityChanged";
  schemaKey: string;
  entityPk: string[];
  fileId: string | null;
  target: MergeConflictSide;
  source: MergeConflictSide;
};
```

### close()

```ts
await lix.close();
```

Closes the Lix handle and its backend resources.

## Transaction

Transactions expose:

| Method                  | Description                                                 |
| ----------------------- | ----------------------------------------------------------- |
| `execute(sql, params?)` | Execute SQL inside the transaction.                         |
| `commit()`              | Commit the transaction and close the transaction handle.    |
| `rollback()`            | Roll back the transaction and close the transaction handle. |

## Row

Rows are returned by `execute()`.

```ts
const row = result.rows[0]!;
```

| Surface                    | Return type                      | Description                                                    |
| -------------------------- | -------------------------------- | -------------------------------------------------------------- |
| `row.columns`              | `string[]`                       | Column names for this row.                                     |
| `row.get(columnName)`      | `LixNativeValue`                 | Native JS value for a column. Throws if the column is missing. |
| `row.tryGet(columnName)`   | `LixNativeValue \| undefined`    | Native JS value, or `undefined` when the column is missing.    |
| `row.value(columnName)`    | `Value`                          | Typed `Value` for a column. Throws if the column is missing.   |
| `row.tryValue(columnName)` | `Value \| undefined`             | Typed `Value`, or `undefined` when the column is missing.      |
| `row.getAt(index)`         | `LixNativeValue`                 | Native JS value by column index.                               |
| `row.valueAt(index)`       | `Value`                          | Typed `Value` by column index.                                 |
| `row.values()`             | `Value[]`                        | All typed values in column order.                              |
| `row.toObject()`           | `Record<string, LixNativeValue>` | Object of native JS values keyed by column name.               |
| `row.toValueMap()`         | `Record<string, Value>`          | Object of typed values keyed by column name.                   |

`LixNativeValue` is `null`, boolean, number, string, JSON, or `Uint8Array`.

## Value

`Value` preserves the SQL type returned by the engine.

Accessors:

| Method        | Return type               | Description                           |
| ------------- | ------------------------- | ------------------------------------- |
| `asInteger()` | `number \| undefined`     | Returns a number for integer values.  |
| `asBoolean()` | `boolean \| undefined`    | Returns a boolean for boolean values. |
| `asReal()`    | `number \| undefined`     | Returns a number for real values.     |
| `asText()`    | `string \| undefined`     | Returns a string for text values.     |
| `asJson()`    | `JsonValue \| undefined`  | Returns a JSON value for JSON values. |
| `asBytes()`   | `Uint8Array \| undefined` | Returns bytes for blob values.        |
| `toJSON()`    | `LixValue`                | Serializes the typed value.           |

Constructors:

| Method                 | Description                                                                    |
| ---------------------- | ------------------------------------------------------------------------------ |
| `Value.null()`         | Create a SQL null value.                                                       |
| `Value.integer(value)` | Create an integer value.                                                       |
| `Value.boolean(value)` | Create a boolean value.                                                        |
| `Value.real(value)`    | Create a real number value.                                                    |
| `Value.text(value)`    | Create a text value.                                                           |
| `Value.json(value)`    | Create a JSON value.                                                           |
| `Value.blob(value)`    | Create a blob value from `Uint8Array`.                                         |
| `Value.from(raw)`      | Convert a JS value, `LixValue`, `Uint8Array`, or `ArrayBuffer` into a `Value`. |
