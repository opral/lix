---
description: "Reference for opening local and remote Lix instances, running SQL, using transactions, and working with branches."
---

# JavaScript API Reference

The main JavaScript SDK exports are `openLix()` and `LocalFilesystem` from `@lix-js/sdk`. `openLix()` returns a `Lix` instance connected to a local or remote repository.

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
```

## openLix()

```ts
const lix = await openLix(options?);
```

Options:

| Option      | Type                                     | Description                                                                      |
| ----------- | ---------------------------------------- | -------------------------------------------------------------------------------- |
| `storage`   | `LocalFilesystem \| LixSnapshotStorage` | Local storage. Omit it for memory.                                               |
| `server`    | `RemoteLixServerOptions`                 | Connect to a remote Lix server. Cannot be combined with local workspace storage. |
| `telemetry` | `LixTelemetryOptions`                    | Optional `onSpan(span)` callback that receives telemetry spans. Local mode only. |

Connect to a remote server:

```ts
const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/workspaces/acme",
    headers: () => ({ Authorization: `Bearer ${token}` }),
  },
});
```

Remote file content, SQL rows, and branches live on the server. An optional `LixSnapshotStorage` in remote mode stores only private client state. Use `headers` for authentication and `fetch` when you need a custom fetch implementation.

Use `LocalFilesystem` for a filesystem workspace directory backed by RocksDB at
`<workspace>/.lix/.internal/rocksdb`:

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new LocalFilesystem({
    path: "./workspace",
    syncAllFiles: true,
  }),
});
```

Pass `lixDir` for filesystem sync with repository metadata in an external
`.lix` directory. This does not write `<workspace>/.lix`:

```ts
const lix = await openLix({
  storage: new LocalFilesystem({
    path: "./workspace",
    lixDir: "/tmp/session/.lix",
    syncAllFiles: true,
  }),
});
```

Set `syncAllFiles: false` to start filesystem sync with no regular workspace
files, then import selected files with `storage.importPaths()`. Imported paths are
exact workspace-relative file paths, not directories or globs. They may be
written with or without a leading slash, for example `"notes/today.md"` or
`"/notes/today.md"`. This scopes disk import, file watching, and
materialization; it does not filter unrelated Lix SQL state.

```ts
const storage = new LocalFilesystem({
  path: "./workspace",
  syncAllFiles: false,
});
const lix = await openLix({ storage });
await storage.importPaths(["notes/today.md"]);
```

Call `storage.syncDiskToLix()` to run one manual sync pass that imports pending
disk changes into Lix. It returns `Promise<void>` and requires an open Lix
instance.

```ts
await storage.syncDiskToLix();
```

## Lix instance

### execute()

```ts
const result = await lix.execute(sql, params?, options?);
```

Executes one DataFusion SQL statement against the active Lix session.

Parameters:

| Parameter | Type                     | Description                                                        |
| --------- | ------------------------ | ------------------------------------------------------------------ |
| `sql`     | `string`                 | One SQL statement. Use DataFusion SQL, not SQLite SQL.             |
| `params`  | `SqlParam[]`             | Optional positional parameters addressed as `$1`, `$2`, and so on. |
| `options` | `ExecuteOptions`         | Optional execution options. See below.                             |

`SqlParam` accepts JSON values, `Uint8Array`, or a `Value`:

```ts
type SqlParam = JsonValue | Uint8Array | Value;
```

`ExecuteOptions`:

| Option           | Type     | Description                                                                                                                                       |
| ---------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `originKey`      | `string` | Optional origin label for the mutation.                                                                                                            |
| `idempotencyKey` | `string` | Stable identity for one logical remote SQL mutation. This is the retry story: supply the same key when retrying after a lost response, and the server applies the mutation only once. Remote Lix generates one per call when omitted. Sent as `Idempotency-Key`, not SQL options. |

Result:

```ts
type ExecuteResult = {
  columns: string[];
  rows: Row[];
  rowsAffected: number;
  notices: { code: string; message: string; hint?: string }[];
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
  "SELECT path, content FROM lix_file WHERE path = $1",
  ["/hello.txt"],
);

const path = result.rows[0]?.get("path");
const content = result.rows[0]?.value("content").asBytes();
```

### executeBatch()

```ts
const results = await lix.executeBatch(statements, options?);
```

Executes multiple statements in one call. `statements` is a non-empty array of
`{ sql, params? }` objects. `options` accepts the same `originKey` and
`idempotencyKey` as `execute()`. Returns one `ExecuteResult` per statement.

```ts
const results = await lix.executeBatch([
  { sql: "INSERT INTO lix_file (path, content) VALUES ($1, $2)", params: ["/a.txt", bytes] },
  { sql: "SELECT count(*) AS n FROM lix_file" },
]);
```

### observe()

```ts
const events = lix.observe(sql, params?);
```

Observes a SQL query. Returns an `ObserveEvents` handle. Call `next()` to await
the next result; it resolves with `{ sequence, mutationSequence, result }` for
the initial result and after each change, or `undefined` after the observation
is closed. Call `close()` to stop observing.

```ts
const events = lix.observe("SELECT path FROM lix_file");
const event = await events.next();
console.log(event?.result.rows.length);
events.close();
```

### beginTransaction()

```ts
const tx = await lix.beginTransaction();
```

Starts a transaction. While it is open, execute statements on the transaction handle.

```ts
const tx = await lix.beginTransaction();
try {
  await tx.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
    "/hello.txt",
    new TextEncoder().encode("hello"),
  ]);
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

### activeBranchId()

```ts
const branchId = await lix.activeBranchId();
```

Returns the id of the branch the Lix instance is currently reading and writing.

### activeAccountId()

```ts
const accountId = await lix.activeAccountId();
```

Returns the id of the active account.

### subscribeActiveBranch()

```ts
const unsubscribe = lix.subscribeActiveBranch(listener);
```

Subscribes to successful branch switches made through this Lix handle. The
`listener` is a function with no arguments. Returns an unsubscribe function.

### createCheckpoint()

```ts
const checkpoint = await lix.createCheckpoint();
```

Creates a checkpoint from the pending working changes on the active branch. See
[Checkpoints](./checkpoints.md).

Result:

```ts
type CreateCheckpointReceipt = {
  commitId: string;
};
```

### undo() / redo()

```ts
const undone = await lix.undo();
const redone = await lix.redo();
```

`undo()` reverts the latest change on the active branch by committing an
inverse commit. `redo()` replays the last undone change.

Results:

```ts
type UndoReceipt = {
  branchId: string;
  targetCommitId: string;
  inverseCommitId: string;
};

type RedoReceipt = {
  branchId: string;
  targetCommitId: string;
  replayCommitId: string;
};
```

### createBranch()

```ts
const branch = await lix.createBranch({
  name: "Explore",
});
```

Creates a branch.

Options:

| Option         | Type     | Description                       |
| -------------- | -------- | --------------------------------- |
| `name`         | `string` | Branch name.                      |
| `id`           | `string` | Optional explicit branch id.      |
| `fromCommitId` | `string` | Optional commit id to start from. |

Result:

```ts
type CreateBranchReceipt = {
  id: string;
  name: string;
  hidden: boolean;
  commitId: string;
};
```

### switchBranch()

```ts
await lix.switchBranch({ branchId });
```

Switches the Lix instance to another branch. Plain SQL tables read and write the active branch.

### mergeBranchPreview()

```ts
const preview = await lix.mergeBranchPreview({
  sourceBranchId: draft.id,
});
```

Computes the merge result from `sourceBranchId` into the active branch without applying it.

Result:

```ts
type MergeBranchPreview = {
  outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted";
  targetBranchId: string;
  sourceBranchId: string;
  baseCommitId: string;
  targetHeadCommitId: string;
  sourceHeadCommitId: string;
  changeStats: MergeChangeStats;
  conflicts: MergeConflict[];
};
```

### mergeBranch()

```ts
const merge = await lix.mergeBranch({
  sourceBranchId: draft.id,
});
```

Merges `sourceBranchId` into the active branch.

Result:

```ts
type MergeBranchReceipt = {
  outcome: "alreadyUpToDate" | "fastForward" | "mergeCommitted";
  targetBranchId: string;
  sourceBranchId: string;
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
  entityPk: unknown;
  fileId: string | null;
  target: MergeConflictSide;
  source: MergeConflictSide;
};
```

### close()

```ts
await lix.close();
```

Closes the Lix handle and its storage resources.

### clientState

`lix.clientState` stores private client-local JSON state with `get`, `set`, `delete`, and `subscribe`; it is available when the storage supports client state, for example a `LixSnapshotStorage` in remote mode.

## Transaction

Transactions expose:

| Method                            | Description                                                    |
| --------------------------------- | -------------------------------------------------------------- |
| `execute(sql, params?, options?)` | Execute SQL inside the transaction. Same `ExecuteOptions` as `lix.execute()`. |
| `commit()`                        | Commit the transaction and close the transaction handle.       |
| `rollback()`                      | Roll back the transaction and close the transaction handle.    |

## Row

Rows are returned by `execute()`.

```ts
const row = result.rows[0]!;
```

| Surface                 | Return type               | Description                                                    |
| ----------------------- | ------------------------- | -------------------------------------------------------------- |
| `row.get(columnName)`   | `unknown`                 | Native JS value for a column. Throws if the column is missing. |
| `row.value(columnName)` | `Value`                   | Typed `Value` for a column. Throws if the column is missing.   |
| `row.toObject()`        | `Record<string, unknown>` | Object of native JS values keyed by column name.               |
| `row.toValueMap()`      | `Record<string, Value>`   | Object of typed values keyed by column name.                   |

## Value

`Value` preserves the SQL type returned by the engine.

Accessors:

| Method      | Return type               | Description                                      |
| ----------- | ------------------------- | ------------------------------------------------ |
| `toJS()`    | `unknown`                 | Returns a defensive copy of the native JS value. |
| `asBytes()` | `Uint8Array \| undefined` | Returns a defensive copy for blob values.        |

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
| `Value.from(raw)`      | Convert a JSON-compatible JS value, `Uint8Array`, or `Value` into a `Value`.   |
