---
description: "Reference for opening local, remote, and synchronized Lix instances, running SQL, using transactions, and working with branches."
---

# JavaScript API Reference

`@lix-js/sdk` exports `openLix()`, `createLix()`, `deleteLix()`, the generic JavaScript storage protocol,
`Value` and `bundledPluginArchives`. `@lix-js/storage-opfs` and
`@lix-js/storage-filesystem` provide concrete storage implementations.
`openLix()` returns a local repository, a thin remote client, or a synchronized
local replica.

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
```

## Hosted repository lifecycle

`createLix()` provisions a hosted repository and returns `{ id, url }`. Its
`server.url` is the host origin; opening and deleting use the repository URL.

```ts
import { createLix, openLix, deleteLix } from "@lix-js/sdk";

const headers = () => ({ Authorization: `Bearer ${token}` });
const repository = await createLix({
  server: { url: "https://example.com", headers },
});
const remote = await openLix({ server: { url: repository.url, headers } });
await remote.close();
await deleteLix({ server: { url: repository.url, headers } });
```

Supply `from: localLix` to copy an existing repository instead of creating an
empty one. Creation takes one consistent snapshot, including files, branches,
history, and untracked rows. It does not attach or change the source handle.
Subsequent source edits are not part of that copy. To attach the original
durable storage afterward, pause writes during creation, close the local handle,
and reopen the same storage with the returned server URL. Lix refuses to replace
unrelated or locally diverged history.

Supply `idempotencyKey` to recover a creation after a lost response. Retry with
the same key and unchanged source snapshot; reusing a key for different content
fails. When omitted, the SDK generates a key for that call. Lifecycle requests
accept `url` and `headers`; custom `fetch` overrides are not supported.

Browser creation from a local repository requires Fetch request streaming;
browsers without it return `LIX_UNSUPPORTED_OPERATION`. Browser Fetch may also
require HTTP/2 or HTTP/3 for these uploads. Lix does not buffer a complete
repository as a fallback. Empty creation does not require request streaming.

Opening a missing hosted repository fails; it never provisions one. Deletion
removes the hosted resource without deleting local copies. Closing only releases
a session. A disconnected replica never recreates a deleted server repository.

## openLix()

```ts
const lix = await openLix(options?);
```

Options:

| Option      | Type                                             | Description                                                                                |
| ----------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `storage`   | `LixStorage`                                     | Local storage selected by a provider package. Omit both `storage` and `server` for memory.                          |
| `server`    | `LixServerOptions` | Connect directly to a server or synchronize a local replica.                               |
| `telemetry` | `LixTelemetryOptions`                            | Optional `onSpan(span)` callback that receives telemetry spans. Local and sync modes only. |

Connect to a remote server:

```ts
const lix = await openLix({
  server: {
    url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
    headers: () => ({ Authorization: `Bearer ${token}` }),
  },
});
```

Remote file content, SQL rows, and branches live on the server. Use `headers` for authentication and `fetch` when you need a custom fetch implementation.

Open a synchronized local replica by combining `storage` with `server`:

```ts
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
  storage: new OpfsStorage({ name: "atelier" }),
  server: {
    url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
    headers: () => ({ Authorization: `Bearer ${token}` }),
  },
});
```

With storage and a server, mutations execute on the server while certified
current-state reads can use the local replica. See
[Collaboration](./collaboration-and-sync.md) for the complete behavior.

Use `OpfsStorage` to persist a local browser Lix across reloads:

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
  storage: new OpfsStorage({ name: "atelier" }),
});
```

Use `FilesystemStorage` for a repository directory backed by RocksDB at
`<repository>/.lix/.internal/rocksdb`:

```ts
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

const lix = await openLix({
  storage: new FilesystemStorage({ path: "./repository" }),
});
```

Use selective synchronization when only explicit paths should be imported:

```ts
const storage = new FilesystemStorage({
  path: "./repository",
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

Executes one PostgreSQL-dialect SQL statement against the active Lix session.
Pass a single statement. To run several statements atomically, call
`executeBatch()` with an array of `{ sql, params? }` objects. Do not concatenate
statements into one SQL string or parse a script on the host.

Parameters:

| Parameter | Type             | Description                                                        |
| --------- | ---------------- | ------------------------------------------------------------------ |
| `sql`     | `string`         | One statement from Lix's PostgreSQL-dialect subset.                |
| `params`  | `SqlParam[]`     | Optional positional parameters addressed as `$1`, `$2`, and so on. |
| `options` | `ExecuteOptions` | Optional execution options. See below.                             |

`SqlParam` accepts JSON values, `Uint8Array`, or a `Value`:

```ts
type SqlParam = JsonValue | Uint8Array | Value;
```

`ExecuteOptions`:

| Option           | Type     | Description                                                                                                                                                                                                                                                                       |
| ---------------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `originKey`      | `string` | Optional origin label for the mutation.                                                                                                                                                                                                                                           |
| `idempotencyKey` | `string` | Stable identity for one logical remote SQL mutation. This is the retry story: supply the same key when retrying after a lost response, and the server applies the mutation only once. Remote Lix generates one per call when omitted. Sent as `Idempotency-Key`, not SQL options. |
| `rowMode`        | `"object" \| "array"` | Return plain objects by default or positional arrays when duplicate column names must remain separately addressable. |

Result:

```ts
type ExecuteResult<TRow = Record<string, unknown>> = {
  statementIndex?: number;
  label?: string;
  columns: { name: string; type: "null" | "boolean" | "integer" | "real" | "text" | "jsonb" | "timestamptz" | "blob" }[];
  rows: TRow[];
  rowsAffected: number;
  notices: { code: string; message: string; hint?: string }[];
  commit?: { before: string; after: string };
};
```

| Field          | Description                                                                 |
| -------------- | --------------------------------------------------------------------------- |
| `columns`      | Column names and SQL value types in result order. Empty for statements that do not return rows. |
| `rows`         | Enumerable plain objects by default. Property access, destructuring, spread, and JSON serialization work directly. |
| `rowsAffected` | Number of rows affected by write statements.                                |
| `notices`      | Non-fatal engine notices with `{ code, message, hint? }`.                   |
| `commit`       | The active-branch commits a write moved between: `before` is the branch head before the write, `after` the head it published, so `lix_diff('lix_file', before, after)` is exactly what it changed. Present for every auto-committed write statement, including `RETURNING` writes and restores, and for every statement of a written batch (all share the batch's span, read statements included); a write that published no commit on the active branch reports both ids equal. Absent for read statements outside a written batch, read-only batches, statements inside an explicit transaction, and the first commit on a branch that had no head yet. |

Example:

```ts
const result = await lix.execute(
  "SELECT path, content FROM lix_file WHERE path = $1",
  ["/hello.txt"],
);

const path = result.rows[0]?.path;
const content = result.rows[0]?.content as Uint8Array | undefined;
```

### executeBatch()

```ts
const results = await lix.executeBatch(statements, options?);
```

Executes multiple statements atomically in one call. `statements` is a non-empty
array of `{ sql, params?, label? }` objects — one statement per entry, already
split by the caller. Lix does not parse a multi-statement script. `options`
accepts the same `originKey` and `idempotencyKey` as `execute()`. Results
preserve input order and include a zero-based `statementIndex`. A supplied label
is echoed unchanged; labels are opaque and may repeat. If a label is omitted,
the result has no `label` property.

```ts
const results = await lix.executeBatch([
  {
    label: "create",
    sql: "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
    params: ["/a.txt", bytes],
  },
  { sql: "SELECT count(*) AS n FROM lix_file" },
]);

console.log(results[0].statementIndex, results[0].label); // 0, "create"
console.log(results[1].statementIndex, results[1].label); // 1, undefined

const returning = await lix.executeBatch([
  {
    label: "update",
    sql: "UPDATE task SET done = true WHERE id = $1 RETURNING id, done",
    params: ["task-1"],
  },
]);
console.log(returning[0].rows[0]?.done);
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

Starts an independent transaction context on this handle's current branch and
account. Execute statements that belong to the transaction through `tx.execute()`;
these reads see its staged writes. Ordinary `lix.execute()` reads and
`lix.observe()` remain available on the original handle and see committed data.
Observers publish relevant updates after commit; rolled-back writes are never
published. Changing the original handle's branch does not retarget the transaction.
Local transactions retain the caller's previously acknowledged plugin-file view,
so plugins can merge stale content against the correct base.

Each handle permits one opening or active explicit transaction at a time. Use
`openAnotherSession()` for another independent handle when needed.

Commit or roll back the transaction before closing the original handle. Closing
with an opening or active transaction still fails with
`LIX_INVALID_TRANSACTION_STATE`.

SQL `UPDATE` and `DELETE` decisions are protected until commit. If another
transaction changes active-branch or shared/global state after this transaction
opens, committing these statements fails with `LIX_TRANSACTION_CONFLICT`. Start
a new transaction and rerun its statements against current state. This is a conservative branch
check, including untracked rows: even changes to unrelated rows can require a
retry. A successfully planned update or delete retains this check if it matches
no rows or subsequently fails and the transaction continues with other writes.

Rows returned by `RETURNING` inside a transaction are provisional. Report a
publication as successful only after `commit()` succeeds. Ordinary local
`execute()` and `executeBatch()` automatically retry transaction conflicts a
bounded number of times by rerunning the statements; they can still return a
conflict if contention persists. This check does not provide general serializable
isolation for arbitrary reads or cross-branch dependencies.

Unconditional `INSERT ... ON CONFLICT DO UPDATE` file saves and explicit branch
merges retain their collaboration semantics. Use `UPDATE ... WHERE` with an
expected revision and require commit success when publication depends on that
revision remaining current.

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

### Checkpoints

Checkpointing uses the canonical SQL surface rather than a separate typed SDK
method:

```ts
const result = await lix.execute(
  "SELECT commit_id FROM lix_create_checkpoint()",
);
const commitId = result.rows[0].commit_id;
```

See [Checkpoints](./checkpoints.md) for scoped row-reference selections.

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

```ts
type SwitchBranchReceipt = { branchId: string };
```

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
  kind: "sameRowChanged";
  rowRef: string;
  fileId: string | null;
  target: MergeConflictSide;
  source: MergeConflictSide;
};

type MergeConflictSide = {
  kind: "added" | "modified" | "removed";
  beforeChangeId: string | null;
  afterChangeId: string | null;
};
```

### close()

```ts
await lix.close();
```

Closes the Lix handle and its storage resources.

## Transaction

Transactions expose:

| Method                            | Description                                                                   |
| --------------------------------- | ----------------------------------------------------------------------------- |
| `execute(sql, params?, options?)` | Execute SQL inside the transaction. Same `ExecuteOptions` as `lix.execute()`. |
| `commit()`                        | Commit the transaction and close the transaction handle.                      |
| `rollback()`                      | Roll back the transaction and close the transaction handle.                   |

## Result rows

`execute()` returns ordinary JavaScript objects.

```ts
const row = result.rows[0]!;
```

Use `row.column_name`, `row[dynamicColumn]`, destructuring, spread, or
`JSON.stringify(row)` directly. Duplicate output names use the last value in
object mode while every descriptor remains in `columns`; pass
`{ rowMode: "array" }` to `execute()` or `executeBatch()` when positional
duplicates are required.

## Value

`Value` constructs explicitly typed SQL parameters. Returned values are native
JavaScript values and their SQL types are described by `result.columns`.

Accessors:

| Method      | Return type               | Description                                      |
| ----------- | ------------------------- | ------------------------------------------------ |
| `toJS()`    | `unknown`                 | Returns a defensive copy of the native JS value. |
| `asBytes()` | `Uint8Array \| undefined` | Returns a defensive copy for blob values.        |

Constructors:

| Method                 | Description                                                                  |
| ---------------------- | ---------------------------------------------------------------------------- |
| `Value.null()`         | Create a SQL null value.                                                     |
| `Value.integer(value)` | Create an integer value.                                                     |
| `Value.boolean(value)` | Create a boolean value.                                                      |
| `Value.real(value)`    | Create a real number value.                                                  |
| `Value.text(value)`    | Create a text value.                                                         |
| `Value.jsonb(value)`   | Create a JSONB value.                                                        |
| `Value.timestamptz(value)` | Create a timestamptz value from an RFC 3339 string.                    |
| `Value.blob(value)`    | Create a blob value from `Uint8Array`.                                       |
| `Value.from(raw)`      | Convert a JSON-compatible JS value, `Uint8Array`, or `Value` into a `Value`. |
