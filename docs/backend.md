---
description: Backends are Lix's system interface for durable storage. Implement the LixBackend interface (a transactional, namespaced key-value store) and Lix runs on top of it.
---

# Backends

Lix's engine is independent of where the bytes live. Storage is exposed through a single interface, `LixBackend`, that any transactional key-value store can implement. Open a Lix with a different backend and the rest of the API (`openLix`, `execute`, `createVersion`, `mergeVersion`, …) is unchanged.

Lix stops at the storage boundary. The engine writes semantic rows and chunks
such as changelog commits, changelog changes, commit change-ref chunks, JSON
payload rows, and tracked-state tree chunks. The backend is responsible for the
physical layout underneath those rows: pages, B-trees, LSM/SST files, WALs,
checksums, caches, locks, compaction, and file/object placement. In other
words, Lix defines _what facts exist_; the backend decides _how bytes are stored_.

## The system interface

The backend is where Lix crosses from the engine into the host system. Local
files, SQLite, OPFS, object storage, locks, caches, and durability belong behind
`LixBackend`.

Plugins are different: they run as sandboxed WebAssembly components. A plugin
receives engine-provided file/state inputs and returns semantic changes or
rendered bytes, but it does not get ambient filesystem, network, or operating
system access. If code transforms Lix data, it is probably a plugin. If code
talks to the outside system where Lix data lives, it is a backend.

## What ships today

| Backend              | Module                          | Use for                         |
| -------------------- | ------------------------------- | ------------------------------- |
| In-memory            | default (no `backend` argument) | tests, demos, ephemeral work    |
| Filesystem workspace | `@lix-js/sdk`                   | local folders with synced files |
| SQLite file          | `@lix-js/sdk`                   | single-file application formats |

```ts
import { FsBackend, openLix } from "@lix-js/sdk";

const lix = await openLix({
	backend: new FsBackend({ path: "/var/data/workspace" }),
});
```

Use `FsBackend` when the Lix should sync a local directory. This is the
recommended persistent backend for filesystem workspaces. The backend stores
its private SQLite database at `<workspace>/.lix/.internal/db.sqlite` and syncs
workspace files, including user-editable files under `.lix/` such as plugin
archives, through Lix.

```ts
import { FsBackend, openLix } from "@lix-js/sdk";

const lix = await openLix({
	backend: new FsBackend({ path: "/Users/me/Downloads", storage: "memory" }),
});
```

Pass `storage: "memory"` when the filesystem should be synced but Lix should not
write `.lix` repository metadata into that folder.

Use `SqliteBackend` when the `.lix` SQLite file is the application document
itself. This is useful when defining a new file format and using Lix as the
application file format: one portable file containing versioned application
state.

```ts
import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
	backend: new SqliteBackend({ path: "/var/data/app.lix" }),
});
```

Anything beyond these shipped backends is not shipped by the Lix team. Custom backends
implement the same contract for the host/runtime they target. This page is the
contract.

## Runtime shape

A backend may use local files, a native database binding, an async service, or
another host-side runtime. It must preserve Lix's transaction, scan, ordering,
and durability semantics while hiding the physical storage details from the
engine.

## Contract shape

At a high level, a backend provides transactions over namespaced key-value
storage:

```ts
type LixBackend = {
	beginReadTransaction(): LixBackendReadTransaction;
	beginWriteTransaction(): LixBackendWriteTransaction;
	close?(): void;
};

type LixBackendReadTransaction = {
	getValues(request: BackendKvGetRequest): BackendKvValueBatch;
	existsMany(request: BackendKvGetRequest): BackendKvExistsBatch;
	scanKeys(request: BackendKvScanRequest): BackendKvKeyPage;
	scanValues(request: BackendKvScanRequest): BackendKvValuePage;
	scanEntries(request: BackendKvScanRequest): BackendKvEntryPage;
	rollback(): void;
};

type LixBackendWriteTransaction = LixBackendReadTransaction & {
	writeKvBatch(batch: BackendKvWriteBatch): BackendKvWriteStats;
	commit(): void;
};

// ── Scan ranges ────────────────────────────────────────────────────────────

type BackendKvScanRange =
	| { kind: "prefix"; prefix: Uint8Array }
	| { kind: "range"; start: Uint8Array; end: Uint8Array };

// ── Get / exists ───────────────────────────────────────────────────────────

type BackendKvGetRequest = {
	groups: BackendKvGetGroup[];
};

type BackendKvGetGroup = {
	namespace: string;
	keys: Uint8Array[];
};

type BackendKvValueBatch = {
	groups: BackendKvValueGroup[];
};

type BackendKvValueGroup = {
	namespace: string;
	values: Array<Uint8Array | null>; // null = key not present
};

type BackendKvExistsBatch = {
	groups: BackendKvExistsGroup[];
};

type BackendKvExistsGroup = {
	namespace: string;
	exists: boolean[];
};

// ── Scan ───────────────────────────────────────────────────────────────────

type BackendKvScanRequest = {
	namespace: string;
	range: BackendKvScanRange;
	after?: Uint8Array | null; // exclusive cursor; returns keys strictly greater
	limit: number;
};

type BackendKvKeyPage = {
	keys: Uint8Array[];
	resumeAfter?: Uint8Array | null;
};

type BackendKvValuePage = {
	values: Uint8Array[];
	resumeAfter?: Uint8Array | null;
};

type BackendKvEntryPage = {
	keys: Uint8Array[];
	values: Uint8Array[];
	resumeAfter?: Uint8Array | null;
};

// ── Write ──────────────────────────────────────────────────────────────────

type BackendKvWriteBatch = {
	groups: BackendKvWriteGroup[];
};

type BackendKvWriteGroup = {
	namespace: string;
	ops: BackendKvWriteOp[];
};

type BackendKvWriteOp =
	| { kind: "put"; key: Uint8Array; value: Uint8Array }
	| { kind: "delete"; key: Uint8Array }
	| { kind: "deleteRange"; range: BackendKvScanRange };

type BackendKvWriteStats = {
	puts: number;
	deletes: number;
	deleteRanges: number;
	bytesWritten: number;
};
```

### Operations

| Method                                    | Purpose                                                                                                                                                                                                                                                             |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getValues`                               | Batch fetch values by exact key, grouped by namespace. Missing keys come back as `null` in the same position.                                                                                                                                                       |
| `existsMany`                              | Same request shape as `getValues`, returns booleans. Used when Lix only needs to know whether a key is present.                                                                                                                                                     |
| `scanKeys` / `scanValues` / `scanEntries` | Range or prefix scan within one namespace, with `limit` and a resumable `after` cursor.                                                                                                                                                                             |
| `writeKvBatch`                            | Atomic batch of ordered `ops`, grouped by namespace. Apply operations in list order. Either all of it lands or none of it does. `deleteRange` removes every key matching the same half-open range or prefix shape used by scans.                                    |
| `commit` / `rollback`                     | Transaction control. After either, the transaction object is finished; do not call further methods on it.                                                                                                                                                           |
| `close()` / `destroy()` (on the backend)  | Lifecycle. `close()` releases handles without affecting durability. `destroy()` (optional, not in the type signature above for backends that don't own their target) removes the entire storage target: file plus WAL/SHM, the OPFS target, the schema, the bucket. |

### Scan semantics

- **Order.** Keys come back in ascending lexicographic order on bytes.
- **Range.** Half-open: `start <= key < end`.
- **Prefix.** Equivalent to `range = { start: prefix, end: incrementLastByteWithCarry(prefix) }`.
- **Cursor.** `after` is **exclusive**: the next page returns keys strictly greater than `after`. `resumeAfter` is the last returned key; pass it back as `after` for the next page. `null` `resumeAfter` means no more pages.

### Namespaces

Every batch operation is grouped by `namespace: string`. Treat namespaces as logical tables; implementations typically map them to separate column families, prefixes, tables, or buckets.

Engine storage may use a small fixed set of namespaces and encode additional
storage-space identity into the key bytes. Backends must still implement
namespace isolation because the public backend contract supports multiple
namespaces and direct backend tests may exercise them.

### Physical boundary

The keys and values Lix writes are already the engine's physical contract. They
are semantic storage rows, not instructions for how a backend should arrange
disk blocks. A backend should not try to understand Lix-level spaces like
`changelog.commit`, `changelog.change`, or `tracked_state.tree_chunk` beyond
storing and scanning the bytes by namespace/key.

This is intentional. SQLite should use its pages, B-trees, WAL, and overflow
pages. RocksDB should use its memtables, WAL, SST blocks, block cache, and
compaction. redb should use its copy-on-write B-trees and MVCC pages. An object
storage backend might build its own manifest/chunk scheme. Those are backend
concerns.

Lix therefore avoids a second giant application-level packfile/segment layer on
top of those systems. If Lix needs locality for a first-class query, it expresses
that as a semantic row or bounded chunk, for example
`commit_id/chunk_no -> commit_change_ref_chunk`. It does not require every
backend to store many unrelated facts inside one opaque value.

## Required guarantees

1. **Atomic write batches.** `writeKvBatch` either applies all ordered operations across all namespaces, or none of them. A partial failure must roll back the batch.
2. **Read isolation within a transaction.** A read transaction sees a consistent snapshot for its lifetime; concurrent commits do not bleed in.
3. **Read-your-writes within a write transaction.** Reads after a put in the same write transaction see the new value; reads after a delete see `null`.
4. **Durable commits.** When `commit()` returns on a write transaction, the changes survive process restart (for persistent backends).
5. **Byte-ordered scans.** Keys come back in ascending lexicographic order of bytes. Stable pagination: the same `after` cursor returns the same next page if no writes happened in between.

## Concurrency model

- **One write transaction at a time.** The engine serializes write transactions itself; you don't need to queue them. A backend may still want a process-wide lock for safety.
- **Read transactions are concurrent with writes.** Multiple read transactions can be open while a write transaction is in flight. Reads must see the snapshot from when they were opened, not the in-progress write.
- **Transactions are short.** The engine doesn't hold transactions across user awaits; treat `beginReadTransaction()` → operations → `commit()`/`rollback()` as a tight sequence.

## Implementation notes by storage type

The contract is small enough that many transactional KV-shaped substrates can
host Lix:

**Local and embedded.** SQLite, in-memory stores, OPFS, RocksDB, LMDB, sled, or
similar systems. Map namespaces to tables, column families, prefixes, or
buckets. Native ranged iterators map directly to `scanKeys`.

**Relational.** Postgres, MySQL, or SQLite. Use one table per namespace, or a
shared `(namespace, key)` primary-key table. Wrap each Lix transaction in a
database transaction. Let the database own page layout, indexes, WAL,
checkpoints, and vacuum/compaction; Lix only needs ordered transactional rows.

**Object storage.** S3, R2, or GCS are not natively transactional. A backend can
coordinate writes with a manifest object and conditional PUT (`If-Match`): stage
chunks, upload, then swap the manifest pointer in one CAS.

**Cloudflare and browser storage.** D1 fits the relational pattern. Durable
Objects give you a single-writer mailbox per object. IndexedDB and OPFS can fit
if the backend preserves the required transaction and scan semantics. Cloudflare
KV is eventually consistent without transactions; not enough on its own.

**Distributed KV.** DynamoDB, FoundationDB, and TiKV can fit when the backend
uses their native transactional semantics. Redis with `MULTI`/`EXEC` is workable
for single-instance setups, but its weak isolation makes multi-writer risky.

## Testing your backend

A conformance test suite is the right way to validate an implementation:

- Round-trip puts and gets within and across namespaces.
- **Atomicity.** A batch with one rejected write leaves everything unchanged.
- **Isolation.** A read transaction opened before a write commits does not see the writer's changes.
- **Read-your-writes.** A write transaction reads the values it just wrote (and not values from concurrent writers).
- **Scan ordering.** Keys come back byte-lex; the same `after` cursor yields the same next page absent writes.
- **Durability.** Close and reopen; committed data is still there.

Run the same suite against the in-memory and SQLite backends as a baseline.

## Why this design

The engine that implements branches, merge, schemas, change journals, and SQL
queries is one piece of code. The storage is another. Keeping the contract small
(namespaced, transactional KV) is what makes it tractable to put Lix on a
SQLite file, Postgres, S3, Durable Objects, or another system interface without
forking the engine.

That boundary is also why Lix writes rows instead of owning a universal
on-disk pack format. Backends are better positioned to decide physical layout
for their substrate. The engine's job is to produce stable semantic facts and
derived read models; the backend's job is to make those rows durable, ordered,
transactional, cached, and compacted.

Same shape DuckDB takes with its readers: one engine, many places to read bytes from. Lix takes it for writes too.
