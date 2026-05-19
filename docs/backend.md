---
description: Lix's storage is pluggable. Implement the LixBackend interface (a synchronous, transactional, namespaced key-value store) and Lix runs on top of it.
---

# Backends

Lix's engine is independent of where the bytes live. Storage is exposed through a single interface, `LixBackend`, that any transactional key-value store can implement. Open a Lix with a different backend and the rest of the API (`openLix`, `execute`, `createVersion`, `mergeVersion`, …) is unchanged.

## What ships today

| Backend                        | Module                          | Use for                              |
| ------------------------------ | ------------------------------- | ------------------------------------ |
| In-memory                      | default (no `backend` argument) | tests, demos, ephemeral work         |
| SQLite file (`better-sqlite3`) | `@lix-js/sdk/sqlite`            | persistent, single-process Node apps |

```ts
import { openLix } from "@lix-js/sdk";
import { createBetterSqlite3Backend } from "@lix-js/sdk/sqlite";

const lix = await openLix({
  backend: createBetterSqlite3Backend({ path: "/var/data/app.lix" }),
});
```

Anything beyond these two is not shipped by the Lix team. Implement the `LixBackend` interface yourself and pass it to `openLix({ backend })`. This page is the contract.

## Sync today, async on the roadmap

> The current `LixBackend` contract is **synchronous**. All methods return values directly, not promises.

The JS SDK runs the engine inside WebAssembly and calls backend methods through synchronous wasm imports. That makes synchronous JS bindings the natural fit (`better-sqlite3` is sync; an in-memory `Map` is sync; native sync KV bindings work). Async-only Node libraries (`pg`, the AWS S3 SDK, IndexedDB, Cloudflare Durable Objects' storage) cannot drive the contract directly today.

Practical paths today:

- **Synchronous bindings.** `better-sqlite3`, in-memory data structures, sync OPFS access (`createSyncAccessHandle`), Neon-binding RocksDB, `node:sqlite` in newer Node versions.
- **Sync-over-async bridges.** Worker threads with `Atomics.wait`, `deasync`, or similar approaches. These add operational complexity and are best avoided for production workloads.

An async backend variant (where methods return `Promise<T>`) is on the roadmap so Postgres, IndexedDB, S3, and Durable Objects become first-class. Until then, treat the substrate list below as guidance for what *will* fit, not what's possible from the JS SDK today.

## The full TypeScript contract

These are the actual exported types from `@lix-js/sdk`:

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

| Method                                    | Purpose                                                                                                                                                                                                                                                              |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getValues`                               | Batch fetch values by exact key, grouped by namespace. Missing keys come back as `null` in the same position.                                                                                                                                                        |
| `existsMany`                              | Same request shape as `getValues`, returns booleans. Used when Lix only needs to know whether a key is present.                                                                                                                                                      |
| `scanKeys` / `scanValues` / `scanEntries` | Range or prefix scan within one namespace, with `limit` and a resumable `after` cursor.                                                                                                                                                                              |
| `writeKvBatch`                            | Atomic batch of ordered `ops`, grouped by namespace. Apply operations in list order. Either all of it lands or none of it does. `deleteRange` removes every key matching the same half-open range or prefix shape used by scans.                                     |
| `commit` / `rollback`                     | Transaction control. After either, the transaction object is finished; do not call further methods on it.                                                                                                                                                            |
| `close()` / `destroy()` (on the backend)  | Lifecycle. `close()` releases handles without affecting durability. `destroy()` (optional, not in the type signature above for backends that don't own their target) removes the entire storage target: file plus WAL/SHM, the OPFS target, the schema, the bucket. |

### Scan semantics

- **Order.** Keys come back in ascending lexicographic order on bytes.
- **Range.** Half-open: `start <= key < end`.
- **Prefix.** Equivalent to `range = { start: prefix, end: incrementLastByteWithCarry(prefix) }`.
- **Cursor.** `after` is **exclusive**: the next page returns keys strictly greater than `after`. `resumeAfter` is the last returned key; pass it back as `after` for the next page. `null` `resumeAfter` means no more pages.

### Namespaces

Every batch operation is grouped by `namespace: string`. Treat namespaces as logical tables; implementations typically map them to separate column families, prefixes, tables, or buckets.

The current JS/WASM engine bridge sends engine storage through one namespace, `"default"`, and encodes Lix storage-space identity into the key bytes. Backends must still implement namespace isolation because the public backend contract supports multiple namespaces and direct backend tests may exercise them, but engine traffic today does not require dynamic namespace creation.

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

The contract is small enough that **any transactional KV store with a synchronous binding can host Lix today**. The substrates below are good fits in principle; ones marked async-only require either a sync-over-async bridge or the upcoming async backend variant.

**Synchronous, ready today.** `better-sqlite3` (shipping), `node:sqlite` (Node 22+, sync), in-memory `Map`, OPFS via `createSyncAccessHandle` (web workers only), Neon/NAPI bindings to RocksDB or LMDB that expose sync APIs.

**Relational (Postgres, MySQL, SQLite-elsewhere)** (*async-only Node bindings*. One table per namespace, or a shared `(namespace, key)` PK table. Wrap each Lix transaction in a SQL transaction. Use repeatable-read isolation for reads, serializable or `SELECT ... FOR UPDATE` for writes. Postgres `bytea` matches Lix's byte-ordered scan requirement.

**Object storage (S3, R2, GCS)** (*async-only*, plus not natively transactional. Coordinate writes via a manifest object plus conditional PUT (`If-Match`). For atomic multi-key batches: stage chunks → upload → swap the manifest pointer in one CAS.

**Cloudflare.** *async-only*. D1 fits the relational pattern. Durable Objects give you a single-writer mailbox per object, a natural fit for a per-tenant Lix. Cloudflare KV is eventually consistent without transactions; not enough on its own.

**Browser.** *async-only* for IndexedDB, *sync if used in a worker* for OPFS. IndexedDB needs object stores declared at `onupgradeneeded`, so the namespace set must be known up front. The auto-commit-on-event-loop trap means buffered-write strategies are the only safe path.

**Embedded KV (RocksDB, LMDB, sled)** fit varies by binding. The closest-shaped substrates; map namespaces to column families or key prefixes. Native ranged iterators map directly to `scanKeys`. Sync via Neon binding or N-API works today; async-only bindings will need the future async backend.

**Distributed KV (DynamoDB, FoundationDB, TiKV)** (*async-only* in JS. Native transactional semantics. Redis with `MULTI`/`EXEC` is workable for single-instance setups, but its weak isolation makes multi-writer risky.

## Testing your backend

A conformance test suite is the right way to validate an implementation:

- Round-trip puts and gets within and across namespaces.
- **Atomicity.** A batch with one rejected write leaves everything unchanged.
- **Isolation.** A read transaction opened before a write commits does not see the writer's changes.
- **Read-your-writes.** A write transaction reads the values it just wrote (and not values from concurrent writers).
- **Scan ordering.** Keys come back byte-lex; the same `after` cursor yields the same next page absent writes.
- **Durability.** Close and reopen; committed data is still there.

Run the same suite against the in-memory and `better-sqlite3` backends as a baseline.

## Why this design

The engine that implements branches, merge, schemas, change journals, and SQL queries is one piece of code. The storage is another. Keeping the contract small (synchronous, namespaced, transactional KV) is what makes it tractable to put Lix on a SQLite file today and on Postgres, S3, or Durable Objects once the async variant lands, without forking the engine.

Same shape DuckDB takes with its readers: one engine, many places to read bytes from. Lix takes it for writes too.
