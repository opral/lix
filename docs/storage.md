---
description: Storage is Lix's physical persistence boundary. Implement the Rust Storage, StorageRead, and StorageWrite traits to run the Lix engine on another ordered transactional store.
---

# Storage

Lix is independent of where its bytes live. The engine owns SQL execution,
branches, merge, schemas, history, and semantic changes. A storage
implementation only provides ordered byte-key persistence, coherent read
views, and atomic writes.

Changing storage does not change the application-facing Lix API. Calls such as
`execute`, `createBranch`, and `mergeBranch` continue to run in the engine above
this boundary.

## The physical boundary

Lix writes rows and bounded chunks for changelog commits, commit graphs, JSON
payloads, indexes, and tracked-state trees. Storage implementations decide how
those bytes are represented physically: pages, B-trees, LSM/SST files, WALs,
checksums, caches, locks, compaction, or objects.

In other words, Lix defines _which facts exist_; storage decides _where and how
their bytes are stored_. A storage implementation does not parse or execute Lix
SQL and should not interpret engine concepts such as branches or changes.

Plugins are a different boundary. They are sandboxed WebAssembly components
that transform engine-provided file and state inputs into semantic changes or
rendered bytes. Code that transforms Lix data is probably a plugin. Code that
persists Lix's ordered keys and values is storage.

## Available implementations

| Storage | Rust | JavaScript | Use for |
| --- | --- | --- | --- |
| `Memory` | `lix_engine` and `lix_sdk` | default when `storage` is omitted | tests, demos, and ephemeral work |
| `SQLite` | `lix_sqlite_storage` and `lix_sdk` (`sqlite`) | `@lix-js/sdk` | a portable single-file application format |
| `SlateDB` | `lix_slatedb_storage` | not exposed | object-storage and LSM-based deployments |
| `RocksDB` | `lix_rocksdb_storage` | not exposed | native embedded persistence |
| `LocalFilesystem` | `lix_sdk` (`local_filesystem`) | `@lix-js/sdk` | a local directory synchronized with Lix |

Rust applications can implement the public `Storage` traits described below.
The JavaScript SDK currently accepts its built-in choices only: omit `storage`
for `Memory`, or pass `SQLite` or `LocalFilesystem`. `SQLite` and
`LocalFilesystem` require Node.js; the default `Memory` storage also works in
browsers.

### Memory

Omit the `storage` option to open an ephemeral in-memory Lix:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
```

### SQLite

Use `SQLite` when a `.lix` SQLite file is the application document itself. This
is useful when defining a new file format whose versioned application state
lives in one portable file.

```ts
import { openLix, SQLite } from "@lix-js/sdk";

const lix = await openLix({
	storage: new SQLite({ path: "/var/data/app.lix" }),
});
```

### LocalFilesystem

Use `LocalFilesystem` when Lix should synchronize a local directory:

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
	storage: new LocalFilesystem({
		path: "/var/data/workspace",
		syncAllFiles: true,
	}),
});
```

`LocalFilesystem` keeps private RocksDB state at
`<workspace>/.lix/.internal/rocksdb` and synchronizes workspace files through
Lix. The `.lix/.internal` directory belongs to Lix and is not materialized as a
workspace file.

Older SQLite filesystem metadata is not migrated. If legacy SQLite metadata is
present in `.lix/.internal` and no RocksDB store exists, Lix clears the internal
directory and initializes a fresh RocksDB store.

To keep repository metadata outside the workspace, pass `lixDir` pointing to an
external `.lix` directory. Workspace files are still imported, watched, and
materialized, but the workspace itself does not receive a `.lix` directory.

Set `syncAllFiles: false` to start without importing regular workspace files,
then call `importPaths()` with exact workspace-relative file paths. Call
`syncDiskToLix()` when an explicit barrier is needed after disk changes.

## Rust contract

The public extension point consists of three asynchronous traits from
`lix_engine`: `Storage`, `StorageRead`, and `StorageWrite`.

```rust
pub trait Storage: Send + Sync {
	type Read<'a>: StorageRead + 'a
	where
		Self: 'a;

	type Write<'a>: StorageWrite + 'a
	where
		Self: 'a;

	fn begin_read(
		&self,
		opts: ReadOptions,
	) -> impl Future<Output = Result<Self::Read<'_>, StorageError>> + Send;

	fn begin_write(
		&self,
		opts: WriteOptions,
	) -> impl Future<Output = Result<Self::Write<'_>, StorageError>> + Send;
}

pub trait StorageRead: Send + Sync {
	fn get_many(
		&self,
		space: SpaceId,
		keys: &[Key],
		opts: GetOptions,
	) -> impl Future<Output = Result<GetManyResult, StorageError>> + Send;

	fn scan(
		&self,
		space: SpaceId,
		range: KeyRange,
		opts: ScanOptions,
	) -> impl Future<Output = Result<ScanChunk, StorageError>> + Send;
}

pub trait StorageWrite: Send {
	fn put_many(
		&mut self,
		space: SpaceId,
		entries: PutBatch,
	) -> impl Future<Output = Result<(), StorageError>> + Send;

	fn delete_many(
		&mut self,
		space: SpaceId,
		keys: &[Key],
	) -> impl Future<Output = Result<(), StorageError>> + Send;

	fn delete_range(
		&mut self,
		space: SpaceId,
		range: KeyRange,
	) -> impl Future<Output = Result<(), StorageError>> + Send;

	fn commit(self)
		-> impl Future<Output = Result<CommitResult, StorageError>> + Send;

	fn rollback(self)
		-> impl Future<Output = Result<(), StorageError>> + Send;
}
```

The future-based boundary allows remote implementations to yield while waiting
for I/O and lets the engine overlap independent operations on one read view.
Asynchronous implementations should preserve that behavior instead of blocking
the caller's executor.

### Spaces, keys, and values

Storage is divided into engine-defined spaces identified by numeric `SpaceId`
values. Every operation addresses exactly one space. Keys are byte strings
ordered lexicographically within that space; values are opaque bytes. Spaces
must not overlap and may map naturally to separate tables, trees, column
families, prefixes, or buckets.

A space comes into existence on its first write. Reading a space that has never
been written behaves as reading an empty space.

The engine may request either `CoreProjection::KeyOnly` or
`CoreProjection::FullValue`. A key-only read must avoid returning value bytes;
a full-value read returns the opaque stored bytes.

### Point reads

`StorageRead::get_many` reads exact keys from one space. Its result contains one
slot per requested key in caller order. Duplicate keys remain duplicated and a
missing key produces `None` in its corresponding slot.

### Scans

`StorageRead::scan` returns entries in ascending byte-key order within one
space. `KeyRange` uses Rust's included, excluded, and unbounded bounds rather
than assuming every range is half-open.

`ScanOptions.resume_after` is an exclusive cursor. It cannot widen the supplied
range: the effective lower bound is the greater of the range's lower bound and
`Excluded(resume_after)`. `ScanChunk.has_more` reports whether another page is
available.

One scan call returns at most `MAX_SCAN_PAGE_ROWS` rows, currently 1,024, even
if `limit_rows` is larger. A zero row limit returns an empty chunk with
`has_more: false`.

Use `Prefix::to_range()` to lower a byte prefix to its equivalent `KeyRange`.

### Writes

A `StorageWrite` handle stages mutations for one atomic commit. `put_many` and
`delete_many` operate on one space at a time and accept at most one mutation per
key in each batch. `delete_range` deletes all keys in the supplied range; a
fully unbounded range clears that space and may be implemented as a truncate.

`commit` atomically publishes the handle's staged mutations and returns a
`CommitResult` with provider commit information and `WriteStats`. `rollback`
discards the staged mutations.

`StorageWrite` deliberately does not implement `StorageRead`: the public
contract does not promise reads through a write handle or read-your-writes
behavior.

## Required guarantees

1. **Space isolation.** Identical keys in different `SpaceId` values never
   collide.
2. **Coherent read views.** A handle returned by `begin_read` observes one
   coherent view for its lifetime.
3. **Ordered scans.** Scan entries are returned in ascending lexicographic byte
   order and pagination respects the exclusive cursor.
4. **Atomic commits.** A successful `commit` publishes all staged mutations;
   failed or rolled-back writes do not publish a partial result.
5. **Durability.** Persistent implementations honor the requested durability
   and retain successful commits across process restart. `Memory` is explicitly
   ephemeral.

Read handles release snapshots and other resources through `Drop`; there is no
read `close` or `rollback` method. The `Storage` trait also does not prescribe
provider lifecycle or destructive methods such as `close` or `destroy`.
Implementations that own such resources may expose provider-specific methods in
addition to the trait.

## Concurrency

The storage implementation is the write-concurrency boundary. Lix does not add
a generic per-storage write lock above `begin_write`. An implementation that
cannot safely support overlapping writers must serialize them, use native
transactional locking, or reject the additional writer with a deterministic
`StorageError`.

`StorageRead` is `Send + Sync`, so independent point reads and scans on one
coherent read view may overlap. A `StorageWrite` is `Send` but is mutated through
`&mut self`.

## Implementation guidance

The contract is intentionally close to transactional ordered key-value
primitives:

- **Embedded stores.** SQLite, SlateDB, RocksDB, LMDB, and similar
  systems can map spaces to tables, column families, trees, or prefixes and use
  native snapshots and write transactions.
- **Relational databases.** Postgres, MySQL, and SQLite can use one table per
  space or a shared `(space, key)` primary-key table. The database should own
  page layout, indexes, WAL, checkpoints, and vacuum behavior.
- **Object storage.** S3, R2, and GCS need an atomic publication scheme above
  their object APIs, such as immutable chunks plus a conditionally swapped
  manifest. Merely uploading objects independently is not an atomic commit.
- **Browser and edge storage.** IndexedDB, OPFS, D1, and Durable Objects can fit
  when the implementation preserves coherent reads, ordered scans, and atomic
  commits. Eventually consistent key-value storage is insufficient on its own.
- **Distributed key-value stores.** FoundationDB, DynamoDB, and TiKV can fit
  when their native transactions provide the required semantics.

Do not add a second universal packfile or segment format merely to implement
this interface. Let the underlying system own its physical layout. When Lix
needs locality, the engine expresses it as a bounded semantic row or chunk; the
storage implementation persists and scans those opaque bytes.

## Testing an implementation

Implement `StorageFactory` and `StorageFixture`, then run the public conformance
suite:

```rust
use lix_engine::run_storage_conformance;

let report = run_storage_conformance(&factory).await;
report.assert_no_failures();
```

The suite covers space isolation, point-read ordering and missing values,
bounded scans and cursors, deletes, atomic commit, rollback, coherent read
views, persistence, and deterministic model-based histories. Mark an
implementation `ephemeral` in `StorageTestConfig` to skip restart persistence
tests.

Use `Memory` and `SQLite` as baseline implementations when diagnosing a custom
storage implementation.
