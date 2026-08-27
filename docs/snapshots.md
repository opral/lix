---
description: Export and restore a complete Lix for reproduction, transfer, and recovery.
---

# Snapshots

A Lix snapshot is a complete, point-in-time copy of a Lix. It is a binary
artifact with the extension `.lixsnap`.

Use a snapshot to:

- attach an exact state to a bug report or test;
- reproduce a problem in another environment;
- move a Lix between supported storage adapters;
- restore a Lix into new persistent storage;
- provide the full-copy payload to a backup system.

## Snapshot versus checkpoint

A [checkpoint](./checkpoints.md) is a meaningful version inside Lix history. A
snapshot is a portable copy outside Lix.

| | Checkpoint | Snapshot |
| --- | --- | --- |
| Location | Inside Lix history | External `.lixsnap` artifact |
| Scope | One version on a branch | All branches, history, data, and blobs |
| Use | Review, undo, and version history | Reproduction, transfer, and recovery |

Creating a snapshot does not create a commit or checkpoint. Restoring one does
not merge new history into an existing Lix.

## What a snapshot contains

Lix opens one coherent read and exports every registered logical storage
space. This includes files, application rows, schemas, branches, history,
blobs, and engine-owned state.

It does not contain inactive migration data, backend WALs or caches, active
sessions or transactions, server configuration, credentials, or encryption
keys. The container adds no export timestamp, hostname, Lix name, random
identifier, or exporter metadata, so equal logical states produce equal bytes.

The artifact describes logical Lix data, not the physical layout of RocksDB,
SlateDB, OPFS, or another adapter. Use backend-native tooling when physical
storage itself needs forensic recovery.

## Exporting in Rust

`export_snapshot()` returns a builder. Its only terminal operation streams the
snapshot to an async writer. Publish backup files through a temporary sibling
so an interrupted export never appears under the final `.lixsnap` name:

```rust
use futures_lite::io::AsyncWriteExt as _;
use std::path::Path;

let final_path = Path::new("reproduction.lixsnap");
let temporary_path = Path::new("reproduction.lixsnap.part");
let mut destination = async_fs::File::create(temporary_path).await?;

let report = lix
    .export_snapshot()
    .write_to(&mut destination)
    .await?;

destination.flush().await?;
destination.sync_all().await?;
drop(destination);
async_fs::rename(temporary_path, final_path).await?;

// Sync the rename itself where directory fsync is supported.
let parent = async_fs::File::open(final_path.parent().unwrap_or(Path::new("."))).await?;
parent.sync_all().await?;
```

The temporary file must be in the same directory as the final file for the
rename to be atomic. Give concurrent exports distinct temporary names, ignore
`.part` files when discovering backups, and remove abandoned temporary files
after a failed export. On platforms without directory fsync, use the platform's
equivalent durable-publication primitive.

For a bounded test fixture, a `Vec<u8>` is also an async writer:

```rust
let mut bytes = Vec::new();

lix.export_snapshot()
    .write_to(&mut bytes)
    .await?;
```

Visible committed state is the default. Backup tooling can require data that
has crossed the adapter's durable-read boundary:

```rust
use lix::storage::ReadDurability;

lix.export_snapshot()
    .durability(ReadDurability::Durable)
    .write_to(&mut destination)
    .await?;
```

An adapter that cannot provide the requested durability fails the export. Lix
does not flush or mutate the source to manufacture durability.

`SnapshotExportReport` returns the entry count, canonical payload byte count,
and BLAKE3 digest written into the snapshot trailer.

## Restoring and opening in Rust

Restore into the default in-memory storage:

```rust
let source = async_fs::File::open("reproduction.lixsnap").await?;

let restored = lix::open_lix()
    .from_snapshot(source)
    .await?;
```

Select a fresh persistent destination before `from_snapshot()`:

```rust
use lix_storage_rocksdb::RocksDB;

let storage = RocksDB::open("/var/lib/lix/restored")?;
let source = async_fs::File::open("reproduction.lixsnap").await?;

let restored = lix::open_lix()
    .with_storage(storage)
    .from_snapshot(source)
    .await?;
```

The destination must be empty of every known Lix-owned logical space,
including retired spaces. Lix verifies the complete stream, writes into an
unpublished storage epoch, publishes it atomically, applies any supported
format migration, and then opens it. It never merges a snapshot into or
overwrites an existing Lix.

There is deliberately no `lix.import_snapshot()`. Replacing storage under a
live handle would invalidate active sessions, transactions, observers, and
caches. To persist a restored in-memory Lix after changing it, export a new
snapshot and restore that artifact into a fresh persistent destination.

## JavaScript

The JavaScript API uses standard web streams and keeps the same small public
surface in browsers and Node.js:

```ts
const snapshot = lix.exportSnapshot();

await snapshot.pipeTo(writable, {
  signal: abortController.signal,
});
```

Restore and open a fresh in-memory Lix from a stream:

```ts
const restored = await openLix.fromSnapshot(snapshotStream);
```

Select a fresh persistent destination with the normal open options:

```ts
const restored = await openLix.fromSnapshot(snapshotStream, {
  storage: new FilesystemStorage({ path: "/var/lib/lix/restored" }),
});
```

`openLix.fromSnapshot()` also accepts a `Uint8Array` for bounded fixtures. It
rejects remote or sync server mode and any destination that already contains
a Lix. There is no separate byte-array export method; bounded callers can use
standard APIs such as `new Response(lix.exportSnapshot()).arrayBuffer()`.

## Remote export

A host exposes the same deterministic stream through the Lix Server Protocol:

```http
GET /lix/v1/{lix_id}/snapshot
Authorization: Bearer <access-token>
Accept: application/vnd.lix.snapshot
```

The response is a backpressured `application/vnd.lix.snapshot` stream with
`Cache-Control: no-store, no-transform`. Snapshot export requires an
authenticated host principal even when selected files from the Lix are public.
`lix.exportSnapshot()` remains a local-handle API; non-SDK backup clients call
this authenticated REST endpoint and stream the body to their destination.
The remote protocol deliberately has no snapshot upload or import route. Restore
the artifact into a fresh local or host-provisioned destination through the
storage APIs described above.

## Consistency, integrity, and format

One coherent read covers the full export. A concurrent commit is included
completely or excluded completely. Given the same logical state, durability,
and format version, export produces byte-identical artifacts.

The binary `LIXSNAP` version 1 container records its container version, Lix
format version, canonical entries, entry count, payload byte count, checksum
algorithm identifier, and BLAKE3 digest. Restore rejects corruption,
truncation, invalid lengths, duplicate or out-of-order entries, unknown
versions or algorithms, and trailing data.

Logical space identifiers and their storage semantics form an append-only wire
registry. New identifiers may be added, but an identifier emitted by a
snapshot is never removed or reused, even after its space is retired from the
current layout. This lets a future engine decode the old bytes before applying
the migration selected by the embedded Lix format version.

The v1 decoder accepts at most 10 million entries and 64 GiB of canonical
payload, with individual keys limited to 16 MiB and values to 256 MiB. These
bounds make malformed or unexpectedly large inputs fail as
`LIX_INVALID_SNAPSHOT` instead of exhausting memory. Larger Lixes need a future
container version or a deliberately revised implementation limit.

The digest detects accidental corruption; it does not authenticate who made
the artifact. A snapshot can contain current data and deleted content retained
in history, so protect it like the source data.

`LIXSNAP` is the only supported snapshot family. The earlier Memory-specific
`LIXMEM` test encoding is intentionally not accepted.

## Snapshots and backups

A `.lixsnap` file can be a full backup artifact, but export alone is not a
backup system. Production recovery also requires scheduling, retention,
encryption, access control, independent storage, monitoring, and restore
drills.

Version 1 does not provide incremental backups, point-in-time log replay,
compression, encryption, signing, automatic upload, or restore over a live
Lix. Those features can be added around or alongside the stable full-snapshot
format when operational demand justifies them.
