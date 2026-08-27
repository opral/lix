---
description: Choose local storage for Lix and optionally connect the repository to a server.
---

# Persistence and Storage

Lix has pluggable storage. A storage adapter decides where local bytes live. The
Lix API stays the same: `execute`, `createBranch`, and `mergeBranch` do not
change with the adapter.

To export or restore a complete Lix as a portable artifact,
see [Snapshots](./snapshots.md). Snapshot export is separate from the
incremental persistence performed by an adapter during normal operation.

Storage and server mode are separate choices:

| Setup  | Storage                                     | Server           | Reads and writes                       |
| :----- | :------------------------------------------ | :--------------- | :------------------------------------- |
| Local  | Memory, OPFS, filesystem, or native storage | None             | Local                                  |
| Remote | None                                        | `mode: "remote"` | Server                                 |
| Sync   | Memory, OPFS, filesystem, or native storage | `mode: "sync"`   | Local, with background synchronization |

See [Collaboration and Sync](./collaboration-and-sync.md) to choose between the
two server modes.

| Adapter             | Available in     | Use for                                 |
| ------------------- | ---------------- | --------------------------------------- |
| `Memory` (default)  | JavaScript, Rust | tests, demos, and ephemeral work        |
| `OpfsStorage`       | JavaScript       | persistent local browser repositories   |
| `FilesystemStorage` | JavaScript, Rust | a local directory synchronized with Lix |
| `RocksDB`           | Rust             | native embedded persistence             |
| `SlateDB`           | Rust             | object storage, for example S3          |

In JavaScript, `FilesystemStorage` requires Node.js. The default `Memory` storage and the separate `@lix-js/storage-opfs` package work in browsers.

## In-memory (default)

Omit the `storage` option to open an ephemeral in-memory Lix:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
// ... use it ...
await lix.close();
```

## Local filesystem

Persist a directory as a Lix repository with `FilesystemStorage`:

```ts
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

const lix = await openLix({
  storage: new FilesystemStorage({ path: "/var/data/repository" }),
});
```

Lix stores its repository state in `<repository>/.lix/.internal` and synchronizes repository files. Reopen the same path to resume the existing state.

In Rust, open the storage, pass it to Lix, then start synchronization on that
repository:

```rust
use lix::open_lix;
use lix_storage_filesystem::FilesystemStorage;

let storage = FilesystemStorage::new("./repository").open()?;
let lix = open_lix().with_storage(storage.clone()).await?;
storage.start_sync(&lix).await?;

storage.sync_disk_to_lix().await?;
storage.stop_sync().await?;
```

`FilesystemStorage` owns synchronization after `start_sync()` returns. Calling
`stop_sync()` is optional for normal applications and recommended for tests, or
before reopening the same directory immediately. Dropping the final storage
or repository instance performs a best-effort shutdown.

Pass `syncAllFiles: false` to begin with no regular repository files and import
selected paths with `storage.importPaths(paths)`.

## Browser OPFS

Persist a local browser repository across reloads with `OpfsStorage`:

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
  storage: new OpfsStorage({ name: "atelier" }),
});
```

The name identifies one Lix database within the current browser origin.
Multiple Lix workers and tabs can attach to the same name through the
package-owned storage worker. The provider uses SQLite Wasm on OPFS and a
cross-tab Web Lock. It implements the same generic storage protocol as other
JavaScript providers; the Lix SDK does not expose OPFS-specific APIs.

Filesystem sync handles regular files only. Symbolic links and other special entries are not imported.

## Connect to a server

Remote mode executes every operation on the server and does not accept local
storage:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/repositories/acme",
  },
});
```

The client needs no local storage option. Files, SQL rows, and branches live on the server.

Sync mode combines local storage with the same hosted repository:

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
  storage: new OpfsStorage({ name: "acme" }),
  server: {
    mode: "sync",
    url: "https://example.com/repositories/acme",
  },
});
```

Reads and writes execute locally while Lix exchanges commits with the server in
the background. See [Collaboration and Sync](./collaboration-and-sync.md) for
offline and lazy-loading behavior.

## Automatic format upgrades

`open_lix()` is the repository lifecycle boundary. When it encounters a
supported older format, Lix copies the repository into an inactive storage
epoch, validates it, and atomically publishes that epoch before opening the
handle. Applications do not call a separate migration API. Rust applications
can attach an `OpenProgressSink`; JavaScript applications can pass
`onProgress` to `openLix()` to make the blocking upgrade visible.

Capacity planning must include two repository generations. Lix retains the
immediately previous generation for rollback. A later upgrade clears the
inactive epoch before reusing it; once a newer epoch is active, Lix reclaims
the legacy pre-epoch layout asynchronously. Budget approximately 2× the live
repository bytes, plus the backend's normal WAL, compaction, and
temporary-write headroom. Upgrade latency is proportional to authoritative
bytes and depends on the storage backend and hardware. Automatic upgrades do
not impose a fixed row or byte ceiling that could strand an otherwise valid
repository; available backend capacity is the practical bound.

The ignored RocksDB capacity profile reproduces the copy with a released-v75
repository and a configurable payload:

```sh
LIX_MIGRATION_PROFILE_MIB=256 cargo test -p lix-storage-rocksdb \
  --features storage-benches --test migration_profile --release -- \
  --ignored --nocapture
```

For S3, the server runs Lix with the Rust `SlateDB` storage on an S3-compatible object store. The server exposes the repository through the [Lix Server Protocol](./server-protocol.md). See [Hosting](./hosting.md). Clients do not pass S3 to `openLix()`.

```text
JS client ── HTTP ──▶ Lix server ──▶ SlateDB ──▶ S3
```

## Closing

Always `await lix.close()` in scripts and tests. Long-lived servers can hold one Lix instance for the process lifetime.

## Custom storage (Rust)

The engine runs on any ordered transactional key-value store. A storage implementation only provides ordered byte-key persistence, coherent read views, and atomic writes. It does not parse Lix SQL and does not interpret engine concepts such as branches or changes.

Implement three asynchronous traits from `lix::storage`: `Storage`, `StorageRead`, and `StorageWrite`. An implementation must guarantee:

1. **Space isolation.** Keys in different spaces never collide.
2. **Coherent read views.** A read handle observes one coherent view for its lifetime.
3. **Ordered scans.** Scans return keys in ascending byte order.
4. **Atomic commits.** A commit publishes all staged mutations or none.
5. **Persistence.** Persistent implementations define their durability boundary. `Memory` is ephemeral.

Validate an implementation with the public conformance suite:

```rust
use lix::storage::conformance::run_storage_conformance;

let report = run_storage_conformance(&factory).await;
report.assert_no_failures();
```

PostgreSQL, OPFS, Cloudflare D1, and similar targets need such a custom implementation.
