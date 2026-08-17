---
description: Lix has pluggable storage. Open Lix in memory, in browser OPFS, on the local filesystem, or against a remote server.
---

# Persistence and Storage

Lix has pluggable storage. A storage adapter decides where the bytes live. The Lix API stays the same: `execute`, `createBranch`, and `mergeBranch` do not change with the adapter.

`openLix()` opens a local repository or connects to a remote Lix server.

| Adapter            | Available in     | Use for                                        |
| ------------------ | ---------------- | ---------------------------------------------- |
| `Memory` (default) | JavaScript, Rust | tests, demos, and ephemeral work               |
| `OpfsStorage`     | JavaScript       | persistent local browser repositories          |
| `FilesystemStorage`  | JavaScript, Rust | a local directory synchronized with Lix        |
| `RocksDB`          | Rust             | native embedded persistence                    |
| `SlateDB`          | Rust             | object storage, for example S3                 |
| Remote server      | any client       | shared repositories; the server owns persistence |

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

Persist a complete local browser repository across reloads with
`OpfsStorage`:

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
  storage: new OpfsStorage({ name: "atelier" }),
});
```

The name identifies one Lix database within the current browser origin. Only
one Lix handle may open that database name at a time, including from other tabs.
The provider uses SQLite Wasm on OPFS and a cross-tab Web Lock. It implements
the same generic storage protocol as other JavaScript providers; the Lix SDK
does not expose OPFS-specific APIs.

Filesystem sync handles regular files only. Symbolic links and other special entries are not imported.

## Remote server

Connect to a hosted repository with `server`:

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
