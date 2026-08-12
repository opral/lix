---
description: Lix has pluggable storage. Open Lix in memory, on the local filesystem, or against a remote server, or implement the Rust storage traits for another store.
---

# Persistence and Storage

Lix has pluggable storage. A storage adapter decides where the bytes live. The Lix API stays the same: `execute`, `createBranch`, and `mergeBranch` do not change with the adapter.

`openLix()` opens a local repository or connects to a remote Lix server.

| Adapter            | Available in     | Use for                                        |
| ------------------ | ---------------- | ---------------------------------------------- |
| `Memory` (default) | JavaScript, Rust | tests, demos, and ephemeral work               |
| `IndexedDbStorage` | JavaScript       | persistent local browser repositories          |
| `LocalFilesystem`  | JavaScript, Rust | a local directory synchronized with Lix        |
| `RocksDB`          | Rust             | native embedded persistence                    |
| `SlateDB`          | Rust             | object storage, for example S3                 |
| Remote server      | any client       | shared workspaces; the server owns persistence |

In JavaScript, `LocalFilesystem` requires Node.js. The default `Memory` storage and `IndexedDbStorage` work in browsers.

## In-memory (default)

Omit the `storage` option to open an ephemeral in-memory Lix:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
// ... use it ...
await lix.close();
```

## Local filesystem

Persist a directory as a Lix workspace with `LocalFilesystem`:

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new LocalFilesystem({
    path: "/var/data/workspace",
    syncAllFiles: true,
  }),
});
```

Lix stores its repository state in `<workspace>/.lix/.internal` and synchronizes workspace files. Reopen the same path to resume the existing state.

Two options change this behavior:

- `lixDir` stores the repository state outside the workspace. The workspace does not receive a `.lix` directory.
- `syncAllFiles: false` starts without importing files. Import exact file paths with `storage.importPaths(["notes/today.md"])`.

## IndexedDB

Persist a complete local browser repository across reloads with
`IndexedDbStorage`:

```ts
import { IndexedDbStorage, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new IndexedDbStorage({ name: "atelier" }),
});
```

The name identifies one Lix database within the current browser origin. Only
one Lix handle may open that database name at a time, including from other tabs.
IndexedDB commits use the same transactional storage boundary as native Lix
storage; Lix does not export or replace a complete repository snapshot after
each mutation.

Filesystem sync handles regular files only. Symbolic links and other special entries are not imported.

## Remote server

Connect to a hosted workspace with `server`:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/workspaces/acme",
  },
});
```

The client needs no local storage option. Files, SQL rows, and branches live on the server.

For S3, the server runs Lix with the Rust SlateDB storage backed by an S3-compatible object store and exposes the workspace through the [Lix Server Protocol](./server-protocol.md). Clients do not pass S3 to `openLix()`.

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
