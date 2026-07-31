---
description: Run Lix in memory, in a local filesystem, in a SQLite file, or through a remote Lix server.
---

# Persistence

`openLix()` can open a local repository or connect to a remote Lix server. Local repositories use memory, `LocalFilesystem`, or SQLite. A remote server owns workspace persistence.

## Remote workspace

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

The client does not need a local workspace storage option. Files, SQL rows, and branches live on the server.

For an S3 deployment, host Lix with the shipped Rust SlateDB storage implementation backed by an S3-compatible object store. Expose the workspace through the [Lix server protocol](https://github.com/opral/lix/blob/main/packages/server-protocol/README.md). JavaScript clients do not pass S3 directly to `openLix()`.

## In-memory (tests, demos)

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
// ... use it ...
await lix.close();
```

## Filesystem workspace (persistent mode)

Persist a directory as a Lix workspace using `LocalFilesystem`:

```bash
npm install @lix-js/sdk
```

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
  storage: new LocalFilesystem({
    path: "/var/data/workspace",
    syncAllFiles: true,
  }),
});

// ... use it ...
await lix.close();
```

Reopening the same path resumes existing RocksDB filesystem storage state. Lix stores its private repository data in `<workspace>/.lix/.internal/rocksdb` and syncs workspace files through Lix.

Older SQLite filesystem storage metadata is not migrated. If Lix detects the
old SQLite metadata files in `<workspace>/.lix/.internal` before a RocksDB store
exists, it clears `.lix/.internal` and initializes a fresh RocksDB storage.

## Filesystem workspace (external `.lix` directory)

Use `lixDir` when you want filesystem sync with Lix repository metadata outside
the workspace directory:

```ts
const lix = await openLix({
  storage: new LocalFilesystem({
    path: "/var/data/workspace",
    lixDir: "/tmp/session/.lix",
    syncAllFiles: true,
  }),
});
```

This imports, watches, and materializes workspace files, but the Lix repository
state is stored under `lixDir` and no `.lix` directory is written in the
workspace. Reusing the same `lixDir` resumes that metadata; using a fresh temp
`.lix` directory reimports the files from disk.

For tests, point at a temp directory so each run is isolated:

```ts
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const dir = mkdtempSync(path.join(tmpdir(), "lix-"));
const lix = await openLix({
  storage: new LocalFilesystem({
    path: path.join(dir, "workspace"),
    syncAllFiles: true,
  }),
});
```

Set `syncAllFiles: false` when filesystem sync should start with no regular
workspace files, then import selected files with `storage.importPaths()`. Imported paths
are exact workspace-relative file paths, not directories or globs. They may be
written with or without a leading slash, for example `"notes/today.md"` or
`"/notes/today.md"`. This scopes disk import, file watching, and
materialization; it does not filter unrelated Lix SQL state.

```ts
const storage = new LocalFilesystem({
  path: "/Users/me/Downloads",
  syncAllFiles: false,
});
const lix = await openLix({ storage });
await storage.importPaths(["notes/today.md"]);
```

## Single .lix application file

Use `SQLite` when the `.lix` SQLite file is the application document itself. This is useful if you are defining a new file format and want Lix to be the application's file format: a single portable file that contains the app's versioned state.

```ts
import { openLix, SQLite } from "@lix-js/sdk";

const lix = await openLix({
  storage: new SQLite({ path: "/var/data/app.lix" }),
});
```

Reopening the same path resumes existing state. Don't open the file with raw SQLite tools; Lix manages its own schema and transactions.

## Closing

Always `await lix.close()` in scripts and tests. Long-lived servers can hold a single Lix instance for the lifetime of the process.

## Other storage targets

PostgreSQL, Cloudflare D1 / Durable Objects, IndexedDB, OPFS, and other storage engines require a custom storage implementation. S3-compatible object storage is available through the shipped Rust SlateDB implementation in a server deployment.

The storage interface is public and small enough to implement yourself. The [Storage](./storage.md) page documents the full contract.
