---
description: Open Lix in memory for tests, persist a filesystem workspace with FsBackend, or use SqliteBackend for a single .lix application file.
---

# Persistence

`openLix()` with no arguments opens an in-memory Lix that vanishes when the process exits. For anything that should survive a restart, pass a backend. For local files and directories, `FsBackend` is the recommended backend.

## In-memory (tests, demos)

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
// ... use it ...
await lix.close();
```

## Filesystem workspace (persistent mode)

Persist a directory as a Lix workspace using `FsBackend`:

```bash
npm install @lix-js/sdk
```

```ts
import { FsBackend, openLix } from "@lix-js/sdk";

const lix = await openLix({
	backend: new FsBackend({ path: "/var/data/workspace" }),
});

// ... use it ...
await lix.close();
```

Reopening the same path resumes existing RocksDB filesystem backend state. Lix stores its private repository data in `<workspace>/.lix/.internal/rocksdb` and syncs workspace files through Lix.

Older SQLite filesystem backend metadata is not migrated. If Lix detects the
old SQLite metadata files in `<workspace>/.lix/.internal` before a RocksDB store
exists, it clears `.lix/.internal` and initializes a fresh RocksDB backend.

## Filesystem workspace (memory storage)

Use `storage: "memory"` when you want filesystem sync without persisting the
Lix repository metadata:

```ts
const lix = await openLix({
	backend: new FsBackend({
		path: "/var/data/workspace",
		storage: "memory",
	}),
});
```

This imports, watches, and materializes workspace files, but the Lix repository
state is kept in memory and no `.lix` directory is written. Reopening the same
path reimports the files from disk instead of resuming prior Lix metadata.

For tests, point at a temp directory so each run is isolated:

```ts
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const dir = mkdtempSync(path.join(tmpdir(), "lix-"));
const lix = await openLix({
	backend: new FsBackend({ path: path.join(dir, "workspace") }),
});
```

Add a filter when only specific files should participate in filesystem sync.
`includePaths` entries are exact workspace-relative file paths, not directories
or globs. They may be written with or without a leading slash, for example
`"notes/today.md"` or `"/notes/today.md"`. The filter scopes disk import, file
watching, and materialization; it does not filter unrelated Lix SQL state.

```ts
const lix = await openLix({
	backend: new FsBackend({
		path: "/Users/me/Downloads",
		filter: { includePaths: ["notes/today.md"] },
	}),
});
```

## Single .lix application file

Use `SqliteBackend` when the `.lix` SQLite file is the application document itself. This is useful if you are defining a new file format and want Lix to be the application's file format: a single portable file that contains the app's versioned state.

```ts
import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
	backend: new SqliteBackend({ path: "/var/data/app.lix" }),
});
```

Reopening the same path resumes existing state. Don't open the file with raw SQLite tools; Lix manages its own schema and transactions.

## Closing

Always `await lix.close()` in scripts and tests. Long-lived servers can hold a single Lix instance for the lifetime of the process.

## Other storage targets

Postgres, S3, Cloudflare D1 / Durable Objects, IndexedDB, OPFS, and other transactional key-value storage targets beyond the shipped backends are not shipped by the Lix team.

The storage interface is public and small enough to implement yourself. The [Backends](./backend.md) page documents the full contract.
