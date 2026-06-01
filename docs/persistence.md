---
description: Open Lix in memory for tests, or persist to a .lix SQLite file via SqliteBackend. For other storage targets, implement the backend interface.
---

# Persistence

`openLix()` with no arguments opens an in-memory Lix that vanishes when the process exits. For anything that should survive a restart, pass a backend.

## In-memory (tests, demos)

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
// ... use it ...
await lix.close();
```

## SQLite file (Node.js)

Persist a Lix as a single `.lix` file using `SqliteBackend`:

```bash
npm install @lix-js/sdk
```

```ts
import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
  backend: new SqliteBackend({ path: "/var/data/app.lix" }),
});

// ... use it ...
await lix.close();
```

Reopening the same path resumes existing state. Don't open the file with raw SQLite tools; Lix manages its own schema and transactions.

For tests, point at a temp directory so each run is isolated:

```ts
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const dir = mkdtempSync(path.join(tmpdir(), "lix-"));
const lix = await openLix({
  backend: new SqliteBackend({ path: path.join(dir, "demo.lix") }),
});
```

## Closing

Always `await lix.close()` in scripts and tests. Long-lived servers can hold a single Lix instance for the lifetime of the process.

## Other storage targets

Postgres, S3, Cloudflare D1 / Durable Objects, IndexedDB, OPFS, RocksDB (anything transactional and key-value-shaped) are not shipped by the Lix team.

The storage interface is public and small enough to implement yourself. The [Backends](./backend.md) page documents the full contract.
