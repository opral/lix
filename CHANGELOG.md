# Changelog

## 0.8.0 - 2026-07-09

### Minor

- Added `FsBackend.syncDiskToLix()` as an awaitable filesystem sync barrier.

  The filesystem backend picks up disk edits in the background with debouncing. `backend.syncDiskToLix()` flushes pending on-disk changes into Lix and resolves once they are materialized, so subsequent queries reflect the current disk state.
- Added a `lixDir` option to `FsBackend` for storing lix state outside the workspace.

  By default, state lives in `<workspace>/.lix`. Passing `lixDir` keeps repository metadata in an external `.lix` directory and writes no `.lix` directory into the workspace. Pointing `lixDir` at a temporary directory gives ephemeral filesystem sync: workspace files are imported and watched without persisting lix state.
- `FsBackend` now requires an explicit `syncAllFiles` option and supports on-demand file sync.

  `new FsBackend({ path, syncAllFiles: true })` syncs the full workspace as before. With `syncAllFiles: false`, the lix opens without workspace files and `backend.importPaths(["notes/today.md"])` syncs selected files on demand. Imported paths are exact workspace-relative file paths, not directories or globs. In Rust, use `FsBackendOpenOptions::new(root, sync_all_files)` and `FsBackend::import_paths()`.
- Added optional origin keys for tagging Lix writes.

  `lix.execute(sql, params, { originKey })` in JavaScript and `execute_with_options(sql, params, options)` in Rust stamp the change records a write produces. The key is exposed as `origin_key` on `lix_change` and as `lixcol_origin_key` on state, file, and history surfaces; writes without an origin key stay `NULL`.

### Patch

- Made the JavaScript SDK's native bindings fully asynchronous.

  Awaited methods previously blocked the calling thread inside the native binding, which could freeze an Electron main process. Opening a lix, `execute`, transactions, branch and merge calls, observers, and `close` now return real promises and run their work off-thread.
- Sped up `INSERT ... ON CONFLICT` entity upserts by scanning only the inserted identity for conflicts instead of the full entity state.
- Improved `lix_file` read and write performance.

  Simple single- and multi-row `lix_file (path, data)` inserts and upserts take a fast path that makes large file writes roughly 10x faster. File bytes are hashed once per write, unchanged chunks skip re-writes, and filesystem sync batches its upserts: in repository benchmarks, a 1,000-row `lix_file` insert dropped from ~95 ms to ~41 ms and a 200-file filesystem cold open from ~780 ms to ~210 ms. `SELECT` queries that project `data` now batch their blob reads.
- Removed a 2 GB size ceiling on file data read through SQL.

  The `data` column on `lix_file`, `lix_file_by_branch`, and `lix_file_history` now uses a large binary representation, so reads no longer fail when file bytes in a result exceed Arrow's 32-bit offset limit.
- Lix is now MIT licensed.

  The Rust crates and the JavaScript SDK npm package declare the MIT license, replacing the previous proprietary license reference.

## 0.7.0 - 2026-06-18

### Minor

- Added `INSERT ... ON CONFLICT` upsert support for entity state.
- Added file format plugins: CSV, Markdown, and plain text files are stored as queryable state instead of blobs.

  Writing a file with a matching plugin stores the changes inside the file as entity state. A CSV cell edit is one row-level change that can be queried, diffed, and merged. Reorders are detected: a moved row or paragraph is recorded as a move, not a delete plus an insert. Files without a plugin keep content-defined chunked blob storage.
- Added filesystem sync: a lix can mirror into a plain directory and back.

  Edits made in the directory with any tool flow into Lix with full history. Switching branches updates the directory contents.
- Added `lix.observe()` for subscribing to SQL query results.

  The Rust and JavaScript SDKs can now create observe streams that emit an initial result and re-run after Lix mutations, making it possible to build reactive views without manual polling.
- Rebuilt the storage engine's physical layout: merges run 1.8x faster, point reads 2.2x faster, and commits write 47% fewer bytes.

  Measured on the repository benchmarks: merge_10k through the e2e CSV plugin pipeline 347.8 ms to 190.0 ms, read_one_by_pk 213.1 us to 96.2 us, bytes written per 1k-row insert commit 827,460 to 436,472, backend puts per commit 2,031 to 1,074. Payloads are now stored exactly once, each engine keyspace maps to its own SQLite table, and keys use binary UUIDs with front-coded chunk encoding. The SQLite file format version moves to 3; v0.7 opens fresh files only and rejects older files with an explicit error.

## 0.6.2 - 2026-06-02

### Patch

- Added SQL file surfaces for storing, reading, querying, and versioning file bytes in Lix:

  ```sql
  INSERT INTO lix_file (path, data) VALUES ('/orders.xlsx', $1);
  SELECT data FROM lix_file WHERE path = '/orders.xlsx';
  SELECT data FROM lix_file_history WHERE path = '/orders.xlsx';
  ```

## 0.6.1 - 2026-05-29

### Patch

- lix-sdk, engine: Improved SQLite backend read performance and native backend snapshot support.
