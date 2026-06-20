# Changelog

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
