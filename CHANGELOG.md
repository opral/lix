# Changelog

## 0.15.1 - 2026-08-27

### Patch

- Concurrent browser sync no longer crashes an in-flight write when its transaction read expires.

  Lix now returns the transient storage error to its bounded write retry path instead of panicking while opening transaction-scoped history readers.

## 0.15.0 - 2026-08-27

### Minor

- Opening a Lix now upgrades supported older repository formats automatically and reports typed progress to Rust and JavaScript applications.

  `open_lix()` is the single repository lifecycle API. The explicit public migration and inspection APIs have been removed, and every opened handle exposes an immutable report describing initialization or migration performed during open.
- Added `lix_latest_checkpoint_commit_id()` for reading the active branch's latest checkpoint directly in SQL.

  The accessor falls back to the repository root when the branch has no checkpoint, allowing reactive working changes to be queried with `lix_diff('lix_file', lix_latest_checkpoint_commit_id(), lix_active_branch_commit_id())`.
- Added immutable, branch-scoped point-in-time reads with `lix_state_at(relation, commit_id)`.

  Diff machinery columns now use the reserved `lixcol_` namespace, and repository format v74 adds the authenticated row-primary-key index used for bounded historical reads. Existing supported repositories are upgraded automatically while opening.
- Hard-cut JavaScript SQL results to plain-object rows and typed column descriptors. `execute`, `executeBatch`, transactions, and observations now return enumerable rows with direct property access and `columns` entries shaped as `{ name, type }`; the `Row` accessor API has been removed. Positional array rows remain available through `rowMode: "array"` for duplicate-column and wire-adapter use cases. The Lix Server Protocol is now version 5.
- Added deterministic, stream-first `.lixsnap` export and restore APIs for Rust and JavaScript.

  Snapshots capture a complete logical Lix for reproduction, transfer, and recovery, verify integrity with BLAKE3, and restore atomically only into fresh storage.
- Standardized relation payload column names in `lix_diff()`.

  Diff queries now use `diff_type` and `row_count`; `lixcol_diff_type` and `lixcol_row_count` were renamed without compatibility aliases. The `lixcol_` prefix remains reserved for engine-owned system metadata.
- Repository format v75 makes every commit a complete state snapshot: `lix_commit.base_commit_id` names the exact global commit whose state composes beneath a local commit's overlay, so branch-scoped and point-in-time reads (`lix_state_at`) are exact rather than replay-derived.

  Opening automatically upgrades v72–v74 repositories — inferring each local commit's base chronologically, repairing filesystem trees that v72-era partial checkpoints left without their ancestor directories, and fencing every step so an interruption is cleanly retryable. Repositories below v72, or repositories whose commit timestamps contradict the chronological inference, are rejected with an explicit error instead of migrating on guessed history.

### Patch

- OPFS repositories remain writable across browser-tab navigation and owner-worker handoffs.

  The shared storage session now survives an OPFS backend restart, so one healthy tab no longer fences another tab using the same repository generation.
- Allow a `LixServerProtocol` owner to stream a coherent snapshot without opening a second engine for the same storage.

## 0.14.0 - 2026-08-25

### Minor

- Added optional public profile URIs to repository accounts.

  Applications can now associate an account with a machine-readable public profile while keeping authentication and authorization separate from presentation metadata.

  v0.14 uses repository format v73. Existing repositories must run the explicit offline migration before opening; historical accounts receive `NULL` for the new field and profile updates remain durable.
- Added durable local-first repository sync for offline-capable applications.

  `openLix({ storage, server: { mode: "sync" } })` keeps reads and writes local while synchronizing with the server in the background. Browser applications can use OPFS for durable offline work, safely share a repository across tabs and workers, and recover when the owning tab closes.
- Redesigned version control around session-scoped SQL relations and commit-to-commit diffs.

  Use `lix_diff(relation, from_commit_id, to_commit_id)` to compare tracked relations, `lix_restore` to move a branch to an ancestor, and `lix_commit_ancestry()` plus commit parent IDs to inspect history. This replaces the former `*_by_branch`, branch descriptor/ref, heterogeneous working-diff, `diff_id`, and commit-edge surfaces; open a separate session for each branch.
- Simplified the public SDK surface for serving, batching, migrations, and telemetry.

  Serve a repository with `open_lix().with_storage(storage).serve().await`, submit atomic statement arrays through `executeBatch`, and run explicit offline migrations through `inspect_lix` and `migrate_lix`. Rust and JavaScript now share one stable telemetry contract. The former protocol constructors, Workerd entry point, SQL script parser, migration names, and legacy telemetry surface have been removed.

### Patch

- Improved browser, remote, and large-repository reliability and performance.

  Concurrent tabs now open, synchronize, and query coherently; remote clients recover sessions and support larger sets of live observations; and diff, checkpoint, and sync operations avoid loading unused rows, payloads, and history.

## 0.12.3 - 2026-08-18

### Patch

- Fixed Node.js worker startup when the host process uses worker-incompatible runtime flags such as `--expose-gc`.

  The JavaScript SDK no longer forwards worker-incompatible runtime flags while preserving host security restrictions.

## 0.12.2 - 2026-08-15

### Patch

- Fixed in-memory Lix on Node.js when the native addon is unavailable by falling back to the bundled WebAssembly engine.

  This restores compatibility for memory-backed consumers on musl-based Linux distributions such as Alpine while keeping native-only features unchanged.

## 0.12.1 - 2026-08-15

### Patch

- Integrated the generated plugin bindings directly into the `lix` crate.

  Rust consumers no longer need the separate column-merger, combined, or file-projection binding crates.

## 0.12.0 - 2026-08-15

### Minor

- Added first-class browser and filesystem storage backends.

  Browser applications can persist repositories through the dedicated `@lix-js/storage-opfs` package, while filesystem storage is available through `@lix-js/storage-filesystem`.
- Plugin authoring and the Lix Server Protocol are now provided directly by Lix.

  Rust plugins use `lix::plugin`, server hosts can use the canonical Server Protocol API, and plugins can read and edit untracked files as rows.
- Lix SQL now uses the PostgreSQL dialect.

  Queries use PostgreSQL syntax and numbered parameters such as `$1`. Row tables expose native SQL types, including `jsonb` and `timestamptz`, with consistent row terminology and typed columns instead of raw snapshots.
- Removed `lix.clientState` and remote client-storage composition.

  Applications now own browser-local UI persistence explicitly, while remote Lix handles remain focused on repository operations and independent branch-pinned sessions.

### Patch

- History, branch, and merge operations now scale with the relevant changes instead of the total repository size.

  History traversal skips unrelated work, file history prunes irrelevant paths and plugin states, and branch-head moves reuse existing state instead of copying the complete working set.
- SQL queries and everyday CRUD operations are substantially faster.

  Lix now reuses SQL sessions and prepared plans, seeks directly for indexed and file-scoped lookups, and avoids unnecessary intermediate materialization when returning typed and JSON results.
- Fixed several correctness and reliability issues across storage, branches, and files.

  This includes stale SlateDB reads, truncated scans, false transaction conflicts, incorrect branch reverts, subquery failures, and directory operations that could leave invalid state.
- Files and repositories use storage more efficiently.

  Binary edits reuse unchanged content, SlateDB durable writes complete faster, deleted branches release their serving storage, and commits retain less internal bookkeeping.

## 0.11.0 - 2026-08-09

### Minor

- Removed the SQLite storage option from `@lix-js/sdk` and `lix_sdk`.

  Use the RocksDB-backed `LocalFilesystem` adapter for persistent local development. The standalone Rust SQLite storage adapter remains available for specialized use.
- Unified the Rust engine and SDK as the `lix` crate, with `open_lix().await?` as the in-memory quick start and builder methods for storage, telemetry, and custom Wasm runtimes.

  This is a breaking Rust API migration: `lix_engine`, `lix_sdk`, `OpenLixOptions`, and the specialized `open_lix_with_*` entry points have been removed. Persistent backends now live in independently versioned `lix-storage-*` crates.

### Patch

- Bounded first publication of columnar current state on long commit histories.

  Lix now authenticates cumulative touched schema families in each commit-state manifest and carries that bounded absence authority across linear, merged, and selected-source lineages. Mutation scopes that cannot be bounded exactly still fail closed.
- Reduced sparse current-state publication latency and serving-index allocation.

  Lix now stores contiguous scoped-range leaves as shared scope runs and encodes immutable node fields through borrowed views, while retaining authenticated point reads, structural sharing, and opaque physical-part payloads.
- Reduced current-state serving-index storage for large tracked repositories.

  Lix now uses one authenticated scoped-range index for point reads, diffs, and sparse state sharing while preserving transactional history and branch semantics.

## 0.10.0 - 2026-08-03

### Minor

- Added repository-native accounts and single-account change attribution across local and remote sessions.

  Every change now has one required account, anonymous work uses the built-in anonymous account, and applications can select an active account through the Rust, JavaScript, SQL, and server-protocol APIs.
- Added persistent undo and redo for tracked branch history across the Rust SDK, JavaScript SDK, Lix Server Protocol, and CLI.

  Undo and redo append inverse and replay commits without rewinding branch history. Atomic batches and transactions remain one undo unit, while untracked state remains unchanged; checkpoints and merge commits form undo boundaries.
- Renamed the `lix_file`, `lix_file_by_branch`, and `lix_file_history` binary payload column from `data` to `content`. Native file read and write APIs now use `content` names as well; the former `data` surface is not supported.
- Git replay can now seed the complete parent tree for a bounded commit window.

  Use `--parent-tree full` when untouched parent files must remain available in current and historical snapshots; the default window-scoped mode remains unchanged.
- Introduced Plugin API v1 and migrated the bundled CSV, JSON, Markdown, Excalidraw, and Git text plugins.

  Plugin API v1 replaces the previous Wasm plugin contract with a fused, host-owned API.
- SQL writes now support `RETURNING` across registered rows and writable filesystem and branch surfaces. INSERT and UPDATE return final post-write values (including generated defaults), while DELETE continues to return the removed row values.

### Patch

- Filesystem sync now reports symlinks and other unsupported entries that block a regular Lix file instead of silently leaving Lix and disk out of sync.

  Git replay now also rejects unsupported paths and entries explicitly instead of representing them as regular files.
- Improved reliability and reporting for large semantic merges.

  Large conflict sets no longer hit small-transition limits, and merge previews and receipts now include plugin-resolved changes in their statistics.
- Improved reliability for large and frequently edited Markdown files.

  Large structured Markdown files no longer exhaust the default plugin memory limit, and sequential localized edits now apply to the latest document state.
- Improved performance and reduced memory and disk use for large repositories.

  History queries, checkpoints, working changes, binary and media storage, remote observations, and large inserts now do less redundant work. Million-row inserts complete more than 20% faster on both RocksDB and SlateDB.

## 0.9.0 - 2026-07-29

### Minor

- Directory paths now use the same canonical syntax as file paths.

  Non-root paths must not end with `/`; the typed file or directory surface determines the row kind. Applications must remove trailing slashes from directory path values.
- Turn automatic edit history into deliberate checkpoints.

  The SDK can create milestones, SQL can query checkpoint history and working diffs, and Lix automatically cleans up superseded automatic commits after a recovery window.
- Rename the filesystem working-diff SQL surfaces for consistent terminology.

  `lix_file_working_diff`, `lix_file_working_diff_by_branch`, `lix_directory_working_diff`, and `lix_directory_working_diff_by_branch` replace their `*_working_change*` predecessors. The old names are not retained as aliases.
- Lix is substantially faster and more storage-efficient for large files and repositories.

  v0.9 adds indexed and batched file operations, faster SQL reads and writes, compressed native storage, lower-copy blob handling, and more efficient tracked-state merges. Remote clients also transfer localized file and query changes instead of repeatedly sending complete payloads.

  This release changes the tracked-state and SlateDB physical formats. Existing repositories created by older engine versions must be recreated.
- History relations are now table-valued functions with explicit commit arguments.

  Use `example_history()` for history from the active head or `example_history($commit)` for an explicit head. The former `lixcol_as_of_commit_id` result column and predicate-based anchor API have been removed.
- Structured files now merge incrementally through the new Component v2 plugin platform.

  Reference plugins for CSV and TSV, JSON, Markdown, Excalidraw, and Git-compatible text turn localized file edits into sparse semantic changes without reparsing or rendering the complete document. Concurrent edits merge at the row level, and plugin authors can build on the same public Rust API used by the bundled plugins.
- Git replay can now target RocksDB or SlateDB and compare the full semantic plugin path with an explicit no-plugin control. Replay profiles identify the selected adapter and include per-commit WASM transition work counters.
- Run Lix repositories remotely with live, low-latency clients.

  `openLix()` can connect to the versioned Lix HTTP protocol for SQL, branches, atomic batches, binary file operations, and multiplexed live queries. Each client gets an isolated branch-pinned session, retries writes safely, persists private local state locally, and sends compact deltas for localized edits.
- Plugin-backed atomic imports now scale independently of document count. The engine automatically reuses its bounded live-Store working set for fresh and existing documents while preserving actively contested same-file leases, so callers no longer need a special single-writer ingestion API or actor-retention policy. Retained session observations also recover from benign working-set eviction when their exact durable semantic root is unchanged.
- Lix SQL and history are more capable and easier to use.

  History queries now default to the active branch head and correctly reconstruct files and directories across merges. The public catalog is smaller, `information_schema.columns` is the authoritative type contract, and the SDK adds atomic SQL batches alongside `DELETE ... RETURNING`, `LIKE` and `ILIKE`, and binary casts.

  Applications using the removed generic state tables, low-level filesystem tables, or former filesystem-history provenance columns must migrate to the typed schema, logical file, and `lixcol_source_changes` surfaces.

### Patch

- The Git text WASM plugin now writes base64 content directly into its final JSON snapshot buffer, avoiding a duplicate large allocation for minified files. WASM Stores retain a bounded 128 MiB ceiling so warm updates of large minified documents can materialize their successor without exhausting linear memory.
- Reduced SlateDB storage and read I/O by compressing newly written SST data with Zstandard.
- Git history replay now installs the bundled CSV plugin alongside the other format plugins, so CSV and TSV files are eagerly materialized as semantic rows.
- Improved 32–64 KiB binary file reads and repeat writes on RocksDB and SlateDB.

  Lix now stores this common size band in one inline manifest and uses a key-only manifest probe to avoid repeated payload rewrites.
- File-constrained semantic plugin reads now use the transaction overlay's candidate index instead of scanning every staged row.
- Schema-constrained semantic plugin reads now use the transaction overlay's candidate index instead of scanning unrelated staged rows.
- Improved 64–128 KiB binary file reads and reduced their storage rows on RocksDB and SlateDB.

  Lix now includes this size band in its manifest-probed inline layout.
- Fresh independent WASM plugin documents now open and drain concurrently within the bounded live-Store working set. Create-reservation preflights use aligned batch reads, while semantic rows are still eagerly validated and persisted.

## 0.8.4 - 2026-07-16

### Patch

- Added SQL script planning to the Rust and Workerd SDKs.

  Lix now parses single and multi-statement SQL into one atomic statement plan with request-wide parameter ranges.
- Enforced the current tracked direct-plane storage format.

  Repositories marked with the predecessor v9 layout now fail closed with `LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT`; recreate them or explicitly export and import their data. The public API is unchanged.

## 0.8.3 - 2026-07-15

### Patch

- Preserved YAML and TOML frontmatter when Lix tracks Markdown files.

  Markdown frontmatter now remains a single editable block instead of being rewritten as thematic breaks and list content.

## 0.8.2 - 2026-07-14

### Patch

- Renamed the Lix backend API to storage across Rust, JavaScript, packages, and documentation.

  Pass `storage` to `openLix()` and use the new types such as `Storage`, `SQLite`, and `LocalFilesystem`. The former backend names have been removed without compatibility aliases.

  Rust storage implementations are now split into `lix_sqlite_storage`, `lix_rocksdb_storage`, and `lix_slatedb_storage`. Replace `lix_backends` with the individual crates you use, and replace `lix_fs_backend` with `lix_local_filesystem`. The Redb implementation has been removed.

## 0.8.1 - 2026-07-13

### Patch

- Added deterministic in-memory snapshot import and export to the Workerd JavaScript SDK entry point.

  Cloudflare Workers and other Workerd hosts can persist the complete physical Lix state outside an isolate and reopen it without changing branch, commit, or revision identities.

## 0.8.0 - 2026-07-09

### Minor

- Added `LocalFilesystem.syncDiskToLix()` as an awaitable filesystem sync barrier.

  The filesystem storage picks up disk edits in the background with debouncing. `storage.syncDiskToLix()` flushes pending on-disk changes into Lix and resolves once they are materialized, so subsequent queries reflect the current disk state.
- Added a `lixDir` option to `LocalFilesystem` for storing lix state outside the repository.

  By default, state lives in `<repository>/.lix`. Passing `lixDir` keeps repository metadata in an external `.lix` directory and writes no `.lix` directory into the repository. Pointing `lixDir` at a temporary directory gives ephemeral filesystem sync: repository files are imported and watched without persisting lix state.
- `LocalFilesystem` now requires an explicit `syncAllFiles` option and supports on-demand file sync.

  `new LocalFilesystem({ path, syncAllFiles: true })` syncs the full repository as before. With `syncAllFiles: false`, the lix opens without repository files and `storage.importPaths(["notes/today.md"])` syncs selected files on demand. Imported paths are exact repository-relative file paths, not directories or globs. In Rust, use `LocalFilesystemOpenOptions::new(root, sync_all_files)` and `LocalFilesystem::import_paths()`.
- Added optional origin keys for tagging Lix writes.

  `lix.execute(sql, params, { originKey })` in JavaScript and `execute_with_options(sql, params, options)` in Rust stamp the change records a write produces. The key is exposed as `origin_key` on `lix_change` and as `lixcol_origin_key` on state, file, and history surfaces; writes without an origin key stay `NULL`.

### Patch

- Made the JavaScript SDK's native bindings fully asynchronous.

  Awaited methods previously blocked the calling thread inside the native binding, which could freeze an Electron main process. Opening a lix, `execute`, transactions, branch and merge calls, observers, and `close` now return real promises and run their work off-thread.
- Sped up `INSERT ... ON CONFLICT` row upserts by scanning only the inserted identity for conflicts instead of the full row state.
- Improved `lix_file` read and write performance.

  Simple single- and multi-row `lix_file (path, data)` inserts and upserts take a fast path that makes large file writes roughly 10x faster. File bytes are hashed once per write, unchanged chunks skip re-writes, and filesystem sync batches its upserts: in repository benchmarks, a 1,000-row `lix_file` insert dropped from ~95 ms to ~41 ms and a 200-file filesystem cold open from ~780 ms to ~210 ms. `SELECT` queries that project `data` now batch their blob reads.
- Removed a 2 GB size ceiling on file data read through SQL.

  The `data` column on `lix_file`, `lix_file_by_branch`, and `lix_file_history` now uses a large binary representation, so reads no longer fail when file bytes in a result exceed Arrow's 32-bit offset limit.
- Lix is now MIT licensed.

  The Rust crates and the JavaScript SDK npm package declare the MIT license, replacing the previous proprietary license reference.

## 0.7.0 - 2026-06-18

### Minor

- Added `INSERT ... ON CONFLICT` upsert support for row state.
- Added file format plugins: CSV, Markdown, and plain text files are stored as queryable state instead of blobs.

  Writing a file with a matching plugin stores the changes inside the file as row state. A CSV cell edit is one row-level change that can be queried, diffed, and merged. Reorders are detected: a moved row or paragraph is recorded as a move, not a delete plus an insert. Files without a plugin keep content-defined chunked blob storage.
- Added filesystem sync: a lix can mirror into a plain directory and back.

  Edits made in the directory with any tool flow into Lix with full history. Switching branches updates the directory contents.
- Added `lix.observe()` for subscribing to SQL query results.

  The Rust and JavaScript SDKs can now create observe streams that emit an initial result and re-run after Lix mutations, making it possible to build reactive views without manual polling.
- Rebuilt the storage engine's physical layout: merges run 1.8x faster, point reads 2.2x faster, and commits write 47% fewer bytes.

  Measured on the repository benchmarks: merge_10k through the e2e CSV plugin pipeline 347.8 ms to 190.0 ms, read_one_by_pk 213.1 us to 96.2 us, bytes written per 1k-row insert commit 827,460 to 436,472, storage puts per commit 2,031 to 1,074. Payloads are now stored exactly once, each engine keyspace maps to its own SQLite table, and keys use binary UUIDs with front-coded chunk encoding. The SQLite file format version moves to 3; v0.7 opens fresh files only and rejects older files with an explicit error.

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

- lix-sdk, engine: Improved SQLite storage read performance and native storage snapshot support.
