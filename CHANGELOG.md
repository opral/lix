# Changelog

## 0.9.0 - 2026-07-24

### Minor

- Added atomic binary batches for ordinary file upserts through the Lix engine, Rust SDK, and server protocol.

  Clients can send up to 1,024 raw file payloads in one request and receive one commit with the standard execution response.
- Remote browser clients can now persist private client state through Lix.

  Pass `storage: new LocalStorage()` to `openLix()` to restore `lix.clientState` JSON values and the client's active branch without uploading either to the remote workspace. The dedicated `@lix-js/sdk/remote` entrypoint has been removed; remote clients use the package root. SQL can read the branch pinned to its current session with `lix_active_branch_id()`.
- Add the canonical `/lix/v1` HTTP handler and connect remote `openLix()` clients to it, including atomic `executeBatch()` and multiplexed `observe()` support.
- Lix can now open a workspace as a thin remote client.

  Use `openLix({ server: { mode: "remote", url } })` to execute SQL and manage branches through the versioned Lix HTTP protocol without loading a local engine.
- Remote Lix clients now support live query observations.

  `lix.observe()` streams server-side Lix results, reconnects transient failures, follows successful branch switches, and closes with the normal Lix lifecycle.
- Lix now supports `executeBatch()` for sequential SQL statements that commit atomically.

  Each statement keeps its own parameters and result, and a failed statement rolls back the complete batch.
- Plugin packages now have explicit validation and resource limits.

  Malformed ZIP packages and manifests, including invalid path globs, now fail with `LIX_ERROR_INVALID_PLUGIN`. Packages are limited to 32 MiB of archive bytes, 128 ZIP entries, 64 MiB declared expansion, 32 MiB expanded per entry, 64 KiB manifests, 1 MiB per schema, 64 declared schemas, 512-byte paths, and 1,024-character globs. ZIP parsing accepts stored or deflated entries, comments, data descriptors, and bounded per-entry ZIP64 sizes, but rejects ZIP64 central directories and archives with more than eight complete footer candidates.
- Reduced tracked-state write amplification with the v3 tree codec.

  Tracked-state keys now use a prefix-friendly ordered encoding. Leaf nodes front-code keys, dictionary repeated commit and state metadata, and compact equal or small timestamps; internal nodes front-code child boundaries. This is a hard storage-format cut; repositories created by older engine versions must be recreated.

  Ordinary tracked-state diffs now retain hash-guided sparse traversal: they bind commit-root first-parent metadata and point-validate every changed row's change record. Winner reachability, inherited timestamps, and whole-root coverage are checked against staged chunks by the full audit in the explicit tracked-state rebuild path before publication, instead of scanning all unchanged rows on every merge. A hierarchical Merkle zipper also preserves subtree skipping when an insert shifts chunk boundaries or changes the root height.
- Typed SQL history now routes exact public primary-key predicates in declared `x-lix-primary-key` order and keeps primary-key columns non-null on deletion rows, including nested identity roots.

  The relation-blind `LIX_HISTORY_NON_IDENTITY_FILTER` notice has been removed. File and directory history filters retain ordinary SQL row semantics: filtering by a historical path returns revisions with that path, while filtering by immutable identity returns the entity lineage.
- Native binary CAS chunks now use zstd level 1 when compression saves at least 128 bytes and 12.5%, with a raw fallback for small or high-entropy content. Browser/WASM writers keep raw chunks to avoid a slower, lower-ratio encoder.

  All runtimes can decode both codecs. Chunk hashes and deduplication remain based on the uncompressed bytes.
- Changed SlateDB storage to a versioned LZ4 physical layout.

  Existing SlateDB-backed Lixes must be recreated. The new layout does not read the previous physical namespace and provides no migration or compatibility fallback.
- Lix SQL now exposes a smaller, application-oriented catalog.

  The generic `lix_state`, `lix_state_by_branch`, and `lix_state_history` tables have been removed. Query and mutate registered schemas through their generated typed tables, use `<schema>_by_branch` for cross-branch state, `<schema>_history` for branch-reachable entity history, and `lix_change` for workspace-wide activity.

  The storage-level `lix_file_descriptor*`, `lix_directory_descriptor*`, and `lix_binary_blob_ref*` tables are no longer public. Use the logical `lix_file*` and `lix_directory*` surfaces instead.

  `lix_key_value*` and `lix_registered_schema*` remain public. Internal schemas, including the hidden filesystem schemas, remain discoverable through `lix_registered_schema` for application interoperability.

  Runtime registration now rejects `x-lix-key: "lix"` and every key beginning with `lix_`; their base or generated SQL names occupy the namespace reserved for schemas bootstrapped by Lix. Application and plugin schemas must use an owner prefix such as `acme_task`. Catalog loading hard-fails for workspaces that already contain a custom key in this namespace; this release has no in-engine schema-rename path, so those workspaces require application-specific export or migration tooling before upgrading.
- History SQL surfaces now default to the active branch head pinned for the statement or coherent read batch. Queries no longer need a `lixcol_as_of_commit_id = lix_active_branch_commit_id()` predicate for the common case.

  The obsolete `LIX_HISTORY_FILTER_REQUIRED` error code is retired.

  Exact equality and non-empty `IN` predicates on the history anchor still override the default for time travel. Other anchor predicates now fail explicitly instead of silently traversing from the active head. Validation follows the resolved history relation through aliases, subqueries, and joins; an unrelated table column with the same name is not an anchor.
- Added SQL `DELETE ... RETURNING` and `LIKE`/`ILIKE` predicates across writable Lix SQL surfaces.

  `DELETE ... RETURNING` returns the pre-delete rows from the same atomic delete operation, including binary file data when requested, while still reporting the full affected-row count for cascading deletes.
- Plugin execution now uses a durable branch-local registry and per-file owner records instead of discovering and reopening plugin archives on current-state reads and writes. Ordinary plugin-free file-data writes stop after the exact registry lookup, and warm plugin execution reuses compiled matchers and hash-matched WASM instances.

  The original `.lixplugin` ZIP remains the filesystem artifact; installation extracts and content-addresses its WASM component once. Adding, replacing, or removing the archive remains the install, update, or uninstall operation; the registry is engine-owned derived state. Pre-registry installations are not discovered or decoded and must be removed and re-added. Registered schema keys are immutable while a declaring plugin is active; uninstall the plugin before a schema migration, then install the updated package.

  Registry v1 supports branch-local plugins only: `GLOBAL` and `UNTRACKED` archives are rejected. Registry entries require every v1 field; path-only matching is represented explicitly by a null content type rather than an omitted field. The internal `lix_plugin_registry_v1` and `lix_plugin_owner_v1` keys are reserved from public `lix_key_value` writes, and each branch may install at most 128 plugins. Uninstall retains plugin-owned document state for reinstall; reads that require an absent plugin fail with `LIX_ERROR_PLUGIN_UNAVAILABLE` instead of silently returning empty bytes.
- Lix SQL history surfaces now use one explicit, prefixed vocabulary.

  Every public history surface exposes `lixcol_entity_pk`, `lixcol_observed_commit_id`, `lixcol_commit_created_at`, `lixcol_as_of_commit_id`, `lixcol_depth`, and `lixcol_is_deleted`. The ambiguous `start_commit_id` and `lixcol_start_commit_id` spellings, along with all bare generic-history column names, were removed without aliases.

  Raw state and typed entity histories expose singular provenance through `lixcol_change_id`, `lixcol_change_created_at`, and `lixcol_origin_key`. Commit time is loaded from the observed commit and no longer silently falls back to change time.

  Composed `lix_file_history` and `lix_directory_history` rows expose `lixcol_source_changes` instead of singular change, schema, origin, snapshot, and metadata columns. The non-null JSON array is ordered by change ID and contains `id`, `entity_pk`, `schema_key`, `file_id`, `snapshot_content`, `metadata`, `created_at`, and `origin_key` for each source change.
- Made `information_schema.columns` the executable Lix SQL contract.

  Public columns now use canonical SQL type names, including `BYTEA` for binary data across reads and writes. JSON-backed text is identified through `lix_value_kind`, while `lix_insert_policy` and `column_default` describe omission independently from read nullability. Defaulted ids may be omitted but explicit `NULL` is rejected.

  The advertised scalar type names now work as explicit casts across reads and runtime-entity writes. Bound writes use the canonical names and retire `BINARY` in favor of `BYTEA`; read expressions retain DataFusion's wider cast dialect.

  Typed `BIGINT` columns normalize mathematically integral JSON numbers such as `1.0` and reject non-integral or out-of-range stored values instead of projecting `NULL`. This contract also applies to typed history, pushed filters, bound numeric predicates, and `DELETE ... RETURNING`. SQL decimal literals are now kept distinct from values produced by `lix_json(...)`, so numeric comparisons no longer acquire JSON comparison rules accidentally. BIGINT writes and predicates parse integer, decimal, and exponent spellings exactly, preventing out-of-range literals from rounding onto an in-range boundary.

  Registered-entity defaults are materialized before `ON CONFLICT` routing and `excluded.*` evaluation. `INSERT ... RETURNING` and `UPDATE ... RETURNING` are now rejected explicitly until those result paths are implemented; they are no longer accepted while silently discarding the requested rows.
- Fixed `lix_file_history` and `lix_directory_history` reconstruction across commit DAGs.

  Composed history rows are now identified by the requested start commit, observed commit, and logical entity. Equal-depth sibling revisions are preserved, while descriptor, blob, direct-directory, plugin-owner, registry, and plugin state is reconstructed from the observed commit's immutable state root instead of inferred from traversal depth.

  Plugin-backed file history now follows the durable per-file owner used by live `lix_file`. Plugin upgrades and uninstalls create projection revisions for files owned by the changed plugin, overlapping plugin globs do not reassign existing files, unavailable historical owners return `LIX_PLUGIN_UNAVAILABLE` when `data` is projected, and plugin-state tombstones remain in composed provenance.

  Exact public `id` predicates are routed through observed-state descriptor, blob, owner, and plugin-state reads, avoiding a second full observed-root materialization. Commit-provenance traversal and unfiltered history remain bulk operations.

  The composed histories now expose `lixcol_source_changes`, a non-null JSON array ordered by change ID. Each element mirrors the stable `lix_change` payload fields: `id`, `entity_pk`, `schema_key`, `file_id`, `snapshot_content`, `metadata`, `created_at`, and `origin_key`. Multiple source changes in one commit produce one logical revision with every source in this array.

  This is a breaking SQL catalog change. The misleading singular `lixcol_schema_key`, `lixcol_file_id`, `lixcol_snapshot_content`, `lixcol_change_id`, `lixcol_origin_key`, and `lixcol_metadata` columns were removed from the composed filesystem histories. Inspect the structured `lixcol_source_changes` objects, or join their `id` fields to `lix_change`, when raw provenance is required.
- Remote Lix clients now use independent, branch-pinned server sessions.

  The initial protocol handshake returns an opaque session identifier that is required on subsequent SQL, branch, and observation requests. Unknown or expired sessions fail closed so a client must reload before writing from a stale view.

  Switching branches changes only that client session, so one client can no longer change the active branch observed by another client.
- Concurrent whole-file writes now merge plugin-backed files at the plugin entity boundary.

  Each session tracks the file state it has actually received. A later blob write applies only that session's semantic additions, edits, and deletions, preserving unseen entities written by other sessions while resolving same-entity races by last write wins. Files without a matching plugin continue to behave as one raw blob entity.

  Blind plugin-backed writes use current semantic state to preserve entity identity, but omitted entities are not deleted until the session has received or submitted them.

  Session bases are tied to the durable plugin-owner incarnation, so a plugin-to-raw-to-plugin transition cannot revive stale delete authority.
- Added a native binary file-read route for remote Lix clients.

  `GET /lix/v1/file` returns raw file bytes without SQL planning or JSON/base64 encoding. Clients can discover the capability in the handshake response as `binaryFileRead`; `Lix-File-Found` distinguishes a missing file from a present empty file.
- Added a native binary file-upsert route for remote Lix clients.

  `POST /lix/v1/file/upsert` accepts an `application/octet-stream` file body and uses Lix's existing transactional file-write path without JSON base64 encoding or SQL planning. Clients can discover the capability in the handshake response as `binaryFileUpsert`.
- Tracked-state roots no longer store derived `lix_commit` rows.

  Commit rows are synthesized from `changelog.commit`, leaving immutable state trees to contain only authored changes. This makes ordinary one-row commits use the tree's singleton path-copy path and substantially reduces write amplification. Commit-root metadata now carries a backend-neutral format marker; repositories written with the previous layout are rejected and must be recreated instead of being silently inherited.
- Removed unreleased storage compatibility APIs and format fallbacks.

  SlateDB callers must use `SlateDB::open()` or `SlateDB::open_object_store_with_options()`. Legacy namespaced CLI snapshots, and change records without origin keys are no longer accepted.
- Lix SQL file writes now support explicit casts to binary data.

  Use `CAST(value AS BYTEA)` when inserting or updating UTF-8 text in `lix_file.data`.
- Changed Rust `Value::Blob` payloads from `Vec<u8>` to a new immutable `Blob` type backed by `bytes::Bytes`.

  Blob values now clone in constant time and share their payload across query parameters, exact `lix_file` writes, transaction staging, CAS encoding, and result fan-out. Rust callers must convert owned vectors with `.into()` when constructing blob values.

### Patch

- Accelerated exact `lix_file` path batch reads by moving owned file bytes directly into SQL results instead of copying them through Arrow.

  Exact batch reads also acknowledge delivered plugin-backed file views so later file writes retain the correct session merge base.
- Exact `lix_file` data and change-ID reads are now substantially faster.

  Canonical ID and path point reads avoid general SQL planning while preserving branch visibility, plugin rendering, and collaboration acknowledgement semantics.
- RocksDB-backed Lix instances now reuse RocksDB's owned buffers for full values of at least 64 KiB instead of copying them into a second allocation.

  Smaller values keep the existing copy path, which remains faster at that size.
- Improved DataFusion-backed SQL write performance by validating regular fallback writes in their authoritative execution session instead of preparing the same providers twice.
- Improved large binary file write performance on native storage.

  Lix now checks compact content-presence markers when deduplicating binary chunks, avoiding reads of unchanged chunk payloads during common localized file updates.
- Reduced idle object-store traffic by batching completed SlateDB compactions for five seconds before publishing them to the manifest.
- Lix now batches exact correlated live-state row reads for ID-constrained `lix_file` queries and writes.

  This removes the previous Cartesian point-read expansion for batches of up to 32 files and the per-file prefix-scan fallback for larger batches. `SELECT`, `UPDATE`, `DELETE`, and ID-based upsert conflict probes use aligned exact identities while preserving branch/global visibility, tombstones, tracked and untracked rows, and staged transaction overlays.
- Native installed-plugin calls now have bounded execution time and Wasmtime resources.

  Plugin instantiation, change detection, and rendering receive a fresh five-second guest deadline, alongside bounded memory, instance, and table allocation.
- Repeated JavaScript SDK opens now reuse prepared WebAssembly plugins.

  Lix keeps a bounded plugin preparation cache while preserving a fresh isolated plugin instance for every open.
- Improved remote active branch reads by reusing the session state established during the initial handshake.

  `activeBranchId()` now avoids a redundant network request and remains synchronized after successful branch switches. Ambiguous switch failures invalidate the cached value so the next read reconciles it with the server.
- Repeated SQL writes now reuse parsed and bound templates plus stable catalog metadata. Transactions derive SQL-visible schemas from their compiled opening catalog instead of rereading durable schema state, while every execution still builds fresh snapshot-specific DataFusion providers and plans.
- Improved RocksDB and SlateDB CRUD performance by reusing compiled schema catalogs across ordinary implicit transactions.

  Schema registrations, merges, branch-head changes, and tracked-state repairs still invalidate the cached catalog atomically before the next transaction opens.
- The native SDK now reuses compiled WASM components across Lix opens, bounds the compiled-component cache, and enforces `WasmLimits.max_memory_bytes` during component instantiation and growth.
- Cancelled read-only protocol requests now release their session locks so a timed-out workspace can close and recover promptly. Writes and durable runtime functions still complete after a client disconnects.
- Remote point-file observations now locate unchanged regions in large blobs with chunked comparisons, reducing CPU time before sending compact deltas for localized edits.
- Remote file writes now locate unchanged regions in cached request blobs with word-sized comparisons, reducing client CPU time for localized edits.
- Reduced remote observation startup work when an interface registers several queries together.

  Same-turn registrations now open one multiplex stream with the complete subscription set instead of repeatedly opening and aborting partial streams.
- Transaction SQL reads now construct each DataFusion table provider once by installing snapshot-backed history providers alongside transaction-overlay writable providers, instead of constructing and then replacing duplicates.
- Reduced CPU time when identical observations fan out large results.

  Lix now compares each new shared query result with its predecessor once and lets subscribers reuse that equivalence decision, while retaining each generation's fresh result metadata.
- Remote Lix clients now gzip large compressible JSON uploads, and protocol servers gzip large finite JSON responses when requested.

  A bounded sample avoids spending time compressing small or incompressible uploads. Servers enforce request limits after decompression and leave live SSE streams unbuffered.
- Reduced remote sync latency and bandwidth for localized edits to large files.

  Remote clients now transparently send compact byte splices after a successful full write, while automatically retrying with full bytes if the server no longer has the required base.
- Remote point observations of `SELECT data FROM lix_file` now send localized blob changes as compact prefix/suffix splices.

  The first result and large replacements remain complete snapshots. Deltas are used only when they reduce the live event by more than 10%, and reconnects always restart from a complete result.
- Remote protocol hosts can now conservatively test whether a workspace server has no live sessions before evicting it.
- Improved the speed of batched `lix_file` path, data, and metadata upserts.

  Common upload statements that replace both file bytes and row metadata now use the native bound-write path while preserving descriptor identity, blob replacement, and plugin reconciliation.
- RocksDB-backed Lix instances now use whole-key Bloom filters to avoid unnecessary SST reads when a requested key is absent.
- Cancelled SlateDB reads no longer delay runtime shutdown or recovery.
- Large file writes now reuse content hashes computed while preparing CAS chunks.

  This removes a second hash of every chunk before staging the write without changing chunking or storage formats.
- Filesystem path and exact file-ID indexes now advance cached branch views from committed descriptor deltas. A singleton descriptor commit no longer makes the next indexed file write scan and reconstruct every visible descriptor; directory changes update only the affected subtree while immutable index roots keep concurrent readers on their original generation.
- Improved existing `lix_file` path upserts by reusing the revisioned filesystem path index instead of scanning and rebuilding the workspace filesystem state.
- Common exact-ID `lix_file` selections now binary-search a collision-safe secondary index when reusing the filesystem path cache, instead of scanning every visible path entry.
- Improved new and mixed `lix_file` path upserts by reusing the revisioned filesystem path index instead of rescanning the complete workspace filesystem state.
- Improved root-directory listings in workspaces with many directories.

  Lix now uses its filesystem path index for `lix_directory` queries filtered by `parent_id IS NULL`, avoiding an unnecessary full descriptor scan.
- Improved root-file listings in workspaces with many nested files.

  Lix now uses its filesystem path index for `lix_file` queries filtered by `directory_id IS NULL`, avoiding an unnecessary full descriptor and blob scan.
- Deferred unchanged `lix_file.data` projections until after DataFusion selects the final metadata rows.

  Metadata filters, ordering, and limits now avoid loading and copying discarded file bytes through Arrow while preserving the general SQL fallback for expressions that consume file data.
- SlateDB-backed writes now create a read snapshot only when a transaction deletes a range.

  Ordinary puts and point deletes avoid the extra worker round trip while range deletes retain a stable base snapshot.
- Improved Lix garbage-collection planning by scanning changelog records in ordered batches instead of reopening the storage backend once per commit.

  This keeps checkpoint cleanup practical for repositories with long automatic commit histories, especially on remote LSM-backed storage.
- Reduced SlateDB open latency and object-store traffic by loading cached SST data on demand instead of preloading live SSTs up to the disk-cache limit.
- Reduced SQL result materialization overhead by moving owned text and blob values out of DataFusion instead of copying them into Lix values.
- Remote Lix clients now multiplex live query observations over one server stream.

  Multiple `lix.observe()` calls no longer consume one HTTP connection each, so thin clients can keep executing queries while many observations are active.
- Improved `executeBatch()` performance for read-only workloads without adding a new API.

  Pure-read batches now reuse one storage snapshot and prepared SQL session, while batches containing writes or durable runtime functions retain their existing transactional semantics.
- Improved batched binary file read latency when multiple files span several storage chunks.

  Lix now overlaps the independent manifest scans for those files with bounded concurrency while retaining ordered results and a single batched chunk fetch.
- Remote session handshakes can now validate storage concurrently instead of serializing behind the session registry.

  Session admission, eviction, and shutdown remain bounded and coordinated while slow storage reads no longer block unrelated handshakes.
- Fixed `lix_file_history` and `lix_directory_history` so changes to an ancestor directory produce revisions for every affected descendant.

  Ancestor renames, subtree moves, deletions, and restorations now revise the composed `path` of nested files and directories without changing their stable `id`. Each revision is reconstructed from the exact observed commit and its direct-parent roots, preserving distinct sibling revisions in a commit DAG.

  `lixcol_source_changes` now includes every same-commit ancestor descriptor that shaped the descendant projection. Recursive deletion rows retain both the descendant's direct tombstone and the tombstones of its deleted ancestors.

  Exact `id` queries keep observed and direct-parent reconstruction scoped to the selected file and its ancestor chain instead of rescanning unrelated filesystem state.
- Large file reads now assemble CAS chunks directly from storage-owned bytes.

  This removes intermediate copies while preserving the existing storage format and file API.
- Read queries now construct only the snapshot-local DataFusion table providers referenced by their parsed SQL, while catalog-wide introspection retains the complete provider set.
- Improved SQL write performance by constructing only the target table's DataFusion provider for UPDATE, DELETE, and VALUES-based INSERT statements. Query-backed INSERT statements retain catalog-wide provider registration.
- Improved the speed of batched file updates in workspaces with many files.

  Lix now reuses the plugin reconciliation view for files in the same write batch, avoiding repeated full filesystem scans during common multi-file uploads.
- Filesystem-backed Lix instances now use the shared RocksDB storage adapter so RocksDB performance improvements apply consistently across storage modes.
- Reduced CPU time and memory copying when remote observations fan out large blob results.

  Immutable query results now share their backing storage across observation subscribers and retained transport delta bases.
- Improved SQL read performance for table-free queries and fixed Lix system surfaces.

  These reads now use immutable system metadata without scanning registered schemas. Runtime schema registrations are rejected only when their generated base, `_by_branch`, or `_history` table names would collide with a fixed system SQL surface.
- Improved metadata-only file updates by checking the affected descriptor directly instead of scanning the entire workspace namespace when its path is unchanged.

  Process-median SlateDB p50 improved from 36.639 ms to 23.209 ms with 1,000 files (36.7%) and from 317.733 ms to 156.995 ms with 10,000 files (50.6%).
- Existing `lix_file` path upserts no longer rewrite file descriptors when the incoming metadata is unchanged, avoiding workspace-wide namespace validation for byte-only overwrites.
- Avoid scanning every committed owner of a unique value when a single existing entity keeps that value unchanged.

  With the namespace shortcut already applied, SlateDB changed-metadata file overwrite p50 improved from 7.767 ms to 5.766 ms with 100 files (25.8%), from 22.077 ms to 6.550 ms with 1,000 files (70.3%), and from 171.892 ms to 9.509 ms with 10,000 files (94.5%).
- Improved ordinary SQL read performance by skipping deterministic runtime-function state work when a query does not call `lix_uuid_v7()` or `lix_timestamp()`.

  Queries that use durable runtime functions retain their existing deterministic sequencing and persistence behavior.
- Remote Lix clients now use native typed-array Base64 conversion when the runtime supports it.

  Large blob uploads and observation results avoid the previous byte-by-byte JavaScript conversion cost while retaining the existing compatibility fallback.
- Reduced cold point-read cache fill and disk usage by aligning SlateDB disk-cache parts with the two-MiB scan read-ahead window.

## 0.8.4 - 2026-07-16

### Patch

- Added SQL script planning to the Rust and Workerd SDKs.

  Lix now parses single and multi-statement SQL into one atomic statement plan with request-wide parameter ranges.

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
- Added a `lixDir` option to `LocalFilesystem` for storing lix state outside the workspace.

  By default, state lives in `<workspace>/.lix`. Passing `lixDir` keeps repository metadata in an external `.lix` directory and writes no `.lix` directory into the workspace. Pointing `lixDir` at a temporary directory gives ephemeral filesystem sync: workspace files are imported and watched without persisting lix state.
- `LocalFilesystem` now requires an explicit `syncAllFiles` option and supports on-demand file sync.

  `new LocalFilesystem({ path, syncAllFiles: true })` syncs the full workspace as before. With `syncAllFiles: false`, the lix opens without workspace files and `storage.importPaths(["notes/today.md"])` syncs selected files on demand. Imported paths are exact workspace-relative file paths, not directories or globs. In Rust, use `LocalFilesystemOpenOptions::new(root, sync_all_files)` and `LocalFilesystem::import_paths()`.
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
