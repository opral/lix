# Changelog

## 0.17.0 - 2026-09-16

### Minor

- Automatic SQL transactions now accept a retry limit.

  Set `maxAutoCommitRetries` on JavaScript `execute()` and `executeBatch()`, or use Rust's `.with_max_auto_commit_retries(...)`, to cap whole-operation retries after transaction contention or an expired snapshot. Zero disables these retries. Without an override, Lix retains its default recovery budgets. Explicit transactions remain caller-controlled, and unknown commit outcomes are never automatically re-executed. Remote idempotent writes can now retry known transaction conflicts after checking for a committed receipt.

  Errors from the retry loop include the number of replays and why retrying stopped.
- Excalidraw scenes now expose root metadata as `scene_json`. Element type and deletion state are edited directly in `element_json`; duplicate `element_type` and `is_deleted` columns are removed. Formatting hints are optional for SQL inserts, and rows with equal order keys sort by native ID.

  Excalidraw preserves durable ordering, numeric spelling, collection layout, and unknown data across file edits, SQL updates, and cold restoration. Scene templates cannot override row content. Warm element SQL edits and grouped file edits use sparse indexed reads; paged indexes and offset rebuilding have scaling regressions and compiled SQL/Wasm coverage.
- Return `from_content` and `to_content` from `lix_diff('lix_file', ...)` and `lix_history('lix_file', ...)`. File metadata filters are applied before reading the selected historical bytes, including on partial replicas.

  Remove the public diff and history `row_count` column. Count the relation being displayed explicitly with `COUNT(*)`, naming the result `changed_files`, `changed_paragraphs`, or another description of that relation.
- Improved JSON plugin lossless roundtrips, SQL row creation, and scalar edit performance.

  JSON objects now preserve duplicate member names, distinguished by an `occurrence` primary-key column that defaults to zero. Rows provide defaults for root identities, top-level parents, and ordering, and nested containers can use caller-supplied identities for SQL creation and renaming. Deeply nested documents no longer overflow the parser or renderer stack, and numeric changes remain content changes even when their native numeric values round identically.

  Scalar SQL edits locate their rows through a paged identity index and read only the affected values instead of loading the entire file or scanning every scalar for each changed row.
- Removed the unused `conflicts` field and conflict types from branch merge previews.

  Lix continues to combine independent column edits and resolve competing edits with last-writer-wins or a plugin merger. Applications should review the changes rather than use a conflict array as an approval gate. Server protocol version 11 marks the response change; clients and servers must use compatible versions.
- Open partial replicas with on-demand sync instead of downloading a full repository.

  JavaScript clients opt in with storage and `server.mode: "partial_replica"`. Opening transfers bounded metadata; SQL fetches missing native inputs and retains them locally. Covered reads and prepared writes execute locally, including offline, while background synchronization updates the working set and uploads commits. Execute the expected SQL query to prefetch its inputs before an interaction; no separate preparation API is required.

  Server mode defaults to `"remote"` and rejects client storage. Existing synchronized callers must explicitly opt into `"partial_replica"`; the former `"sync"` spelling is not supported. Upgrade SDK and server together. Existing full replicas require explicit conversion that preserves their source and pending work; see the partial replica migration guide for supported formats and recovery boundaries.
- Partial replicas continue syncing current data when retained historical data is unavailable on the server.

  Historical retention no longer blocks current-state updates, and background progress discovery no longer depends on optional prefetch. Use `sync_health()` in Rust or `syncHealth()` in JavaScript to distinguish stalled synchronization from successful local reads and compare observed and applied cursors.

  This release requires sync protocol 17. Upgrade SDK and server together; protocol 16 peers are rejected. Existing repository data remains in format 81.
- Plugin API v2 now uses the major-only `lix:plugin-v2` identity independently of Lix and plugin release versions.

  Existing compiled plugins using `lix:plugin@2.0.0` remain supported. Compatible API additions preserve existing plugin behavior; breaking changes require a new API major.
- Plugins can use shared SDK order-key allocation and the SQL `lix_order_between` function instead of implementing fractional ordering themselves.

  The native plugin testing harness now applies generated identities to small row fixtures and reports transition I/O counters. A reusable UUID-to-ordinal private-state index supports bounded lookup.

  Text content updates and ordinary Markdown paragraph updates now use sparse accepted-file reads and state updates. Structural and formatting-sensitive changes retain the existing full-document fallback.
- Separate repository migration from ordinary opening, and make browser replica reads and shared identity admission reliable during concurrent synchronization.

  This is a coordinated breaking upgrade: migrate every authority and local repository with the detached migration tools before admitting it to the current runtime, and upgrade clients and servers together to sync protocol 9 and storage format 81. Migration preserves source repositories and pending local work; dormant browser repositories migrate when their device returns. Ordinary opening no longer performs legacy migrations or remote SQL identity probes.

  Custom JavaScript HTTP transports must implement the typed request and response contract. Replica conversion now belongs to the detached migration API. Buffered foreground reads have a 30-second deadline and a combined 64 MiB / 1,000,000-row result limit; oversized results and exhausted read progress return structured errors. Browser storage close now waits for physical ownership release when the last client disconnects.
- Support `OLD.column`, `NEW.column`, `OLD.*`, and `NEW.*` in SQL RETURNING for inserts, updates, deletes, and upserts. Returned row images describe each statement independently of the committed operation's endpoint diff, with typed NULLs for absent images and selective file content reads.
- Resolve concurrent partial-replica edits through the same native row merge pipeline as branch merges.

  Registered schema/plugin merge hooks remain active. The default for overlapping values is incoming-write precedence in server acceptance order, rather than change-ID ordering. Plugin-managed files serialize the resolved rows; opaque file content remains atomic. Accepted retries retain their original identity and cannot overwrite a later server edit by being treated as a new write.

  Upgrade SDK and server together for sync protocol 14. The explicit local journal migration preserves pending edits and existing acknowledgment identities without resetting browser storage. Opening and resident SQL keep their on-demand and local execution behavior.

  Pending edits are accepted against the server's current branch state even when other writers advance it during upload. Exact retries keep their original outcome, already included rows are not applied again, and newer local edits remain pending for their own acceptance.
- Normalized reference-server error codes and documented the error catalog.

  Server lifecycle errors now use `LIX_ERROR_MIGRATING`, `LIX_ERROR_MIGRATION_FAILED`, `LIX_ERROR_RECOVERING`, `LIX_ERROR_SHUTTING_DOWN`, `LIX_ERROR_CAPACITY`, `LIX_ERROR_CACHE_CLEANUP`, and `LIX_ERROR_TIMEOUT`, removing the redundant second `LIX`. Unsupported storage formats now consistently use `LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT`. This changes the wire codes without legacy aliases; update clients that match the previous spellings together with the server. HTTP statuses and retry semantics are unchanged.
- Resolve historical read and diff commit arguments with scalar subqueries.

  `lix_as_of` and `lix_diff` can now look up commit IDs within the same statement, including through parameters and common table expressions. This removes the separate query previously needed to resolve a branch before reading or comparing its state. The one-argument working diff continues to compare the active branch's working baseline with its current head.
- Return one durable commit receipt per SQL transaction.

  `executeBatch` now returns `{ results, commit }`, replacing the array of results with repeated commit spans. Explicit transaction `commit()` returns `{ commit }`; its statement results carry no receipt. Server protocol 10 carries these contracts through HTTP, native bindings, browser workers, and the JavaScript SDK.

  Explicit SQL reads used to decide later writes now fence the transaction's opening branch snapshot. Concurrent branch changes require retrying the complete transaction, including application checks. Read-only transactions remain valid. Known durable completion errors preserve the receipt in `details.commit` and remain forbidden from automatic mutation retries.

  Explicit SQL transactions publish individually to preserve exact receipts. They no longer combine multiple transactions into one merged commit; concurrent commits still use the coordinator and retain stale-write reconciliation.

### Patch

- Filesystem mirror updates now replace each file atomically, so readers no longer observe partially written contents. This does not add a power-loss durability guarantee for mirrored files or make multi-file updates atomic.
- Automatically upgrade supported older server repositories when opening them.

  Clients await server migration inside the normal open call. The SDK reports upgrade progress through `onProgress`, so applications can display status without owning migration or retry logic. Concurrent opens share one upgrade, which retains source storage and verifies preservation before serving the repository. Failed or unsupported migrations preserve the existing data and return an error.

  Transport-only upgrades no longer reject repositories with compatible storage. Ordinary admission remains independent of repository size.
- Fixed full-replica migration inspections failing when OPFS reads expire during migration heartbeat or candidate writes. Frozen source reads now resume bounded point batches and scan pages while checking the exact migration claim and source revision. Local edits and retained source banks remain protected.
- Fixed browser replica upgrades failing with “Lix session is closed.”

  Existing replicas can now run closed-storage conversion and retry migration cleanup through the SDK worker.
- Fixed missing results from indexed filters and joins after large bulk writes.

  Bulk inserts and replacements now publish index entries alongside their rows, preventing queries from treating existing records as absent. This fixes translation compilation falling back to message keys after larger inlang imports. Databases with older, potentially incomplete indexes use a scan when index completeness cannot be established.

  Also fixed nullable primitive projections and compatible schema amendments: existing rows remain readable, inherited rows retain correct indexed results, and newly added literal and generated defaults are stored once in the amendment transaction. Historical queries preserve the original row values.
- Keep legacy synced replica initialization and snapshot installation coherent while migration heartbeats commit. Preserve archived local edits as recovery exports without blocking conversion of the clean active replica to partial storage. Reject unsupported restoration before it can create pending work that prevents branch switching.
- Fixed migration planning and snapshot verification failing when OPFS reads expire during heartbeat commits. Candidate scans now resume bounded read units under an unchanged bank revision and epoch fence; publication keeps its existing revision preconditions. This preserves stored rows and pending edits without restarting completed migration writes.

  Explicit browser replica conversion now shares the same admission queue as repository opening. Concurrent tabs coordinate conversion, disconnected callers release ownership, and repeated conversion validates the published replica instead of reopening a competing engine. Existing sessions and retained recovery sources are preserved.

  Opening an additional session outside an admitted partial replica scope now returns the scope error before attempting to read unprepared branch metadata.
- Reopen previously admitted browser replicas after a cold offline reload without granting cached credentials a remote lease. Persist only a credential digest and verified local routing identity scoped to the physical store, authority and protocol epochs; remote access still requires fresh admission.

  Handle bodyless HTTP responses (204, 205 and 304) without constructing an invalid response stream, fixing remote session cleanup through wrapped browser transports.
- OPFS partial replicas now share loaded data and offline edits across browser tabs.

  Tabs coordinate one engine automatically through a SharedWorker, keeping the same storage identity and `openLix()` API. Closing a tab leaves the other sessions operational. Checkpoint history also fetches missing commit metadata on demand and retains it for subsequent offline reads.

  File checkpoints upload their blob dependencies before publishing, including after an offline checkpoint or a lost acknowledgment. Each browser session keeps its own telemetry callback and trace parent; shared background spans go to live subscribers.

  Pending edits and checkpoint dependencies upload in bounded waves, including recovery after lost replies. SQL retries preserve the completion boundary: an error reported after execution or commit cannot replay the operation.

  Read-interest journal flushing retries expired snapshots internally, so concurrent tab startup can complete without replaying the SQL that registered its inputs.

  Opening an additional session retries transient read invalidation during branch and admission validation, so another tab or background synchronization can commit while the session opens. Real admission and storage errors still propagate.
- Filesystem repository opens now report `LIX_STORAGE_IN_USE` when another process holds the repository lock. Applications can distinguish ownership contention from storage failures without matching diagnostic text.
- SQL `UPDATE` statements now support scalar expressions such as `replace`, `concat`, `coalesce`, and string concatenation in file and row mutations.

  Update assignments, filters, and returned expressions use the same scalar function rules as reads. Invalid expressions reject the mutation atomically. Partial replicas can use resident file content in an exact-path expression update while offline.

  These additional scalar expressions, such as `upper` and `concat`, remain unsupported in registered-row `INSERT` values, upsert assignments, and insert `RETURNING`; those statements fail without changing rows.
- Keep server S3 requests and response bodies on the server's long-lived Tokio runtime. Closing a repository's SlateDB runtime no longer invalidates shared HTTP connections used by later repository opens or catalog reads.
- Recover partial replicas inside awaited operations.

  Cold SQL renews expired baseline leases, reconciles pending changes, and retries against the recovered state without exposing internal recovery instructions. Unchanged baselines retain local edits and frozen upload identities. Branch switches await pending synchronization internally. Repeated coherent baseline changes can restart an uncommitted operation without replaying completed writes.

  Unsupported local reconciliation adopts server-authoritative state only after pending publication identities are safely settled or fenced. Bounded opening and network-free reads with valid resident inputs remain unchanged.

  Online opening replaces incompatible replica caches with an authenticated partial epoch without migrating their history. Old banks remain detached and intact. Format-78 offline migration validates native checkpoint identities rather than retired checkpoint marker rows.

  Explicit transactions fetch missing immutable inputs while keeping mutable reads pinned. A snapshot whose required inputs are no longer retained fails with a normal transaction conflict instead of waiting for a publication it prevents.

  Local partial snapshot exports now preserve the actual resident cache and pending edits instead of downloading the authority snapshot. Partial snapshots carry an explicit header flag and restore as partial replicas; older readers reject them.

  Deploy SDK and server together for sync protocol 16. Older peers fail version negotiation before attempting the new authenticated upload-abandonment fence. Repository storage format remains unchanged.

  Deployment probes and raw HTTP integrations can read Lix-owned compatibility metadata from the source checkout or `@lix-js/sdk/compatibility`; applications no longer need to maintain their own protocol version numbers.

## Unreleased

### Fixes

- Partial replicas now recover expired baselines and reconcile pending changes inside awaited SQL operations. Unsupported local changes can yield to the authoritative server after outstanding merge attempts are fenced.

### Upgrade notes

- Upgrade the SDK and server together to sync protocol 16. It adds active-attempt abandonment for automatic recovery; older peers fail protocol negotiation. The repository storage format remains 81.

## 0.16.1 - 2026-09-11

### Improvements

- Write results from `execute` and `executeBatch` now include `commit: { before, after }`, so you can inspect what changed without an extra query.
- SQL metadata now includes table and column descriptions, making schemas easier to explore.
- Improved large SQL upload performance and removed the default 64 MiB request limit. Hosts can still set their own limit.

### Fixes

- Fixed Undo and Redo for plugin-backed files, including restoring added and deleted files.
- Fixed file renames after switching branches or reopening a repository, and rename failures during synchronization.
- Fixed filesystem imports failing after files were edited in another Lix session.
- Improved synced repository upgrades and recovery of pending local edits. Recovery tools can export retained data and restore tracked rows to a separate branch.
- Fixed synchronization interruptions caused by concurrent browser writes during reconnect.
- Merge now rejects conflicting file content and format renames before changing the target, with guidance to merge the rename separately.
- Improved error messages for repository migration and browser storage failures.

### Upgrade notes

- Queries using `lixcol_schema_key` must use the relation name to identify the schema instead. `lix_change.schema_key` is unchanged.
- The JavaScript SDK no longer exports `@lix-js/sdk/server-protocol`; use the remote connection API or the documented HTTP protocol.
- Custom storage adapters must replace `BranchEquals` (`branchEquals` in JavaScript) with `KeyValueEquals` for conditional writes.
- Retrying a SQL request with an idempotency key recorded before the upgrade returns `409 LIX_IDEMPOTENCY_KEY_REUSED`. Reconcile uncertain requests before issuing new keys.

## 0.16.0 - 2026-09-09

### Minor

- Consolidate checkpoint storage and version-control SQL with a breaking repository migration.

  Checkpoint membership is immutable `lix_commit.is_checkpoint`; remove `lix_checkpoint` marker writes. Add first-parent `lix_log`, redefine `lix_history` as endpoint changes with checkpoint flags, and rename `lix_state_at` to `lix_as_of`. Working diffs expose their actual endpoints and branches expose `working_base_commit_id`. Replace the latest-checkpoint scalar with filtered logs or the working baseline, according to the query's purpose. Upgrade clients and synchronization peers together.

  Reference hosts can explicitly provision control-plane repository IDs through an internal authenticated operation. Creation and legacy-storage adoption are separate; adoption validates and migrates existing repositories without replacing their data. Quiesce old writers and adopt legacy repositories before switching public traffic to the lifecycle catalog.

  Sparse checkpoint inventory bootstrap preserves deferred jump topology, including checkpoints created from unmarked restore/fork baselines. Validate header graphs without repeated full-inventory scans.
- Synchronize custom and plugin-defined rows, including Markdown, with their schema and typed values intact.

  Sync peers must upgrade together to sync protocol version 8. Older sync peers are rejected during negotiation rather than accepting a session that cannot exchange rows.
- Support structural JSON row edits, including insertion, deletion, reordering, moves, and scalar/container conversion. Structural batches validate and rebuild the final tree, preserve row identities and unchanged scalar spelling, and stream the replacement file. Existing scalar updates retain their byte-splice fast path.

  Container deletion requires removing or moving descendants in the same plugin row batch; invalid trees reject atomically. SQL projects each statement separately. Structural upserts follow normal row last-write-wins behavior, including recreating a previously deleted key.

  Repeated row updates honor the final update even when it restores the original value. Renames adapt existing key-formatting hints, and scalar conversions ignore obsolete empty-container whitespace. File-derived row deltas use deterministic key order so stale edits across multiple parents can compose reliably.
- Sync clients commit edits and checkpoints locally and upload them in the background.

  Current-state reads and writes no longer wait for a server round trip. Durable replicas reopen offline, and historical data is fetched on demand and cached. The server remains authoritative: incompatible concurrent branch updates replace pending local work without a merge-conflict workflow. Upgrade sync clients and servers together for sync protocol version 7.
- Simplified plugin development with native lifecycle testing, validated file edits, and scoped state cleanup.

  Plugin authors can test projection hooks without compiling WebAssembly, read edited file ranges without rebuilding the whole file, and clear related private state in one operation. Markdown and CSV use the shared helpers to reduce duplicated edit and cache bookkeeping. Rebuild plugin components against the updated SDK to use the new state cleanup host operation.

### Patch

- Large offline sync queues reuse a metadata-only upload plan across pages and load only the commit payloads being sent. Ordinary edits remain local and join the next upload wave; restores and server resets invalidate cached work. Checkpoint acknowledgment metadata is retired after all branches converge.
- Fixed CSV edits and reopen operations losing cells, formatting, or the stored dialect.

  CSV now preserves UTF-8 BOMs, literal quote spelling, empty final cells, and missing final line endings when rows move. Multiline and adjacent file edits reconcile the correct rows, and unsupported NUL bytes are rejected before unreadable state is stored.
- Fix synchronization of edits made on disk while a filesystem repository is connected to a server.

  The filesystem watcher now shares the connected repository's write admission and authenticated account, and wakes its existing sync worker after an edit.
- Fixed explicit transactions failing on SlateDB-backed servers when reading cached file and directory state.

  SlateDB reads, writes, and flushes now also work when called from an executor without a Tokio runtime.
- Git replay clears starter files from its fresh output before importing a Git tree, so repositories without Lix's bootstrap README still replay and verify exactly.
- Keep ordinary reads and live queries available while an explicit transaction is open.

  Transactions now use an independent context on the originating handle's branch and account. Transaction reads see staged writes, while ordinary reads and observers see committed data. Commit publishes the changes; rollback leaves observers unaffected. Each originating handle still allows one explicit transaction at a time and must finish it before closing.
- Improved JSON and Excalidraw data and formatting preservation when alternating file and row edits.

  JSON retains number spelling and string escapes when rebuilding files from rows, and inserted array items remain writable after later insertions. Excalidraw preserves unchanged element and embedded-file formatting, including unknown fields. Rebuilt plugin indexes discard stale offsets so subsequent edits address the correct bytes.
- Fixed Markdown edits losing literal content, unrelated formatting, or the original file encoding.

  No-op row updates now preserve file bytes, reference-definition edits refresh affected links, and cached block content no longer reappears after subsequent edits.

## Unreleased

- **Breaking:** checkpoint membership is immutable commit metadata (`is_checkpoint`), replacing the `lix_checkpoint` relation and marker writes. Repository migration preserves existing checkpoint IDs and inventory. Upgrade clients and synchronization peers together.
- **Breaking:** `lix_history(relation [, anchor])` now returns first-parent endpoint differences with change kinds, before/after columns, endpoint commit IDs, checkpoint membership, and position. `lix_log([anchor])` lists retained mainline commits, including empty checkpoints.
- **Breaking:** rename `lix_state_at` to `lix_as_of` and remove `lix_latest_checkpoint_commit_id`. Use filtered log queries for checkpoints and `lix_branch.working_base_commit_id` for working comparison context.
- Diff results expose their actual endpoint commit IDs. Checkpoint creation no longer emits a separate source record; global checkpoint metrics use `lix_commit WHERE is_checkpoint` and commit creation time.

### Breaking

- Unify repository opening around storage and server connections. Server-only opening executes remotely; explicit storage plus server selects synchronization. Remove the mode option and its types without compatibility aliases.
- Add Rust `create_lix` and `delete_lix`, exposed as JavaScript `createLix` and `deleteLix`, for interoperable hosted repository lifecycle operations. Creation optionally copies a complete snapshot including history and untracked rows. Opening missing server resources no longer creates them.

## 0.15.1 - 2026-09-07

### Patch

- Fixed SQL filters missing rows inserted or updated within the same transaction.

  Queries, including CTEs, now evaluate predicates against staged values consistently. Updates and deletes that select rows by those values also see earlier writes in the transaction.

  Branch deletion now respects default-branch changes made in the same transaction, and plugin file materialization recognizes staged binary blob references without losing their durable proof.

## 0.15.0 - 2026-09-07

### Minor

- Added a deployable reference server for the Lix Server Protocol.

  Hosts can run the provided S3-backed container, embed the Rust protocol handler, or provide an independent compatible implementation.
- Changed connected sync clients to serve only authority-certified current state.

  Sync protocol v6 is a semantic hard cut from v5 and older clients that still permit replica-local writes; its existing JSON fields, paths, and operations are unchanged. JavaScript sync mutations and history execute on the authority, local HOT reads wait for a finite authority publication fence, and private replica receipts certify complete live values, provenance, and branch coordinates against the existing snapshot roots. Durable authority/replica storage fences prevent uncaptured local writes. The existing one-argument `lix_diff` reads Working Changes from the certified HOT epoch without hydrating arbitrary history; selected point-in-time file content remains server-first through the existing history surface.

  The Rust and JavaScript public type and function surfaces are unchanged. Rust retains `ServerOptions::sync(...)` with `open_lix().with_server(...)` and its existing remote protocol client, while JavaScript retains `openLix({ server: { mode: "sync" | "remote", ... } })`. No SQL surface was added or removed. The semantic hard cut is intentional: raw Rust sync handles serve certified HOT reads and return `LIX_AUTHORITY_EXECUTION_REQUIRED` for mutations, transactions, observations, and history; server-first Rust applications use the existing protocol client. JavaScript `openLix` performs that authority routing internally.
- Restore `lix_change` record identities as `schema_key`, JSONB `row_pk`, and `file_id`, replacing its public `row_ref`.

  History `lixcol_source_changes` objects use the same record identity. Snapshots and identities describe the same underlying schema record. Public history rows and diff commands continue to use opaque row references.
- Removed the obsolete hidden JSON primary-key projection from current SQL relations.

  Current relations now derive identity exclusively from their declared primary-key columns. Cross-relation addresses continue to use opaque row references, and derived columnar accelerators use a private physical identity field that is not part of any SQL schema.

### Patch

- Use immutable scope certificates and row-primary-key catalogs for bounded packed-state point reads instead of decoding unrelated rows. Preserve file scopes, selected-source identities, and native columnar lookup behavior.
- Fixed tracked-state updates that could drop newly inserted rows or produce inconsistent state roots.

  Sparse updates now repair boundaries across neighboring subtrees while skipping unaffected gaps and preserving existing canonical grouping rules. Key-size combinations that cannot form a finite canonical tree now fail explicitly instead of looping indefinitely.
- Avoid invalidating and warming unchanged schema catalogs when publishing checkpoints, three-way merges, or data-only inherited-base refreshes.

  Invalidate inherited catalogs when their visible definitions actually change, preserving local overrides, tombstones, and collection-generation fences.
- Prevent concurrent writes from incorrectly delaying eligible garbage collection after checkpoint cleanup encounters write conflicts.

  Write contention now delays automatic cleanup scheduling without marking reclamation as failed. Retention protections and backoff for genuine storage failures are unchanged.
- Fixed missing values and directory paths in working diffs.

  Default-range `lix_diff` queries now return the requested before and after values and metadata instead of silently returning null or failing to reconstruct directory paths.
- Prevent concurrent SQL updates and deletes from silently committing stale decisions.

  Transactions that plan an `UPDATE` or `DELETE` now reject an intervening change to their active-branch or shared/global state with `LIX_TRANSACTION_CONFLICT`, instead of merging the stale write. Retry an explicit transaction from the beginning. Local `execute()` and `executeBatch()` retry conflicts automatically within their existing limits. `RETURNING` results inside an explicit transaction remain provisional until commit succeeds. The branch-level check can also reject unrelated concurrent edits; explicit branch merging remains available for collaborative changes.
- Remote Lix handles now support merge previews and merges, and retain snapshot export on child sessions.

  Remote snapshot streams preserve server errors and release requests when cancelled. Local, sync, and remote bindings now share operation forwarding to reduce differences between modes.
- Share immutable state when creating branches and refresh inherited schema catalogs without rebuilding unchanged branch-local rows.

  Preserve private checkpoint before-images, untracked rows, and local overrides across stale-base refreshes, and invalidate cached catalogs when newly inherited schemas become visible.
- Local and remote JavaScript SDK handles now share Wasm initialization, preventing concurrent opens from initializing the same module twice.

## 0.14.0 - 2026-08-28

### Minor

- Standardized remote Lix URLs and added streamed snapshot export to the server protocol.

  Remote clients now connect with an immutable `https://host/lix/{uuid}` locator, while raw HTTP clients use `/lix/v1/{uuid}/...`. The previous host-specific URL plus appended `/lix/v1` shape is no longer supported.
- Made bundled `lix_*` schemas immutable engine authority instead of deriving their availability from branch-visible `lix_registered_schema` rows.

  Repository format v77 migrates v72-v76 repositories through the existing copy-and-activate epoch path. Retained built-in registration rows remain introspection and history projections, while custom registered schemas remain repository-owned. Sync protocol v2 rejects peers with the older catalog semantics.
- Added opaque row references and a single SQL checkpoint function for full and scoped checkpoints.

  Diff and selection surfaces now use `row_ref`, scoped checkpoints accept arrays of row references, and omitted diff commits default to the latest checkpoint through the active branch head. The former typed checkpoint SDK and two-column JSON row-key selection contract have been removed.

### Patch

- Root and latest-checkpoint queries now hydrate deferred commit history on sparse sync replicas.

  Observers and diff commands transparently retry after fetching a missing commit-graph ancestor instead of failing with an internal error.
- Concurrent browser sync no longer crashes an in-flight write when its transaction read expires.

  Lix now returns the transient storage error to its bounded write retry path instead of panicking while opening transaction-scoped history readers.

### Core engine and SDK changes

#### Minor

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

#### Patch

- OPFS repositories remain writable across browser-tab navigation and owner-worker handoffs.

  The shared storage session now survives an OPFS backend restart, so one healthy tab no longer fences another tab using the same repository generation.
- Allow a `LixServerProtocol` owner to stream a coherent snapshot without opening a second engine for the same storage.

### Sync and version-control changes

#### Minor

- Added optional public profile URIs to repository accounts.

  Applications can now associate an account with a machine-readable public profile while keeping authentication and authorization separate from presentation metadata.

  This release uses repository format v77. Supported older repositories upgrade automatically while opening; historical accounts receive `NULL` for the new field and profile updates remain durable.
- Added durable local-first repository sync for offline-capable applications.

  `openLix({ storage, server: { mode: "sync" } })` keeps reads and writes local while synchronizing with the server in the background. Browser applications can use OPFS for durable offline work, safely share a repository across tabs and workers, and recover when the owning tab closes.
- Redesigned version control around session-scoped SQL relations and commit-to-commit diffs.

  Use `lix_diff(relation, from_commit_id, to_commit_id)` to compare tracked relations, `lix_restore` to move a branch to an ancestor, and `lix_commit_ancestry()` plus commit parent IDs to inspect history. This replaces the former `*_by_branch`, branch descriptor/ref, heterogeneous working-diff, `diff_id`, and commit-edge surfaces; open a separate session for each branch.
- Simplified the public SDK surface for serving, batching, migrations, and telemetry.

  Serve a repository with `open_lix().with_storage(storage).serve().await`, submit atomic statement arrays through `executeBatch`, and let `open_lix` perform supported repository upgrades with typed progress. Rust and JavaScript now share one stable telemetry contract. The former protocol constructors, Workerd entry point, SQL script parser, migration names, and legacy telemetry surface have been removed.

#### Patch

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
