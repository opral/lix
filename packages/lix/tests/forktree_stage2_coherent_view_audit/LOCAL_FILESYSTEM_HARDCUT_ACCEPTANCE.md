# LocalFilesystem positional-path hard-cut acceptance checklist

## Pinned baseline and review boundary

- Exact baseline/current main: `c8c7899912a661b7bbd802eaced3c076f52876e5`.
- Baseline tree: `e857186ae74c0e537ec3eff04f284590b86c636e`.
- Review is read-only and begins only after Hetzner-II publishes an immutable ref/head/tree/diff. No moving candidate is inspected.
- Allowed production ownership is LocalFilesystem and its JS/native bridge. Reject changes to SQLite, ForkTree, SQL/sql2, storage formats, or unrelated engine behavior.

## Public hard cut

The sole public constructor/open contract is positional path only:

```ts
const storage = new LocalFilesystem("/absolute/workspace/path");
```

```rust
let storage = LocalFilesystem::open(path).await?;
```

Required deletion, compiler-driven across all callers:

- JS `LocalFilesystemOptions`, object constructor, `lixDir`, `syncAllFiles`, `importPaths`, and `syncDiskToLix` are absent and unnameable.
- Binding config carries only the filesystem path. Native `openLocalFilesystem` carries path plus unrelated telemetry plumbing; no lix-dir/sync/filter options survive.
- Rust `LocalFilesystemOpenOptions`, `open_with_options`, `open_with_options_and_wasm_runtime`, public `import_paths`, and public `sync_disk_to_lix` are absent and unnameable.
- No aliases, deprecated exports, overloads, compatibility decoder, ignored extra arguments, alternate constructor, or native-only predecessor path survives.
- README, examples, generated declarations, package exports, tests, and every internal caller show only the positional path form.

Run the frozen TypeScript negative probe with the candidate package declarations. It must compile clean because every `@ts-expect-error` is exercised; an unused directive is a blocker. Compile the Rust negative probe as an external consumer: all predecessor names must fail, while a separate positive probe containing only `LocalFilesystem::open(path)` must compile.

## One synchronization owner

- Exactly one Rust `FilesystemSupervisor`/worker owns the filesystem watcher, disk-to-Lix reconciliation, Lix-to-disk materialization, event serialization, and shutdown.
- JS and native bindings are descriptors/transports only. They must not create another watcher, poller, queue, synchronization state machine, or manual sync endpoint.
- Watcher callbacks enqueue events to the one worker; they never write Lix or disk directly.
- All disk and Lix reconciliation is serialized by the existing owner lock/queue. No second snapshot, retry loop, dual writer, or best-effort fallback becomes authority.
- Physical `<workspace>/.lix/**` is metadata owned by LocalFilesystem and is excluded from ordinary workspace traversal, watcher import, materialization, rename/delete reconciliation, and self-loop detection. It must never appear as an ordinary user file in Lix.

## Runtime oracle, both JS and Rust boundaries

Use fresh real directories and exact bytes; polling is bounded and reports the last observed state.

1. **Initial/open:** seed root text, nested text, and binary files plus a sentinel under physical `.lix/`. Positional open imports only user files, preserves exact binary bytes, excludes `.lix`, and creates/opens its Rocks metadata once.
2. **Disk → Lix:** independently create, modify, delete, rename, create nested directories/files, and replace a binary file. Each operation appears once with exact path/content; rename leaves no old path. `.lix` changes never appear.
3. **Lix → disk:** independently create, modify, delete, rename, nested-write, and binary-write through public Lix APIs. Awaited Lix commits materialize exact disk state with no temporary/duplicate user-visible path.
4. **Self-loop:** record public change/head identity after each accepted operation, wait at least two watcher debounce windows, and require no additional semantic commit/change from the adapter re-observing its own materialization.
5. **Cold reopen:** close, drop, reopen from the same positional path, and verify exact Lix rows and disk bytes for the final state. No option-dependent path or external metadata owner is needed.
6. **Close drain:** after an awaited Lix commit containing several text/nested/binary writes, call `close()` immediately. Before close resolves, the sole owner must enqueue/complete a Lix-to-disk barrier for every already accepted commit, stop accepting watcher callbacks, drain prior queue work in deterministic FIFO order, close the Lix session, stop the watcher, and join the worker. On return, exact disk bytes must already be visible and a cold reopen must agree.
7. **Close races:** concurrently deliver one disk event and one already accepted Lix write, then close in both orders. The owner may report a deterministic error for work not accepted before close, but cannot lose acknowledged Lix work, deadlock, detach the worker, or apply post-close self-loop writes.

## Source checks for shutdown ordering

The immutable candidate must show an explicit close protocol, not rely on `Drop` channel timing:

1. mark closing / reject new external requests;
2. unregister or stop watcher callbacks;
3. enqueue a barrier behind already accepted Lix-to-disk events and await its result;
4. drain earlier disk events under the same serialized owner (or deterministically reject only events not yet accepted);
5. close the engine/session;
6. send terminal shutdown and join the worker exactly once.

A drain loop that observes `Shutdown` and returns before processing already collected `SyncFromLix` replies is a blocker. Ignoring worker, materialization, watcher-stop, or join errors is a blocker when it can hide loss of acknowledged work.

## Immutable-candidate review recipe

1. Verify remote ref/head/tree/parents, clean detached worktree, full-index diff SHA-256, stable patch ID, changed paths and blobs.
2. Run `local_filesystem_hardcut_residue.sh <candidate> candidate`; manually classify only legitimate private synchronization names.
3. Diff exports/declarations/native signatures and run both negative compile probes plus positive positional consumers.
4. Audit the single watcher/worker and close state machine from source.
5. Run focused LocalFilesystem Rust tests and JS SDK tests for the runtime matrix above, warnings-denied relevant Clippy, JS type/tests, fmt, and diff-check.
6. Confirm the full diff contains no SQLite, ForkTree, SQL/sql2, format, or migration change.
7. Return immutable APPROVE or exact BLOCKER. No candidate mutation or merge.
