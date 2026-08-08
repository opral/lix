# ForkTree merge-analysis deletion plan — exact 479 frontier

Status: `TEST/REPORT-ONLY`. This package makes no production change and makes
no runtime or performance claim. It is the compiler/deletion contract for the
next R5 slice, not an implementation.

## Immutable provenance

| item | identity |
|---|---|
| approved merge-analysis oracle | `103e7fe29c60bcd675cee57f8a69986c133366a3` |
| oracle tree / parent | `439c3e07f27c051501000aab3360ae860a843c52` / `ac8a7bb1823954939662ad4a5255df9a4db2417f` |
| oracle full-index diff / patch | `6520aefc0b8f298fed0be21fd4060a0e9436f45bcc17787fe8b040fae814a5b2` / `c5af57b1b3d34b43ccb173e6927eb70347eb346e` |
| reviewed 479 frontier | `47957d30ae7c16c89c3c523feea23e2f98461fed` |
| 479 tree / parent | `b2e0c8a355fcee64d24cd5fcf77d2351d6fe4170` / `39b12568f86d02ec81327cb672b7ef5f7e936448` |
| b59 base | `b59e1f11a51153e0a787a81f0f25bf104d150aaf` |
| b59..479 full-index diff / patch | `90385cc0d009a1c858e79769288183dec2d5e1e29fd036df709d6695a83e7438` / `d40a2dda07bc83d1a5478636652a0b8d65177df3` |

The 479 frontier is compiler-red and is used only as an immutable source
frontier. Its historical/checkpoint work is explicitly out of scope. The
approved 103e7 model/verifier is the semantic acceptance authority; this
package does not copy or alter it.

## Current ownership and call graph

The current merge route is:

```text
merge_branch_preview / merge_branch
  -> branch_ref_reader_on_opening_read()
  -> commit_graph_reader_on_opening_read().merge_base(target, source)
  -> transaction.forktree_read_facade()                 [opening read]
  -> transaction.with_opening_tracked_reader(...)
       -> session/merge/analysis::analyze(&mut TrackedStateStoreReader, commits)
            -> diff_commits(base, source)
            -> diff_commits(base, target)
            -> exclude checkpoint/undo marker identities
            -> merge_payload_fallback_ids()
            -> load_change_payloads(fallback ids)
            -> tracked_state::plan_merge()
  -> ForkTree facade plugin/file owner reads
  -> plugin resolver / transaction publication
```

Exact sites at 479:

| path and lines | responsibility | future disposition |
|---|---|---|
| `session/merge/analysis.rs:12-110` | session result types, marker filtering, two legacy diffs, payload fallback and plan invocation | preserve the public/session result shape where needed; move the read/plan operation to ForkTree and remove its legacy reader parameter/fallback |
| `session/merge/branch.rs:166-190` | preview callback invocation | call one ForkTree same-view operation |
| `session/merge/branch.rs:285-317` | committed merge callback invocation | call the same operation; no second route |
| `transaction/context.rs:7304-7330` | `tracked_state_reader()` and `with_opening_tracked_reader()` | keep the former for unrelated cohorts; delete the latter after the two merge callers disappear |
| `tracked_state/merge.rs:13-508` | merge plan/picks/conflicts, sorted planner, payload fallback IDs | move the semantic planner/types under the ForkTree merge owner, then delete this merge-owned module/exports/tests |
| `tracked_state/mod.rs:21-22` | merge reexports | delete only merge reexports after the move |
| `tracked_state/context.rs:3743-3759` | test-only reader-backed `plan_merge` helper | move its tests/model to ForkTree and delete the helper with the old planner |
| `tracked_state/diff.rs:349-464` | generic diff and shared payload batch | retain for checkpoint, working-diff, SQL and other cohorts |
| `session/merge/stats.rs`, `conflicts.rs` | session stats and borrowed conflict views | preserve behavior; retarget imports to the ForkTree-owned plan |

`TrackedStateContext::reader` and `tracked_state_reader()` are not merge-only:
they still serve checkpoint, undo/redo, stale-transaction validation, SQL,
initialization, benchmarks, and tests. The only merge callback factory is
`with_opening_tracked_reader`; `rg` finds its two callers in `branch.rs` and
its definition in `transaction/context.rs`.

## Smallest future production slice

Add one owner operation, conceptually:

```text
ForkTreeReadFacade::analyze_merge(base, target, source)
    -> authenticated merge analysis / plan
```

The name is left to R5; the ownership contract is fixed:

1. The caller supplies one `ForkTreeReadFacade`/`CoherentView`. The operation
   never calls `begin_read`, opens a `TrackedStateStoreReader`, refreshes a
   selector, or creates a merge cache. Branch refs, commit topology, all three
   state roots, plugin registry rows, file-owner rows, and payload metadata use
   that same read.
2. `CommitTopologyReader`/`load_commit_topologies` is the sole chronology input.
   Validate base/source/target IDs, generations, parent order, and reachability
   before state scans. Recovery/floor rows, GC queues, and current-layout
   readers are not merge chronology inputs.
3. Load authenticated `CommitCatalog` and commit objects, then bound roots via
   `scan_state_rows_at_commit` or an equivalent ForkTree tree-diff primitive.
   Missing/malformed/wrong-kind/substituted Root, Member, CommitCatalog,
   Commit, Change/Payload, FileOwner, or PluginRegistry objects fail closed.
   A valid root with no selected key is authenticated absence.
4. Preserve public semantics: Added, Updated/Modified, Deleted/Removed,
   explicit NULL and tombstone, unchanged/convergent no-conflict, and
   divergent same-identity conflict with stable identity/side ordering.
   Checkpoint and undo marker rows remain excluded by this one operation.
5. Keep the existing canonical identity-sorted two-pointer planner. Semantic
   equality uses authenticated payload/digest and row metadata from the
   ForkTree-owned member/change records. Delete
   `merge_payload_fallback_ids`, `sorted_merge_payload_fallback_ids`, and
   `load_change_payloads` fallback; do not replace them with a map, cache,
   compatibility reader, or raw CAS authority.
6. File/plugin semantics remain their owners. ForkTree supplies historical
   descriptors, directory ancestry, plugin file-owner rows, registry
   generations, `BlobId`/digest references, and canonical row snapshots.
   Binary CAS remains the sole BlobId byte owner; plugin resolution remains the
   transaction/plugin actor writer. Missing/tombstoned owners, divergent file
   incarnations, registry generations, paths, or schema sets remain conflicts
   or fail-closed errors as current `branch.rs` checks require.

The slice is merge analysis read/plan only. Keep
`derived_plugin_blob_conflicts`, `plugin_merge_conflict_groups`, materialized
plugin rows, resolver, and publication in `branch.rs`; their historical inputs
must come from the same ForkTree view.

## Ordered compiler/deletion wave

1. Add the ForkTree operation and pure/model tests using
   `CoherentView`, `ForkTreeReadFacade`, `CommitTopologyReader`, commit/change/
   member loaders, and authenticated state rows. Add no persisted index,
   selector, cache, format, or fallback.
2. Switch both `branch.rs` entry points and prove preview/commit receive the
   same analysis under one view.
3. Delete `Transaction::with_opening_tracked_reader` only after the compiler
   reports zero merge callers. Keep `Transaction::tracked_state_reader` for
   checkpoint/undo/stale/SQL cohorts.
4. Move plan/pick/conflict types and sorted planner tests to ForkTree; move the
   `TrackedStateContext::plan_merge` tests/helper. Then delete
   `tracked_state/merge.rs`, its reexports, fallback helpers, and the old
   `session/merge/analysis` reader implementation.
5. Run residue scans before any broad test. No merge-owned
   `TrackedStateStoreReader`, `tracked_state.reader`, renamed merge
   reader/factory, callback, payload fallback, merge cache, compatibility or
   retry path, or alternate reader may remain. The unrelated allowlist is only
   checkpoint, undo/redo, GC, initialization, SQL file-history/working-diff,
   retained tracked-state service, and unrelated transaction cohorts. Any hit
   in `session/merge/**` is a blocker.

Do not delete `checkpoint.rs`, `session/checkpoint.rs`, `session/undo_redo.rs`,
SQL history/checkpoint providers, GC, selector/branch control, or the shared
tracked-state diff service. Those are separate R5 cohorts.

## Complexity and elimination ceiling

Let `A` be topology ancestry, `U_b/U_t/U_s` authenticated state rows,
`D_t/D_s` emitted diffs, `F` cross-change payload comparisons, and `O` output.

| route | time | memory | authority cost |
|---|---|---|---|
| current | `O(A + diff(base,target) + diff(base,source) + F + D_t + D_s)`; defensive unsorted input adds `O((D_t+D_s) log(D_t+D_s))` | legacy reader replay/cache plus diff/payload batches, `O(U + D + F)` | merge callback/factory and possible fallback payload reads coexist with ForkTree reads |
| proposed | `O(A + U_b + U_t + U_s + D_t + D_s + O)` for full authenticated scans, or the same bound with `U` replaced by the authenticated tree-diff frontier | one retained view plus `O(D_t+D_s+O)` output; topology cache is read-local only | one ForkTree owner, no legacy reader or fallback payload authority |

The perfect-elimination ceiling is the removed second merge reader/factory,
fallback payload reads, and duplicate decode/materialization. No percentage
performance claim is made. A later performance claim must report wall/CPU/
alloc/RSS/backend reads/bytes/writes/disk and reject any critical regression
above 5% on either adapter.

## Acceptance matrix

The approved 103e7 oracle remains the source/model gate. The production
successor must add executable tests for:

- merge-base, base/source/target identities, generations, parent order,
  disjoint success; source-only pick and target-only no-op;
- Added/Updated/Deleted/Unchanged, NULL, tombstone, convergent equal semantic
  values with distinct payload IDs, and divergent same-identity conflict;
- file descriptor/directory ancestry across all three roots;
- plugin file-owner incarnation, registry key/schema/generation, derived
  BlobId rows, plugin resolution and materialization;
- missing/malformed/wrong-kind/substituted Root, Member, CommitCatalog, Commit,
  Change/Payload, FileOwner and PluginRegistry; valid absent key remains absent;
- one retained read/view identity across preview, merge, owner reads, cold
  reopen, stale publication, and no partial publication on error;
- existing public tests: all `merge_branch_*` cases in
  `packages/lix/tests/integration/branching.rs`, `semantic_merge.rs`,
  `merge_fuzz.rs`, and SQL file/directory history merge callers. Preserve the
  unit tests in `session/merge/branch.rs`, `stats.rs`, `conflicts.rs`, and the
  planner tests moved from `tracked_state/merge.rs`.

## Future commands (not run here)

Memory/source controls:

```sh
CARGO_BUILD_JOBS=1 cargo test -p lix --test semantic_merge -- --nocapture --test-threads=1
CARGO_BUILD_JOBS=1 cargo test -p lix --test integration --features all-simulations -- merge_branch --nocapture --test-threads=1
CARGO_BUILD_JOBS=1 cargo test -p lix --test integration --features all-simulations -- merge_fuzz --nocapture --test-threads=1
```

The durable commands require a focused successor test target; no such merge
adapter target exists on the reviewed frontier, so this is a required landing
artifact, not a claimed existing command:

```sh
CARGO_TARGET_DIR="$PWD/target-merge-analysis" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_merge_analysis_dual_adapter \
  --features storage-benches,slatedb -- rocksdb --exact --nocapture --test-threads=1
CARGO_TARGET_DIR="$PWD/target-merge-analysis" CARGO_BUILD_JOBS=1 \
  cargo test -p lix_benchmarks --test forktree_merge_analysis_dual_adapter \
  --features storage-benches,slatedb -- slatedb --exact --nocapture --test-threads=1
```

Each durable cell uses the same deterministic fixture: branch create/switch,
base/source/target topology, disjoint merge, same-identity conflict,
NULL/tombstone, file/plugin owner agreement, corruption matrix, cold
flush/drop/reopen, and no-partial publication. It emits result digests and,
when instrumented, latency/CPU/alloc/RSS and adapter reads/bytes/writes/disk.

## Freeze rule

This package is GREEN only as a source/deletion plan bound to exact 479 and
approved 103e7. It is not production approval. R5 may implement one successor
from this contract; that successor must pass the source residue verifier and
then obtain independent Memory, RocksDB, and SlateDB correctness approval.
