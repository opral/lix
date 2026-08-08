# TrackedHead hard-cut plan: SQL, generation, working-diff, init, and GC

Status: read-only implementation contract. This package contains no production
source and makes no runtime or benchmark claim. It is anchored to exact b59
and the corrected TrackedHead whole-module oracle v2.

## Immutable anchors

| object | value |
|---|---|
| source frontier | b59e1f11a51153e0a787a81f0f25bf104d150aaf |
| source tree | 700fd04d21bc40c05425c9fc9e10d65c9e1eda24 |
| oracle v2 | 1d9c47728377c6ec7d2646704d51f3aadb11c773 |
| oracle v2 tree | df2a373a1c0e7917f4abbd167c7659efd1c3e6a1 |
| v2 parent to head full-index diff | ac1a0e19661af961a3b9688028a13776b39e025fd85afb591ffcaf94ae26afc3 |
| v2 b59 to head full-index diff | 998d81e31ee686f68a1c214167bf9603ce80a44e591ad23c2d03d45d54696fd8 |
| v2 model binary | 5d9c6a9e5d20de07a55465ba8e267a9ec708185f46e6a4e96b7879662b6a3abf |
| v2 model log | 176e4c840641415c4354591c3fd8d20169c0a8b4cb4131f6b3e6d933ac61925f |
| b59 source-gate normalized RED | f8e3c11af5fa5fe3c35973a727ad31bbfed9e27b4908b23d907ebbdc71d12867 |

The b59 source is the inventory target. The oracle v2 is an acceptance
contract and calibration source, not a production dependency.

## Current oracle limitation

The v2 model is valid for one-read/zero-write, no-fallback, and the seven
existing semantic cases, but its corruption fixtures mutate only state_root.
They do not independently prove malformed, missing, wrong-kind, or
identity-substituted GlobalSelector, BranchSelector, catalog-root, or
checkpoint-root handling. This plan therefore does not call v2 runtime-green.
The next test/report-only v3 must add one stateful fixture per selector/root
domain and require exactly one retained read followed by zero plan, writes,
commits, or selector rotations. Production implementation must not proceed
from this omission by weakening assertions or adding a fallback.

## Sole owner and invariants

The first runnable successor has one caller-owned retained StorageRead and one
authenticated CoherentView. The view binds the raw global selector, requested
branch selector, epoch, state roots, catalog roots, checkpoint roots, and a
non-persisted view_id. Every point, range, history, generation, working-diff,
schema, and GC observation in one operation uses that view.

Public SQL, filesystem, plugin, branch, history, and transaction facades remain
API surfaces. They do not retain a tracked reader factory, durable cache,
selector copy, fallback scan, compatibility reader, or second state authority.
live_state/forktree_reader.rs is promoted only as the implementation seam
behind the view; it is not a second reader authority. Columnar row groups are
rebuildable derived materialization only. EntityPk, NULL, tombstone, ordered
projection, and branch/global precedence remain public semantics.

Publication is exactly one PreparedPublication lowered by into_storage_plan,
checked by one prepare_write_set, and committed once. The same adapter
transaction CASes the opened raw selectors and global epoch. No-op and
unsupported cohorts are classified before opening a plan. They return without
writes, commits, or selector/epoch rotation. Missing, malformed, wrong-domain,
wrong-kind, identity-substituted, stale, or cross-view state fails closed
before durable work. A rejected same-owner CAS does not retry via a different
reader or reconstruct state.

## Exhaustive source classification

The authoritative per-file inventory is SOURCE_INVENTORY.tsv. The compact
classification below is the implementation boundary.

### Migrate in the first reader-first wave

- live_state/context.rs, live_state/mod.rs, and live_state/types.rs: remove
  TrackedHeadContext, HotStateTransactionCache, and tracked reader
  construction. Keep semantic reader/visibility traits and bind them to one
  CoherentView.
- transaction/context.rs: replace opening tracked-reader callbacks, generation
  lookup, current-state reconciliation, and working-diff reads with the
  caller-owned retained view and transaction-local staged rows. Keep savepoint,
  rollback, idempotency, stale publication, and no-op semantics.
- transaction/schema_resolver.rs: resolve schema from the selected state root
  plus staged overlay. Delete tracked-head-specific fixtures.
- session/merge/branch.rs and session/merge/analysis.rs: pass one selected
  view/root into diff and merge analysis. No opening tracked reader callback.
- init.rs, functions/context.rs, and functions/state.rs: bootstrap and expose
  generation/selector state from ForkTree. Do not recreate a marker epoch or
  delta index.
- sql2/providers/working_diff.rs and
  sql2/providers/filesystem_working_diff.rs: use selected-root versus
  transaction-local diff. The public working-diff result remains.
- sql2/providers/checkpoint.rs, diff.rs, and the branch/directory/file facade
  callers: move only the selected-root operation in the reader wave; public
  semantics remain.
- gc.rs: derive roots from typed selectors and authenticated owner edges, then
  consume bounded owner-produced sweep plans. Generation and working-diff
  control observation use the same retained view and epoch.
- test_support.rs and transaction/bench_support.rs: rewrite fixtures to typed
  ForkTree setup or delete them. They cannot keep a hidden old owner.

### Retain, but only as semantic or derived surfaces

- live_state/reader.rs and live_state/visibility.rs: retain the facade and
  local-over-global, tombstone-over-global, staged precedence, NULL, and
  ordered-range semantics; remove legacy fallback implementations.
- live_state/forktree_reader.rs: retain and promote as the view-bound exact and
  range implementation.
- entity_pk.rs, sql2/entity_projection.rs, public SQL/file/history/branch
  types, plugin, and untracked-only facades: retain public contracts.
- live_state/entity_columnar.rs, sql2/entity_columnar_layout.rs,
  transaction/types.rs, and columnar writer helpers: retain only if proven
  rebuildable outputs. They must never answer a tracked read when ForkTree
  lacks the source row.
- storage_bench.rs: retain measurement-only code after replacing or deleting
  old marker-space probes.

### Retain-blockers for the smallest R5 production successor

These are not compatibility allowances. They are explicit prerequisites that
must be delivered by a reviewed successor before the first runnable R5 scope
can claim the corresponding semantics:

- direct SQL entity/primary-key and columnar paths:
  sql2/providers/entity.rs, sql2/entity_batch.rs,
  live_state/forktree_reader.rs integration, live_state/entity_columnar.rs,
  and sql2/entity_columnar_layout.rs;
- branch/directory/file direct SQL providers: sql2/providers/branch.rs,
  directory.rs, and file.rs, unless their call sites move in the same reader
  wave;
- historical providers: sql2/providers/file_history.rs,
  directory_history.rs, entity_history.rs, and history_route.rs;
- session/merge/analysis.rs if the historical-provider successor has not
  supplied its authenticated parent/member/root contract.

The first R5 successor must either move a blocker completely or fail closed
before planning. It may not leave a legacy read behind a wrapper.

### Delete or physically rewrite before the first accepted compile

- tracked_state/context.rs, tracked_state/diff.rs, tracked_state/mod.rs, and
  reader-only tracked-state modules/reexports once all callers move; no
  TrackedStateStoreReader factory may remain.
- live_state/tracked_head.rs and live_state/tracked_head/hot.rs are already
  absent at b59 and must stay absent.
- TrackedHeadContext, HotStateTransactionCache, TrackedWorkingDiff,
  TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage, CurrentStateDeltaRef,
  TrackedHeadDeltaRef, TRACKED_WORKING_DIFF_MARKER_SPACE, and their stage and
  generation helpers.
- with_opening_tracked_reader, load_exact_batch_via_scan_for_test, and any
  test-only callback that opens the superseded reader.
- old branch-control/current-generation marker spaces and old working-diff
  codecs. Their names cannot survive as a compatibility registry.
- old physical scan/reader wrappers only where the cursor/deletion contract has
  separately approved them; this plan does not relax the strict cursor gate.

## Dependency-ordered non-runnable wave

No intermediate edit in this sequence is runnable or publishable.

1. Fence the boundary. Record the exact b59 ancestor and introduce no
   ForkTree adapter. Mark the four direct SQL/columnar paths and historical
   providers as blockers, not fallback consumers.
2. Move readers first. Add the view-bound read shape to current semantic
   facades, schema resolution, transaction opening, reconciliation,
   savepoint/rollback, merge analysis, working-diff consumers, and GC
   observation. Every path receives the same retained read; no helper calls
   begin_read again.
3. Move generation and publication. Classify intent before planning; lower
   supported current/state/history intent to one PreparedPublication.
   Unsupported ref-only, selected-history, file, or multi-branch cohorts fail
   closed with zero plan/write/epoch work until their named successor exists.
4. Move initialization and schema. Bootstrap only typed global/branch
   selectors, root/catalog/checkpoint objects, and one epoch. Delete current
   delta/marker setup. Schema resolver reads selected state plus staged overlay.
5. Move merge/checkpoint/GC consumers. Make branch/diff/merge, checkpoint,
   recovery, undo/redo, and root discovery consume authenticated roots. GC
   observes the same selector/epoch fence and never decodes a legacy table.
6. Move the explicit blockers. The history-provider successor supplies
   parent/member/root validation. The direct SQL successor supplies typed PK,
   projection/order/LIMIT, NULL/tombstone, and columnar-derived semantics.
   These are separate acceptance gates, not bridges.
7. Rewrite fixtures/support. Migrate tests, engine benchmarks, CLI/file
   support, and native/adapter call sites. Delete old test-only setup that
   would make an obsolete owner appear live.
8. Delete the plane. Delete tracked-state reader modules, old spaces, marker
   codecs, branch-control/current-generation owners, and wrappers only after
   the last consumer moved. Run source residue and negative compile probes
   before any accepted compile.
9. First runnable gate. Run the ordered commands below on the immutable
   post-wave head. A failure returns to the source wave; no compatibility
   patch is permitted.

The important cycle is reader-first/writer-last: deleting the old reader before
moving consumers produces compiler errors, but deleting its writer before
moving readers creates silent data loss. The writer and marker-space deletion
must be the final source edits in their family, followed immediately by
physical deletion before the first compile.

## Smallest R5 production successor

R5 should be one immutable, non-runnable implementation slice containing only:

- live_state/context.rs, live_state/mod.rs, live_state/types.rs,
  live_state/reader.rs, live_state/visibility.rs;
- transaction/context.rs, transaction/schema_resolver.rs, and the
  transaction read/staging seam;
- session/merge/branch.rs plus the already-reviewed history-provider
  prerequisite;
- sql2/providers/working_diff.rs,
  sql2/providers/filesystem_working_diff.rs, checkpoint.rs, and diff.rs;
- init.rs, functions/context.rs, functions/state.rs, and gc.rs;
- required support and fixture rewrites.

It must not touch the four direct SQL/columnar paths or claim their runtime
semantics until their independent oracle is bound. It must not add a bridge,
cache, second storage reader, fallback scan, compatibility reader, or direct
publication path. Each supported family exits through the existing one-plan,
one-prepare, one-commit path; deferred families return a typed unsupported
error before durable work.

## Static and negative gates

The future verifier is verify_trackedhead_sql_deletion_plan.sh. It must:

1. prove the candidate contains the exact b59 ancestor and the v2 oracle
   contract is present in acceptance metadata;
2. reject tracked_state/context.rs, tracked_state/diff.rs, old marker spaces,
   TrackedHeadContext, HotStateTransactionCache, TrackedWorkingDiff,
   TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage, CurrentStateDeltaRef,
   TrackedHeadDeltaRef, with_opening_tracked_reader, and all old stage helpers
   in production, tests, benchmarks, and support;
3. require CoherentView, open_coherent_view, view_id, state_point,
   state_range, PreparedPublication, into_storage_plan, prepare_write_set,
   checkpoint_root, and generation in the retained implementation closure;
4. reject candidate diffs that modify the direct SQL/columnar blocker paths
   unless their independent SQL oracle is named in the manifest;
5. compile a direct obsolete-consumer probe and require it to fail because
   old reader/space APIs are absent; compile a typed ForkTree consumer probe
   that uses only the public/facade seam;
6. inspect source declarations item by item rather than truncating at the
   first cfg(test); test-only imports cannot conceal production residue;
7. model malformed, missing, wrong-kind, identity-substituted, stale, and
   cross-view reads as zero plan/write/commit/rotation outcomes;
8. require no raw put/delete or forgeable sweep token, no persisted cache, and
   no compatibility or fallback call path.

The expected b59 baseline is RED; it must retain normalized finding set
f8e3c11af5fa5fe3c35973a727ad31bbfed9e27b4908b23d907ebbdc71d12867.
The verifier is a deletion gate, not evidence that b59 is runnable.

## Future acceptance order

Every cell is separately capped at 20 minutes and uses a fresh isolated
CARGO_TARGET_DIR under /root/repos:

1. source residue, declaration extraction, negative API probes, fmt, and
   git diff --check;
2. warnings-denied focused Clippy and package no-run;
3. standalone model/oracle;
4. Memory exact semantic lifecycle and cold reopen;
5. RocksDB exact semantic lifecycle, flush/drop/reopen, corruption and GC;
6. SlateDB exact semantic lifecycle, flush/drop/reopen, corruption and GC.

Example dormant commands (the candidate supplies the test names):

    CARGO_TARGET_DIR=/root/repos/target-forktree-r5-memory \
      timeout 1200 cargo test -p lix_tests \
      --test forktree_trackedhead_sql_deletion memory \
      -- --exact --nocapture --test-threads=1
    CARGO_TARGET_DIR=/root/repos/target-forktree-r5-rocks \
      timeout 1200 cargo test -p lix_tests \
      --test forktree_trackedhead_sql_deletion rocksdb \
      -- --exact --nocapture --test-threads=1
    CARGO_TARGET_DIR=/root/repos/target-forktree-r5-slate \
      timeout 1200 cargo test -p lix_tests \
      --test forktree_trackedhead_sql_deletion slatedb \
      -- --exact --nocapture --test-threads=1

The real candidate must publish exact invocations and fresh-path rules before
execution. No current-main performance, broad scaling, or comparator gate is
part of this plan.

## Stop conditions

Return to architecture review if any implementation discovery requires a
second tracked root, a legacy reader, an alternate publication path, a
persisted derived index as authority, branch-wide copying, unbounded payload
materialization, a raw mutation token, or a fallback for malformed/missing
objects. A source state that cannot reach one compiler boundary is abandoned;
the bridge is not restored.
