# TrackedHead hard-cut plan: SQL, generation, working-diff, init, and GC

Status: read-only implementation contract. This package contains no production
source and makes no runtime or benchmark claim. It is anchored to exact b59
and the approved six-domain TrackedHead whole-module oracle v3.

## Immutable anchors

| object | value |
|---|---|
| source frontier | b59e1f11a51153e0a787a81f0f25bf104d150aaf |
| source tree | 700fd04d21bc40c05425c9fc9e10d65c9e1eda24 |
| approved corruption oracle v3 | 33aa59975808099dfb5e9ca675a1633d713dccf3 |
| v3 tree | 1ced701e3351af59c48dce75731947dcd1606f3e |
| v3 parent | 1d9c47728377c6ec7d2646704d51f3aadb11c773 |
| v3 parent-to-head full-index diff | 31b9374a14846f5e082d193296f6eb33255e667d5775c041a876077fc7952194 |
| v3 stable patch ID | d4d96f33fa535171d20e32e1b859ee1b58000cb7 |
| v3 package SHA256SUMS | f54422520ea2ac7c47427d0e57f95ea6392b990e6e1861a31d6ae7848f509556 |
| b59 source-gate normalized RED | f8e3c11af5fa5fe3c35973a727ad31bbfed9e27b4908b23d907ebbdc71d12867 |

The b59 source is the inventory target. The v3 oracle is an acceptance
contract and calibration source, not a production dependency.

This successor incorporates the R4 correction. The previous plan omitted
checkpoint TrackedStateStoreReader, session execute branch-control reads,
branch-ref stage writers, commit-graph manifest ownership, and the adapter
mutation-revision spaces. Those omissions are now explicit deletion gates.

## Approved corruption oracle

The v3 oracle is the required six-domain discriminator. It covers
StateRoot, GlobalSelector, BranchSelector, CommitCatalog, ChangeCatalog, and
CheckpointRoot with malformed, missing, wrong-kind, and identity-substituted
stateful fixtures. Every case requires exactly one retained view/read followed
by zero plan, writes, commits, or selector rotations. The oracle is independent
test/report evidence, not a production dependency; no implementation may
weaken these cases or add a fallback.

The exact approved case binding is: **24 cases = 6 domains × 4 corruption modes**.
The six domains are the names above, and the four modes are malformed, missing,
wrong-kind, and identity-substituted. This equation is part of the acceptance
contract, not explanatory arithmetic; the source verifier must require this
literal assertion in this plan as well as the manifest metadata.

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
- BranchHeadControlContext, BranchHeadControlCache, stage_branch_head_control,
  BRANCH_HEAD_CONTROL_SPACE, and their module/reexports.
- MUTATION_REVISION_SPACE, TRACKED_MUTATION_REVISION_SPACE,
  load_mutation_revision, load_tracked_mutation_revision, and adapter mutation
  revision stage/precondition helpers.
- TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE, commit-state manifest loaders,
  and root-rebuild orchestration once CommitCatalog and selected state roots
  serve the same chronology.
- old physical scan/reader wrappers only where the cursor/deletion contract has
  separately approved them; this plan does not relax the strict cursor gate.

## R4 correction: compiler-actionable consumers

COMPILER_WAVE.tsv is the required path-by-path action ledger. A listed
consumer has only two legal outcomes in the first runnable wave:

1. its concrete call site is migrated to the named ForkTree owner, with the
   compiler proving the old signature and writer are gone; or
2. the route is deleted or returns a typed unsupported/fail-closed result
   before opening a plan, reading a legacy root, or rotating an epoch.

An empty result is not fail-closed. A compatibility wrapper, adapter that
reconstructs the old state, cache that hides an old read, or a second writer
is a residue failure.

The required order is:

1. **W4 control fence first:** move BranchHeadControlContext/Cache readers and
   branch/refs stage_branch_head_control to GlobalSelectorV1/BranchSelectorV1
   plus the single epoch/CAS write. Do not delete the selector owner until
   functions, live_state, transaction, GC, session/execute, and branch/refs
   callers have compiler errors resolved.
2. **W1-W3 reader wave:** move checkpoint.rs, session/execute.rs,
   commit_graph/context.rs, engine.rs, SQL working-diff, and history readers.
   Every direct SQL/history route must be concretely migrated or deleted with
   typed fail-closed behavior; no route may be left as a vague retain blocker.
3. **Mutation-revision replacement:** move observe/session/transaction
   preconditions to the global epoch and delete both mutation-revision
   spaces, loaders, stage functions, and write-set appenders.
4. **Writer-last deletion:** delete branch-control, manifest, tracked-state,
   marker, cache, and old mutation-revision owners only after their readers
   and writers have moved. Physically remove spaces before the first accepted
   compile.
5. **Static compiler gate:** run the no-argument verifier, negative consumers,
   declaration extraction, fmt/diff, and warnings-denied checks. A candidate
   that passes only by supplying alternate anchors is invalid.

Direct SQL and history are therefore compiler-actionable in this wave:
entity/entity-batch/columnar routes require a concrete selected-root
migration or a typed deletion; file/directory/entity history and history_route
require authenticated CommitCatalog/ChangeCatalog parent/member validation or
typed deletion. They cannot remain in a retain-blocker category without an
action and fail condition.

## Dependency-ordered non-runnable wave

No intermediate edit in this sequence is runnable or publishable.

1. Fence the boundary. Record the exact b59 ancestor and introduce no
   ForkTree adapter. The verifier pins b59 and the exact v3 oracle identity
   internally; callers cannot override either anchor. Mark direct SQL/columnar and historical
   providers as concrete migrate-or-delete actions.
2. Move W4 readers first. Replace branch-control/cache reads and stage writers
   with the selector pair and global epoch CAS.
3. Move remaining readers. Add the view-bound read shape to current semantic
   facades, schema resolution, transaction opening, reconciliation,
   savepoint/rollback, merge analysis, working-diff consumers, and GC
   observation. Every path receives the same retained read; no helper calls
   begin_read again.
4. Move generation and publication. Classify intent before planning; lower
   supported current/state/history intent to one PreparedPublication.
   Unsupported ref-only, selected-history, file, or multi-branch cohorts fail
   closed with zero plan/write/epoch work until their named successor exists.
5. Move initialization and schema. Bootstrap only typed global/branch
   selectors, root/catalog/checkpoint objects, and one epoch. Delete current
   delta/marker setup. Schema resolver reads selected state plus staged overlay.
6. Move merge/checkpoint/GC consumers. Make branch/diff/merge, checkpoint,
   recovery, undo/redo, and root discovery consume authenticated roots. GC
   observes the same selector/epoch fence and never decodes a legacy table.
7. Resolve direct SQL/history actions. Each row in COMPILER_WAVE.tsv must be
   migrated or deleted/fail-closed; leaving an unowned reader is forbidden.
8. Rewrite fixtures/support. Migrate tests, engine benchmarks, CLI/file
   support, and native/adapter call sites. Delete old test-only setup that
   would make an obsolete owner appear live.
9. Delete the plane. Delete tracked-state reader modules, old spaces, marker
   codecs, branch-control/current-generation owners, and wrappers only after
   the last consumer moved. Run source residue and negative compile probes
   before any accepted compile.
10. First runnable gate. Run the ordered commands below on the immutable
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

The future verifier is verify_trackedhead_sql_deletion_plan.sh. It takes no
arguments and always pins the current worktree to the following constants:
ANCHOR=b59e1f11a51153e0a787a81f0f25bf104d150aaf and
ORACLE=33aa59975808099dfb5e9ca675a1633d713dccf3. It must:

1. prove the exact v3 commit/tree/parent/diff/patch/package identities are
   present in acceptance metadata and that all six corruption domains are
   named;
2. reject tracked_state/context.rs, tracked_state/diff.rs, old marker spaces,
   TrackedHeadContext, HotStateTransactionCache, TrackedWorkingDiff,
   TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage, CurrentStateDeltaRef,
   TrackedHeadDeltaRef, with_opening_tracked_reader, and all old stage helpers
   in production, tests, benchmarks, and support;
3. also reject BranchHeadControlContext, BranchHeadControlCache,
   stage_branch_head_control, BRANCH_HEAD_CONTROL_SPACE,
   MUTATION_REVISION_SPACE, TRACKED_MUTATION_REVISION_SPACE,
   load_mutation_revision, load_tracked_mutation_revision,
   TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE, and legacy manifest loaders;
4. require CoherentView, open_coherent_view, view_id, state_point,
   state_range, PreparedPublication, into_storage_plan, prepare_write_set,
   checkpoint_root, and generation in the retained implementation closure;
5. reject candidate diffs that modify the direct SQL/columnar blocker paths
   unless their independent SQL oracle is named in the manifest;
6. compile a direct obsolete-consumer probe and require it to fail because
   old reader/space APIs are absent; compile a typed ForkTree consumer probe
   that uses only the public/facade seam;
7. inspect source declarations item by item rather than truncating at the
   first cfg(test); test-only imports cannot conceal production residue;
8. bind the v3 model's six-domain malformed, missing, wrong-kind,
   identity-substituted, stale, and cross-view reads to zero
   plan/write/commit/rotation outcomes;
9. require no raw put/delete or forgeable sweep token, no persisted cache, and
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
