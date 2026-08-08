# ForkTree W3 checkpoint/selector writer readiness map

Status: **read-only source map; no compiler, adapter, runtime, or benchmark
verdict**.

This is the b484 successor to the earlier fd2 W3 map. It is deliberately
separate from the accepted b484 nine-seam file-history oracle review. The b484
source correction is not modified here.

## Immutable source

| Item | Identity |
|---|---|
| Anchor/head | `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35` |
| Head tree | `4477c83b246bddac09cd972564bd4ccd67f90f7b` |
| Parent | `fd2be256d763f17e9f127d4c984e36fba191cb82` |
| Parent tree | `20110ca5e3c33d34217630fff0a2b784b545317a` |
| Parent..head full-index binary SHA-256 | `d36495fc406cc213bb5729babae761916f97bd515221de14c1f3ae114ec22610` |
| Parent..head stable patch ID | `e90c9dd93db7c343f67887218049406640a77631` |
| Changed production paths | `packages/lix/src/sql2/providers/file_history.rs`; `packages/lix/src/sql2/providers/filesystem_working_diff.rs` |
| Review mode | exact detached b484 worktree; source-only |

The b484 delta is restricted to the two historical/file working-diff
providers. Their current source blobs are respectively
`6b27ba82e31e28f3880d44d607ddbf8fa88e5ac6` and
`3552bde5ec3821a69693964ef7ad2b06b3c2cf1e`. It does not remove or replace the
legacy branch-control, tracked-head, mutation-revision, checkpoint recovery,
or GC owners mapped below.

## Existing sole ForkTree destination

These are existing owner seams, not new W3 authorities:

- `forktree/model.rs:620-704`: `GlobalSelectorV1` owns repository root and
  epoch; `BranchSelectorV1` owns branch identity, snapshot object, and
  selector generation.
- `forktree/view.rs:34-44,555-677`: `CoherentView` retains one read handle,
  raw selector pair, and `view_id`, then authenticates the selected roots.
- `forktree/publication.rs:66-108,321-398,1067-1080`:
  `PreparedPublication` exact-CASes raw global/branch selectors; snapshot pin
  publication and release-with-catalog-retirement are already typed owner
  operations.
- `forktree/serving.rs:1244-1336`: point/range serving accepts a caller-owned
  `CoherentView`; it must remain the only tracked state read route.
- `transaction/commit.rs:62-80,168-188`: `PreparedForkTreePlan` lowers once
  through `into_storage_plan`, with `Noop` producing no writes or preconditions.
- `forktree/reachability.rs:156-205,318-447`: GC reads the global/progress
  selector pair through one retained read and validates epoch, key, domain,
  and root edges before producing typed maintenance edits.

No `BranchHeadControl`, `TrackedHead`, revision row, checkpoint recovery row,
cache, compatibility reader, or adapter wrapper may be placed around these
seams.

## Source inventory calibration

These are raw source counts from exact b484, using the command definitions in
`REPORT.md`; they include tests and legitimate ForkTree owner names and are
not claimed compiler-error counts:

| Search | Matches |
|---|---:|
| `BranchHeadControl|TrackedHead|current.?generation` | 112 |
| `checkpoint|recovery|snapshot.?pin|undo|redo` | 1,563 |
| `snapshot.?pin` | 16 |
| `GlobalSelectorV1|BranchSelectorV1|global.?epoch|selector` | 772 |
| `stage_branch_head_control|branch_head_control_precondition|stage_mutation_revision|MUTATION_REVISION_SPACE|TRACKED_MUTATION_REVISION_SPACE` | 48 |

No numeric compiler diagnostics are claimed: no build was run. `W3-01` through
`W3-14` below are exact source diagnostic classes for the first non-runnable
compiler wave.

## W3 diagnostic clusters

| ID | Exact b484 paths and locations | Remaining duty / failure class | Sole owner and deletion action |
|---|---|---|---|
| W3-01 | `branch/mod.rs:1-13`; `branch/refs.rs:87,177-203` | Branch-control imports and test writer still construct `BranchHeadControl` and call `stage_branch_head_control`. | Resolve callers to `BranchSelectorV1` + `GlobalSelectorV1`; delete control exports, direct stage writer, and test-only legacy owner. |
| W3-02 | `live_state/context.rs:6,25-47,83,130,309-311,482-529`; `live_state/mod.rs:10` | `BranchHeadControlCache`, fresh control readers, and `TrackedHeadContext` remain in the live-state facade. | Pass one caller-owned `CoherentView`; delete cache fields, refresh-on-miss, and fresh reader constructors. |
| W3-03 | `functions/context.rs:125-145,346-405`; `functions/state.rs:62-160` | Deterministic/untracked lifecycle reads control revision/generation, stages `CurrentStateDeltaRef`, then rewrites control bytes. | Classify global-sequence intent before view; path-copy authenticated global state and rotate `GlobalSelectorV1.epoch` in the same publication. |
| W3-04 | `functions/state.rs:7,81-160`; tests `functions/state.rs:527-580` | Current-state sequence load/stage uses control observation, `TrackedHead` generation, and a control-byte precondition. | One `CoherentView` + `PreparedPublication`; exact raw selector/epoch CAS; same-owner stale fails closed, unrelated-owner publication may succeed; delete retry/fallback. |
| W3-05 | `init.rs:1-32,112-152,254-285,320-345,430-501,890-1048`; `engine.rs:23,33,117,158` | Bootstrap constructs `InitBranchHeadControl`, tracked-state context, delta/working-diff metadata, and repeatedly opens legacy reads. | Initialize authenticated empty `RepositoryRoot`, global selector, main branch selector/snapshot, catalogs, retention/checkpoint roots, and epoch; no old-format bootstrap fallback. |
| W3-06 | `branch/refs.rs:177-205`; `branch/{context,lifecycle,refs}.rs` callers | Create/switch/delete and ref materialization retain a direct branch-head/control staging boundary. | One atomic branch selector/snapshot/ref-change publication under global epoch; no standalone branch-control writer. |
| W3-07 | `transaction/context.rs:431,468,7300-7331,8038-8141` | Transaction stores a control cache, exposes fresh `tracked_state_reader`, `branch_ref_reader`, and callbacks that can create new reads. | Opening transaction retains one `CoherentView`; delete fresh `begin_read` helpers and `TrackedStateStoreReader` factories/callbacks. |
| W3-08 | `transaction/context.rs:758-775,1294-1370,1402-1438,8810-8864` | Open/reconciliation loads mutation revision and branch heads separately; generation/live-count helpers open fresh reads and use control contexts. | Bind open state to one `view_id`, global epoch, and branch selector bytes; preserve same-owner stale rejection and unrelated-owner success without a second snapshot. |
| W3-09 | `transaction/context.rs:1476-1672`; `transaction/commit.rs:62-80,168-188` | ForkTree plan already exists, but commit opens a second commit-time read, appends tracked mutation revision, then lowers/prepare/commits. | Classify complete intent first; use the caller-owned retained view and one `PreparedPublication -> into_storage_plan`, append transaction metadata/idempotency, then exactly one `prepare_write_set` and one commit. |
| W3-10 | `session/checkpoint.rs:2-195`; `transaction/context.rs:7269-7276,7705-7850`; `checkpoint.rs:5-27`; `session/execute.rs:8650-8658` | Checkpoint uses recovery-ref/GC-state rows, branch-ref reads, selected history, marker/floor state, and staged `CheckpointPublication`. | Consume the retained view, authenticated catalogs, branch/checkpoint selectors and root objects; missing/malformed/wrong-kind ancestry fails before any plan or rotation. |
| W3-11 | `gc.rs:15-16,34-117,355-377,779-801,1339-1352,1423-1463,1533-1630,2063,2528-2554` | GC still scans `BranchHeadControl`, `TrackedHead` serving dependencies, and `CHECKPOINT_RECOVERY_REF_SPACE`; it treats mutable controls/recovery rows as roots. | Selector/root universe and owner-produced typed sweep plans are sole root authority. Publication-first and GC-first stale work fails the exact global fence; root prerequisites precede sweep. |
| W3-12 | `storage_adapter/context.rs:103-205,250-270`; `storage_adapter/spaces.rs:9-20`; `observe_invalidation.rs:94-126` | `MUTATION_REVISION_SPACE` and `TRACKED_MUTATION_REVISION_SPACE` have load/stage/precondition/polling paths. | Replace observation with authenticated selector/epoch invalidation; delete both spaces and every adapter revision writer/precondition. |
| W3-13 | `tracked_state/context.rs:4070-4130,4925,5236-5327,8710`; `tracked_state/diff.rs`; `tracked_state/mod.rs`; `session/merge/analysis.rs` | Reader-only tracked-state factories/diff/reexports remain reachable from generation, merge, history, and checkpoint callbacks. | Move point/range/diff/merge to selected roots and one retained view; unsupported cohorts fail before planning; delete factories/modules/reexports after consumers move. |
| W3-14 | `test_support.rs:24-26,156-174`; `transaction/bench_support.rs:8,377-385,564-617`; `storage_bench.rs:2214` | Fixtures and benchmark helpers keep legacy controls, generations, and spaces callable. | Rewrite against typed selectors/object builders or delete. Support code cannot preserve a removed production symbol. |

## Checkpoint, selector, and recovery publication map

1. The accepted W4 owner already exposes `publish_current_snapshot_pin` and
   `release_snapshot_pin_with_catalog_retirement` in `forktree/publication.rs`.
   They derive the target from the retained view, bind raw global bytes, edit
   the authenticated repository catalog/root, and release the selector in one
   publication. W3 must call these APIs, not create a pin space or selector
   cache.
2. The current checkpoint writer begins in `session/checkpoint.rs:46-64` and
   calls `Transaction::checkpoint_publication_state` plus
   `branch_ref_reader_on_opening_read`; this is the correct read-handle shape
   but its output still contains legacy `CheckpointRecoveryRef` and
   `CheckpointGcState` rows. `transaction/context.rs:7269-7276` loads both from
   `opening_read`, while `:7705-7850` repeats the same publication shape for
   diff/checkpoint transitions.
3. The legacy recovery-ref authority is `gc.rs:44-48,355-380,1423-1463,
   1533-1630`. `stage_recovery_ref_rotation` writes
   `CHECKPOINT_RECOVERY_REF_SPACE`; `load_recovery_refs` feeds root discovery
   at `:2063` and `:2552-2554`. W3 must replace the durable edge with a typed
   branch/checkpoint selector or authenticated checkpoint root edge; W5 owns
   final bounded sweep/reclamation, not a second recovery table.
4. GC's accepted selector snapshot at `forktree/reachability.rs:156-205`
   loads the global and progress selector keys with one read and checks
   progress epoch/digest/root closure. Its selector scan rejects missing global
   selector, key-only values, key/identity mismatches, wrong domains, and
   progress/global epoch disagreement. These rejection semantics are retained.

## Concurrency and rejection contract

| Situation | Required result before/after W3 |
|---|---|
| Branch-first publication, stale GC | GC exact raw-global selector/epoch precondition fails; no sweep deletion. |
| GC-first publication, stale transaction/checkpoint | Publication exact global and branch selector CAS fails; no partial plan commit. Retry may restage from a new retained view only. |
| Same-owner stale writer | Fails closed before plan/write/epoch rotation. |
| Unrelated branch owner | May publish if its branch selector and global epoch are current; no `O(branches)` copy or blanket rejection. |
| Missing/malformed/wrong-kind/identity-substituted selector/root/catalog | Open/traversal fails closed; never bootstraps empty or falls back to a legacy reader. |
| No-op | Complete intent classification returns `Noop`: zero plan, writes, commit, selector move, and epoch rotation. |
| Unsupported ref-only/selected/journal/multi-branch cohort | Typed unsupported result before opening a publication plan; zero writes and zero epoch rotation. |
| Rollback/savepoint/idempotency | Transaction-owned staged state is discarded/replayed within the same transaction boundary; no durable legacy writer or independent receipt authority. |
| Reader pin | Old retained view remains valid until release; release and catalog retirement are one fenced publication. |

## Dependency-ordered hard-cut wave

Every intermediate state is intentionally non-runnable. There is no
compatibility bridge or compile checkpoint between these steps.

1. **Consume W4 fence.** Keep selector/object fields private; bind one raw
   global selector/epoch and branch selector expectation. Verify existing
   `CoherentView`, snapshot-pin, and `PreparedPublication` APIs. This is a
   prerequisite, not a new W3 owner.
2. **Bootstrap and branch roots.** Replace init and branch lifecycle with
   authenticated empty-root and branch-selector path copies. Create/switch/
   delete must update branch snapshot/ref-change edges and global epoch in one
   publication.
3. **Reader-first migration.** Move live-state, transaction open, generation,
   current-state sequence, schema, filesystem/file-history facade consumers,
   reconciliation, and working-diff readers to one caller-owned view. The b484
   `ForkTreeReadFacade` correction is retained; `TrackedStateFilter` may remain
   only as a typed query filter, never as a storage authority.
4. **Checkpoint and recovery roots.** Move checkpoint, undo/redo, recovery,
   and checkpoint-floor consumers to authenticated selectors/catalog/root
   edges. Remove `CHECKPOINT_RECOVERY_REF_SPACE` only after all readers and
   writers are moved.
5. **GC root observation.** Move root enumeration to the selector/root
   universe and owner sweep plan. Keep W5's persisted queue/mark/continuation,
   reader-safe-point, final-reference, and bounded-memory work outside W3.
6. **Observation fence.** Remove both mutation-revision spaces and replace
   invalidation/preconditions with selector/epoch observation. Do not retain a
   second adapter writer.
7. **Writer last.** Complete intent classification precedes view-derived
   planning. Supported work builds one `PreparedPublication`, lowers once,
   joins transaction-owned metadata/idempotency, prepares once, and commits
   once. Empty/no-op and unsupported cohorts perform no write or rotation.
8. **Deletion proof before first compile.** Delete BranchHeadControl,
   BranchHeadControlCache, TrackedHeadContext, old reader factories, marker/
   generation helpers, recovery-ref rows, both revision spaces, old stage/load
   helpers, and legacy fixtures/bench probes. Then run the first compiler.

## Explicit W4/W5 boundaries

W3 consumes W4's selector/epoch/coherent-read/publication contract. It does not
change object domains, selector encoding, `view_id`, private owner APIs, or
create a second publication fence.

W3 supplies authenticated root edges and exact epoch/selector preconditions to
W5. W5 owns persisted bounded mark/queue/continuation state, safe-point
waiting, crash/reopen continuation, sweep deletion, and final-reference
reclamation. W3 must not scan or delete physical objects directly.

## Forbidden first-runnable residue

The source/compiler gate must reject any callable or authoritative instance of:

`BranchHeadControl`, `BranchHeadControlContext`, `BranchHeadControlCache`,
`stage_branch_head_control`, `branch_head_control_precondition`,
`BRANCH_HEAD_CONTROL_SPACE`, `TrackedHeadContext`, fresh
`TrackedStateStoreReader` factories/callbacks, `CurrentStateDeltaRef`,
`CHECKPOINT_RECOVERY_REF_SPACE`, `stage_recovery_ref_rotation`,
`MUTATION_REVISION_SPACE`, `TRACKED_MUTATION_REVISION_SPACE`,
`load_mutation_revision*`, `stage_mutation_revision*`, legacy checkpoint/current
state marker readers, helper-created `begin_read`, compatibility/fallback
readers, raw caller object mutation, or any independent second writer.

Public names such as checkpoint, branch, undo/redo, history, NULL/tombstone,
and materialized file rows may survive only when their owning function body
delegates to the retained view and sole publication owner.
