# Acceptance oracle: TrackedHeadContext hard cut

## Immutable anchor and scope

The oracle is bound to e166:

```text
commit e1666edd0b4d814a88d985086ecc5a477b5d32e6
tree   c680bd7e7f7b70cd784676515839af2dcbbc7917
parent 3def82e48ed74ab3d914867767e3bf06def3ffc2
```

It accepts only a future candidate that replaces the remaining
`TrackedHeadContext` responsibilities with ForkTree-owned facts. It does not
authorize restoring `live_state/tracked_head.rs`, adding a wrapper/alias,
keeping a compatibility reader, or introducing a second durable authority.

The existing read-only design contract is frozen in
`/root/repos/lix-evidence/tracked-head-migration-e166/REPORT.md`.

## Ownership and publication invariants

1. One authenticated `CoherentView` owns the selector pair, repository root,
   branch snapshot, and retained storage read for each supported operation.
2. ForkTree selectors own current global/branch root publication and CAS
   epochs. Commit/change catalogs and authenticated commit objects own
   identity and chronology.
3. ForkTree state roots own current tracked/untracked values. Collection
   generation, working diff, GC reachability, serving dependencies, and
   columnar coordinates are rebuildable terminal projections bound to the
   selected view/root, never durable authorities.
4. Transaction-local schema catalogs and staging overlays remain ephemeral;
   they are not a second durable schema/current-state owner and are cleared on
   rollback.
5. Every non-no-op publication has exactly this shape:

   ```text
   one retained CoherentView
     -> one ForkTree mutation / PreparedPublication / into_storage_plan
     -> one existing prepare_write_set
     -> one prepared backend commit
   ```

   There is no independent `PreparedPublication::commit`, helper commit,
   retry writer, or legacy marker publication.
6. A true no-op creates no plan, selector rotation, generation, or backend
   write. An unsupported cohort fails before plan creation.
7. Missing, malformed, wrong-kind, wrong-owner, stale, cyclic, or
   cross-view authority fails closed. It must not become an empty result or
   silently fall back to a TrackedHead/legacy tracked-state reader.

## Responsibility contract

### Transaction working-diff and generation

The candidate must replace these e166 call sites:

- `packages/lix/src/transaction/context.rs:6520-6528` packed identity
  membership;
- `:7420-7441` `working_diff_at_head`;
- `:8139-8167` prepared mutation collection generation;
- `:8916-8970` collection generation and exact live count.

Requirements:

- Membership is derived from the transaction's opening ForkTree view and
  staged overlay, with any cache keyed by authenticated view/root.
- Generation is `(live_count, ordered_identity_digest)` derived from the
  selected state root and overlay. It is not a `lix_collection_generation`
  hot-generation authority.
- Working diff is derived from branch snapshot/head and checkpoint baseline
  through authenticated ForkTree commit/state traversal. Added/modified/
  removed rows, filters, branch identity, ordering, limits, and checkpoint
  exclusion remain stable.
- A stale projection may be rebuilt only from the same view. Corrupt or
  contradictory authority fails closed.

### GC reachability and current-generation staging

Replace `packages/lix/src/gc.rs:98-117`, `:2515-2571`, and `:2626-2629`.

The GC pass must acquire one coherent selector/repository view, seed roots from
authenticated branch snapshots, commit/change catalogs, recovery references,
and retention roots, then derive state/CAS/payload reachability by authenticated
ForkTree traversal. Selector/progress/epoch preconditions and deletions belong
to one GC write plan.

Prove branch-first and GC-first races, stale same-owner and unrelated-owner
preconditions, blocked debt/no-spin behavior, release cadence, shared roots,
history/checkpoint roots, final-reference reclamation, and cold reopen. A
missing/malformed root or catalog mismatch aborts before deletion; it never
produces an empty live set.

### Initialization publication

Replace `packages/lix/src/init.rs:438-506`. Initialization must atomically
publish initial global/local state roots, commit/change catalogs, repository root,
branch snapshots, selectors, schema/catalog facts, idempotency, and recovery
metadata. Both visible branches must cold-reopen from ForkTree roots without a
hot-generation bootstrap. No `stage_current_state_with_working_diff` or
working-diff marker is emitted.

### Deterministic sequence

Replace `packages/lix/src/functions/state.rs:80-201`, `:554-560`, and the test
writer in `functions/context.rs:135`, `:374-382`. The deterministic key/value
row is an engine-owned global ForkTree state mutation. Its state root and
global selector rotate under one CAS-protected publication. No-op does not
rotate; malformed/missing/wrong-owner untracked state fails closed.

### Schema resolver

Keep the transaction-local `TransactionCatalog` cache in
`packages/lix/src/transaction/schema_resolver.rs:16-107`; it is not durable and
must clear on rollback. Its base reader must be ForkTree-owned. Rewrite the
test-only `SplitCurrentAndTrackedReader` cases at `:155-289` as selected
ForkTree-view plus staged-overlay cases. No second schema catalog or tracked
schema authority is permitted.

### SQL `working_diff`

Replace both legacy routes in
`packages/lix/src/sql2/providers/working_diff.rs:133-197`: the direct
`TrackedHeadContext` accelerator and the `TrackedStateContext::diff_commits`
fallback. The provider must use one ForkTree-derived projection or one
authenticated ForkTree historical traversal, while preserving DataFusion
schema, filters, branch-by-branch behavior, ordering, checkpoint exclusion,
and limits.

### Fixtures and obsolete types

Replace hot-writer fixtures in `live_state/context.rs:1139-1209,1638-1729`,
`transaction/bench_support.rs:384-415,564-598`, `test_support.rs:140-186`,
and GC tests at `gc.rs:6110-6116,7541-7547` with ForkTree state-tree/catalog/
selector builders. These are correctness fixtures, not benchmark changes.

Delete or replace without aliases:

```text
TrackedHeadContext
HotStateTransactionCache
TrackedWorkingDiff
TrackedWorkingDiffEpoch
WorkingDiffIndexCoverage
CurrentStateDeltaRef
TrackedHeadDeltaRef
TRACKED_WORKING_DIFF_MARKER_*
stage_current_state_with_working_diff
stage_untracked_generation
working_diff_for_control
stage_collect_stale_current_state_generations
stage_collect_stale_working_diff_indexes
```

`ColumnarBaseCoordinate` and `CertifiedCurrentStatePredecessor` may survive
only as explicitly root-bound, rebuildable terminal/transaction DTOs under a
new owner and name. They must not retain a tracked-head import or authority.

## Required source-negative result

The verifier must prove all of the following in production and compiled test
source:

- no `live_state/tracked_head.rs` or `live_state/tracked_head/hot.rs`;
- no `TrackedHeadContext`, hot transaction cache, old working-diff types,
  old delta refs, marker namespace/space, or old writer/reader methods;
- no `tracked_head:` field in `LiveStateContext`/`LiveStateStoreReader`;
- no old imports/reexports in `live_state/mod.rs`, transaction, init, GC,
  functions, SQL, fixtures, or bench support;
- no `TrackedHeadContext` or `TrackedStateContext` route in SQL
  `working_diff.rs`;
- no alternate current-state publication or fallback that turns malformed
  authority into an empty result.

The e166 baseline is expected to fail this gate because the implementation was
deleted before all call sites were migrated. That failure is the required red
calibration.

## Model case matrix

Every case below must be represented by the pure model and then executed by
the future external adapter harness in Memory, RocksDB, and SlateDB order.

| ID | Case | Required assertion |
|---|---|---|
| WD-01 | add/modify/remove working diff | ForkTree checkpoint-relative result is exact and ordered |
| WD-02 | stale/missing checkpoint | typed fail-closed, never empty success |
| WD-03 | branch-local and global selector | selected branch view only; no cross-branch leakage |
| GEN-01 | count and ordered digest | root-bound derived value matches exact state rows |
| GEN-02 | staged replacement/no-op | overlay changes result; no-op produces zero writes |
| INIT-01 | initial two-branch publication | one plan/prepare/commit and cold-reopen equality |
| DET-01 | deterministic sequence advance | global root/selector CAS and value are atomic |
| DET-02 | deterministic no-op/rollback | no selector rotation; rollback clears local cache |
| GC-01 | retained history/checkpoint roots | all authenticated owners remain reachable |
| GC-02 | shared/final payload roots | shared payload survives; final owner is reclaimed |
| GC-03 | GC-first/branch-first race | stale owner precondition fails without deleting newer root |
| GC-04 | blocked debt/no spin | progress persists and resumes at explicit epoch |
| SCHEMA-01 | schema overlay | selected ForkTree schema plus staged transaction rows |
| SQL-01 | working_diff filters/limits | SQL output digest/schema/order unchanged |
| RACE-01 | stale same-owner | stale selector/global epoch is rejected |
| RACE-02 | unrelated owner | unrelated selector/root cannot satisfy precondition |
| CORR-01 | missing selector/root/catalog | typed corruption, no fallback |
| CORR-02 | malformed/wrong-kind object | typed corruption before publication/deletion |
| CORR-03 | root/catalog/branch mismatch | identity mismatch fails closed |
| CORR-04 | cyclic/duplicate topology | traversal rejects cycle/duplicate/back-edge |
| CORR-05 | stale derived projection | rebuild same view or return typed unavailable |
| REOPEN-01 | flush/drop/cold reopen | result and authoritative roots are equivalent |
| REOPEN-02 | interrupted publication/recovery | atomic batch leaves old or new valid state |
| DEL-01 | obsolete module/reexport/fixture | source verifier is GREEN and old consumer cannot compile |

## Stop conditions

Stop at the first failure in this order: source authority/residue, pure-model
invariant, Memory semantic/corruption, RocksDB semantic/corruption, SlateDB
semantic/corruption. Do not run later adapters after an earlier blocker. No
performance or current-main comparison is part of this oracle.
