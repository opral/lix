# TrackedHeadContext whole-module deletion acceptance oracle, exact b59

## Provenance

The exact b59 source object is available and provenance is valid:

```text
head   b59e1f11a51153e0a787a81f0f25bf104d150aaf
tree   700fd04d21bc40c05425c9fc9e10d65c9e1eda24
parent 713455a3557907ce705d06f720fcdc4486bddd4a
e166 ancestor e1666edd0b4d814a88d985086ecc5a477b5d32e6
```

The 713..b59 full-index binary diff is
`4b2885709ba09034068b321be2fe5f27348d6681b1060133af1df0b7d76bb8d4` and its
stable patch ID is `63dcb8dcecba8a25dea0ce8be19d26cdac264729`. b59 is the
accepted historical fail-closed prerequisite, not a runnable whole-module
candidate. Its caller surface still names TrackedHeadContext duties while the
defining tracked_head module is absent from the exact b59 tree; this is an
intentional compiler-frontier condition for the deletion gate.

The prior 413e package is template evidence only. This package binds directly
to b59 and does not substitute 413e, e166, or a moving ref.

## Hard-cut authority contract

1. ForkTree selectors, repository/branch snapshots, commit/change catalogs,
   and authenticated state roots are the only durable current-state and
   chronology authorities.
2. Working diff, collection generation, GC serving dependencies, current
   reachability, and columnar locations are root-bound derived projections or
   transaction-local values. No hot marker, generation row, side index, or
   fallback may become authoritative.
3. Every supported read uses one retained `CoherentView` and cannot refresh a
   selector or open a second snapshot behind the caller's view.
4. Every non-no-op publication uses one `PreparedPublication`/
   `into_storage_plan`, one existing `prepare_write_set`, and one prepared
   backend commit. No direct `PreparedPublication::commit`, independent GC
   commit, helper commit, retry writer, or legacy current-state writer.
5. True no-op and unsupported cohorts create no plan or selector rotation.
6. Missing, malformed, wrong-kind, wrong-owner, stale, cyclic, duplicate, or
   cross-view authority fails closed. It never becomes an empty result and
   never falls back to TrackedHead or the old SQL tracked-state route.

## Whole-module deletion contract

The verifier must find all of the following absent from production and compiled
test source:

```text
packages/lix/src/live_state/tracked_head.rs
packages/lix/src/live_state/tracked_head/hot.rs
TrackedHeadContext
HotStateTransactionCache
TrackedWorkingDiff
TrackedWorkingDiffEpoch
WorkingDiffIndexCoverage
CurrentStateDeltaRef
TrackedHeadDeltaRef
TRACKED_WORKING_DIFF_MARKER_SPACE
TRACKED_WORKING_DIFF_MARKER_NAMESPACE
stage_current_state_with_working_diff
stage_untracked_generation
working_diff_for_control
stage_collect_stale_current_state_generations
stage_collect_stale_working_diff_indexes
crate::live_state::tracked_head
```

Path-aware checks cover `live_state/mod.rs`, `live_state/context.rs`,
transaction, init, GC, functions, SQL providers, `storage_bench.rs`, common
fixtures, and compiled tests. Generic words such as `working_diff`,
`collection_generation`, or `tracked_state` are not by themselves residue;
the verifier matches obsolete owner symbols and paths.

The SQL provider has an additional hard cut: neither the direct
`TrackedHeadContext` route nor the `TrackedStateContext::diff_commits` fallback
may remain in `sql2/providers/working_diff.rs`.

### Space and owner boundary

`TRACKED_WORKING_DIFF_MARKER_SPACE` and its namespace are the only
TrackedHead-specific persisted names in this gate; both must disappear with the
module. The `tracked_state::*_SPACE` names used by GC and benchmark fixtures
belong to the separate TrackedState storage owner and are not silently deleted
or re-created by this package. A future candidate must either migrate those
callers to the existing ForkTree owner in its explicitly scoped cohort or leave
them as a compiler-visible deferred frontier; it may not add a compatibility
alias, wrapper space, mirror writer, or fallback scan.

## Responsibility gates

### Transaction working-diff/generation/reconciliation

Migrate the exact b59 call sites in `packages/lix/src/transaction/context.rs`:

- `with_opening_tracked_reader` around `:7407`;
- `working_diff_at_head` around `:7437-7441`;
- collection generation/live count around `:8933-8968`.

The derived result must be bound to one ForkTree view/root and staged overlay.
Added, modified, removed, filter, branch, checkpoint, order, and limit
semantics must remain exact. Staged replacement is local overlay state, not a
durable generation row.

Undo/redo and reconciliation callers remain transaction-owned and must borrow
the same selected view. They may not recreate a TrackedHead reader or publish a
second generation selector.

### GC roots/current-generation

Replace `gc.rs` reachability and recovery paths around `:98-117`,
`:2515-2571`, and `:2626-2629` with authenticated selector/repository-root,
catalog, state-tree, object, and CAS traversal. One GC plan owns progress,
epoch, selector preconditions, and retirement. Cover shared/final roots,
history/checkpoint/recovery refs, branch-first/GC-first races, stale same-owner
and unrelated-owner races, blocked debt/no-spin, and release cadence.

### Init publication

Replace `init.rs:438-506` hot-generation/marker seeding with one atomic
ForkTree publication of initial state roots, commit/change catalogs, repository
root, branch snapshots, selectors, schema facts, idempotency, and recovery
metadata. Both branches must cold-reopen without hot bootstrap.

### Deterministic sequence

The global deterministic row in `functions/state.rs:80-201,554-560` and
`functions/context.rs:135,374-382` must be a global ForkTree state mutation.
Selector/root CAS and the value share one plan. No-op, rollback, malformed
selector, and wrong-owner controls are mandatory.

### Schema resolver

`TransactionCatalog` remains transaction-local and is cleared on rollback.
Its base reader must come from the selected ForkTree view. Rewrite the
tracked-head terminology and test fixtures in `transaction/schema_resolver.rs`
around `:155-289` as ForkTree-view plus staged-overlay tests. Do not create a
second durable schema authority. Init and schema resolution must share the
existing repository/branch selectors and authenticated state roots.

### SQL working_diff

The provider at `sql2/providers/working_diff.rs:133-197` must use one
ForkTree-derived working-diff projection or authenticated ForkTree historical
traversal. Preserve DataFusion schema, branch routing, filters, ordering,
checkpoint exclusion, limits, and exact output digests. Corruption must fail
closed rather than selecting the old fallback.

The public-SQL direct entity snapshot/PK/columnar reader slice is explicitly
out of scope here; it is covered by the separate public-SQL acceptance oracle.
This gate covers only SQL working_diff and its TrackedHead dependency.

### Reopen/corruption

Memory, RocksDB, and SlateDB must agree for flush/drop/cold reopen after init,
transaction publication, deterministic sequence, working diff, branch/history,
checkpoint/recovery, GC, and schema overlay. Corrupt or missing selectors,
roots, catalogs, state nodes, checkpoint baselines, recovery refs, and wrong
object kinds must produce typed failures before output, deletion, or commit.

## Model case IDs

The pure model must cover these discriminators before adapter execution:

```text
TX-WD-ADD-MOD-REMOVE       exact checkpoint-relative diff
TX-WD-STALE-CHECKPOINT     typed unavailable/corrupt, never empty success
TX-GEN-DIGEST-OVERLAY      count/digest with staged replacement
TX-NOOP-ZERO-WRITE         no plan, no selector rotation
GC-SHARED-ROOT             retain until final owner disappears
GC-RACE-BRANCH-FIRST       stale GC owner cannot delete new branch root
GC-RACE-GC-FIRST           stale branch owner cannot resurrect old root
GC-DEBT-NO-SPIN            bounded progress and explicit resume epoch
INIT-TWO-BRANCH-REOPEN     one publication and equal cold-reopen results
DET-ADVANCE-ROLLBACK       global root CAS and rollback cache clearing
SCHEMA-OVERLAY             ForkTree base plus transaction-local catalog
SQL-FILTER-LIMIT           output schema/order/digest preserved
CORR-MISSING-MALFORMED     fail closed for selector/root/catalog/node
CORR-WRONG-KIND-OWNER      identity/domain mismatch fails closed
CORR-CYCLE-DUPLICATE       topology corruption fails closed
DELETE-NAMEABLE-LEGACY     old consumer cannot resolve module/reexport/space
```

## Terminal source verdict rule

b59 is a RED calibration, not an accepted implementation: callers still name
TrackedHeadContext, old marker space/staging methods, and SQL working_diff
fallbacks, while the defining tracked_head module is already absent from the
exact b59 tree. A future candidate is source-approved only when the
path-aware verifier is GREEN, the intentional obsolete consumer still fails
to compile, and the adapter sequence passes Memory, then RocksDB, then SlateDB
without broadening after the first blocker.

## Exact b59 caller inventory and exclusions

The source gate inventories every remaining direct TrackedHead symbol use and
the adjacent semantic callers that must move with it:

| duty | exact b59 callers | destination authority |
| --- | --- | --- |
| live-state serving/context ownership | `live_state/context.rs` (`LiveStateContext`/`LiveStateStoreReader` fields and test staging) | one caller-owned ForkTree view and canonical state projection |
| transaction working diff/reconciliation | `transaction/context.rs:60,7407-7441,8933-8968`; `transaction/bench_support.rs:17-18,384-598` | one ForkTree view plus transaction-local staged overlay |
| init publication | `init.rs:19,438-501`; `functions/context.rs:135,374-395`; `functions/state.rs:18,106-175,554-575` | one atomic ForkTree repository/branch selector publication |
| GC roots/current-generation staging | `gc.rs:34,114,2536-2629,3454,4560,6110-6116,7541-7547` | canonical selector/root/reachability owner; one GC retirement proof |
| schema resolution and fixtures | `transaction/schema_resolver.rs:228-289` and its tracked-schema tests | ForkTree base rows plus transaction-local `TransactionCatalog`; no second durable schema authority |
| SQL working diff | `sql2/providers/working_diff.rs:15,133-195` | authenticated ForkTree diff/state projection; no TrackedHead fallback |
| benchmarks and fixtures | `test_support.rs:26,140-158`; `storage_bench.rs:2222`; `tracked_state/bench_support.rs` and transaction benches | test-only callers must use public ForkTree behavior and cannot recreate spaces |
| reexports/spaces | `live_state/mod.rs`, `live_state/context.rs`, `storage_bench.rs:2222`; separate `tracked_state/mod.rs` names are deferred | compiler-deleted TrackedHead owner/module/marker space; no compatibility reexport |

The exact b59 direct `TrackedHeadContext` consumers are therefore
`live_state/context.rs`, `init.rs`, `functions/context.rs`, `functions/state.rs`,
`gc.rs`, `sql2/providers/working_diff.rs`, `test_support.rs`, and
`transaction/bench_support.rs`, with `transaction/context.rs` owning the
transaction wrappers and generation reads. The schema-resolver and
`tracked_state/bench_support.rs` entries are semantic/fixture dependencies,
not claims that they currently construct `TrackedHeadContext`; they remain in
the deletion checklist so compiler fallout cannot be hidden.

The public SQL entity snapshot/typed-PK/columnar slice (`entity.rs`,
`forktree_reader.rs`, and columnar reader/planner owners) is not part of this
package and must not be duplicated here.
