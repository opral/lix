# TrackedHeadContext whole-module deletion acceptance oracle

## Provenance

The requested `413e08a` object is available and provenance is valid:

```text
head   413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d
tree   820fe560da3bbd2b00b788b0b1759c409048cd6e
parent 11442c1e0023e20307a7231d88cd557bc704fd13
e166 ancestor e1666edd0b4d814a88d985086ecc5a477b5d32e6
```

The e166..413e full-index binary diff is
`70bc6bc03524855be515c9d1a5d0c75c77ebd159fbd44d5f646483ce14460329` and its
stable patch ID is `df0747c2c7e026147361aab7edd4f741efca9b33`. The two commits
between e166 and 413e route SQL/entity reads through canonical ForkTree rows,
but do not complete the TrackedHeadContext deletion. This oracle therefore
binds directly to 413e and records e166 as its source lineage, rather than
silently substituting a moving ref.

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

## Responsibility gates

### Transaction working-diff/generation

Migrate e166 call sites in `packages/lix/src/transaction/context.rs`:

- packed identity membership around `:6520-6528`;
- `working_diff_at_head` around `:7420-7441`;
- prepared mutation generation around `:8139-8167`;
- collection generation/live count around `:8916-8970`.

The derived result must be bound to one ForkTree view/root and staged overlay.
Added, modified, removed, filter, branch, checkpoint, order, and limit
semantics must remain exact. Staged replacement is local overlay state, not a
durable generation row.

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
second durable schema authority.

### SQL working_diff

The provider at `sql2/providers/working_diff.rs:133-197` must use one
ForkTree-derived working-diff projection or authenticated ForkTree historical
traversal. Preserve DataFusion schema, branch routing, filters, ordering,
checkpoint exclusion, limits, and exact output digests. Corruption must fail
closed rather than selecting the old fallback.

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

413e is a RED calibration, not an accepted implementation: the old module is
absent but 34 `TrackedHeadContext` references, old marker space, old staging
methods, and SQL fallback remain. A future candidate is source-approved only
when the path-aware verifier is GREEN, the intentional obsolete consumer
still fails to compile, and the adapter sequence passes Memory, then RocksDB,
then SlateDB without broadening after the first blocker.
