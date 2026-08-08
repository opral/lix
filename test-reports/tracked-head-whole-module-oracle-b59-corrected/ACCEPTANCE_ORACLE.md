# Whole-module deletion acceptance contract

## Authority and publication

ForkTree repository/branch selectors, commit/change catalogs, authenticated
state roots, and their validated object closure are the only durable current
state and chronology authority. Working diff, collection generation, GC
reachability, columnar locations, and schema overlays are derived from one
caller-owned coherent view or transaction-local state.

For every supported operation, the selected `CoherentView` is retained from
selector/root authentication through state traversal and terminal lowering.
No second snapshot, retry read, cache substitution, fallback, wrapper writer,
or second authority is allowed. A non-no-op publication has exactly one
prepared publication, storage plan, prepared write set, backend commit, and
selector/epoch CAS. No-op, unsupported, and fail-closed cohorts have zero
plans, writes, commits, and selector rotations. A corrupt/unsupported read may
still have exactly one coherent read for validation; it must fail before any
write or publication.

Missing, malformed, wrong-kind, wrong-owner, stale, duplicate, cyclic, or
cross-view data fails closed; it is never an empty success and never falls
back to TrackedHead or the old SQL tracked-state route.

## Complete b59 caller inventory

The source gate checks these exact production responsibilities and adjacent
fixtures/benchmarks:

| responsibility | b59 paths/call sites | required replacement |
|---|---|---|
| live-state ownership | `live_state/context.rs` fields/tests; `live_state/types.rs` old module path | one ForkTree view/state projection |
| transaction working diff/generation | `transaction/context.rs:60,7407-7441,8933-8968`; `transaction/bench_support.rs:17-18,384-598` | retained view plus staged overlay |
| merge/reconciliation wrapper | `session/merge/branch.rs:176,298`; `transaction/context.rs:7407` | same selected view, no reader recreation |
| init publication | `init.rs:19,438-501`; `functions/context.rs:135,374-395`; `functions/state.rs:18,106-175,554-575` | one ForkTree repository/branch publication |
| GC roots/current generation | `gc.rs:34,114,2536-2629,3454,4560,6109-6116,7540-7547` | authenticated root/reachability plan and one retirement commit |
| schema resolution | `transaction/schema_resolver.rs:155-289` and tracked-schema fixtures | ForkTree base plus transaction-local catalog |
| SQL working diff | `sql2/providers/working_diff.rs:15,133-197` | ForkTree-derived projection; no tracked-state fallback |
| fixtures/benchmarks | `test_support.rs:26,140-158`; `storage_bench.rs:2222`; `transaction/bench_support.rs`; `tracked_state/bench_support.rs` | public ForkTree behavior; no recreated legacy spaces |
| reexports/spaces | `live_state/mod.rs`, `live_state/context.rs`, `storage_bench.rs:2222`; compiled tests and engine benchmarks | delete module, reexports, factories, marker space |

`tracked_state::TrackedStateContext` remains a separate owner where it is not
the SQL working-diff fallback; it is not silently deleted or mirrored here.

## Forbidden production residue

The source gate rejects these paths/symbols in all three Rust source roots:

```text
live_state/tracked_head.rs
live_state/tracked_head/hot.rs
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
super::tracked_head
live_state::tracked_head
TrackedHeadContext::new
TrackedHeadContext::reader
with_opening_tracked_reader
```

The direct public-SQL entity/PK/columnar paths are explicitly out of scope and
are rejected if touched by a candidate diff:

```text
packages/lix/src/sql2/providers/entity.rs
packages/lix/src/live_state/forktree_reader.rs
packages/lix/src/live_state/entity_columnar.rs
packages/lix/src/sql2/entity_columnar_layout.rs
```

## Semantic cohorts

The future Memory→RocksDB→SlateDB gate must cover transaction working diff and
reconciliation, init with two branches, deterministic global sequence,
schema resolution/rollback, SQL working_diff filters/order/checkpoint exclusion,
GC shared/final roots and branch-first/GC-first races, blocked debt/no-spin,
flush/drop/cold reopen, and selector/root/catalog/node corruption. Public
result digests, branch/history/checkpoint semantics, and fail-before-write
behavior must match across adapters.
