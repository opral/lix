# Observe Plan (Built on DependencySpec)

## Goal
Implement cross-process observe correctness using internal ticks while reusing planner-emitted `DependencySpec` for in-process precision and matching compilation.

## Preconditions
- `DependencySpec` implementation from `plan.dependency.md` is complete and consumed by runtime paths.

## Non-goals
- No public API changes (`observe` API remains stable).
- No changes to public `lix_change` contract.

## Architecture
1. Add `lix_internal_observe_tick` table:
   - `tick_seq` monotonic PK
   - `created_at`
   - `writer_key` nullable
2. On every successful mutating transaction, insert exactly one tick row.
3. Observe runtime has two lanes:
   - Lane A: in-process precise invalidation via state-commit stream + `DependencySpec`.
   - Lane B: cross-process coarse invalidation via tick polling.
4. Tick advance triggers query re-execution (coalesced); result-dedup controls emissions.
5. Writer-key suppression applies on external ticks (`same writer -> suppress`).

## Milestones

### O1: Schema + write-path tick insertion
1. Add table creation and migration checks in engine init.
2. Insert one tick row per successful mutating transaction path.
3. Ensure read-only transactions do not emit ticks.

Acceptance:
- Tick table exists in sqlite/postgres.
- Mutation emits tick; read-only does not.

### O2: Observe state model update
1. Extend observe state with `last_seen_tick_seq` + last tick writer key.
2. Add backend-agnostic polling for latest tick.
3. Keep existing in-process lane behavior unchanged initially.

Acceptance:
- Observe can detect external commits in sqlite/postgres via ticks.

### O3: DependencySpec integration in observe
1. Compile observe matching filter from planner `DependencySpec`.
2. Remove local ad-hoc filter derivation where superseded.
3. Preserve conservative fallback semantics.

Acceptance:
- Observe matching behavior uses single dependency source.

### O4: Writer-key external suppression
1. Compare observer writer key with external tick writer key.
2. Suppress re-exec/emission for same writer.
3. Null writer key treated as external.

Acceptance:
- Same/different/null writer-key tests pass.

### O5: Coalescing + dedup
1. Coalesce rapid external tick advances into one re-exec window.
2. Emit only on row delta compared to previous result.

Acceptance:
- Mutating tx with multiple writes produces one emission if final result is one delta.

### O6: Cleanup + hardening
1. Remove sqlite-only special casing that is replaced by tick logic.
2. Keep state commit stream for in-process latency optimization.
3. Document guarantees and limits.

Acceptance:
- External observe tests pass for sqlite and postgres.
- Existing observe tests stay green.

## Required Tests
1. External insert/untracked insert detected (sqlite + postgres).
2. Same writer key suppressed.
3. Different writer key emits.
4. Null writer key emits.
5. Read-only external tx does not emit.
6. Mutating external tx emits once for result delta.
7. Unrelated external mutation does not emit.

## Risks and Mitigations
1. Risk: tick insertion missing for some mutation paths.
   - Mitigation: centralize tick write near transaction commit boundary.
2. Risk: polling overhead.
   - Mitigation: bounded interval + coalescing.
3. Risk: races between local and external lane.
   - Mitigation: monotonic cursor checks and dedup by result.

## Deliverables
1. Internal tick table + write hooks.
2. Observe runtime with cross-process correctness.
3. DependencySpec-backed observe matching.
4. Passing cross-process observe tests.

## Progress log

