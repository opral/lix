# DependencySpec Plan

## Goal
Create one canonical planner artifact (`DependencySpec`) that captures query dependencies and one canonical planner artifact (`EffectSpec`) that captures mutation effects. All downstream pipeline stages must consume these artifacts instead of re-deriving intent from SQL strings, ad-hoc AST walkers, or side-effect heuristics.

## Non-goals
- No backward compatibility layer for old dependency derivation internals.
- No observe tick implementation in this plan (covered in `plan.observe.md`).
- No public API changes required in this phase.

## Current Problems
1. Dependency intent is inferred in multiple places with different logic.
2. Cache invalidation targets are derived in multiple stages (`intent`, `side_effects`, `shared_path`).
3. Query dependency extraction exists in both engine and JS SDK, creating drift risk.
4. Pipeline contracts (`ExecutionPlan`, `PlanRequirements`) are too shallow to carry canonical dependency/effect semantics.

## Desired End State
1. Planner emits `DependencySpec` for read statements and `EffectSpec` for write statements.
2. `ExecutionPlan` carries these specs as first-class fields.
3. All internal invalidation and dependency consumers read from specs, not SQL re-parsing.
4. Existing fallback heuristics removed or reduced to explicit conservative fallback behavior.

## Core Data Structures

### DependencySpec
- `relations: Set<String>` canonical relation/view dependencies.
- `schema_keys: Set<String>` normalized schema dependencies.
- `entity_ids: Set<String>` literal entity-id constraints when representable.
- `file_ids: Set<String>` literal file-id constraints when representable.
- `version_ids: Set<String>` literal version constraints when representable.
- `writer_filter: { include: Set<String>, exclude: Set<String> }` (optional per query).
- `include_untracked: bool`.
- `depends_on_active_version: bool`.
- `precision: enum { Precise, Conservative }` to explicitly signal fallback mode.

### EffectSpec
- `mutations: Vec<MutationEffect>` normalized write effects.
- `cache_invalidation`: 
  - `file_data_targets: Set<(file_id, version_id)>`
  - `file_path_targets: Set<(file_id, version_id)>`
  - `refresh_targets: Set<(file_id, version_id)>`
- `plugin_cache_invalidation: bool`.
- `next_active_version_id: Option<String>`.

### MutationEffect
- `schema_key, entity_id, file_id, version_id, plugin_key`.
- `operation: Insert|Update|Delete`.
- `untracked: bool`.
- `data_is_authoritative: bool`.
- `writer_key: Option<String>`.

## Milestones

### M1: Contract introduction
1. Add `DependencySpec`, `EffectSpec`, and `MutationEffect` to planner contracts.
2. Extend `ExecutionPlan` to include these fields.
3. Keep existing behavior intact by populating specs from current planner artifacts.

Acceptance:
- Builds green.
- New contract types present with unit tests for serialization/debug formatting where relevant.

### M2: Planner-owned dependency derivation
1. Move/centralize dependency derivation into planner stage.
2. Replace direct uses of `derive_state_commit_stream_filter`-style custom derivation with `DependencySpec -> Filter` compilation function.
3. Remove duplicate dependency extraction from engine where feasible.

Acceptance:
- Planner unit tests assert `DependencySpec` for representative query shapes:
  - direct state/file reads
  - version-dependent reads
  - OR/NOT cases producing conservative precision
  - joins/subqueries

### M3: Planner-owned effect derivation
1. Move cache-target/invalidation derivation to `EffectSpec` in planner pipeline.
2. Replace duplicated target derivation in `intent.rs`, `side_effects.rs`, and `shared_path.rs` with `EffectSpec` consumption.
3. Preserve authoritative-write semantics in one place only.

Acceptance:
- No duplicate authoritative-write filtering logic remains.
- Existing behavior tests pass; add targeted tests for `data_is_authoritative` and untracked effects.

### M4: Runtime consumer migration
1. Engine runtime paths consume only `DependencySpec` and `EffectSpec` contracts.
2. JS-side duplication paths marked for removal or bridged from engine-provided spec (if still needed).
3. Add drift regression tests ensuring same query shape yields same dependency behavior across call paths.

Acceptance:
- Drift-sensitive tests pass.
- Removed/reduced ad-hoc dependency extraction in runtime consumers.

### M5: Cleanup and hardening
1. Delete deprecated derivation code paths.
2. Add clear invariants and docs:
   - No false negatives rule for `DependencySpec` matching.
   - `precision` semantics.
3. Add end-to-end tests for mixed tracked/untracked + active version changes.

Acceptance:
- Legacy dependency derivation removed.
- Invariant docs and tests complete.

## Testing Strategy
1. Unit tests (planner): dependency/effect derivation from parsed queries/mutations.
2. Integration tests (engine): identical behavior before/after for existing scenarios.
3. Regression tests: previously observed drift bugs.
4. Determinism tests: same input SQL yields stable `DependencySpec` output.

## Risks and Mitigations
1. Risk: Under-invalidation (false negatives).
   - Mitigation: explicit conservative fallback (`precision = Conservative`) and invariant tests.
2. Risk: Over-invalidation after migration.
   - Mitigation: compare old/new behavior in test matrix and optimize iteratively.
3. Risk: Partial migration leaves hidden drift.
   - Mitigation: milestone gating requiring consumer-by-consumer migration completion.

## Deliverables
1. Planner contract updates with `DependencySpec` + `EffectSpec`.
2. Consumer migrations to spec-driven behavior.
3. Removed duplicate derivation code.
4. Tests and docs proving single-source intent interpretation.

## Progress log 

- 2026-03-03 M1 completed:
  - Added planner contract `DependencySpec` in `packages/engine/src/sql/contracts/dependency_spec.rs`.
  - Extended `ExecutionPlan` with `dependency_spec`.
  - Wired module exports in contracts/planning.
  - Status: build and targeted tests passing.

- 2026-03-03 M2 completed:
  - Added centralized derivation and compilation in `packages/engine/src/sql/planning/dependency_spec.rs`:
    - `derive_dependency_spec_from_statements(...)`
    - `dependency_spec_to_state_commit_stream_filter(...)`
  - Switched `observe` to consume planner-owned dependency derivation and removed duplicated filter-derivation logic from `observe.rs`.
  - Added/ported dependency derivation tests for:
    - direct state/file reads
    - versioned file reads
    - OR/multi-statement conservative precision
    - joins/subqueries
    - placeholder literal extraction
  - Validation runs:
    - `cargo test -p lix_engine dependency_spec --lib -- --nocapture`
    - `cargo test -p lix_engine --test observe -- --nocapture`

- 2026-03-03 M3 progress (partial):
  - Moved mutation-derived cache targeting into planner effects (`PlanEffects`):
    - `direct_state_file_cache_refresh_targets`
    - `descriptor_cache_eviction_targets`
  - `shared_path::derive_cache_targets(...)` now consumes these precomputed effect targets instead of re-deriving from mutations.
  - Added planner invariant consuming `dependency_spec` consistency (`depends_on_active_version` requires `lix_active_version` schema key).
  - Validation runs:
    - `cargo test -p lix_engine dependency_spec --lib`
    - `cargo test -p lix_engine --test observe`
  - Remaining M3 work:
    - Consolidate authoritative pending-write and delete target derivation into a single canonical effect artifact.
    - Remove residual cache-target derivation still living in intent/shared execution path inputs.

- 2026-03-03 M3 completed:
  - `PlanEffects` now carries canonical cache target sets:
    - `file_cache_refresh_targets`
    - `file_path_cache_invalidation_targets`
    - `file_data_cache_invalidation_targets`
  - Planner effect derivation now receives authoritative pending-write targets and pending delete targets as inputs and composes final invalidation targets in one place.
  - `shared_path::derive_cache_targets(...)` now consumes `plan.effects` only (plus postprocess deltas), and no longer derives cache targets from `ExecutionIntent`.
  - Runtime call-sites (`api.rs`, `in_transaction.rs`) updated to the new contract-driven cache target flow.
  - Validation runs:
    - `cargo test -p lix_engine --lib`
    - `cargo test -p lix_engine --test observe`

- 2026-03-03 M4 completed:
  - Engine runtime migration:
    - `observe` path consumes planner-owned `DependencySpec` derivation and filter compilation.
    - SQL execution paths consume planner-owned `PlanEffects` cache targets.
  - SDK runtime consumer migration (reduced ad-hoc duplication):
    - Added canonical `deriveObserveDependencySpec(...)` in `packages/sdk/src/observe/dependency-spec.ts`.
    - Updated `create-observe.ts` and `with-runtime-cache.ts` to consume the shared dependency spec.
  - Drift-focused validation runs:
    - `pnpm vitest run src/observe/create-observe.test.ts src/engine/with-runtime-cache.test.ts --reporter=dot`
    - `pnpm vitest run src/observe/dependency-spec.test.ts --reporter=dot`

- 2026-03-03 M5 completed:
  - Cleanup/hardening:
    - Removed legacy duplicate observe filter-derivation path in engine `observe.rs`.
    - Added dependency contract invariants and documentation:
      - no-false-negatives rule
      - `precision` semantics (`Precise` vs `Conservative`)
    - Added deterministic planner test:
      - `derive_dependency_spec_is_deterministic_for_same_sql_and_params`
    - Added mixed tracked/untracked + active-version e2e observe test:
      - `observe_lix_state_mixed_tracked_and_untracked_changes_emit_only_on_visible_delta`
  - Validation runs:
    - `cargo test -p lix_engine --lib`
    - `cargo test -p lix_engine --test observe`

- 2026-03-03 Post-M3/M4/M5 regression fix:
  - Fixed `file_materialization` regression where pre-read file cache materialization was being immediately invalidated.
  - Root cause:
    - `PlanEffects.file_data_cache_invalidation_targets`/`file_path_cache_invalidation_targets` incorrectly included direct refresh targets unconditionally.
    - For statements with `should_refresh_file_cache = false`, this removed rows that were materialized on-demand before source reads.
  - Fix:
    - Keep direct refresh targets separate (`file_cache_refresh_targets`).
    - Build base invalidation targets from descriptor-eviction + pending-delete (+ pending-write for data).
    - Merge refresh targets into invalidation only when `should_refresh_file_cache` is true.
  - Validation run:
    - `cargo test -p lix_engine --test file_materialization -- --nocapture` (74 passed)
