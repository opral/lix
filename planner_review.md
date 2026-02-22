# Planner Review: Regression Risk and First-Principles Redesign

## Scope
This review covers `packages/engine` query planning and statement rewrite flow, with focus on regression-prone behavior in the current planner architecture.

## Executive diagnosis
The planner is currently **rule-heavy but contract-light**: most correctness depends on local rewrite heuristics and convention, while global semantic invariants are thin. That makes regressions likely when adding new rules, changing SQL rendering behavior, or adjusting backend-specific paths.

The most important architectural issue is that planning is not a pure function in all paths. Once planning includes read-time writes/materialization side effects, reproducibility and debuggability degrade quickly.

## Findings (ordered by severity)

### 1) Correctness invariants are too narrow for a multi-phase planner
Evidence:
- Phase and final read validation mostly checks only unresolved logical view names: `packages/engine/src/sql/pipeline/validator.rs:28`, `packages/engine/src/sql/pipeline/validator.rs:32`, `packages/engine/src/sql/pipeline/validator.rs:105`.
- Analyze-phase relation consistency check is debug-only and a no-op in release: `packages/engine/src/sql/pipeline/rules/query/analyze/relation_discovery.rs:15`.

Risk:
- Semantic regressions (wrong row set, placeholder binding drift, pushdown misplacement) can pass validation if logical view names are gone.

### 2) Planner has read-time side effects (impure planning)
Evidence:
- `rewrite_query_with_backend` for history rewrites also ensures timeline materialization: `packages/engine/src/sql/steps/lix_state_history_view_read.rs:26`, `packages/engine/src/sql/steps/lix_state_history_view_read.rs:46`.
- Timeline status/breakpoint writes happen during read planning flow: `packages/engine/src/sql/steps/lix_state_history_view_read.rs:1042`, `packages/engine/src/sql/steps/lix_state_history_view_read.rs:1062`, `packages/engine/src/sql/steps/lix_state_history_view_read.rs:1063`.

Risk:
- Planning outcome and latency depend on mutable system state.
- Harder incident response because "same SQL + params" can behave differently over time.

### 3) Sync/backend and legacy/new paths are duplicated, increasing drift risk
Evidence:
- Separate sync/backend application methods per rule: `packages/engine/src/sql/pipeline/registry.rs:89`, `packages/engine/src/sql/pipeline/registry.rs:117`.
- Large duplicated sync/backend write canonical loops: `packages/engine/src/sql/pipeline/rules/statement/canonical/mod.rs:118`, `packages/engine/src/sql/pipeline/rules/statement/canonical/mod.rs:317`.
- Canonical write loops include unreachable query/explain branches due earlier Insert/Update/Delete gate: `packages/engine/src/sql/pipeline/rules/statement/canonical/mod.rs:125`, `packages/engine/src/sql/pipeline/rules/statement/canonical/mod.rs:256`, `packages/engine/src/sql/pipeline/rules/statement/canonical/mod.rs:333`, `packages/engine/src/sql/pipeline/rules/statement/canonical/mod.rs:638`.

Risk:
- "Fixed in one path, broken in the other" regressions.
- More surface area for subtle behavioral divergence under load or between test modes.

### 4) Planner logic relies on SQL string heuristics and substring rewrites
Evidence:
- COUNT fast-path detection via rendered projection string comparison: `packages/engine/src/sql/steps/state_pushdown.rs:26`, `packages/engine/src/sql/steps/vtable_read.rs:988`.
- Placeholder hazard detection via manual SQL string scanning: `packages/engine/src/sql/steps/state_pushdown.rs:248`, `packages/engine/src/sql/steps/state_pushdown.rs:253`.
- History fallback decision and remapping rely on substring checks/replacements: `packages/engine/src/sql/steps/lix_state_history_view_read.rs:705`, `packages/engine/src/sql/steps/lix_state_history_view_read.rs:733`.

Risk:
- Behavior can shift when parser formatting/to_string changes.
- Edge SQL shapes can bypass safety logic without explicit compile-time failure.

### 5) Rule phases are nominally separated but semantically blurred
Evidence:
- Canonical pass already rewrites `lix_state` and `lix_state_by_version`: `packages/engine/src/sql/pipeline/rules/query/canonical/logical_views.rs:25`, `packages/engine/src/sql/pipeline/rules/query/canonical/logical_views.rs:27`.
- Optimize pushdown runs the same state rewrite modules again: `packages/engine/src/sql/pipeline/rules/query/optimize/pushdown.rs:3`, `packages/engine/src/sql/pipeline/rules/query/optimize/pushdown.rs:10`.
- Phase order and matching are context-heuristic based: `packages/engine/src/sql/pipeline/query_engine.rs:12`, `packages/engine/src/sql/pipeline/registry.rs:79`.

Risk:
- Phase intent is unclear (desugaring vs optimization).
- Future refactors can accidentally disable or duplicate transformations.

### 6) Analysis context/caching suggests incomplete planner design seams
Evidence:
- `ReadRewriteSession` seeds/absorbs `materialized_schema_keys_cache`: `packages/engine/src/sql/pipeline/query_engine.rs:26`, `packages/engine/src/sql/pipeline/query_engine.rs:32`.
- Context stores cache field but has no planner-owned population path besides setter/getter wiring: `packages/engine/src/sql/pipeline/context.rs:22`, `packages/engine/src/sql/pipeline/context.rs:81`.

Risk:
- Half-implemented optimization seams increase maintenance burden and can mislead future work.

### 7) Generic SQL dialect parse at planning front door is a latent fragility
Evidence:
- Statement parse entry uses `GenericDialect`: `packages/engine/src/sql/pipeline.rs:35`, `packages/engine/src/sql/pipeline.rs:36`.
- Many rewrites build SQL text then reparse (`parse_single_query` / `Parser::parse_sql`): `packages/engine/src/sql/ast_utils.rs:28`, `packages/engine/src/sql/steps/vtable_read.rs:869`, `packages/engine/src/sql/steps/lix_state_history_view_read.rs:623`.

Risk:
- Planner acceptance/rewrite behavior can diverge from backend dialect semantics.

## First-principles architecture to reduce regressions

### Principle 1: Planner must be pure
`plan = f(sql_ast, params, catalog_snapshot, planner_config)`
- No backend writes in planning.
- Read-time timeline/materialization prep moves to an explicit pre-execution maintenance stage.

### Principle 2: One semantic IR, one planner path
- Build a typed logical IR (not SQL strings) after parse/bind.
- Keep one planning path; backend differences become capabilities/config, not separate algorithm branches.

### Principle 3: Rewrite contracts must be explicit and machine-checkable
Each rewrite rule declares:
- Preconditions.
- Postconditions.
- Columns/relations it may introduce/remove.
- Placeholder mapping behavior.

### Principle 4: SQL string generation happens once, at the very end
- Use AST/IR builders for transformations.
- Lower IR to backend SQL in a dedicated emitter stage.
- Parameter positions allocated from a typed parameter map, not textual heuristics.

### Principle 5: Validate semantics, not just unresolved names
Add planner invariants for:
- Placeholder mapping monotonicity and preservation.
- Expected output column shape.
- Row identity semantics for effective-state views.
- Deterministic tie-breakers where dedup/ranking occurs.

## Proposed target pipeline
1. Parse + bind using backend-aware dialect/profile.
2. Resolve logical view nodes into typed logical operators (`LogicalState`, `LogicalStateByVersion`, `LogicalStateHistory`, etc.).
3. Constraint extraction on typed expressions (no `to_string()` heuristics).
4. Canonical semantic normalization (view desugaring only).
5. Physical planning/template selection (including vtable/materialized strategy) as a pure decision.
6. SQL emission + parameter allocation.
7. Post-plan validation + optional trace emission (plan fingerprint, rules applied).

## Regression-hardening program (practical rollout)

### Phase 0 (immediate hardening)
- Remove planner side effects from `lix_state_history` rewrite path.
- Add release-mode relation discovery checks (not debug-only).
- Add plan invariants beyond unresolved view detection.
- Emit structured planner trace for every rewritten statement (phase/rules/invariants).

### Phase 1 (path unification)
- Collapse sync/backend planner branching into one internal planner interface with backend capability adapters.
- Remove unreachable branches in canonical write loops.
- Eliminate duplicate state rewrite responsibilities across canonical/optimize phases.

### Phase 2 (IR introduction)
- Introduce typed logical operators for state/state_by_version/state_history.
- Migrate pushdown extraction from SQL string heuristics to typed expression analysis.
- Move SQL formatting to dedicated emitters.

### Phase 3 (verification)
- Add differential tests: old planner vs IR planner on replay corpus.
- Add property/metamorphic tests for predicate permutations and placeholder numbering.
- Add shadow-plan compare mode in CI for high-risk rewrite rules.

## Testing strategy required for long-term stability
- Golden plan snapshots (logical + physical) for representative query families.
- Differential execution tests across SQLite/Postgres for same logical intent.
- Fuzz/property tests for predicate forms (`AND/OR`, subqueries, placeholders `?/?N/$N`).
- Determinism tests for ranking/dedup paths under equivalent data permutations.
- Canary suite for every logical view (`lix_state`, `lix_state_by_version`, `lix_state_history`, filesystem/entity views) with mixed nested SQL shapes.

## CTO recommendation
Treat planner correctness as a product surface, not an implementation detail. The current design is sophisticated but brittle because it encodes semantics indirectly (string templates + heuristic guards). A typed logical IR with pure planning and explicit rule contracts is the most leverageful way to reduce regression frequency while preserving current vtable-centric architecture.
