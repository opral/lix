# Lix Engine Refactor Plan: Single Intent Pipeline, Zero Stringly Routing

## Problem

`lix_file` writes are currently interpreted in multiple places:

1. SQL rewrite path
2. side-effect extraction path
3. cache target derivation path

This duplication causes drift and silent failures (for example, `UPDATE lix_file SET data = ? ...` no-op behavior with no explicit error), because each phase infers behavior from slightly different inputs.

## Objective

Build one canonical planning step that interprets write intent exactly once, then drives execution, side effects, cache behavior, and events from that single typed output.

## Scope

In scope:

1. Engine planning and execution internals
2. `lix_file` / `lix_file_by_version` write paths
3. cache refresh/invalidation derivation
4. writer key and state commit stream correctness for file writes

Out of scope:

1. Backward compatibility for internal architecture
2. SDK public API redesign
3. unrelated query optimization work

## First Principles

1. One source of truth: write intent is produced once.
2. No stringly routing: no behavior decisions from rendered SQL strings.
3. Typed semantics first: execution consumes typed ops, not inferred SQL text.
4. Determinism: same input SQL + params + active version yields same plan.
5. Fail loud: if a write intent cannot be applied, return error, never silent success.

## Target Architecture

Introduce a canonical `ExecutionIntent` IR produced by one pass over parsed AST + bound params.

`ExecutionIntent` owns:

1. backend statements to run
2. file data writes
3. file path writes
4. file delete targets
5. detected state/domain changes
6. cache refresh targets
7. cache invalidation targets
8. stream event payloads
9. validation requirements

All downstream phases consume this IR directly. No phase recomputes intent from SQL text.

## Proposed Core Types

Add new typed models in engine core (exact module names can be adjusted during implementation):

1. `ExecutionIntent`
2. `IntentOp::SqlStatement`
3. `IntentOp::FileDataWrite`
4. `IntentOp::FilePathWrite`
5. `IntentOp::FileDelete`
6. `IntentOp::StateMutation`
7. `IntentTargets { refresh, invalidate }`
8. `IntentEvents { state_commit_stream_changes }`
9. `IntentDiagnostics { matched_rows, affected_targets }`

## No-Stringly Rules (Hard Constraints)

Forbidden for planner/routing/effects:

1. `statement.to_string()` based decisions
2. `sql.contains(...)` based table/column routing
3. re-parsing generated SQL to infer semantic intent
4. deriving cache targets from textual table-name scans

Required:

1. AST visitors over parsed statements
2. typed identifier normalization for table/column matching
3. placeholder binding done once into typed expression values
4. target derivation from typed ops only

## Milestones

### M0: Baseline and Safety Nets

Deliverables:

1. keep failing regression test for `UPDATE lix_file SET data = ? WHERE id = ?`
2. add a second regression: read-before-update-then-read returns updated bytes
3. add explicit assertion tests for silent no-op prevention contract

Exit criteria:

1. tests reproduce current bug on sqlite and postgres
2. tests are deterministic in CI

### M1: Introduce Intent IR (No Behavior Change Yet)

Deliverables:

1. add `ExecutionIntent` and typed op structs
2. add `IntentBuilder` scaffold that can read one statement
3. wire planner/executor contracts to accept intent objects natively

Exit criteria:

1. compile-only integration with no runtime behavior change
2. golden tests for IR serialization/debug snapshots

### M2: Single-Pass File Update Interpretation

Deliverables:

1. implement `IntentBuilder` for `UPDATE lix_file` and `UPDATE lix_file_by_version`
2. remove duplicate interpretation for this path from:
   - rewrite no-op logic for data-only updates
   - pending file write re-derivation for same statement
   - cache target re-derivation for same statement
3. emit typed `FileDataWrite` and `FilePathWrite` ops directly

Exit criteria:

1. current regression tests pass for sqlite + postgres
2. no code path requires data-only update to become synthetic no-op

### M3: Target Derivation from Intent Only

Deliverables:

1. compute refresh/invalidation targets from intent ops
2. remove table-name scanning logic from requirements/target derivation for file writes
3. enforce invariant: each `FileDataWrite` contributes invalidate target

Exit criteria:

1. cache target tests pass for all file update variants
2. no stale read after update in deterministic tests

### M4: Intent-Driven Apply Phase

Deliverables:

1. executor consumes `ExecutionIntent` directly
2. side-effects apply from typed ops, not re-derived writes
3. add strict check: intended file writes with matched rows must produce applied writes

Exit criteria:

1. no silent success on dropped writes
2. all writer_key + transaction file write tests pass

### M5: Port Insert/Delete File Paths

Deliverables:

1. migrate `INSERT lix_file` and `DELETE lix_file` interpretation to intent pipeline
2. migrate explicit version variants
3. remove remaining duplicate write extraction logic for these paths

Exit criteria:

1. file lifecycle tests pass (insert/update/delete/history/cache)
2. side-effect consistency verified by property tests

### M6: Remove Legacy Duplicate Paths

Deliverables:

1. delete obsolete split logic in legacy modules
2. delete tests that codify legacy no-op rewrite semantics
3. retain compatibility behavior where externally observable and correct

Exit criteria:

1. no planner path interprets same write intent more than once
2. no `to_string()` routing decisions remain in planner/effects path

### M7: Rollout and Guardrails

Deliverables:

1. run full integration suite with intent pipeline as the only path
2. add telemetry counters for dropped-intent prevention and mismatch detection
3. add CI checks for invariants on cache targets and write application counts

Exit criteria:

1. full suite green on sqlite and postgres
2. no invariant violations in CI

### M8: Finalize and Clean Up

Deliverables:

1. remove dead code from legacy split interpretation paths
2. document architecture in engine docs
3. add maintenance checks to prevent stringly regressions

Exit criteria:

1. all suites green
2. lint/check disallows new stringly routing in critical modules

## Testing Plan

Required tests:

1. direct update with `?` params for sqlite + postgres
2. direct update with dialect placeholders and named params
3. read-before-update-then-read consistency
4. transaction and script execution consistency
5. writer_key preservation in stream and state
6. property-based sequence tests for file DML and cache coherence
7. restart persistence checks using snapshot export/restore

## Implementation Notes

Likely module touchpoints:

1. `packages/engine/src/sql/execution/shared_path.rs`
2. `packages/engine/src/sql/execution/apply_effects_tx.rs`
3. `packages/engine/src/filesystem/mutation_rewrite.rs`
4. `packages/engine/src/filesystem/pending_file_writes.rs`
5. `packages/engine/src/sql/semantics/state_resolution/*`
6. `packages/engine/src/sql/planning/*`

## Risks and Mitigations

Risk: regressions in complex multi-statement scripts.
Mitigation: expand transaction/script integration coverage before merge.

Risk: perf regressions in planning.
Mitigation: benchmark planning latency and cache reusable lookup metadata.

Risk: hidden coupling with plugin detection.
Mitigation: keep plugin change detection input generated from intent ops only and add dedicated plugin integration tests.

## Definition of Done

1. `UPDATE lix_file SET data = ? ...` never silently no-ops.
2. write intent is interpreted once.
3. cache targets are derived from typed ops, not SQL text.
4. no string-based routing in critical planner/execution path.
5. sqlite and postgres behavior match for file write semantics.

## Progress Log

1. `Planned` - replaced legacy plan with single-intent migration plan.
2. `M1 (partial)` - introduced typed `ExecutionIntent` in shared execution path and wired planner/executor handoff through `prepared.intent`.
3. `M3 (partial)` - cache invalidation now includes intent-derived pending file write targets (not only mutation/delete-derived targets).
4. `Bug fix verified` - `UPDATE lix_file SET data = ? WHERE id = ?` repro now passes on sqlite and postgres (`execute` test suite and writer-key transaction cache test pass).
5. `M1 (continued)` - extracted dedicated `sql/execution/intent.rs` module with `IntentCollectionPolicy` and centralized intent collection logic.
6. `M4 (partial)` - added strict post-persist verification for authoritative file data writes against `lix_internal_binary_file_version_ref` (hash + size), for both backend and transaction paths.
7. `M0 completed` - added and stabilized regression coverage in `packages/engine/tests/execute.rs` for: (a) `UPDATE lix_file SET data = ? WHERE id = ?` persistence, (b) read-before-update-then-read consistency, and (c) explicit matched-row non-no-op contract assertion.
8. `M2 completed` - `prepare_execution_with_backend(...)` now uses a single `ExecutionIntent` collection handoff for file update semantics and passes intent-derived side-effect data directly into execution/apply phases.
9. `M3 completed` - cache targeting derives from typed intent targets with split invalidation domains (`file_data_cache_invalidation_targets` vs `file_path_cache_invalidation_targets`) to avoid stale reads and path-cache churn regressions.
10. `M4 completed` - apply phase remains intent-driven and includes authoritative write verification before completion; added internal telemetry counters for verification checks/failures in engine runtime.
11. `M5 completed` - insert/update/delete file-write side effects are all consumed through the same intent collection pipeline (`pending_file_writes` + delete targets), including transaction/script paths.
12. `M6 completed` - removed remaining string-rendered statement placeholder advancement in side-effect scanning; replaced with AST-based placeholder-state advancement via `advance_placeholder_state_for_statement_ast(...)`.
13. `M7 completed` - full integration run is green on sqlite/postgres/materialization (`cargo test -p lix_engine --tests`), with intent verification telemetry and guardrail tests active.
14. `M8 completed` - added architecture documentation at `packages/engine/docs/execution-intent-pipeline.md` and new CI guardrail `guardrail_side_effect_placeholder_advancement_is_ast_based` in `packages/engine/tests/sql_guardrails.rs`.
