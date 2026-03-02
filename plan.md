# Plan: Canonical Fix for Nested Logical View Rewrite Coverage

## Goal
Eliminate unresolved logical views in nested query shapes by making logical view rewrite coverage uniform and complete, while keeping validator strict.

## Scope
- Fix read rewrite coverage for filesystem logical views in expression subqueries (`IN (SELECT ...)`, `EXISTS (...)`, nested query bodies).
- Preserve current behavior and SQL output semantics (aliases, projections, filters).
- Add regression tests that prevent reintroducing this class of bug.

## Steps

1. Refactor filesystem read rewrite entrypoint to shared AST traversal
- Replace custom recursive query walker in filesystem read rewrite with planner shared helpers:
  - `rewrite_query_with_select_rewriter(...)`
  - `rewrite_table_factors_in_select(...)`
- Keep `build_filesystem_projection_query(...)` as source-of-truth for generated derived SQL.
- Target files:
  - `packages/engine/src/filesystem/select_rewrite.rs`
  - `packages/engine/src/sql/planning/rewrite_engine/steps/filesystem_step.rs`

2. Preserve rewrite semantics while changing traversal
- Ensure rewritten SQL remains functionally equivalent:
  - same alias behavior
  - same view-to-derived mapping
  - same active-version scoping semantics
- Ensure no behavior changes for top-level `lix_file*` / `lix_directory*` queries.

3. Add unit coverage for nested expression subqueries in filesystem rewrite
- Add tests that assert filesystem logical views are rewritten inside:
  - `IN (SELECT ... FROM lix_file ...)`
  - `EXISTS (SELECT ... FROM lix_file ...)`
  - mixed nesting with logical outer relation (e.g. outer `lix_working_changes`, inner `lix_file`)
- Target file:
  - `packages/engine/src/filesystem/select_rewrite.rs`

4. Add planner-level regression test for unresolved logical view failure
- Add a test for the repro shape that previously failed:
  - outer `lix_working_changes`
  - subquery on `lix_file`
- Assert rewrite completes with no unresolved logical views (validator passes).
- Candidate targets:
  - `packages/engine/src/sql/planning/rewrite_engine/pipeline.rs`
  - or `packages/engine/src/sql/planning/rewrite_engine/pipeline/validator.rs`

5. Add integration regression test through engine execute path
- Add/extend engine test using execute API that runs repro SQL and asserts success.
- Ensure it uses deterministic fixture setup and does not depend on external state.
- Candidate target:
  - `packages/engine/tests/working_changes_view.rs` (or a dedicated regression test file)

6. Run formatting and targeted verification
- `cargo fmt --package lix_engine`
- Targeted tests:
  - `cargo test -p lix_engine --lib`
  - filesystem rewrite tests
  - planner regression tests
  - integration regression test file

7. Validate strictness remains intact
- Confirm validator still rejects truly unresolved logical views.
- Do not add fallbacks that bypass rewrite/validator invariants.

## Non-goals
- Relaxing validator behavior.
- Adding compatibility fallbacks for unresolved logical views.
- Broad query planner redesign beyond this rewrite coverage class.

## Done criteria
- Repro query shape succeeds without unresolved logical view errors.
- New tests cover expression-subquery shapes and fail if traversal regresses.
- Existing logical-view validation remains strict.

## Progress report
- [x] Step 1: Refactored `filesystem/select_rewrite.rs` to shared rewrite traversal via `rewrite_query_with_select_rewriter` + `rewrite_table_factors_in_select`; removed custom recursive query walker.
- [x] Step 3: Added filesystem rewrite unit tests for nested expression subqueries (`IN (SELECT ... FROM lix_file ...)`, `EXISTS (SELECT ... FROM lix_file ...)`).
- [x] Step 4: Added planner-level regression coverage in rewrite pipeline tests for `lix_working_changes` + nested `lix_file` subquery shape.
- [x] Step 5: Added engine integration regression test in `tests/working_changes_view.rs` that executes a nested `lix_file` subquery filter over `lix_working_changes`.
- [x] Step 6: Ran `cargo fmt` and targeted tests for filesystem rewrite, planner rewrite, and integration query paths.
- [x] Additional hardening discovered during validation: `vtable_read` lower phase also missed expression-subquery traversal. Fixed by rewriting nested subqueries inside `Select` and added regression test `rewrites_vtable_in_expression_subquery`.
