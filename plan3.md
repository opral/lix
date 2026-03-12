# SQL2 Self-Hosted Selector Planning Plan

## Objective

Remove sql2's dependency on legacy SQL text binding/parsing helpers during write resolution.

The end state is:

- sql2 does not build helper SQL strings for its own selector reads
- sql2 does not depend on `crate::sql::ast::utils::bind_sql`
- sql2 write resolution uses sql2-owned structured predicate state and sql2-native selector-read execution
- the guardrail in `packages/engine/tests/sql_guardrails.rs` passes for real, not by moving the same legacy dependency elsewhere

## First-Principles Goal

The correct architecture is:

- parse once at the boundary
- bind once at the boundary
- canonicalize into sql2-owned structures
- keep all later planning and read-before-write logic inside sql2-owned IR / AST / semantic types
- never stringify SQL and reparse it just to ask sql2 another question

If sql2 needs a helper selector read, it should call a structured sql2 API, not reconstruct text SQL.

## Current Problem

Today `packages/engine/src/sql/public/planner/semantics/write_resolver.rs` builds selector SQL text, calls the legacy binder from `crate::sql::ast::utils::bind_sql`, reparses the SQL, and then feeds that into `prepare_sql2_read_strict`.

That means:

- sql2 write resolution still depends on legacy text-SQL utility code
- selector semantics are partly represented as `Vec<String>` instead of sql2-owned predicate structures
- the guardrail failure is legitimate

## Target Architecture

### 1. Structured Selector Predicates

Replace SQL-text residual predicates in sql2 write planning with structured sql2-owned predicate state.

Likely direction:

- stop treating selector leftovers as `Vec<String>`
- store either:
  - bound `sqlparser::ast::Expr` values owned by sql2, or
  - a smaller sql2-specific predicate IR if current AST ownership is awkward

Minimum requirement:

- the selector representation must be directly consumable by sql2 read preparation without SQL string assembly

### 2. SQL2-Native Selector Read Entry Point

Add a dedicated sql2 API for read-before-write selector lookups.

Likely shape:

- `prepare_sql2_selector_read(...)`
- or `resolve_selector_rows(...)`

Input should be structured:

- target surface
- selected columns
- predicate expressions / selector IR
- bound parameters
- execution context

Not input as text SQL.

### 3. No Legacy Binder in SQL2 Planner

After the selector-read path exists:

- remove `bind_sql` usage from `write_resolver.rs`
- avoid introducing a renamed sql2-local clone of the same text-SQL round-trip
- keep binding and parameter ownership at the sql2 boundary

### 4. Preserve Parameter Identity Until Selector Lowering

The selector-read path should behave more like SQLite/Postgres/DuckDB prepared statements:

- placeholders remain part of structured query state
- selector planning carries the original bound-parameter environment
- selector lowering decides which parameters are actually needed and emits the final dense backend parameter vector

That means:

- no generic statement rebinder step
- no reparsing after placeholder renumbering
- parameter projection happens as part of selector-read lowering, not as a separate transport shim

### 5. Structured Public Read Planning

The same design should eventually apply to ordinary public reads, not just write-resolution selector reads.

That means:

- generic public reads should stop treating the original `Statement::Query` AST as the primary semantic source
- the planner should derive a structured read model once, near the boundary
- dependency analysis, effective-state planning, and lowering should consume that structured read model directly
- final backend SQL plus dense parameters should be emitted only at the lowering/execution boundary

The selector-read migration is the first narrow slice of this broader change.

## Implementation Phases

## Phase 1: Inventory the SQL2 Text Round-Trip

### Goal

Identify every sql2 path that still:

- assembles SQL text
- binds via legacy utilities
- reparses for internal sql2 use

### Tasks

1. Audit `packages/engine/src/sql/public/planner/semantics/write_resolver.rs`.
2. Find all uses of:
   - `bind_sql`
   - `parse_sql_statements`
   - selector SQL string assembly
3. Confirm whether this pattern exists outside `write_resolver.rs`.
4. Write down the exact data actually needed from the selector-read path.

### Deliverable

A minimal list of sql2-internal text-SQL round-trips to remove.

## Phase 2: Redesign Selector Predicate Storage

### Goal

Make selector residuals structured instead of text-based.

### Tasks

1. Inspect where `residual_predicates: Vec<String>` is produced today.
2. Decide the new representation:
   - bound `Expr`
   - or smaller sql2 predicate IR
3. Update the relevant planner/runtime contracts to carry the new form.
4. Keep enough information for diagnostics and test snapshots if needed.

### Decision Rule

Prefer the smallest representation that is:

- stable inside sql2
- not text-based
- usable by selector-read planning directly

## Phase 3: Add SQL2-Native Selector Read Planning

### Goal

Create a structured read path for selector evaluation without SQL text assembly.

### Tasks

1. Add a new sql2 selector-read helper under `packages/engine/src/sql/public/runtime` or another sql2-owned module.
2. Accept structured selector inputs instead of SQL strings.
3. Reuse existing sql2 read preparation pieces where appropriate.
4. Make the selector-read helper return rows or scalar values in a form write resolution can consume directly.

### Constraint

This helper must not import:

- `crate::sql::ast::utils`
- legacy rewrite/followup modules
- legacy planning bridges

## Phase 3.5: Add Selector Parameter-Environment Lowering

### Goal

Keep selector reads structured while still producing backend-ready dense parameters at the execution boundary.

### Tasks

1. Accept selector `Expr` predicates plus the original bound parameters directly.
2. Compile selector predicates into a simple selector query shape with:
   - one surface
   - one projected selector column
   - conjunction-only selection
3. During that compilation, project original parameter refs into the exact dense parameter array required by the backend SQL.
4. Delete the temporary generic statement rebinder once the selector path owns this lowering.

### Constraint

This is still a transitional step. The broader sql2 read planner is still statement-centric, so selector reads may continue to materialize a synthetic query statement internally until the larger read canonicalizer is redesigned.

## Phase 4: Refactor `write_resolver.rs`

### Goal

Remove the legacy binder/reparse path from sql2 write resolution.

### Tasks

1. Replace `query_entity_ids_for_selector(...)` and similar helpers with the new structured selector-read API.
2. Delete SQL string assembly for selector helper reads.
3. Delete `bind_sql` and `parse_sql_statements` usage from sql2 write resolution.
4. Keep behavior equivalent for:
   - exact selectors
   - residual predicate selectors
   - entity/file/directory/admin/state selector reads

## Phase 5: Tighten Contracts and Naming

### Goal

Make it harder to regress back into text-SQL internal transport.

### Tasks

1. Rename fields like `residual_predicates` if the new representation is no longer textual.
2. Move any still-valid helper types into sql2-owned modules such as:
   - `sql/public/core`
   - `sql/public/planner`
   - `sql/public/runtime`
3. Remove now-misleading comments or APIs that imply selector reads are SQL-text based.

## Phase 6: Tests and Guardrails

### Goal

Prove that sql2 now owns selector planning end-to-end.

### Tasks

1. Keep `guardrail_sql2_stays_isolated_from_legacy_rewrite_followup_and_classifier_modules`.
2. Add focused tests for selector-driven writes that previously depended on the text-SQL path.
3. Ensure entity/file/directory/state selector updates and deletes still resolve correctly.
4. Add regression coverage for quoted/parameterized selector inputs if relevant.

## Phase 7: Add Structured Generic Read Metadata

### Goal

Start moving ordinary public reads away from `CanonicalizedRead.bound_statement.statement` as the semantic source of truth.

### Tasks

1. Add structured fields for generic reads, likely including:
   - top-level surface binding
   - projection requirements
   - residual predicate `Expr`s or read predicate IR
   - ordering keys
   - limit/offset
   - nested public-surface flags if still needed
2. Populate those fields during read canonicalization.
3. Keep `CanonicalizedRead` working during transition, but stop adding new planner dependencies on the original query AST.

### Constraint

This phase should not try to remove all AST usage immediately. It should create a structured read model that can be adopted incrementally.

## Phase 8: Move Effective-State and Dependency Logic to Structured Reads

### Goal

Stop deriving read semantics from `canonicalized.bound_statement.statement`.

### Tasks

1. Move predicate classification to the structured read model.
2. Move required-column discovery to the structured read model.
3. Move dependency derivation off direct query AST walking where possible.
4. Leave AST use only where it is still needed for syntax-preserving lowering or explain/debug output.

### Success Signal

`effective_state_resolver.rs` and `dependency_spec.rs` should no longer need the original top-level `Statement::Query` to understand the read semantics.

## Phase 9: Move Generic Read Lowering to Structured Inputs

### Goal

Make lowerers consume structured generic reads rather than rewriting cloned query ASTs as the main path.

### Tasks

1. Introduce structured lowerer inputs for state/entity/admin/filesystem reads.
2. Lower those structured reads into derived backend queries plus residual filters.
3. Keep syntax-preserving query reconstruction only where necessary for compatibility or explain output.
4. Remove direct dependence on `canonicalized.bound_statement.statement` from the generic lowerers as the primary path.

### Constraint

This is the largest migration step. It should be done per surface family rather than all at once.

## Phase 10: Shrink or Remove `CanonicalizedRead` AST Dependence

### Goal

Turn `CanonicalizedRead` into either:

- a thin transitional container around structured read semantics, or
- something removable for the main read path

### Tasks

1. Remove planner-semantic dependence on `bound_statement.statement`.
2. Keep only the minimal syntax/debug/explain fields still needed.
3. Reassess whether `CanonicalizedRead` is still buying anything once structured read planning is complete.

## Recommended Order

1. Inventory all sql2-internal text-SQL round-trips.
2. Redesign residual predicate storage.
3. Add structured sql2 selector-read helper.
4. Refactor `write_resolver.rs` to use it.
5. Remove legacy binder imports/usages.
6. Run guardrails and selector-write tests.

## Risks

- selector semantics are currently spread across planner, runtime, and write-resolution code
- replacing `Vec<String>` residual predicates may touch more files than the single failing guardrail suggests
- there may be hidden assumptions in diagnostics/tests that depend on textual predicate rendering
- doing a partial refactor that still reconstructs text SQL under a different helper name would preserve the architectural problem

## Success Criteria

The work is complete when:

- `packages/engine/src/sql/public/planner/semantics/write_resolver.rs` no longer imports `crate::sql::ast::utils`
- sql2 selector reads no longer rely on a generic AST rebinder step
- sql2 selector reads are driven from structured sql2-owned data
- generic public reads also derive semantics from structured read data instead of the original query AST
- `cargo test -p lix_engine --test sql_guardrails` passes
- selector-driven sql2 write tests still pass

## Progress Log

- 2026-03-11: Created `plan3.md` for the first-principles sql2 fix: remove internal SQL-string round-trips, replace textual residual predicates with structured sql2-owned state, and route write-resolution selector reads through a sql2-native path.
- 2026-03-11: Replaced the sql2 write-selector text round-trip with a structured path. Added a sql2-owned AST placeholder rebinder under `sql/public/core`, changed write-selector residual predicates to structured `Expr` values, added a sql2-native selector-read execution helper under `sql/public/runtime`, and rewired `write_resolver.rs` to build selector read statements from AST instead of SQL strings. Verified with `cargo check -p lix_engine --tests`, `cargo test -p lix_engine --test sql_guardrails`, and `cargo test -p lix_engine trims_selector_bindings_for_public_update_placeholders`.
- 2026-03-11: Refined the selector-read design to preserve the original parameter environment until selector lowering. Removed the temporary generic statement rebinder, added selector-specific parameter projection in `sql/public/runtime/read.rs`, and rewired `write_resolver.rs` to call the structured selector-read API with surface name, selector column, residual predicate `Expr`s, and original params. Verified with `cargo check -p lix_engine --tests`, `cargo test -p lix_engine --test sql_guardrails guardrail_sql2_stays_isolated_from_legacy_rewrite_followup_and_classifier_modules -- --exact`, `cargo test -p lix_engine trims_selector_bindings_for_public_update_placeholders`, `cargo test -p lix_engine --test state_by_version_view delete_supports_placeholders`, and `cargo test -p lix_engine --test filesystem_view file_update_by_path_clause_succeeds`.
- 2026-03-11: Removed the remaining synthetic selector-read planning input. Selector reads now derive effective-state inputs and lowered selector queries directly from `SurfaceBinding + selector column + residual predicate Exprs + schema hint`, without routing through `CanonicalizedRead` or `prepare_sql2_read_strict`. Also dropped `sql2` naming from the new selector APIs. Verified with `cargo check -p lix_engine --tests`, `cargo test -p lix_engine --test sql_guardrails guardrail_sql2_stays_isolated_from_legacy_rewrite_followup_and_classifier_modules -- --exact`, `cargo test -p lix_engine trims_selector_bindings_for_public_update_placeholders`, `cargo test -p lix_engine --test state_by_version_view delete_supports_placeholders`, and `cargo test -p lix_engine --test filesystem_view file_update_by_path_clause_succeeds`.
- 2026-03-11: Expanded `plan3.md` beyond selector reads. Added follow-on phases to migrate ordinary public reads away from `CanonicalizedRead.bound_statement.statement` toward structured read semantics for dependency analysis, effective-state planning, and lowering.
