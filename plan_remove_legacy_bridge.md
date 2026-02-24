# Plan: Remove `sql2/legacy_bridge` with First-Principles Ownership

Date: 2026-02-24  
Scope: `packages/engine/src` (`sql2` + runtime callsites)

## First Principles

1. `sql2` must own behavior, not only types. A forwarding bridge is still dual runtime.
2. Boundaries must follow semantics (`ast`, `planning`, `execution`, `history`, `storage`), not legacy origin.
3. Conversions should happen at one explicit edge, not everywhere via `to_sql_*`/`from_sql_*`.
4. Dependency direction is one-way: `engine -> sql2 -> backend adapters`; never `sql2 -> sql(legacy)`.

## Constraints

1. Keep runtime flow locked to:  
   `parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`
2. Do not regress integration behavior.
3. Land in small phases (one conceptual change, one commit per phase).

## Phase Plan

### Phase L1: Freeze and Inventory

1. Add guardrail to prevent *new* `legacy_bridge` callsites.
2. Inventory every function in `sql2/legacy_bridge.rs` and map each to a target owner module.
3. Verify with:
   - `rg -n "legacy_bridge::" packages/engine/src`

Exit:

1. A complete migration table exists (`function -> owner -> callsites`).
2. Guardrail fails on newly introduced bridge usage.

### Phase L2: Move AST/Binding Primitives

1. Move placeholder and expression resolution primitives into `sql2/ast` + `sql2/planning`.
2. Replace bridge aliases/types at callsites with native `sql2` types.
3. Verify with:
   - `cargo test -p lix_engine --test transaction_execution --test observe -- --test-threads=1`

Exit:

1. No AST/binding helpers are sourced from `legacy_bridge`.

### Phase L3: Move Escaping and SQL Utility Primitives

1. Move escaping/SQL utility helpers into `sql2/storage` (or `sql2/ast/utils` where appropriate).
2. Update `schema/provider`, `materialization/apply`, `deterministic_mode`, and filesystem callers.
3. Verify with:
   - `cargo test -p lix_engine --test deterministic_mode --test schema_provider -- --test-threads=1`

Exit:

1. Escaping/util helpers no longer route through bridge wrappers.

### Phase L4: Move Read-Rewrite Session Ownership

1. Move read-rewrite session state and rewrite entrypoints into `sql2/history/rewrite`.
2. Replace bridge session/change types with `sql2`-owned equivalents.
3. Verify with:
   - `cargo test -p lix_engine --test file_materialization --test file_history_view -- --test-threads=1`

Exit:

1. Filesystem/history read rewrite has no bridge-owned state types.

### Phase L5: Move Preprocess Ownership

1. Replace `preprocess_sql*` and surface preprocessing bridge entrypoints with `sql2/planning` APIs.
2. Keep planning output expressed only in `sql2/contracts`.
3. Verify with:
   - `cargo test -p lix_engine --test execute --test commit --test state_commit_stream -- --test-threads=1`

Exit:

1. Preprocess path is fully owned by `sql2/planning`.

### Phase L6: Move Postprocess Followup Builders

1. Move followup statement generation into `sql2/execution/postprocess`.
2. Remove remaining followup-related `to_sql_*`/`from_sql_*` conversions.
3. Verify with:
   - `cargo test -p lix_engine --test schema_provider --test transaction_execution -- --test-threads=1`

Exit:

1. Followup generation no longer depends on bridge wrappers.

### Phase L7: Delete `legacy_bridge` and Cut Final Imports

1. Delete `packages/engine/src/sql2/legacy_bridge.rs`.
2. Remove module wiring/imports and clean all references.
3. Verify with:
   - `rg -n "legacy_bridge|crate::sql::|\\bsql::" packages/engine/src --glob '!packages/engine/src/sql/**'`
   - `cargo test -p lix_engine --tests`

Exit:

1. `legacy_bridge` is gone from the build graph.
2. No runtime imports of `crate::sql::*` outside `packages/engine/src/sql/**`.

### Phase L8: Post-Removal Structure Cleanup

1. Split oversized modules created during migration.
2. Document final ownership map in `packages/engine/src/sql2/README.md`.
3. Verify with:
   - `cargo test -p lix_engine`

Exit:

1. File structure reflects clear ownership boundaries.
2. Runtime behavior unchanged from Phase L7.

## Definition of Done

1. `sql2/legacy_bridge` is removed.
2. Runtime does not depend on `crate::sql::*` outside legacy `src/sql/**`.
3. Runtime flow remains:
   `parse -> bind_once -> plan -> derive_requirements/effects -> lower_sql -> execute -> postprocess`.
4. `lix_engine` integration tests pass.
