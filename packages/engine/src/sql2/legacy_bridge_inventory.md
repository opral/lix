# `legacy_bridge` Inventory (Phase L1)

Date: 2026-02-24  
Purpose: migration table for removing `packages/engine/src/sql2/legacy_bridge.rs`.

## Ownership Targets

1. `sql2/ast/*`: parsing helpers, expression/row resolution, placeholder data model.
2. `sql2/planning/*`: preprocess, bind-once, plan fingerprint.
3. `sql2/execution/*`: followup builders and postprocess-specific runtime shaping.
4. `sql2/history/rewrite/*`: read-rewrite sessions and query rewrite state.
5. `sql2/storage/*`: SQL escaping + SQL-string utilities.

## Bridge API Migration Table

| Symbol | Current callsites | Target owner |
| --- | --- | --- |
| `preprocess_plan_fingerprint` | `sql2/planning/trace.rs` | `sql2/planning/trace.rs` + planner-native fingerprint helper |
| `SqlBridgePlaceholderState` | `engine.rs`, `filesystem/mutation_rewrite.rs`, `filesystem/pending_file_writes.rs` | `sql2/ast/utils.rs` (placeholder state type) |
| `SqlBridgeResolvedCell` | `filesystem/mutation_rewrite.rs`, `filesystem/pending_file_writes.rs` | `sql2/ast/utils.rs` (resolved cell type) |
| `SqlBridgeReadRewriteSession` | `filesystem/mutation_rewrite.rs` | `sql2/history/rewrite/*` session type |
| `SqlBridgeDetectedFileDomainChange` | `filesystem/mutation_rewrite.rs` | `sql2/contracts/effects.rs` or `sql2/history/plugin_inputs.rs` |
| `new_sql_bridge_placeholder_state` | `engine.rs` | `sql2/ast/utils.rs` constructor |
| `escape_sql_string_with_sql_bridge` | `deterministic_mode/mod.rs`, `materialization/apply.rs`, `filesystem/*`, `schema/provider.rs` | `sql2/storage/*` shared utility |
| `preprocess_statements_with_provider_with_sql_bridge` | `deterministic_mode/mod.rs` | `sql2/planning/*` |
| `preprocess_sql_with_sql_bridge` | `sql2/side_effects.rs`, `filesystem/pending_file_writes.rs`, `plugin/runtime.rs` | `sql2/planning/*` |
| `bind_sql_with_sql_bridge_state` | `engine.rs`, `filesystem/mutation_rewrite.rs`, `filesystem/pending_file_writes.rs` | `sql2/planning/bind_once.rs` + `sql2/ast/utils.rs` |
| `advance_sql_bridge_placeholder_state` | `engine.rs` | `sql2/ast/utils.rs` |
| `resolve_values_rows_with_sql_bridge` | `filesystem/mutation_rewrite.rs`, `filesystem/pending_file_writes.rs` | `sql2/ast/utils.rs` |
| `resolve_expr_cell_with_sql_bridge` | `filesystem/mutation_rewrite.rs`, `filesystem/pending_file_writes.rs` | `sql2/ast/utils.rs` |
| `lower_statement_with_sql_bridge` | `filesystem/mutation_rewrite.rs` | `sql2/planning/lower_sql.rs` |
| `rewrite_read_query_with_backend_and_params_in_session_with_sql_bridge` | `filesystem/mutation_rewrite.rs` | `sql2/history/rewrite/*` |
| `collect_filesystem_update_side_effects_with_sql_bridge` | `engine.rs` | `filesystem/mutation_rewrite.rs` (already native) + remove bridge wrapper |
| `preprocess_with_sql_surfaces` | `sql2/surfaces/registry.rs` | `sql2/surfaces/registry.rs` + `sql2/planning/*` |
| `build_update_followup_statements_with_sql_bridge` | `sql2/execution/postprocess.rs` | `sql2/execution/postprocess.rs` |
| `build_delete_followup_statements_with_sql_bridge` | `sql2/execution/postprocess.rs` | `sql2/execution/postprocess.rs` |
| `from_sql_preprocess_output` / `to_sql_preprocess_output` | internal bridge-only | delete with bridge |
| `from_sql_prepared_statements` | `sql2/side_effects.rs` | avoid legacy output type; emit `sql2/contracts::PreparedStatement` directly |
| `from_sql_mutations` / `to_sql_mutations` | bridge internal only | move to `sql2/contracts` or delete |
| `from_sql_update_validations` / `to_sql_update_validations` | bridge internal only | move to `sql2/contracts` or delete |
| `to_sql_detected_file_domain_changes*` / `from_sql_detected_file_domain_changes*` | bridge internal only | move to `sql2/history/plugin_inputs.rs` or delete |
| `to_sql_postprocess_plan` / `to_sql_vtable_update_plan` / `to_sql_vtable_delete_plan` | bridge internal only | move to `sql2/execution/postprocess.rs` or delete |

## Removal Order

1. Move AST/binding symbols first (highest fanout).
2. Move escaping and preprocess entrypoints second.
3. Move read-rewrite session and followup builders third.
4. Delete bridge conversion helpers last, once callsites are zero.
