mod analysis;
mod ast_ref;
mod ast_utils;
mod entity_views;
mod escaping;
mod lowering;
mod params;
mod planner;
mod preprocess;
mod read_pipeline;
mod read_views;
mod rewrite;
mod row_resolution;
mod steps;
mod types;
mod write_views;

pub(crate) use analysis::{
    active_version_from_mutations, active_version_from_update_validations,
    file_history_read_materialization_required_for_statements,
    file_read_materialization_scope_for_statements, is_query_only_statements,
    should_invalidate_installed_plugins_cache_for_sql,
    should_invalidate_installed_plugins_cache_for_statements,
    should_refresh_file_cache_for_statements, FileReadMaterializationScope,
};
#[cfg(test)]
pub(crate) use analysis::{
    file_history_read_materialization_required_for_sql, file_read_materialization_scope_for_sql,
    is_query_only_sql, should_refresh_file_cache_for_sql,
};
pub(crate) use ast_ref::{expr_references_column_name, ColumnReferenceOptions};
pub(crate) use ast_utils::{
    default_alias, object_name_matches, parse_single_query, parse_single_query_with_dialect,
    quote_ident, rewrite_query_selects, rewrite_table_factors_in_select_decision,
    visit_query_selects, visit_table_factors_in_select, RewriteDecision,
};
pub(crate) use escaping::escape_sql_string;
pub(crate) use lowering::lower_statement;
pub(crate) use params::{
    bind_sql, bind_sql_with_state, bind_statement, bind_statement_with_state,
    bind_statement_with_state_and_appended_params, PlaceholderState,
};
pub(crate) use planner::{
    compile_statement_with_state, prepare_statement_block_with_transaction_flag, StatementBlock,
};
#[cfg(test)]
pub use preprocess::parse_sql_statements;
#[cfg(test)]
pub use preprocess::preprocess_sql_rewrite_only;
#[allow(unused_imports)]
pub use preprocess::{
    parse_sql_statements_with_dialect,
    preprocess_parsed_statements_with_provider_and_detected_file_domain_changes,
    preprocess_parsed_statements_with_provider_and_detected_file_domain_changes_and_state,
    preprocess_sql, preprocess_sql_with_provider,
    preprocess_sql_with_provider_and_detected_file_domain_changes,
};
pub(crate) use read_pipeline::{
    rewrite_read_query_with_backend, rewrite_read_query_with_backend_and_params_in_session,
    ReadRewriteSession,
};
pub(crate) use read_views::lix_state_history_view_read::ensure_history_timeline_materialized_for_statement_with_state;
#[cfg(test)]
pub(crate) use rewrite::extract_explicit_transaction_script;
pub(crate) use rewrite::extract_explicit_transaction_script_from_statements;
pub(crate) use row_resolution::{
    insert_values_rows_mut, materialize_vtable_insert_select_sources, resolve_expr_cell_with_state,
    resolve_insert_rows, resolve_values_rows, ResolvedCell, RowSourceResolver,
};
pub use steps::vtable_write::{
    build_delete_followup_sql, build_update_followup_sql, DetectedFileDomainChange,
};
pub(crate) use steps::working_projection_refresh::refresh_working_projection_for_read_query;
pub(crate) use types::FileDataAssignmentPlan;
pub use types::PostprocessPlan;
pub(crate) use types::PreparedStatement;
pub use types::SchemaRegistration;
pub use types::{MutationRow, UpdateValidationPlan};
