mod ast_ref;
mod ast_utils;
mod entity_views;
mod escaping;
mod lowering;
mod params;
mod pipeline;
mod row_resolution;
mod steps;
mod types;

pub(crate) use ast_ref::{expr_references_column_name, ColumnReferenceOptions};
pub(crate) use ast_utils::{
    default_alias, object_name_matches, parse_single_query, quote_ident, rewrite_query_selects,
    rewrite_query_with_select_rewriter, rewrite_table_factors_in_select,
    rewrite_table_factors_in_select_decision, visit_query_selects, visit_table_factors_in_select,
    RewriteDecision,
};
pub(crate) use escaping::escape_sql_string;
#[cfg(test)]
pub(crate) use params::bind_sql_with_state_and_appended_params;
pub(crate) use params::{bind_sql, bind_sql_with_state, PlaceholderState};
pub use pipeline::parse_sql_statements;
#[cfg(test)]
pub(crate) use pipeline::preprocess_sql_rewrite_only;
pub(crate) use pipeline::query_engine::{
    rewrite_read_query_with_backend_and_params_in_session, ReadRewriteSession,
};
pub(crate) use pipeline::statement_pipeline::StatementPipeline;
pub(crate) use row_resolution::{
    resolve_expr_cell_with_state, resolve_insert_rows, ResolvedCell, RowSourceResolver,
};
pub(crate) use steps::vtable_write::DetectedFileDomainChange;
pub(crate) use types::{
    MutationOperation, MutationRow, PostprocessPlan, RewriteOutput, SchemaRegistration,
};
