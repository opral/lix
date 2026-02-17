mod ast_ref;
mod ast_utils;
mod entity_views;
mod escaping;
mod lowering;
mod params;
mod pipeline;
mod planner;
mod query_rewriter;
mod route;
mod row_resolution;
mod steps;
mod types;

pub(crate) use ast_ref::{expr_references_column_name, ColumnReferenceOptions};
pub(crate) use ast_utils::{default_alias, object_name_matches, parse_single_query, quote_ident};
pub(crate) use escaping::escape_sql_string;
pub(crate) use lowering::lower_statement;
pub(crate) use params::{bind_sql, bind_sql_with_state, PlaceholderState};
pub(crate) use pipeline::coalesce_vtable_inserts_in_statement_list;
#[allow(unused_imports)]
pub use pipeline::{
    parse_sql_statements, preprocess_sql, preprocess_sql_rewrite_only,
    preprocess_sql_with_provider, preprocess_sql_with_provider_and_detected_file_domain_changes,
    preprocess_statements, preprocess_statements_with_provider,
    preprocess_statements_with_provider_and_writer_key,
};
pub(crate) use query_rewriter::{
    rewrite_query_with_select_rewriter, rewrite_table_factors_in_select,
};
pub(crate) use route::rewrite_read_query_with_backend;
pub(crate) use row_resolution::{
    insert_values_rows_mut, materialize_vtable_insert_select_sources, resolve_expr_cell_with_state,
    resolve_insert_rows, resolve_values_rows, ResolvedCell, RowSourceResolver,
};
pub use steps::vtable_write::{
    build_delete_followup_sql, build_update_followup_sql, DetectedFileDomainChange,
};
pub use types::PostprocessPlan;
pub use types::SchemaRegistration;
pub use types::{MutationOperation, MutationRow, UpdateValidationPlan};
