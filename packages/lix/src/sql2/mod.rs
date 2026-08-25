mod bind;
mod branch_ref;
mod branch_scope;
mod catalog;
mod change_materialization;
mod context;
mod dialect;
mod dml;
mod error;
mod exec;
mod file_view;
mod history_projection;
mod history_route;
mod information_schema;
mod parse;
mod plan;
mod planning_cache;
mod predicate_typecheck;
mod providers;
#[cfg(test)]
pub(crate) use providers::{
    arm_state_at_traversal_probe,
    file_history_anchor_probe_census, file_history_bounded_frontier_census,
    file_history_raw_probe_limit_census, reset_file_history_anchor_probe_census,
    take_state_at_traversal_probe,
};
mod read_only;
mod result_metadata;
mod row_batch;
mod row_columnar_layout;
mod row_projection;
mod runtime;
mod script;
mod session;
#[cfg(test)]
mod test_support;
mod udfs;
mod value_contract;
mod write_normalization;

#[cfg(feature = "storage-benches")]
pub(crate) use error::datafusion_error_to_lix_error;

#[cfg(test)]
pub(crate) use bind::bind_statement;
pub(crate) use bind::{
    BoundStatementRoute, bind_read_statement, bind_statement_route, bind_statement_with_catalog,
    statement_has_durable_runtime_function,
};
pub(crate) use catalog::{
    PublicCatalog, PublicSurfaceKind, SchemaColumnType, SchemaIndexedColumn, SchemaSurfaceSpec,
    derive_schema_surface_spec_from_schema, row_visible_fields,
};
pub(crate) use context::WriteContextLiveness;
pub(crate) use context::{
    ChangelogQuerySource, DiffCommand, DiffCommandOutcome, DiffCommandSelection, HistoryQuerySource,
    SqlChangelogQuerySource, SqlExecutionContext, SqlHistoryQuerySource, SqlWriteContext,
    SqlWriteExecutionContext, WriteAccess, WriteContextBranchRefReader, WriteContextHotStateReader,
};
pub(crate) use exec::bound_public_write::PreparedPathValueReplacementProgram;
#[cfg(feature = "storage-benches")]
pub(crate) use exec::{
    BatchRowCursor, execute_read_statement_in_session_with_batch_stream,
    execute_read_statement_in_session_with_collected_batches,
};
pub(crate) use exec::{SessionReadResult, SessionReadSqlResult, SqlWriteResult};
#[allow(unused_imports)]
pub(crate) use exec::{
    SqlLogicalPlan, append_path_value_replacement_snapshot,
    append_path_value_replacement_snapshot_text, create_write_logical_plan_from_template,
    create_write_plan_template_from_parsed, diff_command_query,
    execute_read_statement_in_session_from_parsed, execute_read_statement_in_session_with_result,
    execute_transaction_read_statement_from_parsed, execute_write_logical_plan_parameter_batch,
    execute_write_logical_plan_prepared_dml_batch, execute_write_logical_plan_result_with_metadata,
    execute_write_logical_plan_value_batch, parameter_record_batch, parameter_row,
    prepare_path_value_replacement_program, prepare_path_value_replacement_row,
    full_checkpoint_command, prepare_read_session, prepare_read_session_at_head, query_result_from_batches,
    query_values_from_batches, write_plan_requires_post_stage_returning_checkpoint,
};
#[cfg(test)]
pub(crate) use exec::{
    WriteExecutorMode, WriteExecutorPath, create_write_logical_plan, execute_write_logical_plan,
    execute_write_logical_plan_with_mode, execute_write_logical_plan_with_mode_and_trace,
    execute_write_logical_plan_with_mode_and_trace_result,
    execute_write_logical_plan_with_mode_result, take_certified_generation_identity_replacements,
    take_certified_replacement_parameter_batch_executions,
    take_certified_row_insert_batch_executions,
    take_certified_row_insert_parameter_batch_executions,
    take_certified_single_path_value_replacements, take_row_update_parameter_batch_executions,
};
pub(crate) use file_view::{
    SessionFileViewKey, SessionFileViewMutation, SessionFileViews, SessionPluginFileView,
};
pub(crate) use parse::parse_statement;
pub(crate) use plan::plan_write;
pub(crate) use planning_cache::{
    CachedPhysicalRead, CachedReadPlan, CachedScanRequest, CachedUpdateLiteralShape,
    PhysicalReadPlanCacheKey, PooledReadSession, SqlPlanningCache,
};
pub(crate) use providers::{
    ExactLixFileReadColumn, ExactLixFileReadSelector, FastLixFilePathWriteConflict,
    execute_exact_lix_directory_root_listing, execute_exact_lix_file_batch_read,
    execute_exact_lix_file_id_manifest_batch_read, execute_exact_lix_file_read,
    execute_exact_lix_file_root_listing, execute_exact_schema_batch_read,
    execute_exact_schema_point_read, execute_fast_lix_file_path_writes,
    execute_fast_lix_file_prepared_path_write,
};
pub(crate) use row_batch::{CurrentRowSnapshotReader, RowSnapshotReader};
pub(crate) use row_columnar_layout::{
    EncodedRowGroups, LOW_CARDINALITY_CLUSTER_MAX_VALUES,
    ROW_COLUMNAR_BASE_COORDINATES_METADATA_KEY, ROW_COLUMNAR_LAYOUT_FINGERPRINT_METADATA_KEY,
    ROW_COLUMNAR_LOSSLESS_SNAPSHOT_METADATA_KEY, ROW_COLUMNAR_ROW_PK_FIELD, RowColumnarRowRef,
    RowGroupLocations, encode_registered_row_groups, encode_unclustered_registered_row_groups,
};
pub(crate) use row_projection::RowProjectionDecoder;
mod aggregate_statistics;
