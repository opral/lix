mod bind;
mod branch_ref;
mod branch_scope;
mod catalog;
mod change_materialization;
mod context;
mod dml;
mod error;
mod exec;
mod file_view;
mod history_projection;
mod history_route;
mod optimize;
mod parse;
mod plan;
mod planning_cache;
mod predicate_typecheck;
mod providers;
mod read_only;
mod result_metadata;
mod runtime;
mod script;
mod session;
#[cfg(test)]
mod test_support;
mod udfs;
mod write_normalization;

#[cfg(test)]
pub(crate) use bind::bind_statement;
pub(crate) use bind::{
    BoundStatementRoute, bind_read_statement, bind_statement_route, bind_statement_with_catalog,
    statement_has_durable_runtime_function,
};
pub(crate) use catalog::PublicCatalog;
pub(crate) use context::{
    ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource, SqlJsonReader, SqlWriteContext, SqlWriteExecutionContext, WriteAccess,
    WriteContextBranchRefReader, WriteContextLiveStateReader,
};
pub(crate) use exec::{SessionReadSqlResult, SqlWriteResult};
#[allow(unused_imports)]
pub(crate) use exec::{
    SqlLogicalPlan, create_write_logical_plan_from_template,
    create_write_plan_template_from_parsed, execute_read_statement_from_parsed,
    execute_read_statement_in_session_from_parsed, execute_transaction_read_statement_from_parsed,
    execute_write_logical_plan_result, prepare_read_session, prepare_read_session_at_head,
};
#[cfg(test)]
pub(crate) use exec::{
    WriteExecutorMode, WriteExecutorPath, create_write_logical_plan, execute_write_logical_plan,
    execute_write_logical_plan_with_mode, execute_write_logical_plan_with_mode_and_trace,
    execute_write_logical_plan_with_mode_and_trace_result,
    execute_write_logical_plan_with_mode_result,
};
pub(crate) use file_view::{
    SessionFileViewKey, SessionFileViewMutation, SessionFileViews, SessionPluginFileView,
};
#[cfg(test)]
pub(crate) use parse::parse_statement;
pub(crate) use plan::plan_write;
pub(crate) use planning_cache::SqlPlanningCache;
pub use script::{SqlScriptPlan, SqlScriptStatement, parse_sql_script};
