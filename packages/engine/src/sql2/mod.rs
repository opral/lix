mod bind;
mod branch_scope;
mod catalog;
mod change_materialization;
mod context;
mod dml;
mod error;
mod exec;
mod history_projection;
mod history_route;
mod optimize;
mod parse;
mod plan;
mod predicate_typecheck;
mod providers;
mod read_only;
mod result_metadata;
mod runtime;
mod session;
#[cfg(test)]
mod test_support;
mod udfs;
mod write_normalization;

pub(crate) use bind::{
    BoundStatementRoute, bind_read_statement, bind_statement, bind_statement_route,
    statement_has_durable_runtime_function,
};
pub(crate) use context::{
    ChangelogQuerySource, HistoryQuerySource, SqlChangelogQuerySource, SqlExecutionContext,
    SqlHistoryQuerySource, SqlJsonReader, SqlWriteContext, SqlWriteExecutionContext, WriteAccess,
    WriteContextBranchRefReader, WriteContextLiveStateReader,
};
pub(crate) use exec::SessionReadSqlResult;
#[allow(unused_imports)]
pub(crate) use exec::{
    SqlLogicalPlan, create_write_logical_plan_from_parsed, execute_read_statement_from_parsed,
    execute_transaction_read_statement_from_parsed, execute_write_logical_plan,
};
#[cfg(test)]
pub(crate) use exec::{
    WriteExecutorMode, WriteExecutorPath, create_write_logical_plan,
    execute_write_logical_plan_with_mode, execute_write_logical_plan_with_mode_and_trace,
};
pub(crate) use parse::parse_statement;
pub(crate) use plan::plan_write;

pub(crate) const FILE_DATA_HYDRATION_LIMIT: usize = 32;
pub(crate) const FILE_DATA_NEEDS_HYDRATION_CODE: &str = "LIX_FILESYSTEM_DATA_NEEDS_HYDRATION";
pub(crate) const FILE_DATA_HYDRATION_LIMIT_CODE: &str = "LIX_FILESYSTEM_DATA_HYDRATION_LIMIT";

pub(crate) fn file_data_needs_hydration_error(paths: Vec<String>) -> crate::LixError {
    crate::LixError::new(
        FILE_DATA_NEEDS_HYDRATION_CODE,
        "filesystem file data needs hydration before it can be read",
    )
    .with_details(serde_json::json!({ "paths": paths }))
}

pub(crate) fn file_data_hydration_limit_error(paths: &[String]) -> crate::LixError {
    crate::LixError::new(
        FILE_DATA_HYDRATION_LIMIT_CODE,
        format!("query would hydrate more than {FILE_DATA_HYDRATION_LIMIT} filesystem files"),
    )
    .with_hint("Narrow the query or read file data in smaller batches.")
    .with_details(serde_json::json!({ "paths": paths }))
}

pub(crate) fn file_data_hydration_paths(error: &crate::LixError) -> Option<Vec<String>> {
    if error.code != FILE_DATA_NEEDS_HYDRATION_CODE {
        return None;
    }
    let paths = error.details.as_ref()?.get("paths")?.as_array()?;
    Some(
        paths
            .iter()
            .filter_map(|value| value.as_str().map(ToOwned::to_owned))
            .collect(),
    )
}

pub(crate) fn file_data_unresolved_error(path: &str) -> crate::LixError {
    crate::LixError::new(
        "LIX_FILESYSTEM_DATA_UNRESOLVED",
        format!("filesystem data for path {path:?} is not available"),
    )
    .with_hint("Hydrate the filesystem file data before selecting lix_file.data.")
}
