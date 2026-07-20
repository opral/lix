pub(crate) mod bound_public_write;
pub(crate) mod datafusion;
pub(crate) mod fast_write;
pub(crate) mod write;

use crate::SqlQueryResult;

/// Internal write outcome.  DML normally only reports its affected-row count,
/// while `DELETE … RETURNING` additionally carries the pre-delete result set.
/// Keeping both values explicit matters for deletes that stage extra cascade
/// rows: the number of returned direct targets can differ from rows affected.
pub(crate) struct SqlWriteResult {
    pub(crate) rows_affected: u64,
    pub(crate) returning: Option<SqlQueryResult>,
}

impl SqlWriteResult {
    pub(crate) fn affected(rows_affected: u64) -> Self {
        Self {
            rows_affected,
            returning: None,
        }
    }

    pub(crate) fn returning(rows_affected: u64, returning: SqlQueryResult) -> Self {
        Self {
            rows_affected,
            returning: Some(returning),
        }
    }
}

pub(crate) use datafusion::{
    DataFusionLogicalPlan as SqlDataFusionLogicalPlan, SessionReadSqlResult,
    execute_read_statement_from_parsed, execute_transaction_read_statement_from_parsed,
};
#[cfg(test)]
pub(crate) use write::{
    WriteExecutorMode, WriteExecutorPath, create_write_logical_plan, execute_write_logical_plan,
    execute_write_logical_plan_with_mode, execute_write_logical_plan_with_mode_and_trace,
    execute_write_logical_plan_with_mode_and_trace_result,
    execute_write_logical_plan_with_mode_result,
};
pub(crate) use write::{
    WriteLogicalPlan as SqlWriteLogicalPlan, create_write_logical_plan_from_parsed,
    execute_write_logical_plan_result,
};

pub(crate) enum SqlLogicalPlan {
    DataFusion(SqlDataFusionLogicalPlan),
    Write(SqlWriteLogicalPlan),
}
