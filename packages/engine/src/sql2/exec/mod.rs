pub(crate) mod bound_public_write;
pub(crate) mod datafusion;
pub(crate) mod fast_write;
pub(crate) mod write;

pub(crate) use datafusion::{
    DataFusionLogicalPlan as SqlDataFusionLogicalPlan, SessionReadSqlResult,
    execute_read_statement_from_parsed, execute_transaction_read_statement_from_parsed,
};
#[cfg(test)]
pub(crate) use write::{
    WriteExecutorMode, WriteExecutorPath, create_write_logical_plan,
    execute_write_logical_plan_with_mode, execute_write_logical_plan_with_mode_and_trace,
};
pub(crate) use write::{
    WriteLogicalPlan as SqlWriteLogicalPlan, create_write_logical_plan_from_parsed,
    execute_write_logical_plan,
};

pub(crate) enum SqlLogicalPlan {
    DataFusion(SqlDataFusionLogicalPlan),
    Write(SqlWriteLogicalPlan),
}
