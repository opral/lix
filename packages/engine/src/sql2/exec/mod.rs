pub(crate) mod bound_public_write;
pub(crate) mod datafusion;
pub(crate) mod fast_write;
pub(crate) mod write;

pub(crate) use datafusion::{
    create_logical_plan, create_logical_plan_from_parsed,
    create_transaction_read_logical_plan_from_parsed, execute_logical_plan, execute_sql,
    DataFusionLogicalPlan as SqlDataFusionLogicalPlan,
};
pub(crate) use write::{
    create_write_logical_plan, create_write_logical_plan_from_parsed, execute_write_logical_plan,
    WriteLogicalPlan as SqlWriteLogicalPlan,
};
#[cfg(test)]
pub(crate) use write::{
    execute_write_logical_plan_with_mode, execute_write_logical_plan_with_mode_and_trace,
    WriteExecutorMode, WriteExecutorPath,
};

pub(crate) enum SqlLogicalPlan {
    DataFusion(SqlDataFusionLogicalPlan),
    Write(SqlWriteLogicalPlan),
}
