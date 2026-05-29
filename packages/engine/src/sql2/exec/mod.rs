pub(crate) mod bound_public_write;
pub(crate) mod datafusion;
pub(crate) mod fast_write;
pub(crate) mod write;

pub(crate) use datafusion::{
    DataFusionLogicalPlan as SqlDataFusionLogicalPlan, create_logical_plan_from_parsed,
    create_transaction_read_logical_plan_from_parsed, execute_logical_plan,
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
