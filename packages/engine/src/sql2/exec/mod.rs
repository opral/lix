pub(crate) mod datafusion;
pub(crate) mod fast_write;
pub(crate) mod read;
pub(crate) mod result;
pub(crate) mod write;

use crate::sql2::SqlStatementKind;

pub(crate) use datafusion::{
    create_logical_plan, create_logical_plan_from_parsed,
    create_transaction_read_logical_plan_from_parsed, execute_logical_plan, execute_sql,
    DataFusionLogicalPlan as SqlDataFusionLogicalPlan,
};
pub(crate) use write::{
    create_write_logical_plan, create_write_logical_plan_from_parsed, execute_write_logical_plan,
    WriteLogicalPlan as SqlWriteLogicalPlan,
};

#[allow(dead_code)]
pub(crate) enum SqlLogicalPlan {
    DataFusion(SqlDataFusionLogicalPlan),
    Write(SqlWriteLogicalPlan),
}

impl SqlLogicalPlan {
    #[allow(dead_code)]
    pub(crate) fn kind(&self) -> SqlStatementKind {
        match self {
            Self::DataFusion(plan) => plan.kind(),
            Self::Write(_) => SqlStatementKind::Write,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_write(&self) -> bool {
        self.kind() == SqlStatementKind::Write
    }
}
