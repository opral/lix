use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::contracts::prepared_statement::PreparedStatement;

pub(crate) fn lower_to_prepared_statements(plan: &ExecutionPlan) -> Vec<PreparedStatement> {
    plan.preprocess.prepared_statements.clone()
}
