use crate::query_runtime::contracts::execution_plan::ExecutionPlan;
use crate::query_runtime::contracts::prepared_statement::PreparedStatement;

pub(crate) fn lower_to_prepared_statements(plan: &ExecutionPlan) -> Vec<PreparedStatement> {
    plan.preprocess.prepared_statements.clone()
}
