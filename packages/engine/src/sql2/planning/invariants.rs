use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::planner_error::PlannerError;

pub(crate) fn validate_execution_plan(plan: &ExecutionPlan) -> Result<(), PlannerError> {
    if plan.preprocess.prepared_statements.is_empty() {
        return Err(PlannerError::invariant(
            "sql2 planner produced an execution plan without statements",
        ));
    }
    if plan.preprocess.postprocess.is_some() && plan.preprocess.prepared_statements.len() != 1 {
        return Err(PlannerError::invariant(
            "sql2 planner produced invalid postprocess plan with multiple statements",
        ));
    }
    if plan.preprocess.postprocess.is_some() && !plan.preprocess.mutations.is_empty() {
        return Err(PlannerError::invariant(
            "sql2 planner produced postprocess plan with unexpected mutation rows",
        ));
    }
    Ok(())
}
