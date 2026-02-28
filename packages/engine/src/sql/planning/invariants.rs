use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::planner_error::PlannerError;
use super::super::contracts::result_contract::ResultContract;
use super::super::vtable::registry::validate_postprocess_plan;

pub(crate) fn validate_execution_plan(plan: &ExecutionPlan) -> Result<(), PlannerError> {
    if plan.preprocess.prepared_statements.is_empty() {
        return Err(PlannerError::invariant(
            "sql planner produced an execution plan without statements",
        ));
    }
    if plan.preprocess.postprocess.is_some() && plan.preprocess.prepared_statements.len() != 1 {
        return Err(PlannerError::invariant(
            "sql planner produced invalid postprocess plan with multiple statements",
        ));
    }
    if plan.preprocess.postprocess.is_some() && !plan.preprocess.mutations.is_empty() {
        return Err(PlannerError::invariant(
            "sql planner produced postprocess plan with unexpected mutation rows",
        ));
    }
    if let Some(postprocess) = plan.preprocess.postprocess.as_ref() {
        validate_postprocess_plan(postprocess).map_err(PlannerError::preprocess)?;
    }
    if plan.preprocess.postprocess.is_some()
        && matches!(
            plan.result_contract,
            ResultContract::Select | ResultContract::Other
        )
    {
        return Err(PlannerError::invariant(
            "sql planner produced postprocess plan for non-DML contract",
        ));
    }
    if plan.preprocess.postprocess.is_some() && plan.result_contract.expects_postprocess_output() {
        return Err(PlannerError::invariant(
            "sql planner cannot expose postprocess internal rows as public DML RETURNING output",
        ));
    }
    Ok(())
}
