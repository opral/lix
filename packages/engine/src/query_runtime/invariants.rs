use crate::internal_state::{
    requires_single_statement_internal_state_plan, validate_internal_state_plan,
};
use crate::query_runtime::contracts::execution_plan::ExecutionPlan;
use crate::query_runtime::contracts::planner_error::PlannerError;
use crate::query_runtime::contracts::result_contract::ResultContract;

pub(crate) fn validate_execution_plan(plan: &ExecutionPlan) -> Result<(), PlannerError> {
    if plan.preprocess.prepared_statements.is_empty() && !allows_effect_only_execution(plan) {
        return Err(PlannerError::invariant(
            "sql planner produced an execution plan without statements",
        ));
    }
    if requires_single_statement_internal_state_plan(plan.preprocess.internal_state.as_ref())
        && plan.preprocess.prepared_statements.len() != 1
    {
        return Err(PlannerError::invariant(
            "sql planner produced invalid postprocess plan with multiple statements",
        ));
    }
    if plan.preprocess.internal_state.is_some() && !plan.preprocess.mutations.is_empty() {
        return Err(PlannerError::invariant(
            "sql planner produced postprocess plan with unexpected mutation rows",
        ));
    }
    validate_internal_state_plan(plan.preprocess.internal_state.as_ref())
        .map_err(PlannerError::preprocess)?;
    if plan.preprocess.internal_state.is_some()
        && matches!(
            plan.result_contract,
            ResultContract::Select | ResultContract::Other
        )
    {
        return Err(PlannerError::invariant(
            "sql planner produced postprocess plan for non-DML contract",
        ));
    }
    if plan.preprocess.internal_state.is_some() && plan.result_contract.expects_postprocess_output()
    {
        return Err(PlannerError::invariant(
            "sql planner cannot expose postprocess internal rows as public DML RETURNING output",
        ));
    }
    if plan.dependency_spec.depends_on_active_version
        && !plan
            .dependency_spec
            .schema_keys
            .contains("lix_active_version")
    {
        return Err(PlannerError::invariant(
            "dependency spec marks active-version dependency but omits lix_active_version schema key",
        ));
    }
    Ok(())
}

fn allows_effect_only_execution(plan: &ExecutionPlan) -> bool {
    matches!(plan.result_contract, ResultContract::DmlNoReturning)
        && plan.preprocess.internal_state.is_none()
        && plan.preprocess.mutations.is_empty()
        && plan.preprocess.update_validations.is_empty()
}
