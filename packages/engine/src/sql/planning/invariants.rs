use super::super::contracts::execution_plan::ExecutionPlan;
use crate::internal_state::PostprocessPlan;
use super::super::contracts::planner_error::PlannerError;
use super::super::contracts::result_contract::ResultContract;
use crate::LixError;

pub(crate) fn validate_execution_plan(plan: &ExecutionPlan) -> Result<(), PlannerError> {
    if plan.preprocess.prepared_statements.is_empty() && !allows_effect_only_execution(plan) {
        return Err(PlannerError::invariant(
            "sql planner produced an execution plan without statements",
        ));
    }
    if requires_single_statement_postprocess(plan.preprocess.postprocess.as_ref())
        && plan.preprocess.prepared_statements.len() != 1
    {
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

fn validate_postprocess_plan(plan: &PostprocessPlan) -> Result<(), LixError> {
    let schema_key = match plan {
        PostprocessPlan::VtableUpdate(update) => &update.schema_key,
        PostprocessPlan::VtableDelete(delete) => &delete.schema_key,
    };
    if schema_key_is_valid(schema_key) {
        return Ok(());
    }
    Err(LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "vtable postprocess plan requires a valid schema_key".to_string(),
    })
}

fn schema_key_is_valid(schema_key: &str) -> bool {
    !schema_key.trim().is_empty()
        && !schema_key.contains(char::is_whitespace)
        && !schema_key.contains('\'')
}

fn allows_effect_only_execution(plan: &ExecutionPlan) -> bool {
    matches!(plan.result_contract, ResultContract::DmlNoReturning)
        && plan.preprocess.postprocess.is_none()
        && plan.preprocess.mutations.is_empty()
        && plan.preprocess.update_validations.is_empty()
}

fn requires_single_statement_postprocess(
    plan: Option<&crate::internal_state::PostprocessPlan>,
) -> bool {
    matches!(
        plan,
        Some(crate::internal_state::PostprocessPlan::VtableDelete(_))
    )
}
