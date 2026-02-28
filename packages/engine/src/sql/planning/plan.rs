use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::LixBackend;
use crate::Value;

use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::planner_error::PlannerError;
use super::super::contracts::result_contract::ResultContract;
use super::super::surfaces::registry::{
    preprocess_with_surfaces, DetectedFileDomainChangesByStatement,
};
use super::derive_effects::derive_plan_effects;
use super::derive_requirements::derive_plan_requirements;
use super::invariants::validate_execution_plan;
use sqlparser::ast::Statement;

pub(crate) async fn build_execution_plan<P>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &DetectedFileDomainChangesByStatement,
    writer_key: Option<&str>,
) -> Result<ExecutionPlan, PlannerError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let result_contract = derive_result_contract(&parsed_statements)?;
    let preprocess = preprocess_with_surfaces(
        backend,
        evaluator,
        parsed_statements.clone(),
        params,
        functions,
        detected_file_domain_changes_by_statement,
        writer_key,
    )
    .await
    .map_err(PlannerError::preprocess)?;

    let requirements = derive_plan_requirements(&parsed_statements);
    let effects = derive_plan_effects(&preprocess, writer_key)?;

    let plan = ExecutionPlan {
        preprocess,
        result_contract,
        requirements,
        effects,
    };
    validate_execution_plan(&plan)?;
    Ok(plan)
}

fn derive_result_contract(statements: &[Statement]) -> Result<ResultContract, PlannerError> {
    let statement = statements.last().ok_or_else(|| {
        PlannerError::invariant("sql planner cannot derive result contract from empty statements")
    })?;
    match statement {
        Statement::Query(_) | Statement::Explain { .. } => Ok(ResultContract::Select),
        Statement::Insert(insert) => {
            if insert.returning.is_some() {
                Ok(ResultContract::DmlReturning)
            } else {
                Ok(ResultContract::DmlNoReturning)
            }
        }
        Statement::Update(update) => {
            if update.returning.is_some() {
                Ok(ResultContract::DmlReturning)
            } else {
                Ok(ResultContract::DmlNoReturning)
            }
        }
        Statement::Delete(delete) => {
            if delete.returning.is_some() {
                Ok(ResultContract::DmlReturning)
            } else {
                Ok(ResultContract::DmlNoReturning)
            }
        }
        _ => Ok(ResultContract::Other),
    }
}
