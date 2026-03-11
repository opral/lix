use std::collections::BTreeSet;

use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::execution::contracts::dependency_spec::DependencySpec;
use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::contracts::planner_error::PlannerError;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::{LixBackend, Value};
use sqlparser::ast::Statement;

use crate::sql::execution::dependency_spec::derive_dependency_spec_from_statements;
use crate::sql::execution::derive_effects::derive_plan_effects;
use crate::sql::execution::derive_requirements::derive_plan_requirements;
use crate::sql::execution::invariants::validate_execution_plan;
use crate::sql::execution::preprocess::preprocess_with_surfaces_to_plan;

pub(crate) async fn build_execution_plan<P>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    dependency_spec_override: Option<DependencySpec>,
    functions: SharedFunctionProvider<P>,
    pending_file_delete_targets: &BTreeSet<(String, String)>,
    authoritative_pending_file_write_targets: &BTreeSet<(String, String)>,
    writer_key: Option<&str>,
) -> Result<ExecutionPlan, PlannerError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let result_contract = derive_result_contract(&parsed_statements)?;
    let preprocess = preprocess_with_surfaces_to_plan(
        backend,
        evaluator,
        parsed_statements.clone(),
        params,
        functions,
        writer_key,
    )
    .await
    .map_err(PlannerError::preprocess)?;

    let requirements = derive_plan_requirements(&parsed_statements);
    let dependency_spec = match dependency_spec_override {
        Some(spec) => spec,
        None => derive_dependency_spec_from_statements(&parsed_statements, params)
            .map_err(PlannerError::parse)?,
    };
    let effects = derive_plan_effects(
        &preprocess,
        writer_key,
        pending_file_delete_targets,
        authoritative_pending_file_write_targets,
    )?;

    let plan = ExecutionPlan {
        preprocess,
        result_contract,
        requirements,
        dependency_spec,
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
