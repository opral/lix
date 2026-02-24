use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::LixBackend;
use crate::Value;

use super::super::ast::nodes::Statement;
use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::planner_error::PlannerError;
use super::super::surfaces::registry::{
    preprocess_with_surfaces, DetectedFileDomainChangesByStatement,
};
use super::derive_effects::derive_plan_effects;
use super::derive_requirements::derive_plan_requirements;
use super::invariants::validate_execution_plan;

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
        requirements,
        effects,
    };
    validate_execution_plan(&plan)?;
    Ok(plan)
}
