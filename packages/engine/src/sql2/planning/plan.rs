use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::{
    preprocess_parsed_statements_with_provider_and_detected_file_domain_changes,
    DetectedFileDomainChange,
};
use crate::LixBackend;
use crate::Value;

use super::super::ast::nodes::Statement;
use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::planner_error::PlannerError;
use super::super::type_bridge::from_sql_preprocess_output;
use super::derive_effects::derive_plan_effects;
use super::derive_requirements::derive_plan_requirements;
use super::invariants::validate_execution_plan;
use super::trace::plan_fingerprint;

pub(crate) async fn build_execution_plan<P>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    parsed_statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
    writer_key: Option<&str>,
) -> Result<ExecutionPlan, PlannerError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let preprocess = preprocess_parsed_statements_with_provider_and_detected_file_domain_changes(
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
    let preprocess = from_sql_preprocess_output(preprocess);

    let requirements = derive_plan_requirements(&parsed_statements);
    let effects = derive_plan_effects(&preprocess, writer_key)?;
    let fingerprint = plan_fingerprint(&preprocess);

    let plan = ExecutionPlan {
        preprocess,
        requirements,
        effects,
        fingerprint,
    };
    validate_execution_plan(&plan)?;
    Ok(plan)
}
