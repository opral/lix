use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::sql::executor::contracts::planned_statement::PlannedStatementSet;
use crate::sql::executor::public_runtime::statement_references_public_surface_with_backend;
use crate::sql::logical_plan::{
    result_contract_for_statements, verify_logical_plan, InternalLogicalPlan, LogicalPlan,
};
use crate::sql::parser::parse_sql_statements;
use crate::sql::semantic_ir::prepare_internal_statements_with_backend_to_plan;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::Statement;

pub(crate) async fn preprocess_sql_to_plan(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<PlannedStatementSet, LixError> {
    let functions = SharedFunctionProvider::new(SystemFunctionProvider);
    preprocess_sql_with_provider(backend, evaluator, sql_text, params, functions).await
}

async fn preprocess_sql_with_provider<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    preprocess_sql_with_provider_and_writer_key(
        backend, evaluator, sql_text, params, functions, None,
    )
    .await
}

async fn preprocess_sql_with_provider_and_writer_key<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    preprocess_with_surfaces_to_plan(
        backend,
        evaluator,
        parse_sql_statements(sql_text)?,
        params,
        functions,
        writer_key,
    )
    .await
}

pub(crate) async fn preprocess_with_surfaces_to_plan<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    Ok(preprocess_with_surfaces_to_logical_plan(
        backend, evaluator, statements, params, functions, writer_key,
    )
    .await?
    .normalized_statements
    .into())
}

pub(crate) async fn preprocess_with_surfaces_to_logical_plan<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    writer_key: Option<&str>,
) -> Result<InternalLogicalPlan, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    reject_public_surface_statements_with_backend(backend, &statements).await?;
    let result_contract = result_contract_for_statements(&statements);
    let normalized_statements = prepare_internal_statements_with_backend_to_plan(
        backend, evaluator, statements, params, functions, writer_key,
    )
    .await?;
    let logical_plan = InternalLogicalPlan {
        normalized_statements,
        result_contract,
    };
    verify_logical_plan(&LogicalPlan::Internal(logical_plan.clone())).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "internal logical plan verification failed during preprocess: {}",
                error.message
            ),
        )
    })?;
    Ok(logical_plan)
}

async fn reject_public_surface_statements_with_backend(
    backend: &dyn LixBackend,
    statements: &[Statement],
) -> Result<(), LixError> {
    for statement in statements {
        if statement_references_public_surface_with_backend(backend, statement).await {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public surface statements must route through public lowering",
            ));
        }
    }
    Ok(())
}
