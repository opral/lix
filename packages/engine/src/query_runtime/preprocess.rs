use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::internal_state::{
    prepare_statements_sync_to_plan, prepare_statements_with_backend_to_plan,
};
use crate::query_runtime::contracts::planned_statement::PlannedStatementSet;
use crate::sql2::runtime::{
    statement_references_public_sql2_surface, statement_references_public_sql2_surface_with_backend,
};
use crate::sql_shared::ast::parse_sql_statements;
use crate::{LixBackend, LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

pub(crate) fn preprocess_statements_with_provider_to_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PlannedStatementSet, LixError> {
    reject_public_surface_statements(&statements)?;
    Ok(prepare_statements_sync_to_plan(statements, params, provider, dialect, None)?.into())
}

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
    reject_public_surface_statements_with_backend(backend, &statements).await?;
    Ok(prepare_statements_with_backend_to_plan(
        backend, evaluator, statements, params, functions, writer_key,
    )
    .await?
    .into())
}

fn reject_public_surface_statements(statements: &[Statement]) -> Result<(), LixError> {
    if statements
        .iter()
        .any(statement_references_public_sql2_surface)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public surface statements must route through sql2",
        ));
    }
    Ok(())
}

async fn reject_public_surface_statements_with_backend(
    backend: &dyn LixBackend,
    statements: &[Statement],
) -> Result<(), LixError> {
    for statement in statements {
        if statement_references_public_sql2_surface_with_backend(backend, statement).await {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public surface statements must route through sql2",
            ));
        }
    }
    Ok(())
}
