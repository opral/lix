use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::internal_state::{
    prepare_statements_sync_to_plan, prepare_statements_with_backend_to_plan,
};
use crate::query_runtime::contracts::planned_statement::PlannedStatementSet;
use crate::sql_shared::ast::parse_sql_statements;
use crate::{LixBackend, LixError, SqlDialect, Value};
use sqlparser::ast::Statement;

pub(crate) fn preprocess_statements_with_provider_to_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PlannedStatementSet, LixError> {
    Ok(
        prepare_statements_sync_to_plan(statements, params, provider, dialect, None)?.into(),
    )
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
    Ok(
        prepare_statements_with_backend_to_plan(
            backend,
            evaluator,
            statements,
            params,
            functions,
            writer_key,
        )
        .await?
        .into(),
    )
}
