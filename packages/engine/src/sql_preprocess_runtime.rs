use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::{LixBackend, LixError, SqlDialect, Value};

use super::sql2::ast::nodes::Statement;
use super::sql2::contracts::effects::DetectedFileDomainChange;
use super::sql2::contracts::planned_statement::PlannedStatementSet;

pub(crate) type LegacyRewriteOutput = super::super::sql::Sql2RewriteOutput;

pub(crate) fn preprocess_statements_with_provider_to_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PlannedStatementSet, LixError> {
    super::sql2::planning::preprocess::preprocess_statements_with_provider_to_plan(
        statements, params, provider, dialect,
    )
}

pub(crate) async fn preprocess_sql_to_plan(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<PlannedStatementSet, LixError> {
    super::sql2::planning::preprocess::preprocess_sql_to_plan(backend, evaluator, sql_text, params)
        .await
}

pub(crate) async fn preprocess_with_surfaces_to_plan<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &[Vec<DetectedFileDomainChange>],
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    super::sql2::planning::preprocess::preprocess_with_surfaces_to_plan(
        backend,
        evaluator,
        statements,
        params,
        functions,
        detected_file_domain_changes_by_statement,
        writer_key,
    )
    .await
}

pub(crate) fn legacy_rewrite_statement_with_provider<P: LixFunctionProvider>(
    params: &[Value],
    writer_key: Option<&str>,
    statement: Statement,
    provider: &mut P,
) -> Result<LegacyRewriteOutput, LixError> {
    super::super::sql::rewrite_statement_with_provider_to_sql2(
        params, writer_key, statement, provider,
    )
}

pub(crate) async fn legacy_rewrite_statement_with_backend<P>(
    backend: &dyn LixBackend,
    params: &[Value],
    writer_key: Option<&str>,
    statement: Statement,
    provider: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> Result<LegacyRewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    super::super::sql::rewrite_statement_with_backend_to_sql2(
        backend,
        params,
        writer_key,
        statement,
        provider,
        detected_file_domain_changes,
    )
    .await
}

pub(crate) fn legacy_inline_lix_functions_with_provider<P: LixFunctionProvider>(
    statement: Statement,
    provider: &mut P,
) -> Statement {
    super::super::sql::inline_lix_functions_with_provider_for_sql2(statement, provider)
}

pub(crate) async fn legacy_materialize_vtable_insert_select_sources(
    backend: &dyn LixBackend,
    statements: &mut [Statement],
    params: &[Value],
) -> Result<(), LixError> {
    super::super::sql::materialize_vtable_insert_select_sources_for_sql2(
        backend, statements, params,
    )
    .await
}
