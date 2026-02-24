use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::{LixBackend, LixError, SqlDialect, Value};

use super::sql2::ast::nodes::Statement;
use super::sql2::contracts::effects::DetectedFileDomainChange;
use super::sql2::contracts::planned_statement::PlannedStatementSet;
use super::sql2::planning::rewrite_output::StatementRewriteOutput;

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
) -> Result<StatementRewriteOutput, LixError> {
    let output = super::super::sql::rewrite_statement_with_provider_to_sql2(
        params, writer_key, statement, provider,
    )?;
    Ok(from_legacy_rewrite_output(output))
}

pub(crate) async fn legacy_rewrite_statement_with_backend<P>(
    backend: &dyn LixBackend,
    params: &[Value],
    writer_key: Option<&str>,
    statement: Statement,
    provider: &mut P,
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> Result<StatementRewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let output = super::super::sql::rewrite_statement_with_backend_to_sql2(
        backend,
        params,
        writer_key,
        statement,
        provider,
        detected_file_domain_changes,
    )
    .await?;
    Ok(from_legacy_rewrite_output(output))
}

fn from_legacy_rewrite_output(
    output: super::super::sql::Sql2RewriteOutput,
) -> StatementRewriteOutput {
    StatementRewriteOutput {
        statements: output.statements,
        params: output.params,
        registrations: output.registrations,
        postprocess: output.postprocess,
        mutations: output.mutations,
        update_validations: output.update_validations,
    }
}
