use crate::cel::CelEvaluator;
use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};

use super::sql2::ast::nodes::Statement;
use super::sql2::contracts::effects::DetectedFileDomainChange;
use super::sql2::contracts::planned_statement::PlannedStatementSet;
use super::sql2::planning::rewrite_output::StatementRewriteOutput;

pub(crate) async fn preprocess_sql_to_plan(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<PlannedStatementSet, LixError> {
    super::sql2::planning::preprocess::preprocess_sql_to_plan(backend, evaluator, sql_text, params)
        .await
}

pub(crate) fn legacy_rewrite_statement_with_provider<P: LixFunctionProvider>(
    params: &[Value],
    writer_key: Option<&str>,
    statement: Statement,
    provider: &mut P,
) -> Result<StatementRewriteOutput, LixError> {
    super::sql_rewrite_runtime::rewrite_statement_with_provider(
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
) -> Result<StatementRewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    super::sql_rewrite_runtime::rewrite_statement_with_backend(
        backend,
        params,
        writer_key,
        statement,
        provider,
        detected_file_domain_changes,
    )
    .await
}
