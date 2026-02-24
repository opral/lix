use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql as legacy_sql;
use crate::{LixBackend, LixError, SqlDialect, Value};

use super::super::ast::nodes::Statement;
use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::legacy_sql::from_legacy_preprocess_output;
use super::super::contracts::planned_statement::PlannedStatementSet;

pub(crate) fn preprocess_statements_with_provider_to_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PlannedStatementSet, LixError> {
    let output = legacy_sql::preprocess_statements_with_provider(statements, params, provider, dialect)?;
    Ok(from_legacy_preprocess_output(output))
}

pub(crate) async fn preprocess_sql_to_plan(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<PlannedStatementSet, LixError> {
    let output = legacy_sql::preprocess_sql(backend, evaluator, sql_text, params).await?;
    Ok(from_legacy_preprocess_output(output))
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
    let sql_detected_file_domain_changes_by_statement = to_legacy_detected_file_domain_changes_by_statement(
        detected_file_domain_changes_by_statement,
    );
    let output = legacy_sql::preprocess_parsed_statements_with_provider_and_detected_file_domain_changes(
        backend,
        evaluator,
        statements,
        params,
        functions,
        &sql_detected_file_domain_changes_by_statement,
        writer_key,
    )
    .await?;
    Ok(from_legacy_preprocess_output(output))
}

fn to_legacy_detected_file_domain_changes_by_statement(
    changes_by_statement: &[Vec<DetectedFileDomainChange>],
) -> Vec<Vec<legacy_sql::DetectedFileDomainChange>> {
    changes_by_statement
        .iter()
        .map(|changes| {
            changes
                .iter()
                .map(|change| legacy_sql::DetectedFileDomainChange {
                    entity_id: change.entity_id.clone(),
                    schema_key: change.schema_key.clone(),
                    schema_version: change.schema_version.clone(),
                    file_id: change.file_id.clone(),
                    version_id: change.version_id.clone(),
                    plugin_key: change.plugin_key.clone(),
                    snapshot_content: change.snapshot_content.clone(),
                    metadata: change.metadata.clone(),
                    writer_key: change.writer_key.clone(),
                })
                .collect()
        })
        .collect()
}
