use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql as legacy_sql;
use crate::sql::{
    rewrite_read_query_with_backend_and_params_in_session as legacy_rewrite_read_query_with_backend_and_params_in_session,
    ReadRewriteSession as LegacyReadRewriteSession,
};
use crate::{LixBackend, LixError, SqlDialect, Value};
use sqlparser::ast::Query;

use super::sql2::ast::nodes::Statement;
use super::sql2::contracts::effects::DetectedFileDomainChange;
use super::sql2::contracts::planned_statement::{
    MutationOperation, MutationRow, PlannedStatementSet, SchemaRegistration, UpdateValidationPlan,
};
use super::sql2::contracts::postprocess_actions::{
    PostprocessPlan, VtableDeletePlan, VtableUpdatePlan,
};
use super::sql2::contracts::prepared_statement::PreparedStatement;

#[derive(Debug, Default, Clone)]
pub(crate) struct ReadRewriteSession {
    inner: LegacyReadRewriteSession,
}

impl ReadRewriteSession {
    pub(crate) fn cached_version_chain(&self, version_id: &str) -> Option<&[String]> {
        self.inner.cached_version_chain(version_id)
    }

    pub(crate) fn cache_version_chain(&mut self, version_id: String, chain: Vec<String>) {
        self.inner.cache_version_chain(version_id, chain);
    }
}

pub(crate) async fn rewrite_read_query_with_backend_and_params_in_session(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    session: &mut ReadRewriteSession,
) -> Result<Query, LixError> {
    legacy_rewrite_read_query_with_backend_and_params_in_session(
        backend,
        query,
        params,
        &mut session.inner,
    )
    .await
}

pub(crate) fn preprocess_statements_with_provider_to_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PlannedStatementSet, LixError> {
    let output =
        legacy_sql::preprocess_statements_with_provider(statements, params, provider, dialect)?;
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
    let sql_detected_file_domain_changes_by_statement =
        to_legacy_detected_file_domain_changes_by_statement(
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

fn from_legacy_preprocess_output(output: legacy_sql::PreprocessOutput) -> PlannedStatementSet {
    PlannedStatementSet {
        sql: output.sql,
        prepared_statements: output
            .prepared_statements
            .into_iter()
            .map(from_legacy_prepared_statement)
            .collect(),
        registrations: output
            .registrations
            .into_iter()
            .map(from_legacy_schema_registration)
            .collect(),
        postprocess: output.postprocess.map(from_legacy_postprocess_plan),
        mutations: output
            .mutations
            .into_iter()
            .map(from_legacy_mutation_row)
            .collect(),
        update_validations: output
            .update_validations
            .into_iter()
            .map(from_legacy_update_validation_plan)
            .collect(),
    }
}

fn from_legacy_prepared_statement(statement: legacy_sql::PreparedStatement) -> PreparedStatement {
    PreparedStatement {
        sql: statement.sql,
        params: statement.params,
    }
}

fn from_legacy_schema_registration(
    registration: legacy_sql::SchemaRegistration,
) -> SchemaRegistration {
    SchemaRegistration {
        schema_key: registration.schema_key,
    }
}

fn from_legacy_postprocess_plan(plan: legacy_sql::PostprocessPlan) -> PostprocessPlan {
    match plan {
        legacy_sql::PostprocessPlan::VtableUpdate(update) => {
            PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: update.schema_key,
                explicit_writer_key: update.explicit_writer_key,
                writer_key_assignment_present: update.writer_key_assignment_present,
            })
        }
        legacy_sql::PostprocessPlan::VtableDelete(delete) => {
            PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: delete.schema_key,
                effective_scope_fallback: delete.effective_scope_fallback,
                effective_scope_selection_sql: delete.effective_scope_selection_sql,
            })
        }
    }
}

fn from_legacy_mutation_row(row: legacy_sql::MutationRow) -> MutationRow {
    MutationRow {
        operation: from_legacy_mutation_operation(row.operation),
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        file_id: row.file_id,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        snapshot_content: row.snapshot_content,
        untracked: row.untracked,
    }
}

fn from_legacy_mutation_operation(operation: legacy_sql::MutationOperation) -> MutationOperation {
    match operation {
        legacy_sql::MutationOperation::Insert => MutationOperation::Insert,
        legacy_sql::MutationOperation::Update => MutationOperation::Update,
        legacy_sql::MutationOperation::Delete => MutationOperation::Delete,
    }
}

fn from_legacy_update_validation_plan(
    plan: legacy_sql::UpdateValidationPlan,
) -> UpdateValidationPlan {
    UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}
