use crate::cel::CelEvaluator;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::{LixBackend, LixError, SqlDialect, Value};

use super::sql2::ast::nodes::Statement;
use super::sql2::contracts::effects::DetectedFileDomainChange;
use super::sql2::contracts::planned_statement::{
    MutationOperation, MutationRow, PlannedStatementSet, SchemaRegistration, UpdateValidationPlan,
};
use super::sql2::contracts::postprocess_actions::{
    PostprocessPlan, VtableDeletePlan, VtableUpdatePlan,
};

#[derive(Debug, Clone)]
pub(crate) struct LegacyRewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) params: Vec<Value>,
    pub(crate) registrations: Vec<SchemaRegistration>,
    pub(crate) postprocess: Option<PostprocessPlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

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
    let output = crate::sql::StatementPipeline::new(params, writer_key)
        .rewrite_statement(statement, provider)?;
    Ok(from_legacy_rewrite_output(output))
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
    let legacy_detected_file_domain_changes =
        to_legacy_detected_file_domain_changes(detected_file_domain_changes);
    let output = crate::sql::StatementPipeline::new(params, writer_key)
        .rewrite_statement_with_backend(
            backend,
            statement,
            provider,
            &legacy_detected_file_domain_changes,
        )
        .await?;
    Ok(from_legacy_rewrite_output(output))
}

pub(crate) fn legacy_inline_lix_functions_with_provider<P: LixFunctionProvider>(
    statement: Statement,
    provider: &mut P,
) -> Statement {
    crate::sql::inline_lix_functions_with_provider(statement, provider)
}

pub(crate) async fn legacy_materialize_vtable_insert_select_sources(
    backend: &dyn LixBackend,
    statements: &mut [Statement],
    params: &[Value],
) -> Result<(), LixError> {
    crate::sql::materialize_vtable_insert_select_sources(backend, statements, params).await
}

fn to_legacy_detected_file_domain_changes(
    detected_file_domain_changes: &[DetectedFileDomainChange],
) -> Vec<crate::sql::DetectedFileDomainChange> {
    detected_file_domain_changes
        .iter()
        .map(|change| crate::sql::DetectedFileDomainChange {
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
}

fn from_legacy_rewrite_output(output: crate::sql::RewriteOutput) -> LegacyRewriteOutput {
    LegacyRewriteOutput {
        statements: output.statements,
        params: output.params,
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

fn from_legacy_schema_registration(
    registration: crate::sql::SchemaRegistration,
) -> SchemaRegistration {
    SchemaRegistration {
        schema_key: registration.schema_key,
    }
}

fn from_legacy_postprocess_plan(plan: crate::sql::PostprocessPlan) -> PostprocessPlan {
    match plan {
        crate::sql::PostprocessPlan::VtableUpdate(update) => {
            PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: update.schema_key,
                explicit_writer_key: update.explicit_writer_key,
                writer_key_assignment_present: update.writer_key_assignment_present,
            })
        }
        crate::sql::PostprocessPlan::VtableDelete(delete) => {
            PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: delete.schema_key,
                effective_scope_fallback: delete.effective_scope_fallback,
                effective_scope_selection_sql: delete.effective_scope_selection_sql,
            })
        }
    }
}

fn from_legacy_mutation_row(row: crate::sql::MutationRow) -> MutationRow {
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

fn from_legacy_mutation_operation(operation: crate::sql::MutationOperation) -> MutationOperation {
    match operation {
        crate::sql::MutationOperation::Insert => MutationOperation::Insert,
        crate::sql::MutationOperation::Update => MutationOperation::Update,
        crate::sql::MutationOperation::Delete => MutationOperation::Delete,
    }
}

fn from_legacy_update_validation_plan(
    plan: crate::sql::UpdateValidationPlan,
) -> UpdateValidationPlan {
    UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}
