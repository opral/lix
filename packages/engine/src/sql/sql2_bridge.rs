use crate::engine::sql2::contracts::effects::DetectedFileDomainChange as Sql2DetectedFileDomainChange;
use crate::engine::sql2::contracts::planned_statement::{
    MutationOperation as Sql2MutationOperation, MutationRow as Sql2MutationRow,
    SchemaRegistration as Sql2SchemaRegistration, UpdateValidationPlan as Sql2UpdateValidationPlan,
};
use crate::engine::sql2::contracts::postprocess_actions::{
    PostprocessPlan as Sql2PostprocessPlan, VtableDeletePlan as Sql2VtableDeletePlan,
    VtableUpdatePlan as Sql2VtableUpdatePlan,
};
use crate::functions::LixFunctionProvider;
use crate::{LixBackend, LixError, Value};

use crate::engine::sql2::ast::nodes::Statement;

#[derive(Debug, Clone)]
pub(crate) struct Sql2RewriteOutput {
    pub(crate) statements: Vec<Statement>,
    pub(crate) params: Vec<Value>,
    pub(crate) registrations: Vec<Sql2SchemaRegistration>,
    pub(crate) postprocess: Option<Sql2PostprocessPlan>,
    pub(crate) mutations: Vec<Sql2MutationRow>,
    pub(crate) update_validations: Vec<Sql2UpdateValidationPlan>,
}

pub(crate) fn rewrite_statement_with_provider_to_sql2<P: LixFunctionProvider>(
    params: &[Value],
    writer_key: Option<&str>,
    statement: Statement,
    provider: &mut P,
) -> Result<Sql2RewriteOutput, LixError> {
    let output = super::StatementPipeline::new(params, writer_key).rewrite_statement(statement, provider)?;
    Ok(from_rewrite_output(output))
}

pub(crate) async fn rewrite_statement_with_backend_to_sql2<P: LixFunctionProvider>(
    backend: &dyn LixBackend,
    params: &[Value],
    writer_key: Option<&str>,
    statement: Statement,
    provider: &mut P,
    detected_file_domain_changes: &[Sql2DetectedFileDomainChange],
) -> Result<Sql2RewriteOutput, LixError>
where
    P: LixFunctionProvider + Clone + Send + 'static,
{
    let legacy_detected_changes = to_legacy_detected_file_domain_changes(detected_file_domain_changes);
    let output = super::StatementPipeline::new(params, writer_key)
        .rewrite_statement_with_backend(backend, statement, provider, &legacy_detected_changes)
        .await?;
    Ok(from_rewrite_output(output))
}

pub(crate) fn inline_lix_functions_with_provider_for_sql2<P: LixFunctionProvider>(
    statement: Statement,
    provider: &mut P,
) -> Statement {
    super::inline_lix_functions_with_provider(statement, provider)
}

pub(crate) async fn materialize_vtable_insert_select_sources_for_sql2(
    backend: &dyn LixBackend,
    statements: &mut [Statement],
    params: &[Value],
) -> Result<(), LixError> {
    super::materialize_vtable_insert_select_sources(backend, statements, params).await
}

fn to_legacy_detected_file_domain_changes(
    changes: &[Sql2DetectedFileDomainChange],
) -> Vec<super::DetectedFileDomainChange> {
    changes
        .iter()
        .map(|change| super::DetectedFileDomainChange {
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

fn from_rewrite_output(output: super::RewriteOutput) -> Sql2RewriteOutput {
    Sql2RewriteOutput {
        statements: output.statements,
        params: output.params,
        registrations: output
            .registrations
            .into_iter()
            .map(from_schema_registration)
            .collect(),
        postprocess: output.postprocess.map(from_postprocess_plan),
        mutations: output
            .mutations
            .into_iter()
            .map(from_mutation_row)
            .collect(),
        update_validations: output
            .update_validations
            .into_iter()
            .map(from_update_validation_plan)
        .collect(),
    }
}

fn from_schema_registration(registration: super::SchemaRegistration) -> Sql2SchemaRegistration {
    Sql2SchemaRegistration {
        schema_key: registration.schema_key,
    }
}

fn from_postprocess_plan(plan: super::PostprocessPlan) -> Sql2PostprocessPlan {
    match plan {
        super::PostprocessPlan::VtableUpdate(update) => {
            Sql2PostprocessPlan::VtableUpdate(Sql2VtableUpdatePlan {
                schema_key: update.schema_key,
                explicit_writer_key: update.explicit_writer_key,
                writer_key_assignment_present: update.writer_key_assignment_present,
            })
        }
        super::PostprocessPlan::VtableDelete(delete) => {
            Sql2PostprocessPlan::VtableDelete(Sql2VtableDeletePlan {
                schema_key: delete.schema_key,
                effective_scope_fallback: delete.effective_scope_fallback,
                effective_scope_selection_sql: delete.effective_scope_selection_sql,
            })
        }
    }
}

fn from_mutation_row(row: super::MutationRow) -> Sql2MutationRow {
    Sql2MutationRow {
        operation: from_mutation_operation(row.operation),
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

fn from_mutation_operation(operation: super::MutationOperation) -> Sql2MutationOperation {
    match operation {
        super::MutationOperation::Insert => Sql2MutationOperation::Insert,
        super::MutationOperation::Update => Sql2MutationOperation::Update,
        super::MutationOperation::Delete => Sql2MutationOperation::Delete,
    }
}

fn from_update_validation_plan(plan: super::UpdateValidationPlan) -> Sql2UpdateValidationPlan {
    Sql2UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}
