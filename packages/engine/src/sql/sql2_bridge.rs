use crate::cel::CelEvaluator;
use crate::engine::sql2::contracts::effects::DetectedFileDomainChange as Sql2DetectedFileDomainChange;
use crate::engine::sql2::contracts::planned_statement::{
    MutationOperation as Sql2MutationOperation, MutationRow as Sql2MutationRow,
    PlannedStatementSet, SchemaRegistration as Sql2SchemaRegistration,
    UpdateValidationPlan as Sql2UpdateValidationPlan,
};
use crate::engine::sql2::contracts::postprocess_actions::{
    PostprocessPlan as Sql2PostprocessPlan, VtableDeletePlan as Sql2VtableDeletePlan,
    VtableUpdatePlan as Sql2VtableUpdatePlan,
};
use crate::engine::sql2::contracts::prepared_statement::PreparedStatement as Sql2PreparedStatement;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::{LixBackend, LixError, SqlDialect, Value};

use crate::engine::sql2::ast::nodes::Statement;

pub(crate) fn preprocess_statements_with_provider_to_sql2_plan<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<PlannedStatementSet, LixError> {
    let output = super::preprocess_statements_with_provider(statements, params, provider, dialect)?;
    Ok(from_preprocess_output(output))
}

pub(crate) async fn preprocess_sql_to_sql2_plan(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<PlannedStatementSet, LixError> {
    let output = super::preprocess_sql(backend, evaluator, sql_text, params).await?;
    Ok(from_preprocess_output(output))
}

pub(crate) async fn preprocess_parsed_statements_with_provider_and_detected_file_domain_changes_to_sql2_plan<
    P: LixFunctionProvider,
>(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    statements: Vec<Statement>,
    params: &[Value],
    functions: SharedFunctionProvider<P>,
    detected_file_domain_changes_by_statement: &[Vec<Sql2DetectedFileDomainChange>],
    writer_key: Option<&str>,
) -> Result<PlannedStatementSet, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let legacy_detected_changes = to_legacy_detected_file_domain_changes_by_statement(
        detected_file_domain_changes_by_statement,
    );
    let output =
        super::preprocess_parsed_statements_with_provider_and_detected_file_domain_changes(
            backend,
            evaluator,
            statements,
            params,
            functions,
            &legacy_detected_changes,
            writer_key,
        )
        .await?;
    Ok(from_preprocess_output(output))
}

fn to_legacy_detected_file_domain_changes_by_statement(
    changes_by_statement: &[Vec<Sql2DetectedFileDomainChange>],
) -> Vec<Vec<super::DetectedFileDomainChange>> {
    changes_by_statement
        .iter()
        .map(|changes| {
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
        })
        .collect()
}

fn from_preprocess_output(output: super::PreprocessOutput) -> PlannedStatementSet {
    PlannedStatementSet {
        sql: output.sql,
        prepared_statements: output
            .prepared_statements
            .into_iter()
            .map(from_prepared_statement)
            .collect(),
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

fn from_prepared_statement(statement: super::PreparedStatement) -> Sql2PreparedStatement {
    Sql2PreparedStatement {
        sql: statement.sql,
        params: statement.params,
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
