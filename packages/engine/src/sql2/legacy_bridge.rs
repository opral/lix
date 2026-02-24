use crate::sql as legacy_sql;
use crate::{LixBackend, LixError, LixTransaction, Value};
use crate::{deterministic_mode::RuntimeFunctionProvider, functions::SharedFunctionProvider};
use crate::cel::CelEvaluator;
use crate::functions::LixFunctionProvider;
use crate::SqlDialect;
use sqlparser::ast::{Query, Update};

use super::ast::nodes::Statement;
use super::ast::utils::PlaceholderState;
use super::contracts::effects::DetectedFileDomainChange;
use super::contracts::planned_statement::{
    MutationOperation, MutationRow, PlannedStatementSet, SchemaRegistration, UpdateValidationPlan,
};
use super::contracts::postprocess_actions::{PostprocessPlan, VtableDeletePlan, VtableUpdatePlan};
use super::contracts::prepared_statement::PreparedStatement;

pub(crate) fn preprocess_plan_fingerprint(output: &PlannedStatementSet) -> String {
    let sql_output = to_sql_preprocess_output(output);
    legacy_sql::preprocess_plan_fingerprint(&sql_output)
}

pub(crate) type SqlBridgeReadRewriteSession = legacy_sql::ReadRewriteSession;
pub(crate) type SqlBridgeDetectedFileDomainChange = legacy_sql::DetectedFileDomainChange;

pub(crate) fn preprocess_statements_with_provider_with_sql_bridge<P: LixFunctionProvider>(
    statements: Vec<Statement>,
    params: &[Value],
    provider: &mut P,
    dialect: SqlDialect,
) -> Result<legacy_sql::PreprocessOutput, LixError> {
    legacy_sql::preprocess_statements_with_provider(statements, params, provider, dialect)
}

pub(crate) async fn preprocess_sql_with_sql_bridge(
    backend: &dyn LixBackend,
    evaluator: &CelEvaluator,
    sql_text: &str,
    params: &[Value],
) -> Result<legacy_sql::PreprocessOutput, LixError> {
    legacy_sql::preprocess_sql(backend, evaluator, sql_text, params).await
}

pub(crate) fn lower_statement_with_sql_bridge(
    statement: Statement,
    dialect: SqlDialect,
) -> Result<Statement, LixError> {
    legacy_sql::lower_statement(statement, dialect)
}

pub(crate) async fn rewrite_read_query_with_backend_and_params_in_session_with_sql_bridge(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
    session: &mut SqlBridgeReadRewriteSession,
) -> Result<Query, LixError> {
    legacy_sql::rewrite_read_query_with_backend_and_params_in_session(backend, query, params, session)
        .await
}

pub(crate) struct FilesystemUpdateSideEffects {
    pub(crate) tracked_directory_changes: Vec<DetectedFileDomainChange>,
    pub(crate) untracked_directory_changes: Vec<DetectedFileDomainChange>,
}

pub(crate) async fn collect_filesystem_update_side_effects_with_sql_bridge(
    backend: &dyn LixBackend,
    update: &Update,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Result<FilesystemUpdateSideEffects, LixError> {
    let side_effects = crate::filesystem::mutation_rewrite::update_side_effects_with_backend(
        backend,
        update,
        params,
        placeholder_state,
    )
    .await?;
    Ok(FilesystemUpdateSideEffects {
        tracked_directory_changes: from_sql_detected_file_domain_changes(
            side_effects.tracked_directory_changes,
        ),
        untracked_directory_changes: from_sql_detected_file_domain_changes(
            side_effects.untracked_directory_changes,
        ),
    })
}

pub(crate) async fn preprocess_with_sql_surfaces<P: LixFunctionProvider>(
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
        to_sql_detected_file_domain_changes_by_statement(detected_file_domain_changes_by_statement);
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
    Ok(from_sql_preprocess_output(output))
}

pub(crate) async fn build_update_followup_statements_with_sql_bridge(
    transaction: &mut dyn LixTransaction,
    plan: &VtableUpdatePlan,
    rows: &[Vec<Value>],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let sql_plan = to_sql_vtable_update_plan(plan);
    let sql_detected_file_domain_changes =
        to_sql_detected_file_domain_changes(detected_file_domain_changes);
    let statements = legacy_sql::build_update_followup_sql(
        transaction,
        &sql_plan,
        rows,
        &sql_detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await?;
    Ok(from_sql_prepared_statements(statements))
}

pub(crate) async fn build_delete_followup_statements_with_sql_bridge(
    transaction: &mut dyn LixTransaction,
    plan: &VtableDeletePlan,
    rows: &[Vec<Value>],
    params: &[Value],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let sql_plan = to_sql_vtable_delete_plan(plan);
    let sql_detected_file_domain_changes =
        to_sql_detected_file_domain_changes(detected_file_domain_changes);
    let statements = legacy_sql::build_delete_followup_sql(
        transaction,
        &sql_plan,
        rows,
        params,
        &sql_detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await?;
    Ok(from_sql_prepared_statements(statements))
}

pub(crate) fn from_sql_preprocess_output(output: legacy_sql::PreprocessOutput) -> PlannedStatementSet {
    PlannedStatementSet {
        sql: output.sql,
        prepared_statements: output
            .prepared_statements
            .into_iter()
            .map(from_sql_prepared_statement)
            .collect(),
        registrations: output
            .registrations
            .into_iter()
            .map(from_sql_schema_registration)
            .collect(),
        postprocess: output.postprocess.map(from_sql_postprocess_plan),
        mutations: output
            .mutations
            .into_iter()
            .map(from_sql_mutation_row)
            .collect(),
        update_validations: output
            .update_validations
            .into_iter()
            .map(from_sql_update_validation_plan)
            .collect(),
    }
}

pub(crate) fn to_sql_preprocess_output(output: &PlannedStatementSet) -> legacy_sql::PreprocessOutput {
    legacy_sql::PreprocessOutput {
        sql: output.sql.clone(),
        prepared_statements: output
            .prepared_statements
            .iter()
            .cloned()
            .map(to_sql_prepared_statement)
            .collect(),
        registrations: output
            .registrations
            .iter()
            .cloned()
            .map(to_sql_schema_registration)
            .collect(),
        postprocess: output.postprocess.as_ref().map(to_sql_postprocess_plan),
        mutations: output
            .mutations
            .iter()
            .cloned()
            .map(to_sql_mutation_row)
            .collect(),
        update_validations: output
            .update_validations
            .iter()
            .cloned()
            .map(to_sql_update_validation_plan)
            .collect(),
    }
}

pub(crate) fn from_sql_prepared_statements(
    statements: Vec<legacy_sql::PreparedStatement>,
) -> Vec<PreparedStatement> {
    statements
        .into_iter()
        .map(from_sql_prepared_statement)
        .collect()
}

pub(crate) fn from_sql_mutations(mutations: Vec<legacy_sql::MutationRow>) -> Vec<MutationRow> {
    mutations.into_iter().map(from_sql_mutation_row).collect()
}

pub(crate) fn from_sql_update_validations(
    plans: Vec<legacy_sql::UpdateValidationPlan>,
) -> Vec<UpdateValidationPlan> {
    plans
        .into_iter()
        .map(from_sql_update_validation_plan)
        .collect()
}

pub(crate) fn to_sql_mutations(mutations: &[MutationRow]) -> Vec<legacy_sql::MutationRow> {
    mutations.iter().cloned().map(to_sql_mutation_row).collect()
}

pub(crate) fn to_sql_update_validations(
    plans: &[UpdateValidationPlan],
) -> Vec<legacy_sql::UpdateValidationPlan> {
    plans
        .iter()
        .cloned()
        .map(to_sql_update_validation_plan)
        .collect()
}

pub(crate) fn to_sql_detected_file_domain_changes(
    changes: &[DetectedFileDomainChange],
) -> Vec<legacy_sql::DetectedFileDomainChange> {
    changes
        .iter()
        .cloned()
        .map(to_sql_detected_file_domain_change)
        .collect()
}

pub(crate) fn to_sql_detected_file_domain_changes_by_statement(
    changes_by_statement: &[Vec<DetectedFileDomainChange>],
) -> Vec<Vec<legacy_sql::DetectedFileDomainChange>> {
    changes_by_statement
        .iter()
        .map(|changes| to_sql_detected_file_domain_changes(changes))
        .collect()
}

pub(crate) fn from_sql_detected_file_domain_changes(
    changes: Vec<legacy_sql::DetectedFileDomainChange>,
) -> Vec<DetectedFileDomainChange> {
    changes
        .into_iter()
        .map(from_sql_detected_file_domain_change)
        .collect()
}

pub(crate) fn from_sql_detected_file_domain_changes_by_statement(
    changes_by_statement: Vec<Vec<legacy_sql::DetectedFileDomainChange>>,
) -> Vec<Vec<DetectedFileDomainChange>> {
    changes_by_statement
        .into_iter()
        .map(from_sql_detected_file_domain_changes)
        .collect()
}

pub(crate) fn to_sql_postprocess_plan(plan: &PostprocessPlan) -> legacy_sql::PostprocessPlan {
    match plan {
        PostprocessPlan::VtableUpdate(update) => {
            legacy_sql::PostprocessPlan::VtableUpdate(to_sql_vtable_update_plan(update))
        }
        PostprocessPlan::VtableDelete(delete) => {
            legacy_sql::PostprocessPlan::VtableDelete(to_sql_vtable_delete_plan(delete))
        }
    }
}

pub(crate) fn to_sql_vtable_update_plan(plan: &VtableUpdatePlan) -> legacy_sql::VtableUpdatePlan {
    legacy_sql::VtableUpdatePlan {
        schema_key: plan.schema_key.clone(),
        explicit_writer_key: plan.explicit_writer_key.clone(),
        writer_key_assignment_present: plan.writer_key_assignment_present,
    }
}

pub(crate) fn to_sql_vtable_delete_plan(plan: &VtableDeletePlan) -> legacy_sql::VtableDeletePlan {
    legacy_sql::VtableDeletePlan {
        schema_key: plan.schema_key.clone(),
        effective_scope_fallback: plan.effective_scope_fallback,
        effective_scope_selection_sql: plan.effective_scope_selection_sql.clone(),
    }
}

fn from_sql_prepared_statement(statement: legacy_sql::PreparedStatement) -> PreparedStatement {
    PreparedStatement {
        sql: statement.sql,
        params: statement.params,
    }
}

fn to_sql_prepared_statement(statement: PreparedStatement) -> legacy_sql::PreparedStatement {
    legacy_sql::PreparedStatement {
        sql: statement.sql,
        params: statement.params,
    }
}

fn from_sql_schema_registration(registration: legacy_sql::SchemaRegistration) -> SchemaRegistration {
    SchemaRegistration {
        schema_key: registration.schema_key,
    }
}

fn to_sql_schema_registration(registration: SchemaRegistration) -> legacy_sql::SchemaRegistration {
    legacy_sql::SchemaRegistration {
        schema_key: registration.schema_key,
    }
}

fn from_sql_postprocess_plan(plan: legacy_sql::PostprocessPlan) -> PostprocessPlan {
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

fn from_sql_mutation_row(row: legacy_sql::MutationRow) -> MutationRow {
    MutationRow {
        operation: from_sql_mutation_operation(row.operation),
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

fn to_sql_mutation_row(row: MutationRow) -> legacy_sql::MutationRow {
    legacy_sql::MutationRow {
        operation: to_sql_mutation_operation(row.operation),
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

fn from_sql_mutation_operation(operation: legacy_sql::MutationOperation) -> MutationOperation {
    match operation {
        legacy_sql::MutationOperation::Insert => MutationOperation::Insert,
        legacy_sql::MutationOperation::Update => MutationOperation::Update,
        legacy_sql::MutationOperation::Delete => MutationOperation::Delete,
    }
}

fn to_sql_mutation_operation(operation: MutationOperation) -> legacy_sql::MutationOperation {
    match operation {
        MutationOperation::Insert => legacy_sql::MutationOperation::Insert,
        MutationOperation::Update => legacy_sql::MutationOperation::Update,
        MutationOperation::Delete => legacy_sql::MutationOperation::Delete,
    }
}

fn from_sql_update_validation_plan(plan: legacy_sql::UpdateValidationPlan) -> UpdateValidationPlan {
    UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}

fn to_sql_update_validation_plan(plan: UpdateValidationPlan) -> legacy_sql::UpdateValidationPlan {
    legacy_sql::UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}

fn to_sql_detected_file_domain_change(
    change: DetectedFileDomainChange,
) -> legacy_sql::DetectedFileDomainChange {
    legacy_sql::DetectedFileDomainChange {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        file_id: change.file_id,
        version_id: change.version_id,
        plugin_key: change.plugin_key,
        snapshot_content: change.snapshot_content,
        metadata: change.metadata,
        writer_key: change.writer_key,
    }
}

fn from_sql_detected_file_domain_change(
    change: legacy_sql::DetectedFileDomainChange,
) -> DetectedFileDomainChange {
    DetectedFileDomainChange {
        entity_id: change.entity_id,
        schema_key: change.schema_key,
        schema_version: change.schema_version,
        file_id: change.file_id,
        version_id: change.version_id,
        plugin_key: change.plugin_key,
        snapshot_content: change.snapshot_content,
        metadata: change.metadata,
        writer_key: change.writer_key,
    }
}
