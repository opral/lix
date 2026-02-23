use crate::sql;

use super::contracts::effects::DetectedFileDomainChange;
use super::contracts::planned_statement::{
    MutationOperation, MutationRow, PlannedStatementSet, SchemaRegistration, UpdateValidationPlan,
};
use super::contracts::postprocess_actions::{PostprocessPlan, VtableDeletePlan, VtableUpdatePlan};
use super::contracts::prepared_statement::PreparedStatement;

pub(crate) fn preprocess_plan_fingerprint(output: &PlannedStatementSet) -> String {
    let sql_output = to_sql_preprocess_output(output);
    sql::preprocess_plan_fingerprint(&sql_output)
}

pub(crate) fn from_sql_preprocess_output(output: sql::PreprocessOutput) -> PlannedStatementSet {
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

pub(crate) fn to_sql_preprocess_output(output: &PlannedStatementSet) -> sql::PreprocessOutput {
    sql::PreprocessOutput {
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
    statements: Vec<sql::PreparedStatement>,
) -> Vec<PreparedStatement> {
    statements
        .into_iter()
        .map(from_sql_prepared_statement)
        .collect()
}

pub(crate) fn from_sql_mutations(mutations: Vec<sql::MutationRow>) -> Vec<MutationRow> {
    mutations.into_iter().map(from_sql_mutation_row).collect()
}

pub(crate) fn from_sql_update_validations(
    plans: Vec<sql::UpdateValidationPlan>,
) -> Vec<UpdateValidationPlan> {
    plans
        .into_iter()
        .map(from_sql_update_validation_plan)
        .collect()
}

pub(crate) fn to_sql_mutations(mutations: &[MutationRow]) -> Vec<sql::MutationRow> {
    mutations.iter().cloned().map(to_sql_mutation_row).collect()
}

pub(crate) fn to_sql_update_validations(
    plans: &[UpdateValidationPlan],
) -> Vec<sql::UpdateValidationPlan> {
    plans
        .iter()
        .cloned()
        .map(to_sql_update_validation_plan)
        .collect()
}

pub(crate) fn to_sql_detected_file_domain_changes(
    changes: &[DetectedFileDomainChange],
) -> Vec<sql::DetectedFileDomainChange> {
    changes
        .iter()
        .cloned()
        .map(to_sql_detected_file_domain_change)
        .collect()
}

pub(crate) fn to_sql_detected_file_domain_changes_by_statement(
    changes_by_statement: &[Vec<DetectedFileDomainChange>],
) -> Vec<Vec<sql::DetectedFileDomainChange>> {
    changes_by_statement
        .iter()
        .map(|changes| to_sql_detected_file_domain_changes(changes))
        .collect()
}

pub(crate) fn from_sql_detected_file_domain_changes(
    changes: Vec<sql::DetectedFileDomainChange>,
) -> Vec<DetectedFileDomainChange> {
    changes
        .into_iter()
        .map(from_sql_detected_file_domain_change)
        .collect()
}

pub(crate) fn from_sql_detected_file_domain_changes_by_statement(
    changes_by_statement: Vec<Vec<sql::DetectedFileDomainChange>>,
) -> Vec<Vec<DetectedFileDomainChange>> {
    changes_by_statement
        .into_iter()
        .map(from_sql_detected_file_domain_changes)
        .collect()
}

pub(crate) fn to_sql_postprocess_plan(plan: &PostprocessPlan) -> sql::PostprocessPlan {
    match plan {
        PostprocessPlan::VtableUpdate(update) => {
            sql::PostprocessPlan::VtableUpdate(to_sql_vtable_update_plan(update))
        }
        PostprocessPlan::VtableDelete(delete) => {
            sql::PostprocessPlan::VtableDelete(to_sql_vtable_delete_plan(delete))
        }
    }
}

pub(crate) fn to_sql_vtable_update_plan(plan: &VtableUpdatePlan) -> sql::VtableUpdatePlan {
    sql::VtableUpdatePlan {
        schema_key: plan.schema_key.clone(),
        explicit_writer_key: plan.explicit_writer_key.clone(),
        writer_key_assignment_present: plan.writer_key_assignment_present,
    }
}

pub(crate) fn to_sql_vtable_delete_plan(plan: &VtableDeletePlan) -> sql::VtableDeletePlan {
    sql::VtableDeletePlan {
        schema_key: plan.schema_key.clone(),
        effective_scope_fallback: plan.effective_scope_fallback,
        effective_scope_selection_sql: plan.effective_scope_selection_sql.clone(),
    }
}

fn from_sql_prepared_statement(statement: sql::PreparedStatement) -> PreparedStatement {
    PreparedStatement {
        sql: statement.sql,
        params: statement.params,
    }
}

fn to_sql_prepared_statement(statement: PreparedStatement) -> sql::PreparedStatement {
    sql::PreparedStatement {
        sql: statement.sql,
        params: statement.params,
    }
}

fn from_sql_schema_registration(registration: sql::SchemaRegistration) -> SchemaRegistration {
    SchemaRegistration {
        schema_key: registration.schema_key,
    }
}

fn to_sql_schema_registration(registration: SchemaRegistration) -> sql::SchemaRegistration {
    sql::SchemaRegistration {
        schema_key: registration.schema_key,
    }
}

fn from_sql_postprocess_plan(plan: sql::PostprocessPlan) -> PostprocessPlan {
    match plan {
        sql::PostprocessPlan::VtableUpdate(update) => {
            PostprocessPlan::VtableUpdate(VtableUpdatePlan {
                schema_key: update.schema_key,
                explicit_writer_key: update.explicit_writer_key,
                writer_key_assignment_present: update.writer_key_assignment_present,
            })
        }
        sql::PostprocessPlan::VtableDelete(delete) => {
            PostprocessPlan::VtableDelete(VtableDeletePlan {
                schema_key: delete.schema_key,
                effective_scope_fallback: delete.effective_scope_fallback,
                effective_scope_selection_sql: delete.effective_scope_selection_sql,
            })
        }
    }
}

fn from_sql_mutation_row(row: sql::MutationRow) -> MutationRow {
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

fn to_sql_mutation_row(row: MutationRow) -> sql::MutationRow {
    sql::MutationRow {
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

fn from_sql_mutation_operation(operation: sql::MutationOperation) -> MutationOperation {
    match operation {
        sql::MutationOperation::Insert => MutationOperation::Insert,
        sql::MutationOperation::Update => MutationOperation::Update,
        sql::MutationOperation::Delete => MutationOperation::Delete,
    }
}

fn to_sql_mutation_operation(operation: MutationOperation) -> sql::MutationOperation {
    match operation {
        MutationOperation::Insert => sql::MutationOperation::Insert,
        MutationOperation::Update => sql::MutationOperation::Update,
        MutationOperation::Delete => sql::MutationOperation::Delete,
    }
}

fn from_sql_update_validation_plan(plan: sql::UpdateValidationPlan) -> UpdateValidationPlan {
    UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}

fn to_sql_update_validation_plan(plan: UpdateValidationPlan) -> sql::UpdateValidationPlan {
    sql::UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}

fn to_sql_detected_file_domain_change(
    change: DetectedFileDomainChange,
) -> sql::DetectedFileDomainChange {
    sql::DetectedFileDomainChange {
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
    change: sql::DetectedFileDomainChange,
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
