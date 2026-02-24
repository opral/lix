use crate::sql as legacy_sql;

use super::super::planned_statement::{
    MutationOperation, MutationRow, PlannedStatementSet, SchemaRegistration, UpdateValidationPlan,
};
use super::super::postprocess_actions::{PostprocessPlan, VtableDeletePlan, VtableUpdatePlan};
use super::super::prepared_statement::PreparedStatement;

pub(crate) fn preprocess_plan_fingerprint(output: &PlannedStatementSet) -> String {
    let legacy_output = to_legacy_preprocess_output(output);
    legacy_sql::preprocess_plan_fingerprint(&legacy_output)
}

pub(crate) fn from_legacy_preprocess_output(
    output: legacy_sql::PreprocessOutput,
) -> PlannedStatementSet {
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

pub(super) fn to_legacy_preprocess_output(
    output: &PlannedStatementSet,
) -> legacy_sql::PreprocessOutput {
    legacy_sql::PreprocessOutput {
        sql: output.sql.clone(),
        prepared_statements: output
            .prepared_statements
            .iter()
            .cloned()
            .map(to_legacy_prepared_statement)
            .collect(),
        registrations: output
            .registrations
            .iter()
            .cloned()
            .map(to_legacy_schema_registration)
            .collect(),
        postprocess: output.postprocess.as_ref().map(to_legacy_postprocess_plan),
        mutations: output
            .mutations
            .iter()
            .cloned()
            .map(to_legacy_mutation_row)
            .collect(),
        update_validations: output
            .update_validations
            .iter()
            .cloned()
            .map(to_legacy_update_validation_plan)
            .collect(),
    }
}

fn from_legacy_prepared_statement(statement: legacy_sql::PreparedStatement) -> PreparedStatement {
    PreparedStatement {
        sql: statement.sql,
        params: statement.params,
    }
}

fn to_legacy_prepared_statement(statement: PreparedStatement) -> legacy_sql::PreparedStatement {
    legacy_sql::PreparedStatement {
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

fn to_legacy_schema_registration(registration: SchemaRegistration) -> legacy_sql::SchemaRegistration {
    legacy_sql::SchemaRegistration {
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

fn to_legacy_postprocess_plan(plan: &PostprocessPlan) -> legacy_sql::PostprocessPlan {
    match plan {
        PostprocessPlan::VtableUpdate(update) => {
            legacy_sql::PostprocessPlan::VtableUpdate(legacy_sql::VtableUpdatePlan {
                schema_key: update.schema_key.clone(),
                explicit_writer_key: update.explicit_writer_key.clone(),
                writer_key_assignment_present: update.writer_key_assignment_present,
            })
        }
        PostprocessPlan::VtableDelete(delete) => {
            legacy_sql::PostprocessPlan::VtableDelete(legacy_sql::VtableDeletePlan {
                schema_key: delete.schema_key.clone(),
                effective_scope_fallback: delete.effective_scope_fallback,
                effective_scope_selection_sql: delete.effective_scope_selection_sql.clone(),
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

fn to_legacy_mutation_row(row: MutationRow) -> legacy_sql::MutationRow {
    legacy_sql::MutationRow {
        operation: to_legacy_mutation_operation(row.operation),
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

fn to_legacy_mutation_operation(operation: MutationOperation) -> legacy_sql::MutationOperation {
    match operation {
        MutationOperation::Insert => legacy_sql::MutationOperation::Insert,
        MutationOperation::Update => legacy_sql::MutationOperation::Update,
        MutationOperation::Delete => legacy_sql::MutationOperation::Delete,
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

fn to_legacy_update_validation_plan(
    plan: UpdateValidationPlan,
) -> legacy_sql::UpdateValidationPlan {
    legacy_sql::UpdateValidationPlan {
        table: plan.table,
        where_clause: plan.where_clause,
        snapshot_content: plan.snapshot_content,
        snapshot_patch: plan.snapshot_patch,
    }
}
