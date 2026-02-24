use crate::sql as legacy_sql;
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::SharedFunctionProvider;
use crate::{LixError, LixTransaction, Value};

use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::postprocess_actions::{VtableDeletePlan, VtableUpdatePlan};
use super::super::contracts::prepared_statement::PreparedStatement;

pub(crate) async fn build_update_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableUpdatePlan,
    rows: &[Vec<Value>],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let legacy_plan = to_legacy_vtable_update_plan(plan);
    let legacy_changes = to_legacy_detected_file_domain_changes(detected_file_domain_changes);
    let statements = legacy_sql::build_update_followup_sql(
        transaction,
        &legacy_plan,
        rows,
        &legacy_changes,
        writer_key,
        functions,
    )
    .await?;
    Ok(from_legacy_prepared_statements(statements))
}

pub(crate) async fn build_delete_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableDeletePlan,
    rows: &[Vec<Value>],
    params: &[Value],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let legacy_plan = to_legacy_vtable_delete_plan(plan);
    let legacy_changes = to_legacy_detected_file_domain_changes(detected_file_domain_changes);
    let statements = legacy_sql::build_delete_followup_sql(
        transaction,
        &legacy_plan,
        rows,
        params,
        &legacy_changes,
        writer_key,
        functions,
    )
    .await?;
    Ok(from_legacy_prepared_statements(statements))
}

fn from_legacy_prepared_statements(
    statements: Vec<legacy_sql::PreparedStatement>,
) -> Vec<PreparedStatement> {
    statements
        .into_iter()
        .map(|statement| PreparedStatement {
            sql: statement.sql,
            params: statement.params,
        })
        .collect()
}

fn to_legacy_detected_file_domain_changes(
    changes: &[DetectedFileDomainChange],
) -> Vec<legacy_sql::DetectedFileDomainChange> {
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
}

fn to_legacy_vtable_update_plan(plan: &VtableUpdatePlan) -> legacy_sql::VtableUpdatePlan {
    legacy_sql::VtableUpdatePlan {
        schema_key: plan.schema_key.clone(),
        explicit_writer_key: plan.explicit_writer_key.clone(),
        writer_key_assignment_present: plan.writer_key_assignment_present,
    }
}

fn to_legacy_vtable_delete_plan(plan: &VtableDeletePlan) -> legacy_sql::VtableDeletePlan {
    legacy_sql::VtableDeletePlan {
        schema_key: plan.schema_key.clone(),
        effective_scope_fallback: plan.effective_scope_fallback,
        effective_scope_selection_sql: plan.effective_scope_selection_sql.clone(),
    }
}
