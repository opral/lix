use std::collections::BTreeSet;

use crate::canonical::append_changes;
use crate::canonical::CanonicalChangeWrite;
use crate::live_state::{write_live_rows, LiveRow};
use crate::sql::PlannedStateRow;
use crate::transaction::pipeline::WriteExecutionOutcome;
use crate::transaction::{
    compile_filesystem_finalization_from_state_in_transaction, PlannedPublicUntrackedWriteUnit,
    WriteExecutionContext,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, QueryResult, Value};

use super::registered_schema_mirror::mirror_registered_schema_planned_rows_in_transaction;

pub(crate) async fn execute_public_untracked_transaction_write_unit_with_transaction(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    plan: &PlannedPublicUntrackedWriteUnit,
) -> Result<Option<WriteExecutionOutcome>, LixError> {
    let mut runtime_functions = plan.function_bindings.provider().clone();

    if plan.execution.persist_filesystem_payloads_before_write {
        // Untracked filesystem writes materialize blob payloads eagerly, but keep
        // descriptor visibility in the untracked live tables owned here.
    }

    let rows = live_rows_from_planned_rows(
        &plan.execution.intended_post_state,
        &plan.canonical_changes,
        plan.writer_key.as_deref(),
    )?;
    append_changes(transaction, &plan.canonical_changes, &mut runtime_functions).await?;
    write_live_rows(transaction, &rows).await?;
    mirror_registered_schema_planned_rows_in_transaction(
        transaction,
        &plan.execution.intended_post_state,
        &plan.canonical_changes,
        true,
    )
    .await?;

    let filesystem_finalization = compile_filesystem_finalization_from_state_in_transaction(
        transaction,
        &plan.filesystem_state,
        plan.writer_key.as_deref(),
        &[],
    )
    .await?;
    if plan.execution.persist_filesystem_payloads_before_write
        && !filesystem_finalization.binary_blob_writes.is_empty()
    {
        execution_context
            .persist_binary_blob_writes_in_transaction(
                transaction,
                &filesystem_finalization.binary_blob_writes,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "public untracked filesystem payload persistence failed inside write txn: {}",
                    error.description
                ),
            })?;
    }
    if filesystem_finalization.should_run_gc {
        execution_context
            .garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await?;
    }

    execution_context
        .persist_runtime_sequence_in_transaction(transaction, plan.function_bindings.provider())
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public untracked runtime-sequence persistence failed inside write txn: {}",
                error.description
            ),
        })?;

    Ok(Some(WriteExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        direct_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        canonical_commit_receipt: None,
        plan_effects_override: Some(plan.execution.semantic_effects.clone()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }))
}

fn live_rows_from_planned_rows(
    rows: &[PlannedStateRow],
    canonical_changes: &[CanonicalChangeWrite],
    execution_writer_key: Option<&str>,
) -> Result<Vec<LiveRow>, LixError> {
    if rows.len() != canonical_changes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "public untracked execution expected {} canonical changes for {} planned rows",
                rows.len(),
                canonical_changes.len()
            ),
        ));
    }

    rows.iter()
        .zip(canonical_changes.iter())
        .map(|(row, change)| live_row_from_planned_row(row, change, execution_writer_key))
        .collect()
}

fn live_row_from_planned_row(
    row: &PlannedStateRow,
    change: &CanonicalChangeWrite,
    execution_writer_key: Option<&str>,
) -> Result<LiveRow, LixError> {
    let file_id = planned_row_optional_text_value(row, "file_id").map(ToString::to_string);
    let plugin_key = planned_row_optional_text_value(row, "plugin_key").map(ToString::to_string);
    let schema_version = planned_row_text_value(row, "schema_version")?;
    let global = row
        .values
        .get("global")
        .and_then(value_as_bool)
        .unwrap_or_else(|| row.version_id.as_deref() == Some(GLOBAL_VERSION_ID));

    Ok(LiveRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: schema_version.to_string(),
        file_id,
        version_id: row
            .version_id
            .clone()
            .unwrap_or_else(|| GLOBAL_VERSION_ID.to_string()),
        global,
        plugin_key,
        metadata: planned_row_optional_text_value(row, "metadata").map(ToString::to_string),
        change_id: Some(change.id.clone()),
        writer_key: row
            .writer_key
            .as_deref()
            .or(execution_writer_key)
            .map(ToString::to_string),
        untracked: true,
        created_at: Some(change.created_at.clone()),
        updated_at: Some(change.created_at.clone()),
        snapshot_content: (!row.tombstone)
            .then(|| planned_row_json_text_value(row, "snapshot_content"))
            .transpose()?,
    })
}

fn planned_row_text_value<'a>(row: &'a PlannedStateRow, key: &str) -> Result<&'a str, LixError> {
    planned_row_optional_text_value(row, key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("public untracked execution requires '{key}' in the resolved row"),
    })
}

fn planned_row_optional_text_value<'a>(row: &'a PlannedStateRow, key: &str) -> Option<&'a str> {
    row.values.get(key).and_then(|value| match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    })
}

fn planned_row_json_text_value(row: &PlannedStateRow, key: &str) -> Result<String, LixError> {
    row.values
        .get(key)
        .and_then(|value| match value {
            Value::Text(value) => Some(value.clone()),
            Value::Json(value) => Some(value.to_string()),
            _ => None,
        })
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("public untracked execution requires JSON '{key}'"),
        })
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        _ => None,
    }
}
