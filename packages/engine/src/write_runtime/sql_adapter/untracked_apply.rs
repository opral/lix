use std::collections::BTreeSet;

use crate::contracts::artifacts::{
    PlannedStateRow, UntrackedWriteBatch, UntrackedWriteOperation, UntrackedWriteRow,
};
use crate::contracts::traits::UntrackedWriteParticipant;
use crate::engine::Engine;
use crate::filesystem::runtime::resolve_binary_blob_writes_in_transaction;
use crate::runtime::functions::LixFunctionProvider;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, QueryResult, Value};

use super::planned_write::PlannedPublicUntrackedWriteUnit;
use super::runtime::SqlExecutionOutcome;

pub(super) async fn run_public_untracked_write_txn_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixBackendTransaction,
    plan: &PlannedPublicUntrackedWriteUnit,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let mut runtime_functions = plan.runtime_state.provider().clone();
    let timestamp = runtime_functions.timestamp();

    if plan.execution.persist_filesystem_payloads_before_write {
        // Untracked filesystem writes materialize blob payloads eagerly, but keep
        // descriptor-domain visibility in the untracked live tables owned here.
    }

    let batch = untracked_write_batch_from_planned_rows(
        &plan.execution.intended_post_state,
        &timestamp,
        plan.writer_key.as_deref(),
    )?;
    transaction.apply_untracked_write_batch(&batch).await?;

    let filesystem_finalization = engine
        .compile_filesystem_finalization_from_state_in_transaction(
            transaction,
            &plan.filesystem_state,
            plan.writer_key.as_deref(),
            &[],
        )
        .await?;
    if plan.execution.persist_filesystem_payloads_before_write
        && !filesystem_finalization.binary_blob_writes.is_empty()
    {
        let resolved_binary_blob_writes = resolve_binary_blob_writes_in_transaction(
            transaction,
            &filesystem_finalization.binary_blob_writes,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public untracked filesystem payload resolution failed inside write txn: {}",
                error.description
            ),
        })?;
        crate::binary_cas::write::persist_resolved_binary_blob_writes_in_transaction(
            transaction,
            &resolved_binary_blob_writes,
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
        engine
            .garbage_collect_unreachable_binary_cas_in_transaction(transaction)
            .await?;
    }

    engine
        .persist_runtime_sequence_in_transaction(
            transaction,
            plan.runtime_state.settings(),
            plan.runtime_state.provider(),
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public untracked runtime-sequence persistence failed inside write txn: {}",
                error.description
            ),
        })?;

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        internal_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        canonical_commit_receipt: None,
        plan_effects_override: Some(plan.execution.semantic_effects.clone()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }))
}

fn untracked_write_batch_from_planned_rows(
    rows: &[PlannedStateRow],
    timestamp: &str,
    execution_writer_key: Option<&str>,
) -> Result<UntrackedWriteBatch, LixError> {
    rows.iter()
        .map(|row| untracked_write_row_from_planned_row(row, timestamp, execution_writer_key))
        .collect()
}

fn untracked_write_row_from_planned_row(
    row: &PlannedStateRow,
    timestamp: &str,
    execution_writer_key: Option<&str>,
) -> Result<UntrackedWriteRow, LixError> {
    let file_id = planned_row_text_value(row, "file_id")?;
    let plugin_key = planned_row_text_value(row, "plugin_key")?;
    let schema_version = planned_row_text_value(row, "schema_version")?;
    let global = row
        .values
        .get("global")
        .and_then(value_as_bool)
        .unwrap_or_else(|| row.version_id.as_deref() == Some(GLOBAL_VERSION_ID));

    Ok(UntrackedWriteRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: schema_version.to_string(),
        file_id: file_id.to_string(),
        version_id: row
            .version_id
            .clone()
            .unwrap_or_else(|| GLOBAL_VERSION_ID.to_string()),
        global,
        plugin_key: plugin_key.to_string(),
        metadata: planned_row_optional_text_value(row, "metadata").map(ToString::to_string),
        writer_key: row
            .writer_key
            .as_deref()
            .or(execution_writer_key)
            .map(ToString::to_string),
        snapshot_content: (!row.tombstone)
            .then(|| planned_row_json_text_value(row, "snapshot_content"))
            .transpose()?,
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: if row.tombstone {
            UntrackedWriteOperation::Delete
        } else {
            UntrackedWriteOperation::Upsert
        },
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
