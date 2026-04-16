use std::collections::BTreeSet;

use crate::canonical::{
    append_changes, append_untracked_change_visibility_rows,
    canonical_untracked_visibility_write_from_change_visibility,
    compact_untracked_changes_for_touched_rows_in_transaction, CanonicalUntrackedVisibilityWrite,
};
use crate::live_state::{finalize_live_state_after_immediate_write, write_live_rows, LiveRow};
use crate::sql::{PlanEffects, PlannedStateRow, SessionStateDelta, WriteMode};
use crate::streams::{
    state_commit_stream_changes_from_changes, StateChangeRecord, StateCommitStreamOperation,
    StateCommitStreamRuntimeMetadata,
};
use crate::transaction::pipeline::WriteExecutionOutcome;
use crate::transaction::{
    compile_filesystem_finalization_from_state_in_transaction, PendingCommitState,
    PublicWriteTxnUnit, WriteExecutionContext,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError, QueryResult, Value};

use super::registered_schema_mirror::mirror_registered_schema_planned_rows_in_transaction;

pub(crate) async fn execute_public_transaction_write_unit_with_transaction(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    unit: &PublicWriteTxnUnit,
    pending_commit_state: Option<&mut Option<PendingCommitState>>,
) -> Result<Option<WriteExecutionOutcome>, LixError> {
    match unit.execution.execution_mode {
        WriteMode::Tracked => {
            execute_public_commit_member_write_with_transaction(
                execution_context,
                transaction,
                unit,
                pending_commit_state,
            )
            .await
        }
        WriteMode::Untracked => {
            execute_public_immediate_write_with_transaction(execution_context, transaction, unit)
                .await
        }
    }
}

async fn execute_public_commit_member_write_with_transaction(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    unit: &PublicWriteTxnUnit,
    pending_commit_state: Option<&mut Option<PendingCommitState>>,
) -> Result<Option<WriteExecutionOutcome>, LixError> {
    debug_assert!(unit.is_commit_member_write());

    let commit_write_outcome = execution_context
        .execute_public_commit_write_txn_with_transaction(transaction, unit, pending_commit_state)
        .await?;

    let plan_effects_override = if commit_write_outcome.plugin_changes_committed {
        if unit.has_compiler_only_filesystem_changes() {
            plan_effects_from_commit_changes(
                &commit_write_outcome.applied_changes,
                unit.public_write
                    .contract
                    .operation_kind
                    .state_commit_stream_operation(),
                unit.writer_key.as_deref(),
                commit_write_outcome.next_active_version_id.clone(),
            )?
        } else {
            unit.execution.semantic_effects.clone()
        }
    } else {
        PlanEffects::default()
    };

    Ok(Some(WriteExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        direct_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: commit_write_outcome.plugin_changes_committed,
        canonical_commit_receipt: commit_write_outcome.receipt,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: commit_write_outcome.plugin_changes_committed
            && unit.should_emit_observe_tick(),
    }))
}

async fn execute_public_immediate_write_with_transaction(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    unit: &PublicWriteTxnUnit,
) -> Result<Option<WriteExecutionOutcome>, LixError> {
    debug_assert!(unit.is_immediate_write());

    let mut runtime_functions = unit.function_bindings.provider().clone();

    if unit.execution.persist_filesystem_payloads_before_write {
        // Untracked filesystem writes materialize blob payloads eagerly, but keep
        // descriptor visibility in the untracked live tables owned here.
    }

    let rows = live_rows_from_planned_rows(
        &unit.execution.intended_post_state,
        &unit.canonical_changes,
        unit.writer_key.as_deref(),
    )?;
    let visibility_rows =
        canonical_untracked_visibility_rows_from_live_rows(&rows, &unit.canonical_changes)?;
    append_changes(transaction, &unit.canonical_changes, &mut runtime_functions).await?;
    append_untracked_change_visibility_rows(transaction, &visibility_rows).await?;
    write_live_rows(transaction, &rows).await?;
    mirror_registered_schema_planned_rows_in_transaction(
        transaction,
        &unit.execution.intended_post_state,
        &unit.canonical_changes,
        true,
    )
    .await?;
    finalize_live_state_after_immediate_write(transaction).await?;
    compact_untracked_changes_for_touched_rows_in_transaction(transaction, &visibility_rows)
        .await?;

    let filesystem_finalization = compile_filesystem_finalization_from_state_in_transaction(
        transaction,
        &unit.filesystem_state,
        unit.writer_key.as_deref(),
        &[],
    )
    .await?;
    if unit.execution.persist_filesystem_payloads_before_write
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
        .persist_runtime_sequence_in_transaction(transaction, unit.function_bindings.provider())
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
        plan_effects_override: Some(unit.execution.semantic_effects.clone()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }))
}

fn plan_effects_from_commit_changes<Change: StateChangeRecord>(
    changes: &[Change],
    stream_operation: StateCommitStreamOperation,
    writer_key: Option<&str>,
    next_active_version_id: Option<String>,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_changes(
            changes,
            stream_operation,
            StateCommitStreamRuntimeMetadata::from_runtime_writer_key(writer_key),
        )?,
        session_delta: SessionStateDelta {
            next_active_version_id,
            next_active_account_ids: None,
            persist_workspace: false,
        },
        file_cache_refresh_targets: file_cache_refresh_targets_from_changes(changes),
    })
}

fn file_cache_refresh_targets_from_changes<Change: StateChangeRecord>(
    changes: &[Change],
) -> BTreeSet<(String, String)> {
    changes
        .iter()
        .filter(|change| change.file_id() != Some("lix"))
        .filter(|change| change.schema_key() != "lix_file_descriptor")
        .filter(|change| change.schema_key() != "lix_directory_descriptor")
        .filter_map(|change| {
            change
                .file_id()
                .map(|file_id| (file_id.to_string(), change.version_id().to_string()))
        })
        .collect()
}

fn live_rows_from_planned_rows(
    rows: &[PlannedStateRow],
    canonical_changes: &[crate::canonical::CanonicalChangeWrite],
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

fn canonical_untracked_visibility_rows_from_live_rows(
    rows: &[LiveRow],
    canonical_changes: &[crate::canonical::CanonicalChangeWrite],
) -> Result<Vec<CanonicalUntrackedVisibilityWrite>, LixError> {
    if rows.len() != canonical_changes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "public untracked execution expected {} canonical changes for {} visibility rows",
                canonical_changes.len(),
                rows.len()
            ),
        ));
    }

    Ok(rows
        .iter()
        .zip(canonical_changes.iter())
        .map(|(row, change)| {
            canonical_untracked_visibility_write_from_change_visibility(
                change,
                &row.version_id,
                row.global,
                row.created_at.as_deref(),
            )
        })
        .collect())
}

fn live_row_from_planned_row(
    row: &PlannedStateRow,
    change: &crate::canonical::CanonicalChangeWrite,
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
