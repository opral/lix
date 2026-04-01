use crate::commit::{append_tracked, CreateCommitArgs};
use crate::functions::LixFunctionProvider;
use crate::version::context::{
    exact_current_head_preconditions, require_version_context_with_executor, ResolvedVersionTarget,
    VersionContextSource,
};
use crate::{LixError, Session, SessionTransaction};

use super::store::insert_undo_redo_operation_in_transaction;
use super::{
    build_forward_proposed_change, load_target_commit_change_effects,
    rebuild_semantic_undo_redo_stacks, resolve_target_version_id_in_session, RedoOptions,
    RedoResult, UndoRedoOperationKind, UndoRedoOperationRecord,
};

pub(crate) async fn redo_with_options_in_session(
    session: &Session,
    options: RedoOptions,
) -> Result<RedoResult, LixError> {
    session
        .transaction(crate::ExecuteOptions::default(), move |tx| {
            let options = options.clone();
            Box::pin(async move { redo_in_transaction(tx, options).await })
        })
        .await
}

async fn redo_in_transaction(
    tx: &mut SessionTransaction<'_>,
    options: RedoOptions,
) -> Result<RedoResult, LixError> {
    let engine = tx.engine;
    let active_account_ids = tx.context.active_account_ids.clone();
    let version_id =
        resolve_target_version_id_in_session(tx, options.version_id.as_deref()).await?;
    let (result, state_commit_stream_changes, canonical_commit_receipt) = {
        let transaction = tx.backend_transaction_mut()?;
        let stacks = rebuild_semantic_undo_redo_stacks(transaction, &version_id).await?;
        let target_commit_id = stacks.redo_stack.last().cloned().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_NOTHING_TO_REDO",
                format!("nothing to redo for version '{}'", version_id),
            )
        })?;
        let effects =
            load_target_commit_change_effects(transaction, &version_id, &target_commit_id).await?;
        if effects.is_empty() {
            return Err(LixError::unknown(format!(
                "target commit '{}' has no redoable changes",
                target_commit_id
            )));
        }

        let mut forward_changes = Vec::with_capacity(effects.len());
        let mut state_commit_stream_changes = Vec::with_capacity(effects.len());
        for effect in &effects {
            let proposed = build_forward_proposed_change(&version_id, &effect.forward_change)?;
            state_commit_stream_changes.push(crate::runtime::streams::StateCommitStreamChange {
                operation: effect.forward_operation,
                entity_id: proposed.entity_id.to_string(),
                schema_key: proposed.schema_key.to_string(),
                schema_version: proposed
                    .schema_version
                    .clone()
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                file_id: proposed
                    .file_id
                    .clone()
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                version_id: version_id.clone(),
                plugin_key: proposed
                    .plugin_key
                    .clone()
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                snapshot_content: proposed
                    .snapshot_content
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|error| {
                        LixError::unknown(format!("redo snapshot_content is invalid JSON: {error}"))
                    })?,
                untracked: false,
                writer_key: None,
            });
            forward_changes.push(proposed);
        }

        let backend = crate::runtime::TransactionBackendAdapter::new(transaction);
        let (_settings, functions) = engine
            .prepare_runtime_functions_with_backend(&backend)
            .await?;
        engine
            .ensure_runtime_sequence_initialized_in_transaction(transaction, &functions)
            .await?;
        let mut functions = functions;
        let timestamp = functions.timestamp();
        let mut head_executor = crate::runtime::TransactionBackendAdapter::new(transaction);
        let version_context = require_version_context_with_executor(
            &mut head_executor,
            ResolvedVersionTarget {
                version_id: version_id.clone(),
                source: VersionContextSource::ExplicitArgument,
            },
            "version",
        )
        .await?;
        let create_result = append_tracked(
            transaction,
            CreateCommitArgs {
                timestamp: Some(timestamp.clone()),
                changes: forward_changes,
                filesystem_state: Default::default(),
                preconditions: exact_current_head_preconditions(
                    &version_context,
                    format!(
                        "redo:{}:{}:{}",
                        version_id,
                        target_commit_id,
                        version_context.head_commit_id()
                    ),
                ),
                active_account_ids: active_account_ids.clone(),
                lane_parent_commit_ids_override: None,
                allow_empty_commit: false,
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await?;
        let canonical_commit_receipt = create_result.receipt.clone();
        let replay_commit_id = create_result.committed_head;
        insert_undo_redo_operation_in_transaction(
            transaction,
            &UndoRedoOperationRecord {
                version_id: version_id.clone(),
                operation_commit_id: replay_commit_id.clone(),
                operation_kind: UndoRedoOperationKind::Redo,
                target_commit_id: target_commit_id.clone(),
                created_at: timestamp,
            },
        )
        .await?;

        (
            RedoResult {
                version_id,
                target_commit_id,
                replay_commit_id,
            },
            state_commit_stream_changes,
            canonical_commit_receipt,
        )
    };

    if let Some(receipt) = canonical_commit_receipt {
        tx.record_canonical_commit_receipt(receipt)?;
    }
    tx.record_state_commit_stream_changes(state_commit_stream_changes)?;
    Ok(result)
}
