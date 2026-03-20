use crate::engine::Engine;
use crate::functions::LixFunctionProvider;
use crate::sql::execution::shared_path::create_commit_error_to_lix_error;
use crate::state::commit::{
    create_commit, load_committed_version_head_commit_id_from_live_state, CreateCommitArgs,
    CreateCommitExpectedHead, CreateCommitIdempotencyKey, CreateCommitPreconditions,
    CreateCommitWriteLane,
};
use crate::{EngineTransaction, LixError};

use super::store::insert_undo_redo_operation_in_transaction;
use super::{
    build_forward_proposed_change, load_target_commit_change_effects,
    rebuild_semantic_undo_redo_stacks, resolve_target_version_id, RedoOptions, RedoResult,
    UndoRedoOperationKind, UndoRedoOperationRecord,
};

pub(crate) async fn redo(engine: &Engine) -> Result<RedoResult, LixError> {
    redo_with_options(engine, RedoOptions::default()).await
}

pub(crate) async fn redo_with_options(
    engine: &Engine,
    options: RedoOptions,
) -> Result<RedoResult, LixError> {
    engine
        .transaction(crate::ExecuteOptions::default(), move |tx| {
            let options = options.clone();
            Box::pin(async move { redo_in_transaction(tx, options).await })
        })
        .await
}

async fn redo_in_transaction(
    tx: &mut EngineTransaction<'_>,
    options: RedoOptions,
) -> Result<RedoResult, LixError> {
    let engine = tx.engine;
    let version_id = resolve_target_version_id(tx, options.version_id.as_deref()).await?;
    let (result, state_commit_stream_changes) = {
        let transaction = tx
            .transaction
            .as_mut()
            .map(|transaction| transaction.as_mut())
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
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
            state_commit_stream_changes.push(crate::state::stream::StateCommitStreamChange {
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

        let backend = crate::engine::TransactionBackendAdapter::new(transaction);
        let (_settings, _sequence_start, functions) = engine
            .prepare_runtime_functions_with_backend(&backend, true)
            .await?;
        engine
            .ensure_runtime_sequence_initialized_in_transaction(transaction, &functions)
            .await?;
        let mut functions = functions;
        let timestamp = functions.timestamp();
        let mut head_executor = crate::engine::TransactionBackendAdapter::new(transaction);
        let current_head_commit_id =
            load_committed_version_head_commit_id_from_live_state(&mut head_executor, &version_id)
                .await?
                .ok_or_else(|| {
                    LixError::unknown(format!(
                        "cannot redo in version '{}' without a current head commit",
                        version_id
                    ))
                })?;
        let create_result = create_commit(
            transaction,
            CreateCommitArgs {
                timestamp: Some(timestamp.clone()),
                changes: forward_changes,
                filesystem_state: Default::default(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version(version_id.clone()),
                    expected_head: CreateCommitExpectedHead::CurrentHead,
                    idempotency_key: CreateCommitIdempotencyKey::Exact(format!(
                        "redo:{}:{}:{}",
                        version_id, target_commit_id, current_head_commit_id
                    )),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
                writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .map_err(create_commit_error_to_lix_error)?;
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
        )
    };

    tx.core
        .pending_state_commit_stream_changes
        .extend(state_commit_stream_changes);
    Ok(result)
}
