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
    build_restore_proposed_change, build_tombstone_proposed_change,
    load_target_commit_change_effects, rebuild_semantic_undo_redo_stacks,
    resolve_target_version_id, UndoOptions, UndoRedoOperationKind, UndoRedoOperationRecord,
    UndoResult,
};

pub(crate) async fn undo(engine: &Engine) -> Result<UndoResult, LixError> {
    undo_with_options(engine, UndoOptions::default()).await
}

pub(crate) async fn undo_with_options(
    engine: &Engine,
    options: UndoOptions,
) -> Result<UndoResult, LixError> {
    engine
        .transaction(crate::ExecuteOptions::default(), move |tx| {
            let options = options.clone();
            Box::pin(async move { undo_in_transaction(tx, options).await })
        })
        .await
}

async fn undo_in_transaction(
    tx: &mut EngineTransaction<'_>,
    options: UndoOptions,
) -> Result<UndoResult, LixError> {
    let engine = tx.engine;
    let version_id = resolve_target_version_id(tx, options.version_id.as_deref()).await?;
    let (result, state_commit_stream_changes) = {
        let transaction = tx
            .transaction
            .as_mut()
            .map(|transaction| transaction.as_mut())
            .ok_or_else(|| LixError::unknown("transaction is no longer active"))?;
        let stacks = rebuild_semantic_undo_redo_stacks(transaction, &version_id).await?;
        let target_commit_id = stacks.undo_stack.last().cloned().ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_NOTHING_TO_UNDO",
                format!("nothing to undo for version '{}'", version_id),
            )
        })?;
        let effects =
            load_target_commit_change_effects(transaction, &version_id, &target_commit_id).await?;
        if effects.is_empty() {
            return Err(LixError::unknown(format!(
                "target commit '{}' has no undoable changes",
                target_commit_id
            )));
        }

        let mut inverse_changes = Vec::with_capacity(effects.len());
        let mut state_commit_stream_changes = Vec::with_capacity(effects.len());
        for effect in &effects {
            match effect.forward_operation {
                crate::state::stream::StateCommitStreamOperation::Insert => {
                    inverse_changes.push(build_tombstone_proposed_change(
                        &version_id,
                        &effect.forward_change,
                    ));
                    state_commit_stream_changes.push(
                        crate::state::stream::StateCommitStreamChange {
                            operation: crate::state::stream::StateCommitStreamOperation::Delete,
                            entity_id: effect.forward_change.entity_id.clone(),
                            schema_key: effect.forward_change.schema_key.clone(),
                            schema_version: effect.forward_change.schema_version.clone(),
                            file_id: effect.forward_change.file_id.clone(),
                            version_id: version_id.clone(),
                            plugin_key: effect.forward_change.plugin_key.clone(),
                            snapshot_content: None,
                            untracked: false,
                            writer_key: None,
                        },
                    );
                }
                crate::state::stream::StateCommitStreamOperation::Update => {
                    let previous_row = effect.previous_row.as_ref().ok_or_else(|| {
                        LixError::unknown(format!(
                            "undo for commit '{}' requires prior row for updated change '{}'",
                            target_commit_id, effect.forward_change.id
                        ))
                    })?;
                    let restored = build_restore_proposed_change(&version_id, previous_row)?;
                    state_commit_stream_changes.push(
                        crate::state::stream::StateCommitStreamChange {
                            operation: crate::state::stream::StateCommitStreamOperation::Update,
                            entity_id: restored.entity_id.clone(),
                            schema_key: restored.schema_key.clone(),
                            schema_version: restored.schema_version.clone().unwrap_or_default(),
                            file_id: restored.file_id.clone().unwrap_or_default(),
                            version_id: version_id.clone(),
                            plugin_key: restored.plugin_key.clone().unwrap_or_default(),
                            snapshot_content: restored
                                .snapshot_content
                                .as_deref()
                                .map(serde_json::from_str)
                                .transpose()
                                .map_err(|error| {
                                    LixError::unknown(format!(
                                        "undo restored snapshot_content is invalid JSON: {error}"
                                    ))
                                })?,
                            untracked: false,
                            writer_key: None,
                        },
                    );
                    inverse_changes.push(restored);
                }
                crate::state::stream::StateCommitStreamOperation::Delete => {
                    let previous_row = effect.previous_row.as_ref().ok_or_else(|| {
                        LixError::unknown(format!(
                            "undo for commit '{}' requires prior row for deleted change '{}'",
                            target_commit_id, effect.forward_change.id
                        ))
                    })?;
                    let restored = build_restore_proposed_change(&version_id, previous_row)?;
                    state_commit_stream_changes.push(
                        crate::state::stream::StateCommitStreamChange {
                            operation: crate::state::stream::StateCommitStreamOperation::Insert,
                            entity_id: restored.entity_id.clone(),
                            schema_key: restored.schema_key.clone(),
                            schema_version: restored.schema_version.clone().unwrap_or_default(),
                            file_id: restored.file_id.clone().unwrap_or_default(),
                            version_id: version_id.clone(),
                            plugin_key: restored.plugin_key.clone().unwrap_or_default(),
                            snapshot_content: restored
                                .snapshot_content
                                .as_deref()
                                .map(serde_json::from_str)
                                .transpose()
                                .map_err(|error| {
                                    LixError::unknown(format!(
                                        "undo restored snapshot_content is invalid JSON: {error}"
                                    ))
                                })?,
                            untracked: false,
                            writer_key: None,
                        },
                    );
                    inverse_changes.push(restored);
                }
            }
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
                        "cannot undo in version '{}' without a current head commit",
                        version_id
                    ))
                })?;
        let create_result = create_commit(
            transaction,
            CreateCommitArgs {
                timestamp: Some(timestamp.clone()),
                changes: inverse_changes,
                lazy_exact_file_updates: Vec::new(),
                additional_binary_blob_payloads: Vec::new(),
                preconditions: CreateCommitPreconditions {
                    write_lane: CreateCommitWriteLane::Version(version_id.clone()),
                    expected_head: CreateCommitExpectedHead::CurrentHead,
                    idempotency_key: CreateCommitIdempotencyKey::Exact(format!(
                        "undo:{}:{}:{}",
                        version_id, target_commit_id, current_head_commit_id
                    )),
                },
                should_emit_observe_tick: false,
                observe_tick_writer_key: None,
            },
            &mut functions,
            None,
        )
        .await
        .map_err(create_commit_error_to_lix_error)?;
        let inverse_commit_id = create_result.committed_head;
        insert_undo_redo_operation_in_transaction(
            transaction,
            &UndoRedoOperationRecord {
                version_id: version_id.clone(),
                operation_commit_id: inverse_commit_id.clone(),
                operation_kind: UndoRedoOperationKind::Undo,
                target_commit_id: target_commit_id.clone(),
                created_at: timestamp,
            },
        )
        .await?;

        (
            UndoResult {
                version_id,
                target_commit_id,
                inverse_commit_id,
            },
            state_commit_stream_changes,
        )
    };

    tx.core
        .pending_state_commit_stream_changes
        .extend(state_commit_stream_changes);
    Ok(result)
}
