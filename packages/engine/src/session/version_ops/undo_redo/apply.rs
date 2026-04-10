use std::collections::BTreeMap;

use crate::backend::QueryExecutor;
use crate::canonical::{
    load_change, load_commit, load_exact_row_at_commit, CanonicalChange, CanonicalStateIdentity,
    CanonicalStateRow,
};
use crate::contracts::artifacts::{StateCommitStreamChange, StateCommitStreamOperation};
use crate::runtime::execution_state::ExecutionRuntimeState;
use crate::runtime::functions::LixFunctionProvider;
use crate::runtime::TransactionBackendAdapter;
use crate::session::version_ops::commit::{
    append_tracked, CanonicalCommitReceipt, CreateCommitArgs, StagedChange,
};
use crate::{LixBackendTransaction, LixError, SessionTransaction, Value};

use super::super::context::{
    exact_current_head_preconditions, load_version_context_with_executor,
    require_target_version_context_in_transaction, resolve_target_version_in_transaction,
    ResolvedVersionTarget, VersionContextSource,
};
use super::{RedoResult, UndoResult, UNDO_REDO_OPERATION_TABLE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UndoRedoOperationKind {
    Undo,
    Redo,
}

impl UndoRedoOperationKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Undo => "undo",
            Self::Redo => "redo",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "undo" => Some(Self::Undo),
            "redo" => Some(Self::Redo),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UndoRedoOperationRecord {
    version_id: String,
    operation_commit_id: String,
    operation_kind: UndoRedoOperationKind,
    target_commit_id: String,
    created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct SemanticUndoRedoStacks {
    undo_stack: Vec<String>,
    redo_stack: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct TargetCommitChangeEffect {
    forward_change: CanonicalChange,
    previous_row: Option<CanonicalStateRow>,
    forward_operation: StateCommitStreamOperation,
}

struct AppliedUndoRedoCommit {
    committed_head: String,
    canonical_commit_receipt: Option<CanonicalCommitReceipt>,
}

pub(super) async fn undo_in_session_transaction(
    tx: &mut SessionTransaction<'_>,
    requested_version_id: Option<&str>,
) -> Result<UndoResult, LixError> {
    let active_account_ids = tx.context.active_account_ids.clone();
    let version_id = resolve_target_version_id_in_session(tx, requested_version_id).await?;
    let (target_commit_id, inverse_changes, state_commit_stream_changes) = {
        let transaction = tx.backend_transaction_mut()?;
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
                StateCommitStreamOperation::Insert => {
                    inverse_changes.push(build_tombstone_proposed_change(
                        &version_id,
                        &effect.forward_change,
                    )?);
                    state_commit_stream_changes.push(StateCommitStreamChange {
                        operation: StateCommitStreamOperation::Delete,
                        entity_id: effect.forward_change.entity_id.clone(),
                        schema_key: effect.forward_change.schema_key.clone(),
                        schema_version: effect.forward_change.schema_version.clone(),
                        file_id: effect.forward_change.file_id.clone(),
                        version_id: version_id.clone(),
                        plugin_key: effect.forward_change.plugin_key.clone(),
                        snapshot_content: None,
                        untracked: false,
                        writer_key: None,
                    });
                }
                StateCommitStreamOperation::Update => {
                    let previous_row = effect.previous_row.as_ref().ok_or_else(|| {
                        LixError::unknown(format!(
                            "undo for commit '{}' requires prior row for updated change '{}'",
                            target_commit_id, effect.forward_change.id
                        ))
                    })?;
                    let restored = build_restore_proposed_change(&version_id, previous_row)?;
                    state_commit_stream_changes.push(StateCommitStreamChange {
                        operation: StateCommitStreamOperation::Update,
                        entity_id: restored.entity_id.to_string(),
                        schema_key: restored.schema_key.to_string(),
                        schema_version: restored
                            .schema_version
                            .clone()
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
                        file_id: restored
                            .file_id
                            .clone()
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
                        version_id: version_id.clone(),
                        plugin_key: restored
                            .plugin_key
                            .clone()
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
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
                    });
                    inverse_changes.push(restored);
                }
                StateCommitStreamOperation::Delete => {
                    let previous_row = effect.previous_row.as_ref().ok_or_else(|| {
                        LixError::unknown(format!(
                            "undo for commit '{}' requires prior row for deleted change '{}'",
                            target_commit_id, effect.forward_change.id
                        ))
                    })?;
                    let restored = build_restore_proposed_change(&version_id, previous_row)?;
                    state_commit_stream_changes.push(StateCommitStreamChange {
                        operation: StateCommitStreamOperation::Insert,
                        entity_id: restored.entity_id.to_string(),
                        schema_key: restored.schema_key.to_string(),
                        schema_version: restored
                            .schema_version
                            .clone()
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
                        file_id: restored
                            .file_id
                            .clone()
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
                        version_id: version_id.clone(),
                        plugin_key: restored
                            .plugin_key
                            .clone()
                            .map(|value| value.to_string())
                            .unwrap_or_default(),
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
                    });
                    inverse_changes.push(restored);
                }
            }
        }

        (
            target_commit_id,
            inverse_changes,
            state_commit_stream_changes,
        )
    };

    let applied = append_undo_redo_commit_in_transaction(
        tx,
        &version_id,
        &target_commit_id,
        inverse_changes,
        active_account_ids,
        UndoRedoOperationKind::Undo,
    )
    .await?;
    if let Some(receipt) = applied.canonical_commit_receipt {
        tx.record_canonical_commit_receipt(receipt)?;
    }
    tx.record_state_commit_stream_changes(state_commit_stream_changes)?;
    Ok(UndoResult {
        version_id,
        target_commit_id,
        inverse_commit_id: applied.committed_head,
    })
}

pub(super) async fn redo_in_session_transaction(
    tx: &mut SessionTransaction<'_>,
    requested_version_id: Option<&str>,
) -> Result<RedoResult, LixError> {
    let active_account_ids = tx.context.active_account_ids.clone();
    let version_id = resolve_target_version_id_in_session(tx, requested_version_id).await?;
    let (target_commit_id, forward_changes, state_commit_stream_changes) = {
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
            state_commit_stream_changes.push(StateCommitStreamChange {
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

        (
            target_commit_id,
            forward_changes,
            state_commit_stream_changes,
        )
    };

    let applied = append_undo_redo_commit_in_transaction(
        tx,
        &version_id,
        &target_commit_id,
        forward_changes,
        active_account_ids,
        UndoRedoOperationKind::Redo,
    )
    .await?;
    if let Some(receipt) = applied.canonical_commit_receipt {
        tx.record_canonical_commit_receipt(receipt)?;
    }
    tx.record_state_commit_stream_changes(state_commit_stream_changes)?;
    Ok(RedoResult {
        version_id,
        target_commit_id,
        replay_commit_id: applied.committed_head,
    })
}

async fn append_undo_redo_commit_in_transaction(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
    target_commit_id: &str,
    changes: Vec<StagedChange>,
    active_account_ids: Vec<String>,
    operation_kind: UndoRedoOperationKind,
) -> Result<AppliedUndoRedoCommit, LixError> {
    let runtime_state = checkpoint_runtime_state(tx).await?;
    let mut functions = runtime_state.provider().clone();
    crate::runtime::deterministic_mode::ensure_runtime_sequence_initialized_in_transaction(
        tx.backend_transaction_mut()?,
        &mut functions,
    )
    .await?;
    let timestamp = functions.timestamp();
    let version_context = require_target_version_context_in_transaction(
        tx,
        Some(version_id),
        "version_id",
        "version",
    )
    .await?;
    let create_result = append_tracked(
        tx.backend_transaction_mut()?,
        CreateCommitArgs {
            timestamp: Some(timestamp.clone()),
            changes,
            filesystem_state: Default::default(),
            preconditions: exact_current_head_preconditions(
                &version_context,
                format!(
                    "{}:{}:{}:{}",
                    operation_kind.as_str(),
                    version_id,
                    target_commit_id,
                    version_context.head_commit_id()
                ),
            ),
            active_account_ids,
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
    insert_undo_redo_operation_in_transaction(
        tx.backend_transaction_mut()?,
        &UndoRedoOperationRecord {
            version_id: version_id.to_string(),
            operation_commit_id: create_result.committed_head.clone(),
            operation_kind,
            target_commit_id: target_commit_id.to_string(),
            created_at: timestamp.clone(),
        },
    )
    .await?;

    Ok(AppliedUndoRedoCommit {
        committed_head: create_result.committed_head,
        canonical_commit_receipt: create_result.receipt,
    })
}

async fn checkpoint_runtime_state(
    tx: &mut SessionTransaction<'_>,
) -> Result<ExecutionRuntimeState, LixError> {
    if let Some(runtime_state) = tx.context.execution_runtime_state().cloned() {
        return Ok(runtime_state);
    }

    let collaborators = tx.collaborators();
    let backend = TransactionBackendAdapter::new(tx.backend_transaction_mut()?);
    let runtime_state = collaborators
        .prepare_execution_runtime_state(&backend)
        .await?;
    tx.context
        .set_execution_runtime_state(runtime_state.clone());
    Ok(runtime_state)
}

async fn resolve_target_version_id_in_session(
    tx: &mut SessionTransaction<'_>,
    requested_version_id: Option<&str>,
) -> Result<String, LixError> {
    let target =
        resolve_target_version_in_transaction(tx, requested_version_id, "version_id").await?;
    ensure_version_exists_in_session(tx, &target.version_id).await?;
    Ok(target.version_id)
}

async fn ensure_version_exists_in_session(
    tx: &mut SessionTransaction<'_>,
    version_id: &str,
) -> Result<(), LixError> {
    ensure_version_exists_with_transaction(tx.backend_transaction_mut()?, version_id).await
}

async fn ensure_version_exists_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
) -> Result<(), LixError> {
    let mut executor = TransactionBackendAdapter::new(transaction);
    crate::session::version_ops::context::ensure_version_exists_with_executor(
        &mut executor,
        version_id,
    )
    .await
}

async fn rebuild_semantic_undo_redo_stacks(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
) -> Result<SemanticUndoRedoStacks, LixError> {
    let mut executor = TransactionBackendAdapter::new(transaction);
    let Some(version_context) = load_version_context_with_executor(
        &mut executor,
        ResolvedVersionTarget {
            version_id: version_id.to_string(),
            source: VersionContextSource::ExplicitArgument,
        },
    )
    .await?
    else {
        return Ok(SemanticUndoRedoStacks::default());
    };
    let lineage =
        load_linear_commit_lineage(&mut executor, version_context.head_commit_id()).await?;
    let operations =
        load_undo_redo_operations_for_version_in_transaction(transaction, version_id).await?;
    let operations_by_commit = operations
        .into_iter()
        .map(|record| (record.operation_commit_id.clone(), record))
        .collect::<BTreeMap<_, _>>();

    let mut stacks = SemanticUndoRedoStacks::default();
    for commit in lineage {
        if let Some(operation) = operations_by_commit.get(&commit.id) {
            match operation.operation_kind {
                UndoRedoOperationKind::Undo => {
                    let Some(last_undo) = stacks.undo_stack.pop() else {
                        return Err(LixError::unknown(format!(
                            "undo/redo lineage is inconsistent for version '{}': undo operation '{}' has no matching undo target",
                            version_id, commit.id
                        )));
                    };
                    if last_undo != operation.target_commit_id {
                        return Err(LixError::unknown(format!(
                            "undo/redo lineage is inconsistent for version '{}': undo operation '{}' expected target '{}', found '{}'",
                            version_id, commit.id, operation.target_commit_id, last_undo
                        )));
                    }
                    stacks.redo_stack.push(operation.target_commit_id.clone());
                }
                UndoRedoOperationKind::Redo => {
                    let Some(last_redo) = stacks.redo_stack.pop() else {
                        return Err(LixError::unknown(format!(
                            "undo/redo lineage is inconsistent for version '{}': redo operation '{}' has no matching redo target",
                            version_id, commit.id
                        )));
                    };
                    if last_redo != operation.target_commit_id {
                        return Err(LixError::unknown(format!(
                            "undo/redo lineage is inconsistent for version '{}': redo operation '{}' expected target '{}', found '{}'",
                            version_id, commit.id, operation.target_commit_id, last_redo
                        )));
                    }
                    stacks.undo_stack.push(operation.target_commit_id.clone());
                }
            }
            continue;
        }

        if !commit.change_ids.is_empty() && !commit.parent_commit_ids.is_empty() {
            stacks.undo_stack.push(commit.id);
            stacks.redo_stack.clear();
        }
    }

    Ok(stacks)
}

async fn load_undo_redo_operations_for_version_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
) -> Result<Vec<UndoRedoOperationRecord>, LixError> {
    let sql = format!(
        "SELECT version_id, operation_commit_id, operation_kind, target_commit_id, created_at \
         FROM {table} \
         WHERE version_id = $1 \
         ORDER BY created_at ASC, operation_commit_id ASC",
        table = UNDO_REDO_OPERATION_TABLE,
    );
    let result = transaction
        .execute(&sql, &[Value::Text(version_id.to_string())])
        .await?;

    result
        .rows
        .iter()
        .map(|row| parse_undo_redo_operation_record(row))
        .collect()
}

async fn insert_undo_redo_operation_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    record: &UndoRedoOperationRecord,
) -> Result<(), LixError> {
    let sql = format!(
        "INSERT INTO {table} (\
         version_id, operation_commit_id, operation_kind, target_commit_id, created_at\
         ) VALUES ($1, $2, $3, $4, $5)",
        table = UNDO_REDO_OPERATION_TABLE,
    );
    transaction
        .execute(
            &sql,
            &[
                Value::Text(record.version_id.clone()),
                Value::Text(record.operation_commit_id.clone()),
                Value::Text(record.operation_kind.as_str().to_string()),
                Value::Text(record.target_commit_id.clone()),
                Value::Text(record.created_at.clone()),
            ],
        )
        .await?;
    Ok(())
}

fn parse_undo_redo_operation_record(row: &[Value]) -> Result<UndoRedoOperationRecord, LixError> {
    let version_id = required_text(row, 0, "version_id")?;
    let operation_commit_id = required_text(row, 1, "operation_commit_id")?;
    let operation_kind_raw = required_text(row, 2, "operation_kind")?;
    let target_commit_id = required_text(row, 3, "target_commit_id")?;
    let created_at = required_text(row, 4, "created_at")?;
    let operation_kind = UndoRedoOperationKind::parse(&operation_kind_raw).ok_or_else(|| {
        LixError::unknown(format!(
            "unknown undo/redo operation kind '{}'",
            operation_kind_raw
        ))
    })?;

    Ok(UndoRedoOperationRecord {
        version_id,
        operation_commit_id,
        operation_kind,
        target_commit_id,
        created_at,
    })
}

fn required_text(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(LixError::unknown(format!("{field} is empty"))),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(LixError::unknown(format!(
            "expected text-like value for {field}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!("missing {field}"))),
    }
}

async fn load_linear_commit_lineage<E>(
    executor: &mut E,
    head_commit_id: &str,
) -> Result<Vec<crate::canonical::CanonicalCommit>, LixError>
where
    E: QueryExecutor,
{
    let mut lineage = Vec::new();
    let mut current_commit_id = Some(head_commit_id.to_string());
    while let Some(commit_id) = current_commit_id.take() {
        let Some(commit) = load_commit(executor, &commit_id).await? else {
            return Err(LixError::unknown(format!(
                "commit '{}' is missing from lineage lookup",
                commit_id
            )));
        };
        if commit.parent_commit_ids.len() > 1 {
            return Err(LixError::unknown(format!(
                "undo/redo does not support merge commit '{}' with {} parents",
                commit.id,
                commit.parent_commit_ids.len()
            )));
        }
        current_commit_id = commit.parent_commit_ids.first().cloned();
        lineage.push(commit);
    }
    lineage.reverse();
    Ok(lineage)
}

async fn load_target_commit_change_effects(
    transaction: &mut dyn LixBackendTransaction,
    _version_id: &str,
    target_commit_id: &str,
) -> Result<Vec<TargetCommitChangeEffect>, LixError> {
    let mut executor = TransactionBackendAdapter::new(transaction);
    let Some(target_commit) = load_commit(&mut executor, target_commit_id).await? else {
        return Err(LixError::unknown(format!(
            "target commit '{}' is missing",
            target_commit_id
        )));
    };
    if target_commit.parent_commit_ids.len() > 1 {
        return Err(LixError::unknown(format!(
            "undo/redo does not support merge commit '{}' with {} parents",
            target_commit.id,
            target_commit.parent_commit_ids.len()
        )));
    }
    let parent_commit_id = target_commit.parent_commit_ids.first().cloned();

    let mut effects = Vec::with_capacity(target_commit.change_ids.len());
    for change_id in &target_commit.change_ids {
        let Some(forward_change) = load_change(&mut executor, change_id).await? else {
            return Err(LixError::unknown(format!(
                "target commit '{}' references missing change '{}'",
                target_commit_id, change_id
            )));
        };
        let previous_row = if let Some(parent_commit_id) = parent_commit_id.as_deref() {
            let identity = CanonicalStateIdentity {
                entity_id: forward_change.entity_id.clone(),
                schema_key: forward_change.schema_key.clone(),
                file_id: forward_change.file_id.clone(),
            };
            load_exact_row_at_commit(&mut executor, parent_commit_id, &identity).await?
        } else {
            None
        };
        let forward_operation =
            classify_forward_operation(target_commit_id, &forward_change, previous_row.as_ref())?;
        effects.push(TargetCommitChangeEffect {
            forward_change,
            previous_row,
            forward_operation,
        });
    }
    Ok(effects)
}

fn build_forward_proposed_change(
    version_id: &str,
    change: &CanonicalChange,
) -> Result<StagedChange, LixError> {
    Ok(StagedChange {
        id: None,
        entity_id: require_identity(change.entity_id.clone(), "undo forward entity_id")?,
        schema_key: require_identity(change.schema_key.clone(), "undo forward schema_key")?,
        schema_version: Some(require_identity(
            change.schema_version.clone(),
            "undo forward schema_version",
        )?),
        file_id: Some(require_identity(
            change.file_id.clone(),
            "undo forward file_id",
        )?),
        plugin_key: Some(require_identity(
            change.plugin_key.clone(),
            "undo forward plugin_key",
        )?),
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        version_id: require_identity(version_id.to_string(), "undo forward version_id")?,
        writer_key: None,
        created_at: None,
    })
}

fn build_restore_proposed_change(
    version_id: &str,
    row: &CanonicalStateRow,
) -> Result<StagedChange, LixError> {
    Ok(StagedChange {
        id: None,
        entity_id: require_identity(row.entity_id.clone(), "undo restore entity_id")?,
        schema_key: require_identity(row.schema_key.clone(), "undo restore schema_key")?,
        schema_version: Some(require_identity(
            row.schema_version.clone(),
            "undo restore schema_version",
        )?),
        file_id: Some(require_identity(
            row.file_id.clone(),
            "undo restore file_id",
        )?),
        plugin_key: Some(require_identity(
            row.plugin_key.clone(),
            "undo restore plugin_key",
        )?),
        snapshot_content: Some(row.snapshot_content.clone()),
        metadata: row.metadata.clone(),
        version_id: require_identity(version_id.to_string(), "undo restore version_id")?,
        writer_key: None,
        created_at: None,
    })
}

fn build_tombstone_proposed_change(
    version_id: &str,
    change: &CanonicalChange,
) -> Result<StagedChange, LixError> {
    Ok(StagedChange {
        id: None,
        entity_id: require_identity(change.entity_id.clone(), "undo tombstone entity_id")?,
        schema_key: require_identity(change.schema_key.clone(), "undo tombstone schema_key")?,
        schema_version: Some(require_identity(
            change.schema_version.clone(),
            "undo tombstone schema_version",
        )?),
        file_id: Some(require_identity(
            change.file_id.clone(),
            "undo tombstone file_id",
        )?),
        plugin_key: Some(require_identity(
            change.plugin_key.clone(),
            "undo tombstone plugin_key",
        )?),
        snapshot_content: None,
        metadata: None,
        version_id: require_identity(version_id.to_string(), "undo tombstone version_id")?,
        writer_key: None,
        created_at: None,
    })
}

fn classify_forward_operation(
    target_commit_id: &str,
    change: &CanonicalChange,
    previous_row: Option<&CanonicalStateRow>,
) -> Result<StateCommitStreamOperation, LixError> {
    match (previous_row.is_some(), change.snapshot_content.is_some()) {
        (false, true) => Ok(StateCommitStreamOperation::Insert),
        (true, true) => Ok(StateCommitStreamOperation::Update),
        (true, false) => Ok(StateCommitStreamOperation::Delete),
        (false, false) => Err(LixError::unknown(format!(
            "target commit '{}' contains invalid tombstone-only change '{}'",
            target_commit_id, change.id
        ))),
    }
}

fn require_identity<T>(value: impl Into<String>, context: &str) -> Result<T, LixError>
where
    T: TryFrom<String, Error = LixError>,
{
    let value = value.into();
    T::try_from(value.clone()).map_err(|_| {
        LixError::unknown(format!(
            "{context} must be a non-empty canonical identity, got '{}'",
            value
        ))
    })
}
