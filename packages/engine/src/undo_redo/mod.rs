mod redo;
mod store;
mod types;
mod undo;

use std::collections::BTreeMap;

use crate::canonical::readers::{
    load_canonical_change_row_by_id, load_commit_lineage_entry_by_id,
    load_committed_version_head_commit_id_from_live_state,
    load_exact_committed_state_row_from_commit_with_executor, CommitLineageEntry,
    CommitQueryExecutor, CommittedCanonicalChangeRow, ExactCommittedStateRow,
    ExactCommittedStateRowRequest,
};
use crate::canonical::ProposedDomainChange;
use crate::state::stream::StateCommitStreamOperation;
use crate::{LixBackendTransaction, LixError, SessionTransaction};

pub use types::{RedoOptions, RedoResult, UndoOptions, UndoResult};
pub(crate) use types::{SemanticUndoRedoStacks, UndoRedoOperationKind, UndoRedoOperationRecord};

pub(crate) use redo::redo_with_options_in_session;
pub(crate) use undo::undo_with_options_in_session;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TargetCommitChangeEffect {
    pub(crate) forward_change: CommittedCanonicalChangeRow,
    pub(crate) previous_row: Option<ExactCommittedStateRow>,
    pub(crate) forward_operation: StateCommitStreamOperation,
}

pub(crate) async fn resolve_target_version_id_in_session(
    tx: &mut SessionTransaction<'_>,
    requested_version_id: Option<&str>,
) -> Result<String, LixError> {
    if let Some(version_id) = requested_version_id {
        ensure_version_exists_in_session(tx, version_id).await?;
        return Ok(version_id.to_string());
    }

    Ok(tx.context.active_version_id.clone())
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
    let mut executor = crate::engine::TransactionBackendAdapter::new(transaction);
    if !crate::live_state::version_exists_with_executor(&mut executor, version_id).await? {
        return Err(LixError::unknown(format!(
            "version '{}' does not exist",
            version_id
        )));
    }
    Ok(())
}

pub(crate) async fn rebuild_semantic_undo_redo_stacks(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
) -> Result<SemanticUndoRedoStacks, LixError> {
    let mut executor = crate::engine::TransactionBackendAdapter::new(transaction);
    let Some(head_commit_id) =
        load_committed_version_head_commit_id_from_live_state(&mut executor, version_id).await?
    else {
        return Ok(SemanticUndoRedoStacks::default());
    };
    let lineage = load_linear_commit_lineage(&mut executor, &head_commit_id).await?;
    let operations =
        store::load_undo_redo_operations_for_version_in_transaction(transaction, version_id)
            .await?;
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

        // Treat the root/bootstrap commit as a hard undo boundary. It can carry
        // internal seed changes, but it must never become an undo target.
        if !commit.change_ids.is_empty() && !commit.parent_commit_ids.is_empty() {
            stacks.undo_stack.push(commit.id);
            stacks.redo_stack.clear();
        }
    }

    Ok(stacks)
}

pub(crate) async fn load_linear_commit_lineage<E>(
    executor: &mut E,
    head_commit_id: &str,
) -> Result<Vec<CommitLineageEntry>, LixError>
where
    E: CommitQueryExecutor,
{
    let mut lineage = Vec::new();
    let mut current_commit_id = Some(head_commit_id.to_string());
    while let Some(commit_id) = current_commit_id.take() {
        let Some(commit) = load_commit_lineage_entry_by_id(executor, &commit_id).await? else {
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

pub(crate) async fn load_target_commit_change_effects(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
    target_commit_id: &str,
) -> Result<Vec<TargetCommitChangeEffect>, LixError> {
    let mut executor = crate::engine::TransactionBackendAdapter::new(transaction);
    let Some(target_commit) =
        load_commit_lineage_entry_by_id(&mut executor, target_commit_id).await?
    else {
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
        let Some(forward_change) =
            load_canonical_change_row_by_id(&mut executor, change_id).await?
        else {
            return Err(LixError::unknown(format!(
                "target commit '{}' references missing change '{}'",
                target_commit_id, change_id
            )));
        };
        let previous_row = if let Some(parent_commit_id) = parent_commit_id.as_deref() {
            let request = ExactCommittedStateRowRequest {
                entity_id: forward_change.entity_id.clone(),
                schema_key: forward_change.schema_key.clone(),
                version_id: version_id.to_string(),
                exact_filters: BTreeMap::from([
                    (
                        "file_id".to_string(),
                        crate::Value::Text(forward_change.file_id.clone()),
                    ),
                    (
                        "plugin_key".to_string(),
                        crate::Value::Text(forward_change.plugin_key.clone()),
                    ),
                    (
                        "schema_version".to_string(),
                        crate::Value::Text(forward_change.schema_version.clone()),
                    ),
                ]),
            };
            load_exact_committed_state_row_from_commit_with_executor(
                &mut executor,
                parent_commit_id,
                &request,
            )
            .await?
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

pub(crate) fn build_forward_proposed_change(
    version_id: &str,
    change: &CommittedCanonicalChangeRow,
) -> Result<ProposedDomainChange, LixError> {
    Ok(ProposedDomainChange {
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
    })
}

pub(crate) fn build_restore_proposed_change(
    version_id: &str,
    row: &ExactCommittedStateRow,
) -> Result<ProposedDomainChange, LixError> {
    Ok(ProposedDomainChange {
        entity_id: require_identity(row.entity_id.clone(), "undo restore entity_id")?,
        schema_key: require_identity(row.schema_key.clone(), "undo restore schema_key")?,
        schema_version: Some(require_identity(
            required_snapshot_text(row.values.get("schema_version"), "schema_version")?,
            "undo restore schema_version",
        )?),
        file_id: Some(require_identity(
            row.file_id.clone(),
            "undo restore file_id",
        )?),
        plugin_key: Some(require_identity(
            required_snapshot_text(row.values.get("plugin_key"), "plugin_key")?,
            "undo restore plugin_key",
        )?),
        snapshot_content: Some(required_snapshot_text(
            row.values.get("snapshot_content"),
            "snapshot_content",
        )?),
        metadata: row
            .values
            .get("metadata")
            .and_then(|value| value_as_text(Some(value))),
        version_id: require_identity(version_id.to_string(), "undo restore version_id")?,
        writer_key: None,
    })
}

pub(crate) fn build_tombstone_proposed_change(
    version_id: &str,
    change: &CommittedCanonicalChangeRow,
) -> Result<ProposedDomainChange, LixError> {
    Ok(ProposedDomainChange {
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
    })
}

fn classify_forward_operation(
    target_commit_id: &str,
    change: &CommittedCanonicalChangeRow,
    previous_row: Option<&ExactCommittedStateRow>,
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

fn required_snapshot_text(value: Option<&crate::Value>, field: &str) -> Result<String, LixError> {
    value_as_text(value).ok_or_else(|| LixError::unknown(format!("missing {field}")))
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

fn value_as_text(value: Option<&crate::Value>) -> Option<String> {
    match value {
        Some(crate::Value::Text(value)) => Some(value.clone()),
        Some(crate::Value::Integer(value)) => Some(value.to_string()),
        Some(crate::Value::Boolean(value)) => Some(value.to_string()),
        Some(crate::Value::Real(value)) => Some(value.to_string()),
        _ => None,
    }
}
