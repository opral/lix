use std::collections::BTreeSet;

use crate::LixError;
use crate::changelog::CommitId;
use crate::checkpoint::CHECKPOINT_MARKER_SCHEMA_KEY;
use crate::entity_pk::EntityPk;
use crate::forktree::{ForkTreeReadFacade, HistoricalStateRow, StateKey};
use crate::sql2::SqlWriteExecutionContext;
use crate::storage_adapter::{Storage, StorageAdapterRead};
use crate::transaction::Transaction;
use crate::transaction::types::{RawWriteBatch, TransactionWrite, TransactionWriteMode};
use crate::undo_redo::{
    UNDO_REDO_MARKER_SCHEMA_KEY, UndoRedoKind, UndoRedoMarker, marker_stage_row,
};

use super::SessionContext;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UndoReceipt {
    pub branch_id: String,
    pub target_commit_id: String,
    pub inverse_commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedoReceipt {
    pub branch_id: String,
    pub target_commit_id: String,
    pub replay_commit_id: String,
}

#[derive(Debug, Clone, Copy, Default)]
struct SemanticState {
    undo_top: Option<CommitId>,
    redo_top: Option<CommitId>,
    redo_target: Option<CommitId>,
    redo_next: Option<CommitId>,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Reverses the latest undoable tracked commit on the active branch.
    pub async fn undo(&self) -> Result<UndoReceipt, LixError> {
        self.with_write_transaction_lending(async move |transaction| {
            undo_in_transaction(transaction).await
        })
        .await
    }

    /// Replays the latest tracked commit abandoned by undo on the active branch.
    pub async fn redo(&self) -> Result<RedoReceipt, LixError> {
        self.with_write_transaction_lending(async move |transaction| {
            redo_in_transaction(transaction).await
        })
        .await
    }
}

async fn undo_in_transaction<S>(transaction: &mut Transaction<S>) -> Result<UndoReceipt, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch_id = transaction.active_branch_id().to_string();
    let head = transaction
        .load_branch_head(&branch_id)
        .await?
        .ok_or_else(|| LixError::branch_not_found(&branch_id, "undo", "target"))?;
    let facade = transaction.forktree_read_facade();
    let head_record = load_node(transaction, head).await?;
    let (state, head_delta) =
        semantic_state_for_record(&facade, &branch_id, head, &head_record).await?;
    let target = state.undo_top.ok_or_else(|| {
        LixError::new(
            LixError::CODE_NOTHING_TO_UNDO,
            format!("nothing to undo on branch '{branch_id}'"),
        )
    })?;
    let target_record = if target == head {
        head_record
    } else {
        load_node(transaction, target).await?
    };
    let parent = only_parent(&target_record.parent_commit_ids, target, "undo")?;
    let state_before_target = semantic_state_at(transaction, &facade, &branch_id, parent).await?;

    let target_delta = if target == head {
        head_delta
    } else {
        load_commit_delta(&facade, target).await?
    };
    let outcome = apply_state_diff(
        transaction,
        &facade,
        head,
        parent,
        target,
        false,
        &target_delta,
    )
    .await?;
    let inverse_commit_id = outcome.commit_id.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "undo did not stage an inverse commit",
        )
    })?;
    stage_marker(
        transaction,
        UndoRedoMarker {
            branch_id: branch_id.clone(),
            kind: UndoRedoKind::Undo,
            target_commit_id: target,
            undo_target_after: state_before_target.undo_top,
            redo_top_after: None,
            redo_next: state.redo_top,
        },
    )
    .await?;

    Ok(UndoReceipt {
        branch_id,
        target_commit_id: target.to_string(),
        inverse_commit_id,
    })
}

async fn redo_in_transaction<S>(transaction: &mut Transaction<S>) -> Result<RedoReceipt, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch_id = transaction.active_branch_id().to_string();
    let head = transaction
        .load_branch_head(&branch_id)
        .await?
        .ok_or_else(|| LixError::branch_not_found(&branch_id, "redo", "target"))?;
    let facade = transaction.forktree_read_facade();
    let head_record = load_node(transaction, head).await?;
    let (state, _) = semantic_state_for_record(&facade, &branch_id, head, &head_record).await?;
    let redo_node = state.redo_top.ok_or_else(|| {
        LixError::new(
            LixError::CODE_NOTHING_TO_REDO,
            format!("nothing to redo on branch '{branch_id}'"),
        )
    })?;
    let (target, redo_next) = if redo_node == head {
        match state.redo_target {
            Some(target) => (target, state.redo_next),
            None => return Err(missing_redo_node(redo_node)),
        }
    } else {
        let node = operation_marker_at(&facade, &branch_id, redo_node)
            .await?
            .filter(|marker| marker.kind == UndoRedoKind::Undo)
            .ok_or_else(|| missing_redo_node(redo_node))?;
        (node.target_commit_id, node.redo_next)
    };
    let target_record = load_node(transaction, target).await?;
    only_parent(&target_record.parent_commit_ids, target, "redo")?;

    let target_delta = load_commit_delta(&facade, target).await?;
    let outcome = apply_state_diff(
        transaction,
        &facade,
        head,
        target,
        target,
        true,
        &target_delta,
    )
    .await?;
    let replay_commit_id = outcome.commit_id.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "redo did not stage a replay commit",
        )
    })?;
    stage_marker(
        transaction,
        UndoRedoMarker {
            branch_id: branch_id.clone(),
            kind: UndoRedoKind::Redo,
            target_commit_id: target,
            undo_target_after: Some(target),
            redo_top_after: redo_next,
            redo_next: None,
        },
    )
    .await?;

    Ok(RedoReceipt {
        branch_id,
        target_commit_id: target.to_string(),
        replay_commit_id,
    })
}

async fn semantic_state_at<S>(
    transaction: &mut Transaction<S>,
    facade: &ForkTreeReadFacade<impl StorageAdapterRead>,
    branch_id: &str,
    commit_id: CommitId,
) -> Result<SemanticState, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let record = load_node(transaction, commit_id).await?;
    if record.parent_commit_ids.len() != 1 {
        return Ok(SemanticState::default());
    }
    let keys = semantic_keys(branch_id)?;
    let marker_delta = facade.load_commit_delta_rows(commit_id, None).await?;
    let mut has_checkpoint = false;
    let mut has_foreign_operation = false;
    let mut has_local_operation = false;
    for row in marker_delta.iter().filter(|row| !row.deleted) {
        match row.key.schema_key.as_str() {
            CHECKPOINT_MARKER_SCHEMA_KEY => has_checkpoint = true,
            UNDO_REDO_MARKER_SCHEMA_KEY if row.key.entity_pk == keys[1].entity_pk => {
                has_local_operation = true;
            }
            UNDO_REDO_MARKER_SCHEMA_KEY => has_foreign_operation = true,
            _ => {}
        }
    }
    if has_checkpoint || has_foreign_operation {
        return Ok(SemanticState::default());
    }
    if !has_local_operation {
        return Ok(SemanticState {
            undo_top: Some(commit_id),
            redo_top: None,
            redo_target: None,
            redo_next: None,
        });
    }
    let marker = operation_marker_at(facade, branch_id, commit_id)
        .await?
        .ok_or_else(|| missing_operation_marker(commit_id))?;
    Ok(semantic_state_from_marker(marker, commit_id))
}

fn semantic_keys(branch_id: &str) -> Result<[StateKey; 2], LixError> {
    let branch_pk = EntityPk::uuid_from_canonical(branch_id).map_err(|error| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("undo branch id must be a canonical UUID: {error}"),
        )
    })?;
    Ok([
        StateKey {
            schema_key: CHECKPOINT_MARKER_SCHEMA_KEY.to_string(),
            file_id: None,
            entity_pk: branch_pk.clone(),
        },
        StateKey {
            schema_key: UNDO_REDO_MARKER_SCHEMA_KEY.to_string(),
            file_id: None,
            entity_pk: branch_pk,
        },
    ])
}

async fn semantic_state_for_record(
    facade: &ForkTreeReadFacade<impl StorageAdapterRead>,
    branch_id: &str,
    commit_id: CommitId,
    record: &crate::commit_graph::CommitGraphNode,
) -> Result<(SemanticState, Vec<HistoricalStateRow>), LixError> {
    if record.parent_commit_ids.len() != 1 {
        return Ok((SemanticState::default(), Vec::new()));
    }

    let keys = semantic_keys(branch_id)?;
    let delta_rows = facade.load_commit_delta_rows(commit_id, None).await?;
    if delta_rows
        .iter()
        .any(|row| row.key.schema_key == CHECKPOINT_MARKER_SCHEMA_KEY && !row.deleted)
    {
        return Ok((SemanticState::default(), delta_rows));
    }
    let operation_marker = delta_rows
        .iter()
        .find(|row| row.key.schema_key == UNDO_REDO_MARKER_SCHEMA_KEY && !row.deleted);
    let Some(operation_row) = operation_marker else {
        return Ok((
            SemanticState {
                undo_top: Some(commit_id),
                redo_top: None,
                redo_target: None,
                redo_next: None,
            },
            delta_rows,
        ));
    };
    let operation_key = StateKey {
        schema_key: keys[1].schema_key.clone(),
        file_id: keys[1].file_id.clone(),
        entity_pk: keys[1].entity_pk.clone(),
    };
    if operation_row.key != operation_key {
        return Ok((SemanticState::default(), delta_rows));
    }
    let marker = parse_marker(operation_row.snapshot_content.as_ref(), commit_id)?;
    let state = semantic_state_from_marker(marker, commit_id);
    Ok((state, delta_rows))
}

fn semantic_state_from_marker(marker: UndoRedoMarker, commit_id: CommitId) -> SemanticState {
    match marker.kind {
        UndoRedoKind::Undo => SemanticState {
            undo_top: marker.undo_target_after,
            redo_top: Some(commit_id),
            redo_target: Some(marker.target_commit_id),
            redo_next: marker.redo_next,
        },
        UndoRedoKind::Redo => SemanticState {
            undo_top: Some(marker.target_commit_id),
            redo_top: marker.redo_top_after,
            redo_target: None,
            redo_next: None,
        },
    }
}

async fn operation_marker_at(
    facade: &ForkTreeReadFacade<impl StorageAdapterRead>,
    branch_id: &str,
    commit_id: CommitId,
) -> Result<Option<UndoRedoMarker>, LixError> {
    let branch_pk = EntityPk::uuid_from_canonical(branch_id)
        .map_err(|error| LixError::new(LixError::CODE_INVALID_PARAM, error.to_string()))?;
    let key = StateKey {
        schema_key: UNDO_REDO_MARKER_SCHEMA_KEY.to_string(),
        file_id: None,
        entity_pk: branch_pk,
    };
    let rows = facade
        .load_state_rows_at_commit(&commit_id.to_string(), std::slice::from_ref(&key))
        .await?;
    let Some(row) = rows
        .into_iter()
        .next()
        .flatten()
        .filter(|row| !row.deleted && row.commit_id == commit_id)
    else {
        return Ok(None);
    };
    parse_marker(row.snapshot_content.as_ref(), commit_id).map(Some)
}

fn parse_marker(
    snapshot: Option<&crate::SharedStr>,
    commit_id: CommitId,
) -> Result<UndoRedoMarker, LixError> {
    let snapshot = snapshot.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("undo/redo marker at '{commit_id}' has no snapshot"),
        )
    })?;
    serde_json::from_str::<UndoRedoMarker>(snapshot.as_str()).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("undo/redo marker at '{commit_id}' is invalid: {error}"),
        )
    })
}

fn missing_redo_node(commit_id: CommitId) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("redo node '{commit_id}' has no undo marker"),
    )
}

fn missing_operation_marker(commit_id: CommitId) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!(
            "commit delta for '{commit_id}' contains an undo/redo marker that cannot be loaded"
        ),
    )
}

async fn load_commit_delta(
    facade: &ForkTreeReadFacade<impl StorageAdapterRead>,
    commit_id: CommitId,
) -> Result<Vec<HistoricalStateRow>, LixError> {
    facade.load_commit_delta_rows(commit_id, None).await
}

async fn load_node<S>(
    transaction: &mut Transaction<S>,
    commit_id: CommitId,
) -> Result<crate::commit_graph::CommitGraphNode, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    transaction
        .commit_graph_reader_on_opening_read()
        .load_node(&commit_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_COMMIT_NOT_FOUND,
                format!("commit '{commit_id}' does not exist"),
            )
        })
}

fn only_parent(
    parents: &[CommitId],
    commit_id: CommitId,
    operation: &str,
) -> Result<CommitId, LixError> {
    match parents {
        [parent] => Ok(*parent),
        [] => Err(LixError::new(
            LixError::CODE_NOTHING_TO_UNDO,
            format!("cannot {operation} root commit '{commit_id}'"),
        )),
        _ => Err(LixError::new(
            LixError::CODE_INVALID_MERGE,
            format!("cannot {operation} merge commit '{commit_id}'"),
        )),
    }
}

async fn apply_state_diff<S>(
    transaction: &mut Transaction<S>,
    facade: &ForkTreeReadFacade<impl StorageAdapterRead>,
    current: CommitId,
    desired: CommitId,
    target: CommitId,
    desired_is_target: bool,
    target_delta: &[HistoricalStateRow],
) -> Result<crate::sql2::DiffCommandOutcome, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let cascade_file_ids = descriptor_dependency_cascade_file_ids(target_delta)?;
    let keys = if cascade_file_ids.is_empty() {
        target_delta
            .iter()
            .map(|row| row.key.clone())
            .collect::<Vec<_>>()
    } else {
        let visible_schema_keys = transaction.visible_schema_keys()?;
        let dependency_commit = if desired_is_target { current } else { desired };
        descriptor_dependency_closure(
            facade,
            current,
            desired,
            dependency_commit,
            target_delta,
            &cascade_file_ids,
            &visible_schema_keys,
        )
        .await?
    };
    let keys = keys
        .into_iter()
        .filter(|key| {
            key.schema_key != CHECKPOINT_MARKER_SCHEMA_KEY
                && key.schema_key != UNDO_REDO_MARKER_SCHEMA_KEY
        })
        .collect::<Vec<_>>();
    transaction
        .execute_state_transition_with_facade(facade, current, desired, keys)
        .await
}

async fn descriptor_dependency_closure(
    facade: &ForkTreeReadFacade<impl StorageAdapterRead>,
    current: CommitId,
    desired: CommitId,
    dependency_commit: CommitId,
    target_delta: &[HistoricalStateRow],
    file_ids: &[String],
    visible_schema_keys: &[String],
) -> Result<Vec<StateKey>, LixError> {
    let mut keys = target_delta
        .iter()
        .map(|row| row.key.clone())
        .collect::<BTreeSet<_>>();
    let mut schema_keys = visible_schema_keys.iter().cloned().collect::<BTreeSet<_>>();
    if target_delta
        .iter()
        .any(|row| row.key.schema_key == "lix_registered_schema")
    {
        for endpoint in [current, desired] {
            for row in facade.scan_state_rows_at_commit(endpoint).await? {
                if row.key.schema_key == "lix_registered_schema" && !row.deleted {
                    schema_keys.insert(row.key.entity_pk.as_single_string_owned().map_err(
                        |error| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "registered schema dependency identity is invalid: {error}"
                                ),
                            )
                        },
                    )?);
                }
            }
        }
    }
    for row in facade.scan_state_rows_at_commit(dependency_commit).await? {
        if row.deleted
            || !row
                .key
                .file_id
                .as_ref()
                .is_some_and(|file_id| file_ids.contains(file_id))
            || !schema_keys.contains(&row.key.schema_key)
        {
            continue;
        }
        keys.insert(row.key);
    }
    Ok(keys.into_iter().collect())
}

fn descriptor_dependency_cascade_file_ids(
    target_delta: &[HistoricalStateRow],
) -> Result<Vec<String>, LixError> {
    let mut file_ids = BTreeSet::new();
    for row in target_delta {
        if row.key.schema_key != "lix_file_descriptor" || !row.deleted {
            continue;
        }
        let file_id = row
            .key
            .entity_pk
            .as_single_string_owned()
            .map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("file descriptor tombstone has invalid identity: {error}"),
                )
            })?;
        file_ids.insert(file_id);
    }
    Ok(file_ids.into_iter().collect())
}

async fn stage_marker<S>(
    transaction: &mut Transaction<S>,
    marker: UndoRedoMarker,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut rows = RawWriteBatch::with_capacity(1);
    rows.push(marker_stage_row(&marker));
    transaction
        .stage_write(TransactionWrite::Rows {
            mode: TransactionWriteMode::Replace,
            rows,
        })
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use super::{load_commit_delta, load_node, only_parent};
    use crate::engine::Engine;
    use crate::sql2::SqlWriteExecutionContext;
    use crate::storage::Memory;
    use crate::{
        Blob, CreateBranchOptions, ExecuteBatchStatement, LixError, MergeBranchOptions,
        MergeBranchOutcome, Value,
    };

    async fn setup_engine() -> Engine<Memory> {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage initializes");
        Engine::new(storage).await.expect("engine opens")
    }

    async fn setup() -> crate::session::SessionContext<Memory> {
        setup_engine()
            .await
            .open_workspace_session()
            .await
            .expect("session opens")
    }

    async fn value(session: &crate::session::SessionContext<Memory>, key: &str) -> Option<String> {
        let result = session
            .execute(
                "SELECT value FROM lix_key_value WHERE key = $1",
                &[Value::Text(key.to_string())],
            )
            .await
            .expect("value reads");
        result
            .rows()
            .first()
            .and_then(|row| row.get::<Value>("value").ok())
            .and_then(|value| match value {
                Value::Text(value) => Some(value),
                Value::Json(JsonValue::String(value)) => Some(value),
                _ => None,
            })
    }

    #[tokio::test]
    async fn undo_redo_tracks_branch_actions_without_rewinding_history() {
        let session = setup().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('theme', 'light')",
                &[],
            )
            .await
            .expect("insert commits");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'dark' WHERE key = 'theme'",
                &[],
            )
            .await
            .expect("update commits");

        let update = session.undo().await.expect("update undoes");
        assert_eq!(value(&session, "theme").await.as_deref(), Some("light"));
        let insert = session.undo().await.expect("insert undoes");
        assert_ne!(update.target_commit_id, insert.target_commit_id);
        assert_eq!(value(&session, "theme").await, None);

        session.redo().await.expect("insert redoes");
        assert_eq!(value(&session, "theme").await.as_deref(), Some("light"));
        session.redo().await.expect("update redoes");
        assert_eq!(value(&session, "theme").await.as_deref(), Some("dark"));
    }

    #[tokio::test]
    async fn typed_transition_rejects_duplicate_keys_and_non_head_sources() {
        let session = setup().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('typed', 'before')",
                &[],
            )
            .await
            .expect("insert commits");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'after' WHERE key = 'typed'",
                &[],
            )
            .await
            .expect("update commits");

        let duplicate = session
            .with_write_transaction_lending(async move |transaction| {
                let branch_id = transaction.active_branch_id().to_string();
                let facade = transaction.forktree_read_facade();
                let head = transaction
                    .load_branch_head(&branch_id)
                    .await?
                    .expect("branch has a head");
                let record = load_node(transaction, head).await?;
                let parent = only_parent(&record.parent_commit_ids, head, "test")?;
                let key = load_commit_delta(&facade, head)
                    .await?
                    .into_iter()
                    .find(|row| row.key.schema_key == "lix_key_value")
                    .expect("update delta contains the key-value row")
                    .key
                    .clone();
                transaction
                    .execute_state_transition(head, parent, vec![key.clone(), key])
                    .await
            })
            .await
            .expect_err("duplicate transition identities are rejected");
        assert_eq!(duplicate.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(value(&session, "typed").await.as_deref(), Some("after"));

        let stale = session
            .with_write_transaction_lending(async move |transaction| {
                let branch_id = transaction.active_branch_id().to_string();
                let facade = transaction.forktree_read_facade();
                let head = transaction
                    .load_branch_head(&branch_id)
                    .await?
                    .expect("branch has a head");
                let record = load_node(transaction, head).await?;
                let parent = only_parent(&record.parent_commit_ids, head, "test")?;
                let key = load_commit_delta(&facade, head)
                    .await?
                    .into_iter()
                    .find(|row| row.key.schema_key == "lix_key_value")
                    .expect("update delta contains the key-value row")
                    .key
                    .clone();
                transaction
                    .execute_state_transition(parent, head, vec![key])
                    .await
            })
            .await
            .expect_err("non-head transition sources are rejected");
        assert_eq!(stale.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert_eq!(value(&session, "typed").await.as_deref(), Some("after"));
    }

    #[tokio::test]
    async fn ordinary_commit_after_undo_discards_old_redo_path() {
        let session = setup().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('one', '1')",
                &[],
            )
            .await
            .expect("first insert commits");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('two', '2')",
                &[],
            )
            .await
            .expect("second insert commits");
        session.undo().await.expect("second insert undoes");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('three', '3')",
                &[],
            )
            .await
            .expect("replacement action commits");

        let error = session.redo().await.expect_err("old redo is discarded");
        assert_eq!(error.code, LixError::CODE_NOTHING_TO_REDO);
        session.undo().await.expect("replacement action undoes");
        assert_eq!(value(&session, "three").await, None);
        session.undo().await.expect("first action remains undoable");
        assert_eq!(value(&session, "one").await, None);
    }

    #[tokio::test]
    async fn checkpoint_is_an_undo_floor() {
        let session = setup().await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('before', 'checkpoint')",
                &[],
            )
            .await
            .expect("pre-checkpoint insert commits");
        session
            .create_checkpoint()
            .await
            .expect("checkpoint commits");
        let error = session.undo().await.expect_err("checkpoint blocks undo");
        assert_eq!(error.code, LixError::CODE_NOTHING_TO_UNDO);

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('after', 'checkpoint')",
                &[],
            )
            .await
            .expect("post-checkpoint insert commits");
        session.undo().await.expect("post-checkpoint action undoes");
        let error = session.undo().await.expect_err("undo stops at checkpoint");
        assert_eq!(error.code, LixError::CODE_NOTHING_TO_UNDO);
        assert_eq!(
            value(&session, "before").await.as_deref(),
            Some("checkpoint")
        );
    }

    #[tokio::test]
    async fn branch_forked_at_checkpoint_starts_at_an_undo_floor() {
        let engine = setup_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("session opens");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('before-fork', 'kept')",
                &[],
            )
            .await
            .expect("pre-checkpoint insert commits");
        let checkpoint = session
            .create_checkpoint()
            .await
            .expect("checkpoint commits");
        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-0000000000a1".to_string()),
                name: "checkpoint-fork".to_string(),
                from_commit_id: Some(checkpoint.commit_id),
            })
            .await
            .expect("branch creates");
        let fork = engine.open_session(branch.id).await.expect("fork opens");

        let error = fork
            .undo()
            .await
            .expect_err("foreign checkpoint is a floor");
        assert_eq!(error.code, LixError::CODE_NOTHING_TO_UNDO);
        assert_eq!(value(&fork, "before-fork").await.as_deref(), Some("kept"));
    }

    #[tokio::test]
    async fn branch_forked_at_undo_commit_resets_undo_history() {
        let engine = setup_engine().await;
        let session = engine
            .open_workspace_session()
            .await
            .expect("session opens");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('abandoned', 'change')",
                &[],
            )
            .await
            .expect("insert commits");
        let undone = session.undo().await.expect("insert undoes");
        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-0000000000a2".to_string()),
                name: "undo-fork".to_string(),
                from_commit_id: Some(undone.inverse_commit_id),
            })
            .await
            .expect("branch creates");
        let fork = engine.open_session(branch.id).await.expect("fork opens");

        let error = fork
            .undo()
            .await
            .expect_err("foreign operation commit is a floor");
        assert_eq!(error.code, LixError::CODE_NOTHING_TO_UNDO);
        assert_eq!(value(&fork, "abandoned").await, None);
    }

    #[tokio::test]
    async fn atomic_batch_is_one_undo_unit() {
        let session = setup().await;
        session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_key_value (key, value) VALUES ('left', '1')".into(),
                    params: vec![],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_key_value (key, value) VALUES ('right', '2')".into(),
                    params: vec![],
                },
            ])
            .await
            .expect("batch commits");
        session.undo().await.expect("batch undoes");
        assert_eq!(value(&session, "left").await, None);
        assert_eq!(value(&session, "right").await, None);
        let error = session.undo().await.expect_err("batch was one unit");
        assert_eq!(error.code, LixError::CODE_NOTHING_TO_UNDO);
    }

    #[tokio::test]
    async fn file_create_and_update_roundtrip_through_undo_redo() {
        let session = setup().await;
        session
            .upsert_file_content("/note.txt".into(), Blob::from("one".as_bytes()))
            .await
            .expect("file creates");
        session
            .upsert_file_content("/note.txt".into(), Blob::from("two".as_bytes()))
            .await
            .expect("file updates");

        session.undo().await.expect("file update undoes");
        assert_eq!(
            session
                .read_file_content("/note.txt".into(), None)
                .await
                .expect("file reads")
                .expect("file exists")
                .content()
                .as_ref(),
            b"one"
        );
        session.undo().await.expect("file create undoes");
        assert_eq!(
            session
                .read_file_content("/note.txt".into(), None)
                .await
                .expect("file reads"),
            None
        );
        session.redo().await.expect("file create redoes");
        session.redo().await.expect("file update redoes");
        assert_eq!(
            session
                .read_file_content("/note.txt".into(), None)
                .await
                .expect("file reads")
                .expect("file exists")
                .content()
                .as_ref(),
            b"two"
        );
    }

    #[tokio::test]
    async fn file_delete_roundtrips_exact_tracked_dependency_closure() {
        let session = setup().await;
        session
            .upsert_file_content("/deleted.txt".into(), Blob::from("restored".as_bytes()))
            .await
            .expect("file creates");
        session
            .upsert_file_content("/unrelated.txt".into(), Blob::from("untouched".as_bytes()))
            .await
            .expect("unrelated file creates");
        let files = session
            .execute(
                "SELECT id, path FROM lix_file WHERE path IN ('/deleted.txt', '/unrelated.txt')",
                &[],
            )
            .await
            .expect("file identities read");
        let file_id = |path: &str| {
            files
                .rows()
                .iter()
                .find(|row| row.get::<String>("path").ok().as_deref() == Some(path))
                .and_then(|row| row.get::<String>("id").ok())
                .expect("file identity exists")
        };
        let deleted_file_id = file_id("/deleted.txt");
        let unrelated_file_id = file_id("/unrelated.txt");
        session
            .execute_batch(&[
                ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_key_value (key, value, lixcol_file_id) VALUES ('deleted-state', 'restore-me', $1)".to_string(),
                    params: vec![Value::Text(deleted_file_id)],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_key_value (key, value, lixcol_file_id) VALUES ('unrelated-state', 'keep-me', $1)".to_string(),
                    params: vec![Value::Text(unrelated_file_id)],
                },
                ExecuteBatchStatement {
                    label: None,
                    sql: "INSERT INTO lix_key_value (key, value) VALUES ('global-state', 'keep-me')"
                        .to_string(),
                    params: vec![],
                },
            ])
            .await
            .expect("tracked dependency rows write");
        session
            .execute("DELETE FROM lix_file WHERE path = '/deleted.txt'", &[])
            .await
            .expect("file deletes");
        assert_eq!(
            session
                .read_file_content("/deleted.txt".into(), None)
                .await
                .expect("deleted file reads"),
            None
        );
        assert_eq!(value(&session, "deleted-state").await, None);
        assert_eq!(
            value(&session, "unrelated-state").await.as_deref(),
            Some("keep-me")
        );
        assert_eq!(
            value(&session, "global-state").await.as_deref(),
            Some("keep-me")
        );

        session.undo().await.expect("file deletion undoes");
        assert_eq!(
            session
                .read_file_content("/deleted.txt".into(), None)
                .await
                .expect("restored file reads")
                .expect("file is restored")
                .content()
                .as_ref(),
            b"restored"
        );
        assert_eq!(
            value(&session, "deleted-state").await.as_deref(),
            Some("restore-me")
        );
        assert_eq!(
            value(&session, "unrelated-state").await.as_deref(),
            Some("keep-me")
        );
        assert_eq!(
            value(&session, "global-state").await.as_deref(),
            Some("keep-me")
        );
        session.redo().await.expect("file deletion redoes");
        assert_eq!(
            session
                .read_file_content("/deleted.txt".into(), None)
                .await
                .expect("redeleted file reads"),
            None
        );
        assert_eq!(value(&session, "deleted-state").await, None);
        assert_eq!(
            value(&session, "unrelated-state").await.as_deref(),
            Some("keep-me")
        );
        assert_eq!(
            value(&session, "global-state").await.as_deref(),
            Some("keep-me")
        );
    }

    #[tokio::test]
    async fn redo_cursor_is_durable_across_fresh_sessions() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage initializes");
        let engine = Engine::new(storage).await.expect("engine opens");
        let first = engine
            .open_workspace_session()
            .await
            .expect("first session opens");
        first
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('durable', 'yes')",
                &[],
            )
            .await
            .expect("insert commits");
        first.undo().await.expect("insert undoes");
        let branch_id = first.active_branch_id().await.expect("branch resolves");
        drop(first);

        let reopened = engine
            .open_session(branch_id)
            .await
            .expect("fresh pinned session opens");
        reopened.redo().await.expect("redo survives session loss");
        assert_eq!(value(&reopened, "durable").await.as_deref(), Some("yes"));
    }

    #[tokio::test]
    async fn merge_commit_is_an_undo_floor() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage initializes");
        let engine = Engine::new(storage).await.expect("engine opens");
        let main = engine
            .open_workspace_session()
            .await
            .expect("main session opens");
        let draft = main
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-000000000099".to_string()),
                name: "draft".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("draft creates");
        let draft_session = engine
            .open_session(draft.id.clone())
            .await
            .expect("draft session opens");
        draft_session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('draft', 'change')",
                &[],
            )
            .await
            .expect("draft diverges");
        main.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('main', 'change')",
            &[],
        )
        .await
        .expect("main diverges");
        let merge = main
            .merge_branch(MergeBranchOptions {
                source_branch_id: draft.id,
            })
            .await
            .expect("merge commits");
        assert_eq!(merge.outcome, MergeBranchOutcome::MergeCommitted);

        let error = main.undo().await.expect_err("merge blocks undo");
        assert_eq!(error.code, LixError::CODE_NOTHING_TO_UNDO);
    }

    #[tokio::test]
    async fn observers_receive_undo_and_redo_state() {
        let session = setup().await;
        let mut events = session
            .observe(
                "SELECT value FROM lix_key_value WHERE key = 'observed'",
                &[],
            )
            .expect("observation opens");
        events.next().await.expect("initial event reads");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observed', 'yes')",
                &[],
            )
            .await
            .expect("insert commits");
        assert_eq!(
            events
                .next()
                .await
                .expect("insert event reads")
                .expect("insert event exists")
                .rows
                .rows()
                .len(),
            1
        );
        session.undo().await.expect("insert undoes");
        assert_eq!(
            events
                .next()
                .await
                .expect("undo event reads")
                .expect("undo event exists")
                .rows
                .rows()
                .len(),
            0
        );
        session.redo().await.expect("insert redoes");
        assert_eq!(
            events
                .next()
                .await
                .expect("redo event reads")
                .expect("redo event exists")
                .rows
                .rows()
                .len(),
            1
        );
    }
}
