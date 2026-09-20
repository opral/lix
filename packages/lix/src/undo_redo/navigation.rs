use std::collections::BTreeSet;

use crate::LixError;
use crate::changelog::{ChangeRecordProjection, CommitId};
use crate::checkpoint::CHECKPOINT_SCHEMA_KEY;
use crate::row_pk::RowPk;
use crate::sql2::{
    DiffCommand, DiffCommandOutcome, DiffCommandSelection, SqlWriteExecutionContext,
};
use crate::storage_adapter::Storage;
use crate::tracked_state::{TrackedStateDiffRequest, TrackedStateKey};
use crate::transaction::Transaction;
use crate::transaction_types::{
    RawWriteBatch, TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};
use crate::undo_redo::{
    UNDO_REDO_MARKER_SCHEMA_KEY, UNDO_STATE_SCHEMA_KEY, UndoRedoKind, UndoRedoMarker,
    marker_stage_row,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
struct SemanticState {
    undo_top: Option<CommitId>,
    redo_top: Option<CommitId>,
}

/// One point-addressable state row per original target or undo receipt. No
/// operation rewrites the complete branch history to advance an undo cursor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum UndoState {
    Epoch {
        checkpoint: CommitId,
    },
    Target {
        epoch: Option<CommitId>,
        parent: CommitId,
        checkpoint: bool,
        all: BTreeSet<String>,
        undone: BTreeSet<String>,
        retired: bool,
    },
    Receipt {
        epoch: Option<CommitId>,
        pending: BTreeSet<String>,
        metadata_pending: bool,
    },
}

fn no_change() -> DiffCommandOutcome {
    DiffCommandOutcome {
        rows_affected: 0,
        commit_id: None,
        parent_commit_id: None,
    }
}

pub(crate) fn is_undo_metadata(schema: &str) -> bool {
    matches!(
        schema,
        CHECKPOINT_SCHEMA_KEY | UNDO_REDO_MARKER_SCHEMA_KEY | UNDO_STATE_SCHEMA_KEY
    )
}

#[tracing::instrument(target = "lix_perf", name = "lix.perf.undo_redo", skip_all, fields(redo, explicit_target = requested.is_some(), selected = scope.is_some()))]
pub(crate) async fn execute<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    redo: bool,
    requested: Option<CommitId>,
    scope: Option<Vec<DiffCommandSelection>>,
) -> Result<DiffCommandOutcome, LixError> {
    let whole_scope = scope.is_none();
    let branch = tx.active_branch_id().to_string();
    let head = tx
        .load_branch_head(&branch)
        .await?
        .ok_or_else(|| LixError::branch_not_found(&branch, "undo/redo", "branch"))?;
    let mut state = semantic_state_at(tx, &branch, head).await?;
    if requested.is_none() {
        normalize_cursor(tx, &branch, head, redo, &mut state).await?;
    }
    let Some(requested_id) = requested.or(if redo { state.redo_top } else { state.undo_top })
    else {
        return Ok(no_change());
    };
    if requested.is_some()
        && tx
            .commit_graph_reader()
            .await?
            .merge_base(&head, &requested_id)
            .await?
            != requested_id
        && load_state(tx, head, requested_id).await?.is_none()
    {
        return Err(LixError::new(
            if redo {
                "LIX_INVALID_REDO_TARGET"
            } else {
                "LIX_INVALID_UNDO_TARGET"
            },
            "undo/redo target is not incorporated on this branch",
        ));
    }
    // Resolve an internal logical frame separately from public target roles.
    let requested_marker = operation_marker_any(tx, requested_id).await?;
    let (target, frame_effects, undo_next) = if !redo && requested.is_none() {
        if let Some(marker) = &requested_marker {
            if marker.kind != UndoRedoKind::Redo {
                return Err(LixError::unknown("invalid logical undo frame"));
            }
            (
                marker.target_commit_id,
                Some(marker.effects.clone()),
                marker.undo_target_after,
            )
        } else {
            let node = load_node(tx, requested_id).await?;
            let parent = only_parent(&node.parent_commit_ids, requested_id, "undo")?;
            (
                requested_id,
                None,
                semantic_state_at(tx, &branch, parent).await?.undo_top,
            )
        }
    } else if redo {
        let marker = requested_marker
            .as_ref()
            .filter(|m| m.kind == UndoRedoKind::Undo)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_INVALID_REDO_TARGET",
                    "redo requires the commit returned by undo",
                )
            })?;
        (marker.target_commit_id, None, state.undo_top)
    } else {
        if requested_marker.is_some() {
            return Err(LixError::new(
                "LIX_INVALID_UNDO_TARGET",
                "undo requires an original action, not an undo/redo receipt",
            ));
        }
        let node = load_node(tx, requested_id).await?;
        let parent = only_parent(&node.parent_commit_ids, requested_id, "undo")?;
        let next = if state.undo_top == Some(requested_id) && scope.is_some() {
            Some(requested_id)
        } else if state.undo_top == Some(requested_id) {
            semantic_state_at(tx, &branch, parent).await?.undo_top
        } else {
            state.undo_top
        };
        (requested_id, None, next)
    };
    let node = load_node(tx, target).await?;
    let parent = only_parent(
        &node.parent_commit_ids,
        target,
        if redo { "redo" } else { "undo" },
    )?;
    let mut target_state = load_state(tx, head, target).await?;
    let baseline = tx.load_branch_working_base(&branch).await?;
    let epoch_id =
        CommitId::parse("00000000-0000-0000-0000-000000000001").expect("internal epoch UUID");
    let epoch = if node.is_checkpoint {
        let stored = match load_state(tx, head, epoch_id).await? {
            Some(UndoState::Epoch { checkpoint }) => Some(checkpoint),
            _ => None,
        };
        let mut latest = stored;
        if let Some(baseline) = baseline {
            let baseline_node = load_node(tx, baseline).await?;
            if baseline_node.is_checkpoint
                && match stored {
                    Some(stored) => {
                        tx.commit_graph_reader().await?.merge_base(&baseline, &stored).await? != baseline
                    }
                    None => true,
                }
            {
                latest = Some(baseline);
            }
        }
        latest.or(Some(target))
    } else {
        None
    };
    let mut receipt_state = if redo {
        load_state(tx, head, requested_id).await?
    } else {
        None
    };
    if node.is_checkpoint {
        let same_epoch = matches!(&target_state, Some(UndoState::Target { epoch: previous, .. }) if *previous == epoch);
        if !redo && baseline == Some(target) && !same_epoch {
            // A formerly superseded checkpoint can start a fresh undo cycle
            // when it is again the active baseline. Old receipts keep their epoch.
            target_state = None;
        }
        let receipt_current = !redo
            || matches!(&receipt_state, Some(UndoState::Receipt { epoch: previous, .. }) if *previous == epoch);
        let eligible = receipt_current
            && match &target_state {
                Some(UndoState::Target {
                    epoch: previous,
                    retired: true,
                    parent,
                    ..
                }) => *previous == epoch && baseline == Some(*parent),
                _ => baseline == Some(target),
            };
        if !eligible {
            return Err(LixError::new(
                "LIX_STALE_CHECKPOINT_UNDO",
                "checkpoint undo cycle was superseded; use restore or revert for historical content",
            ));
        }
    }
    if scope.as_ref().is_some_and(Vec::is_empty) {
        return Ok(no_change());
    }
    let from = target.to_string();
    let to = parent.to_string();
    let mut selected_files = BTreeSet::new();
    if redo && target_state.is_none() {
        return Err(LixError::new("LIX_INVALID_REDO_TARGET", "undo receipt lacks its incorporated target state"));
    }
    let full = if redo && scope.is_none() {
        selected_files = requested_marker.as_ref().expect("validated undo receipt").selected_files.clone();
        match &receipt_state {
            Some(UndoState::Receipt { pending, .. }) => Some(pending.clone()),
            _ => None,
        }
    } else if scope.is_none() || (node.is_checkpoint && target_state.is_none()) {
        let diff = tx
            .tracked_state_reader()
            .await?
            .diff_commits(&from, &to, &TrackedStateDiffRequest { retain_payloads: false, ..TrackedStateDiffRequest::default() })
            .await?;
        let mut ids = BTreeSet::new();
        for entry in diff.entries {
            if is_undo_metadata(entry.identity.schema_key()) {
                continue;
            }
            if entry.identity.schema_key() == "lix_file_descriptor" {
                selected_files.insert(entry.identity.row_pk().as_single_string_owned()?);
            }
            ids.insert(entry.diff_id()?);
        }
        Some(ids)
    } else {
        None
    };
    let selected = if let Some(mut scope) = scope {
        selected_files.clear();
        for row in &mut scope {
            row.source_commits = Some((from.clone(), to.clone()));
            if row.relation == "lix_file" {
                selected_files.insert(row.row_pk.as_single_string_owned()?);
            }
        }
        tx.resolve_diff_command_selections(DiffCommand::Apply, &scope)
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>()
    } else {
        full.clone().unwrap_or_default()
    };
    if target_state.is_none() {
        target_state = Some(UndoState::Target {
            epoch,
            parent,
            checkpoint: node.is_checkpoint,
            all: if node.is_checkpoint {
                full.unwrap_or_default()
            } else {
                BTreeSet::new()
            },
            undone: BTreeSet::new(),
            retired: false,
        });
    }
    let Some(UndoState::Target {
        all,
        undone,
        retired,
        ..
    }) = target_state.as_mut()
    else {
        return Err(LixError::unknown("invalid undo target state"));
    };
    let effects = if redo {
        let Some(UndoState::Receipt { pending, .. }) = &receipt_state else {
            return Err(LixError::new(
                "LIX_INVALID_REDO_TARGET",
                "undo receipt is not incorporated on this branch",
            ));
        };
        selected
            .intersection(pending)
            .cloned()
            .collect::<BTreeSet<_>>()
    } else {
        let mut effects = selected
            .difference(undone)
            .cloned()
            .collect::<BTreeSet<_>>();
        if let Some(frame_effects) = frame_effects {
            effects.retain(|id| frame_effects.contains(id));
        }
        effects
    };
    let metadata_only = whole_scope
        && node.is_checkpoint
        && all.is_empty()
        && if redo {
            matches!(
                receipt_state,
                Some(UndoState::Receipt {
                    metadata_pending: true,
                    ..
                })
            )
        } else {
            !*retired
        };
    if effects.is_empty() && !metadata_only {
        return Ok(no_change());
    }
    let old_retired = *retired;
    if redo {
        for effect in &effects {
            undone.remove(effect);
        }
        if metadata_only || undone.is_empty() {
            *retired = false;
        }
    } else {
        undone.extend(effects.iter().cloned());
        if node.is_checkpoint && (metadata_only || all.is_subset(undone)) {
            *retired = true;
        }
    }
    let new_retired = *retired;
    let patch = effects
        .iter()
        .map(|id| {
            if redo {
                let sides = crate::tracked_state::decode_diff_id(id)?;
                crate::tracked_state::encode_diff_id(sides.after, sides.before)
            } else {
                Ok(id.clone())
            }
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    if redo {
        selected_files = selected_files
            .intersection(&requested_marker.as_ref().expect("receipt").selected_files)
            .cloned()
            .collect();
    }
    let mut outcome = if patch.is_empty() {
        no_change()
    } else {
        tx.execute_undo_patch(patch, selected_files.clone()).await?
    };
    let redo_next = if redo {
        let marker = requested_marker.as_ref().expect("receipt");
        let Some(UndoState::Receipt {
            pending,
            metadata_pending,
            ..
        }) = receipt_state.as_mut()
        else {
            unreachable!()
        };
        for effect in &effects {
            pending.remove(effect);
        }
        if metadata_only {
            *metadata_pending = false;
        }
        if state.redo_top == Some(requested_id) && pending.is_empty() && !*metadata_pending {
            marker.redo_next
        } else {
            state.redo_top
        }
    } else {
        state.redo_top
    };
    stage_marker(
        tx,
        UndoRedoMarker {
            branch_id: branch.clone(),
            kind: if redo {
                UndoRedoKind::Redo
            } else {
                UndoRedoKind::Undo
            },
            target_commit_id: target,
            undo_target_after: undo_next,
            redo_top_after: if redo { redo_next } else { None },
            redo_next,
            effects: effects.iter().cloned().collect(),
            selected_files,
            checkpoint: node.is_checkpoint,
            source_undo_commit_id: redo.then_some(requested_id),
            baseline_before: (node.is_checkpoint && old_retired != new_retired)
                .then_some(baseline)
                .flatten(),
            baseline_after: (node.is_checkpoint && old_retired != new_retired)
                .then_some(if new_retired { parent } else { target }),
        },
    )
    .await?;
    let commit = tx.staged_undo_commit_id()?;
    if redo {
        stage_state(
            tx,
            requested_id,
            receipt_state.as_ref().expect("receipt state"),
        )
        .await?;
    } else {
        stage_state(
            tx,
            commit,
            &UndoState::Receipt {
                epoch,
                pending: effects,
                metadata_pending: metadata_only,
            },
        )
        .await?;
    }
    stage_state(tx, target, target_state.as_ref().expect("target state")).await?;
    if let Some(checkpoint) = epoch {
        stage_state(tx, epoch_id, &UndoState::Epoch { checkpoint }).await?;
    }
    if node.is_checkpoint && old_retired != new_retired {
        tx.stage_undo_baseline(
            baseline.expect("validated checkpoint baseline"),
            if new_retired { parent } else { target },
        )
        .await?;
    }
    outcome.commit_id = Some(commit.to_string());
    outcome.rows_affected = outcome.rows_affected.max(1);
    Ok(outcome)
}

// Explicit operations can consume a non-top receipt or only part of an action.
// Resolve those durable frames lazily for editor navigation, without enumerating
// unrelated target effects on the explicit, row-scoped write path.
async fn normalize_cursor<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    branch: &str,
    head: CommitId,
    redo: bool,
    state: &mut SemanticState,
) -> Result<(), LixError> {
    let cursor = if redo {
        &mut state.redo_top
    } else {
        &mut state.undo_top
    };
    let mut visited = BTreeSet::new();
    while let Some(id) = *cursor {
        if !visited.insert(id) {
            return Err(LixError::unknown("cyclic undo/redo cursor"));
        }
        let marker = operation_marker_any(tx, id).await?;
        if redo {
            if matches!(load_state(tx, head, id).await?, Some(UndoState::Receipt { pending, metadata_pending: false, .. }) if pending.is_empty())
            {
                *cursor = marker.and_then(|marker| marker.redo_next);
                continue;
            }
            break;
        }
        let target = marker.as_ref().map_or(id, |marker| marker.target_commit_id);
        let Some(UndoState::Target { undone, .. }) = load_state(tx, head, target).await? else {
            break;
        };
        if let Some(marker) = marker {
            if marker.kind == UndoRedoKind::Redo
                && marker.effects.iter().all(|effect| undone.contains(effect))
                && !marker.effects.is_empty()
            {
                *cursor = marker.undo_target_after;
                continue;
            }
            break;
        }
        let node = load_node(tx, target).await?;
        let parent = only_parent(&node.parent_commit_ids, target, "undo")?;
        let diff = tx
            .tracked_state_reader()
            .await?
            .diff_commits(
                &target.to_string(),
                &parent.to_string(),
                &TrackedStateDiffRequest { retain_payloads: false, ..TrackedStateDiffRequest::default() },
            )
            .await?;
        let mut remaining = false;
        for entry in diff.entries {
            if !is_undo_metadata(entry.identity.schema_key()) && !undone.contains(&entry.diff_id()?)
            {
                remaining = true;
                break;
            }
        }
        if remaining {
            break;
        }
        *cursor = semantic_state_at(tx, branch, parent).await?.undo_top;
    }
    Ok(())
}

async fn load_state<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    head: CommitId,
    id: CommitId,
) -> Result<Option<UndoState>, LixError> {
    let rows = tx
        .tracked_state_reader()
        .await?
        .load_projected_batch_at_commit(
            &head.to_string(),
            &[TrackedStateKey {
                schema_key: UNDO_STATE_SCHEMA_KEY.into(),
                file_id: None,
                row_pk: RowPk::uuid_from_canonical(&id.to_string())
                    .map_err(|e| LixError::unknown(e.to_string()))?,
            }],
            &ChangeRecordProjection::from_columns(&["snapshot_content".into()]),
        )
        .await?;
    let Some(row) = rows.row(0).filter(|row| !row.deleted()) else {
        return Ok(None);
    };
    #[derive(Deserialize)]
    struct StateSnapshot {
        state: UndoState,
    }
    let snapshot: StateSnapshot = serde_json::from_str(
        row.snapshot_content()
            .ok_or_else(|| LixError::unknown("missing undo state"))?
            .as_str(),
    )
    .map_err(|e| LixError::unknown(format!("invalid undo state: {e}")))?;
    Ok(Some(snapshot.state))
}

async fn stage_state<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    id: CommitId,
    state: &UndoState,
) -> Result<(), LixError> {
    let mut rows = RawWriteBatch::with_capacity(1);
    rows.push(TransactionWriteRow {
        row_pk: None,
        schema_key: UNDO_STATE_SCHEMA_KEY.into(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(
            serde_json::json!({"id": id, "state": state}),
        )),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: tx.active_branch_id().to_string().into(),
    });
    tx.stage_write(TransactionWrite::Rows {
        mode: TransactionWriteMode::Replace,
        rows,
    })
    .await
    .map(|_| ())
}

async fn semantic_state_at<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    branch: &str,
    id: CommitId,
) -> Result<SemanticState, LixError> {
    let node = load_node(tx, id).await?;
    if node.parent_commit_ids.len() != 1 || tx.is_current_checkpoint_commit(branch, id).await? {
        return Ok(SemanticState::default());
    }
    match operation_marker_any(tx, id).await? {
        Some(marker) if marker.branch_id != branch => Ok(SemanticState::default()),
        Some(marker) => Ok(match marker.kind {
            UndoRedoKind::Undo => SemanticState {
                undo_top: marker.undo_target_after,
                redo_top: Some(id),
            },
            UndoRedoKind::Redo => SemanticState {
                undo_top: Some(id),
                redo_top: marker.redo_top_after,
            },
        }),
        None => Ok(SemanticState {
            undo_top: Some(id),
            redo_top: None,
        }),
    }
}

/// Recovery writes new change identities. Only a recorded undo/redo effect can
/// prove that such a change represents an older revision; equal bytes from an
/// unrelated edit are deliberately insufficient.
pub(crate) async fn replayed_change_matches<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    writer: CommitId,
    expected: crate::changelog::ChangeId,
    cache: &mut std::collections::BTreeMap<CommitId, BTreeSet<crate::changelog::ChangeId>>,
) -> Result<bool, LixError> {
    if let std::collections::btree_map::Entry::Vacant(entry) = cache.entry(writer) {
        let mut restored = BTreeSet::new();
        if let Some(marker) = operation_marker_any(tx, writer).await? {
            for effect in marker.effects {
                let sides = crate::tracked_state::decode_diff_id(&effect)?;
                restored.extend(match marker.kind {
                    UndoRedoKind::Undo => sides.after,
                    UndoRedoKind::Redo => sides.before,
                });
            }
        }
        entry.insert(restored);
    }
    Ok(cache[&writer].contains(&expected))
}

async fn operation_marker_any<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    id: CommitId,
) -> Result<Option<UndoRedoMarker>, LixError> {
    let rows = tx
        .tracked_state_reader()
        .await?
        .commit_delta_values_for_schemas(id, &[UNDO_REDO_MARKER_SCHEMA_KEY.into()])
        .await?;
    let Some(entry) = rows.iter().find(|row| !row.value().deleted) else {
        return Ok(None);
    };
    let key = entry.key_ref();
    let key = TrackedStateKey {
        schema_key: key.schema_key.into(),
        file_id: key.file_id.map(str::to_string),
        row_pk: key.row_pk.clone(),
    };
    let projected = tx
        .tracked_state_reader()
        .await?
        .load_projected_batch_at_commit(
            &id.to_string(),
            &[key],
            &ChangeRecordProjection::from_columns(&["snapshot_content".into()]),
        )
        .await?;
    let snapshot = projected
        .row(0)
        .and_then(|row| row.snapshot_content())
        .ok_or_else(|| LixError::unknown("missing undo marker"))?;
    serde_json::from_str(snapshot.as_str())
        .map(Some)
        .map_err(|e| LixError::unknown(format!("invalid undo marker: {e}")))
}

async fn load_node<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    id: CommitId,
) -> Result<crate::commit_graph::CommitGraphNode, LixError> {
    tx.commit_graph_reader()
        .await?
        .load_node(&id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_COMMIT_NOT_FOUND,
                format!("commit '{id}' does not exist"),
            )
        })
}

fn only_parent(parents: &[CommitId], id: CommitId, operation: &str) -> Result<CommitId, LixError> {
    match parents {
        [parent] => Ok(*parent),
        [] => Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!("cannot {operation} root commit '{id}'"),
        )),
        _ => Err(LixError::new(
            LixError::CODE_INVALID_MERGE,
            format!("cannot {operation} merge commit '{id}'"),
        )),
    }
}

async fn stage_marker<S: Storage + Clone + Send + Sync + 'static>(
    tx: &mut Transaction<S>,
    marker: UndoRedoMarker,
) -> Result<(), LixError> {
    let mut rows = RawWriteBatch::with_capacity(1);
    rows.push(marker_stage_row(&marker));
    tx.stage_write(TransactionWrite::Rows {
        mode: TransactionWriteMode::Replace,
        rows,
    })
    .await
    .map(|_| ())
}
#[cfg(test)]
mod tests {
    use crate::engine::Engine;
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
            .open_session()
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
                Value::Jsonb(value) => value.as_json_string(),
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

        let update = session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("update undoes");
        assert_eq!(value(&session, "theme").await.as_deref(), Some("light"));
        let insert = session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("insert undoes");
        assert_ne!(
            update.rows()[0].get::<String>("commit_id").unwrap(),
            insert.rows()[0].get::<String>("commit_id").unwrap()
        );
        assert_eq!(value(&session, "theme").await, None);

        session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("insert redoes");
        assert_eq!(value(&session, "theme").await.as_deref(), Some("light"));
        session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("update redoes");
        assert_eq!(value(&session, "theme").await.as_deref(), Some("dark"));
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
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("second insert undoes");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('three', '3')",
                &[],
            )
            .await
            .expect("replacement action commits");

        let error = session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("old redo is discarded");
        assert_eq!(
            error.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("replacement action undoes");
        assert_eq!(value(&session, "three").await, None);
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("first action remains undoable");
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
        let error = session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("checkpoint blocks undo");
        assert_eq!(
            error.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('after', 'checkpoint')",
                &[],
            )
            .await
            .expect("post-checkpoint insert commits");
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("post-checkpoint action undoes");
        let error = session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("undo stops at checkpoint");
        assert_eq!(
            error.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );
        assert_eq!(
            value(&session, "before").await.as_deref(),
            Some("checkpoint")
        );
    }

    #[tokio::test]
    async fn branch_forked_at_checkpoint_starts_at_an_undo_floor() {
        let engine = setup_engine().await;
        let session = engine.open_session().await.expect("session opens");
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
        let fork = engine.open_session_at(branch.id).await.expect("fork opens");

        let error = fork
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("foreign checkpoint is a floor");
        assert_eq!(
            error.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );
        assert_eq!(value(&fork, "before-fork").await.as_deref(), Some("kept"));
    }

    #[tokio::test]
    async fn branch_forked_at_undo_commit_resets_undo_history() {
        let engine = setup_engine().await;
        let session = engine.open_session().await.expect("session opens");
        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('abandoned', 'change')",
                &[],
            )
            .await
            .expect("insert commits");
        let undone = session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("insert undoes");
        let branch = session
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-0000000000a2".to_string()),
                name: "undo-fork".to_string(),
                from_commit_id: Some(undone.rows()[0].get::<String>("commit_id").unwrap()),
            })
            .await
            .expect("branch creates");
        let fork = engine.open_session_at(branch.id).await.expect("fork opens");

        let error = fork
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("foreign operation commit is a floor");
        assert_eq!(
            error.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );
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
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("batch undoes");
        assert_eq!(value(&session, "left").await, None);
        assert_eq!(value(&session, "right").await, None);
        let error = session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("batch was one unit");
        assert_eq!(
            error.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );
    }

    #[tokio::test]
    async fn mixed_transaction_leaves_untracked_state_untouched() {
        let session = setup().await;
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction opens");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('tracked', 'yes')",
                &[],
            )
            .await
            .expect("tracked row stages");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('ui', 'open', true)",
                &[],
            )
            .await
            .expect("untracked row stages");
        transaction
            .commit()
            .await
            .expect("mixed transaction commits");

        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("tracked portion undoes");
        assert_eq!(value(&session, "tracked").await, None);
        assert_eq!(value(&session, "ui").await.as_deref(), Some("open"));
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

        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("file update undoes");
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
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("file create undoes");
        assert_eq!(
            session
                .read_file_content("/note.txt".into(), None)
                .await
                .expect("file reads"),
            None
        );
        session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("file create redoes");
        session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("file update redoes");
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

        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("file deletion undoes");
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
        session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("file deletion redoes");
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

    /// The mixed state that made undo lossy can no longer be created.
    ///
    /// This test used to write an untracked row into a *tracked* file and then
    /// assert that `undo` refused to proceed — a guard
    /// (`reject_untracked_descriptor_cascade`) standing in for an invariant the
    /// engine did not enforce. PR D enforces the invariant at the write, so the
    /// INSERT is now rejected and the guard has nothing left to catch.
    #[tokio::test]
    async fn untracked_row_cannot_be_owned_by_a_tracked_file() {
        let session = setup().await;
        session
            .upsert_file_content("/owned.txt".into(), Blob::from("tracked".as_bytes()))
            .await
            .expect("file creates");
        let file = session
            .execute("SELECT id FROM lix_file WHERE path = '/owned.txt'", &[])
            .await
            .expect("file id reads");
        let file_id = match file.rows()[0].get::<Value>("id").expect("id projects") {
            Value::Text(value) => value,
            value => panic!("expected text file id, got {value:?}"),
        };
        let error = session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_file_id, lixcol_untracked) \
                 VALUES ('file-ui', 'open', $1, true)",
                &[Value::Text(file_id)],
            )
            .await
            .expect_err("an untracked row must not be owned by a tracked file");
        assert_eq!(error.code, LixError::CODE_CONSTRAINT_VIOLATION);
        assert!(
            error.message.contains("which exists but is tracked"),
            "the rejection must name the file's lane, got: {}",
            error.message
        );

        // With the mixed state unreachable, undo is ordinary again: it removes
        // the file it created, and there is no untracked state to strand.
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("undo removes the tracked file");
        assert!(
            session
                .read_file_content("/owned.txt".into(), None)
                .await
                .expect("file reads")
                .is_none(),
            "undo must remove the file its target commit created"
        );
        assert_eq!(value(&session, "file-ui").await, None);
    }

    #[tokio::test]
    async fn redo_cursor_is_durable_across_fresh_sessions() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage initializes");
        let engine = Engine::new(storage).await.expect("engine opens");
        let first = engine.open_session().await.expect("first session opens");
        first
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('durable', 'yes')",
                &[],
            )
            .await
            .expect("insert commits");
        first
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("insert undoes");
        let branch_id = first.active_branch_id().await.expect("branch resolves");
        drop(first);

        let reopened = engine
            .open_session_at(branch_id)
            .await
            .expect("fresh pinned session opens");
        reopened
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("redo survives session loss");
        assert_eq!(value(&reopened, "durable").await.as_deref(), Some("yes"));
    }

    #[tokio::test]
    async fn merge_commit_is_an_undo_floor() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("storage initializes");
        let engine = Engine::new(storage).await.expect("engine opens");
        let main = engine.open_session().await.expect("main session opens");
        let draft = main
            .create_branch(CreateBranchOptions {
                id: Some("01930000-0000-7000-8000-000000000099".to_string()),
                name: "draft".to_string(),
                from_commit_id: None,
            })
            .await
            .expect("draft creates");
        let draft_session = engine
            .open_session_at(draft.id.clone())
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

        let error = main
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("merge blocks undo");
        assert_eq!(
            error.rows()[0].get::<Value>("commit_id").unwrap(),
            Value::Null
        );
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
        session
            .execute("SELECT commit_id FROM lix_undo()", &[])
            .await
            .expect("insert undoes");
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
        session
            .execute("SELECT commit_id FROM lix_redo()", &[])
            .await
            .expect("insert redoes");
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
