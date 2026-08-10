use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::checkpoint::{CHECKPOINT_MARKER_SCHEMA_KEY, checkpoint_marker_stage_row};
use crate::forktree::{StateCell, StateKey, StateSource, load_commit_summary};
use crate::state::{ForkTreeStateView, StateRoots};
use crate::storage_adapter::Storage;
use crate::transaction::types::{
    RawWriteBatch, StagedCommitChangeBatchBuilder, TransactionWrite, TransactionWriteMode,
};

use super::context::SessionContext;

/// Receipt returned after compacting the active working interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointReceipt {
    pub commit_id: String,
}

struct CreateCheckpointOutcome {
    receipt: CreateCheckpointReceipt,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a checkpoint for the active branch.
    ///
    /// The new commit contains the net tracked changes since the previous
    /// checkpoint and parents that checkpoint directly. The old branch head is
    /// retained as a local recovery root for the GC grace window. Publication
    /// returns as soon as that durable root is committed; due garbage
    /// collection runs asynchronously so a history-sized sweep cannot extend
    /// the foreground checkpoint latency.
    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        let outcome = self
            .with_write_transaction_lending(async move |transaction| {
                    let branch_id = transaction.active_branch_id().to_string();
                    let _previous_recovery = transaction
                        .checkpoint_publication_state(&branch_id)
                        .await?;
                    let head_commit_id = {
                        let reader = transaction.branch_ref_reader_on_opening_read();
                        BranchLifecycle::new(&reader)
                            .require_existing_commit_id(
                                &branch_id,
                                BranchOperation::CreateCheckpoint,
                                BranchReferenceRole::Target,
                            )
                            .await?
                    };
                    let historical = transaction.forktree_read_facade();
                    let previous_checkpoint_commit_id = historical
                        .checkpoint_history_from_head(head_commit_id, &branch_id)
                        .await?
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                format!(
                                    "branch '{branch_id}' has no checkpoint baseline in its first-parent history"
                                ),
                            )
                        })?
                        .commit_id;
                    let interval_has_commits =
                        head_commit_id != previous_checkpoint_commit_id;
                    let selected_changes = {
                        let view = historical.branch(&branch_id).await?;
                        let before = load_commit_summary(&view, previous_checkpoint_commit_id)
                            .await?
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_COMMIT_NOT_FOUND,
                                    "checkpoint baseline commit is missing",
                                )
                            })?;
                        let after = load_commit_summary(&view, head_commit_id)
                            .await?
                            .ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_COMMIT_NOT_FOUND,
                                    "checkpoint head commit is missing",
                                )
                            })?;
                        let state_view = ForkTreeStateView::new(view);
                        let entries = state_view
                            .diff_roots(
                                StateRoots {
                                    global: before.global_state_root,
                                    local: Some(before.local_state_root),
                                },
                                StateRoots {
                                    global: after.global_state_root,
                                    local: Some(after.local_state_root),
                                },
                            )
                            .await?;
                        let mut selected_changes =
                            StagedCommitChangeBatchBuilder::with_capacity(entries.len());
                        let mut source_membership_exact = true;
                        for entry in entries.into_iter().filter(|entry| {
                            entry
                                .before
                                .as_ref()
                                .or(entry.after.as_ref())
                                .is_some_and(|value| {
                                    value.source == StateSource::Branch
                                        && entry.key.schema_key != CHECKPOINT_MARKER_SCHEMA_KEY
                                        && entry.key.schema_key
                                            != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                                })
                        }) {
                            let row = entry.after.ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "working diff for schema '{}' entity {:?} has no target row",
                                        entry.key.schema_key.as_str(),
                                        &entry.key.entity_pk,
                                    ),
                                )
                            })?;
                            let kind = match entry.before.as_ref() {
                                Some(before)
                                    if row.value.cell == StateCell::Tombstone
                                        && before.value.cell != StateCell::Tombstone =>
                                {
                                    CheckpointChangeKind::Removed
                                }
                                Some(before)
                                    if before.value.cell == StateCell::Tombstone
                                        && row.value.cell != StateCell::Tombstone =>
                                {
                                    CheckpointChangeKind::Added
                                }
                                Some(_) => CheckpointChangeKind::Modified,
                                None if row.value.cell == StateCell::Tombstone => {
                                    CheckpointChangeKind::Removed
                                }
                                None => CheckpointChangeKind::Added,
                            };
                            source_membership_exact &= push_selected_change(
                                &mut selected_changes,
                                &entry.key,
                                &row.value,
                                kind,
                            );
                        }
                        if source_membership_exact {
                            selected_changes.finish_source_certified()
                        } else {
                            selected_changes.finish()
                        }
                    };
                    let mut marker_rows = RawWriteBatch::with_capacity(1);
                    marker_rows.push(checkpoint_marker_stage_row(&branch_id));
                    transaction
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Replace,
                            rows: marker_rows,
                        })
                        .await?;
                    let commit_id = transaction.stage_checkpoint_commit(
                        branch_id,
                        previous_checkpoint_commit_id,
                        head_commit_id,
                        interval_has_commits,
                        selected_changes,
                    )?;
                    Ok(CreateCheckpointOutcome {
                        receipt: CreateCheckpointReceipt { commit_id },
                    })
            })
            .await?;
        // One bounded collector step is scheduled after publication. Its
        // authenticated progress selector is durable and its write set is
        // atomic, so interruption resumes from the last committed cursor.
        let gc_session = self.clone();
        tokio::spawn(async move {
            gc_session.collect_checkpoint_garbage_best_effort().await;
        });
        Ok(outcome.receipt)
    }
}

#[derive(Clone, Copy)]
enum CheckpointChangeKind {
    Added,
    Modified,
    Removed,
}

fn push_selected_change(
    selected_changes: &mut StagedCommitChangeBatchBuilder,
    key: &StateKey,
    value: &crate::forktree::StateValue,
    kind: CheckpointChangeKind,
) -> bool {
    // A changelog change durably stores one timestamp, which rebuild uses for
    // both timestamps when the entity is absent from the parent checkpoint.
    // Canonicalize newly added rows to that representation so checkpoint roots
    // remain content-equivalent after their compacted auto-commits are swept.
    let created_at = match kind {
        CheckpointChangeKind::Added => value.updated_at,
        CheckpointChangeKind::Modified | CheckpointChangeKind::Removed => value.created_at,
    };
    let source_membership_exact = created_at == value.created_at;
    let deleted = value.cell == StateCell::Tombstone;
    let source_commit_id = value.commit_id;
    let change_id = value.change_id;
    let updated_at = value.updated_at;
    selected_changes.push(
        key.clone(),
        source_commit_id,
        change_id,
        deleted,
        created_at,
        updated_at,
    );
    source_membership_exact
}
