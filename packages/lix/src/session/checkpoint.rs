use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::checkpoint::{CHECKPOINT_MARKER_SCHEMA_KEY, checkpoint_marker_stage_row};
use crate::storage_adapter::Storage;
use crate::tracked_state::{
    TrackedStateDiffIdentity, TrackedStateDiffKind, TrackedStateDiffRow, TrackedStateKey,
};
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
                        let entries = historical
                            .diff_branch_state_rows_between_commits(
                                previous_checkpoint_commit_id,
                                head_commit_id,
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
                                .is_some_and(|row| {
                                    row.key.schema_key != CHECKPOINT_MARKER_SCHEMA_KEY
                                        && row.key.schema_key
                                            != crate::undo_redo::UNDO_REDO_MARKER_SCHEMA_KEY
                                })
                        }) {
                            let row = entry.after.ok_or_else(|| {
                                LixError::new(
                                    LixError::CODE_INTERNAL_ERROR,
                                    format!(
                                        "working diff for schema '{}' entity {:?} has no target row",
                                        entry
                                            .before
                                            .as_ref()
                                            .map(|row| row.key.schema_key.as_str())
                                            .unwrap_or("<unknown>"),
                                        entry
                                            .before
                                            .as_ref()
                                            .map(|row| &row.key.entity_pk)
                                            .expect("diff entry has one side")
                                    ),
                                )
                            })?;
                            let kind = match entry.before.as_ref() {
                                Some(before) if row.deleted && !before.deleted => {
                                    TrackedStateDiffKind::Removed
                                }
                                Some(before) if before.deleted && !row.deleted => {
                                    TrackedStateDiffKind::Added
                                }
                                Some(_) => TrackedStateDiffKind::Modified,
                                None if row.deleted => TrackedStateDiffKind::Removed,
                                None => TrackedStateDiffKind::Added,
                            };
                            let tracked_row = TrackedStateDiffRow {
                                identity: TrackedStateDiffIdentity::from_key(TrackedStateKey {
                                    schema_key: row.key.schema_key.clone(),
                                    file_id: row.key.file_id.clone(),
                                    entity_pk: row.key.entity_pk.clone(),
                                }),
                                deleted: row.deleted,
                                created_at: row.created_at,
                                updated_at: row.updated_at,
                                change_id: row.change_id,
                                commit_id: row.commit_id,
                            };
                            source_membership_exact &=
                                push_selected_change(&mut selected_changes, tracked_row, kind);
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

fn push_selected_change(
    selected_changes: &mut StagedCommitChangeBatchBuilder,
    row: TrackedStateDiffRow,
    kind: TrackedStateDiffKind,
) -> bool {
    // A changelog change durably stores one timestamp, which rebuild uses for
    // both timestamps when the entity is absent from the parent checkpoint.
    // Canonicalize newly added rows to that representation so checkpoint roots
    // remain content-equivalent after their compacted auto-commits are swept.
    let created_at = match kind {
        TrackedStateDiffKind::Added => row.updated_at,
        TrackedStateDiffKind::Modified | TrackedStateDiffKind::Removed => row.created_at,
    };
    let source_membership_exact = created_at == row.created_at;
    let deleted = row.deleted;
    let source_commit_id = row.commit_id;
    let change_id = row.change_id;
    let updated_at = row.updated_at;
    selected_changes.push(
        row.identity,
        source_commit_id,
        change_id,
        deleted,
        created_at,
        updated_at,
    );
    source_membership_exact
}

#[cfg(test)]
mod tests {
    use super::push_selected_change;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::entity_pk::EntityPk;
    use crate::tracked_state::{
        TrackedStateDiffIdentity, TrackedStateDiffKind, TrackedStateDiffRow, TrackedStateKey,
    };
    use crate::transaction::types::StagedCommitChangeBatchBuilder;

    #[test]
    fn canonicalized_added_timestamp_declines_source_membership_certificate() {
        let created_at = LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00Z");
        let updated_at = LixTimestamp::expect_parse("updated_at", "2026-01-02T00:00:00Z");
        let mut selected = StagedCommitChangeBatchBuilder::with_capacity(1);
        let source_membership_exact = push_selected_change(
            &mut selected,
            TrackedStateDiffRow {
                identity: TrackedStateDiffIdentity::from_key(TrackedStateKey {
                    schema_key: "test_schema".to_string(),
                    file_id: None,
                    entity_pk: EntityPk::single("entity"),
                }),
                deleted: false,
                created_at,
                updated_at,
                change_id: ChangeId::for_test_label("checkpoint-canonicalized-change"),
                commit_id: CommitId::for_test_label("checkpoint-canonicalized-commit"),
            },
            TrackedStateDiffKind::Added,
        );
        let selected = if source_membership_exact {
            selected.finish_source_certified()
        } else {
            selected.finish()
        };

        assert!(!selected.source_membership_certified());
        assert_eq!(
            selected.iter().next().expect("one selected row").created_at,
            updated_at
        );
    }
}
