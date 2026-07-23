use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::checkpoint::{
    CHECKPOINT_MARKER_SCHEMA_KEY, checkpoint_history_from_head, checkpoint_marker_stage_row,
};
use crate::storage_adapter::Storage;
use crate::tracked_state::{TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateDiffRow};
use crate::transaction::types::{StagedCommitChangeRef, TransactionWrite, TransactionWriteMode};

use super::context::SessionContext;

/// Receipt returned after compacting the active working interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointReceipt {
    pub commit_id: String,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a checkpoint for the active branch.
    ///
    /// The new commit contains the net tracked changes since the previous
    /// checkpoint and parents that checkpoint directly. The old branch head is
    /// retained as a local recovery root for the GC grace window.
    pub async fn create_checkpoint(&self) -> Result<CreateCheckpointReceipt, LixError> {
        let receipt = self
            .with_write_transaction(|transaction| {
                Box::pin(async move {
                    let branch_id = transaction.active_branch_id().to_string();
                    let head_commit_id = {
                        let reader = transaction.branch_ref_reader().await;
                        BranchLifecycle::new(&reader)
                            .require_existing_commit_id(
                                &branch_id,
                                BranchOperation::CreateCheckpoint,
                                BranchReferenceRole::Target,
                            )
                            .await?
                    };
                    let previous_checkpoint_commit_id = {
                        let mut reader = transaction.commit_graph_reader().await;
                        checkpoint_history_from_head(&mut reader, &head_commit_id)
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
                            .commit_id
                    };
                    let selected_changes = {
                        let mut reader = transaction.tracked_state_reader().await;
                        reader
                            .diff_commits(
                                &previous_checkpoint_commit_id.to_string(),
                                &head_commit_id.to_string(),
                                &TrackedStateDiffRequest::default(),
                            )
                            .await?
                            .entries
                            .into_iter()
                            .filter(|entry| {
                                entry.identity.schema_key != CHECKPOINT_MARKER_SCHEMA_KEY
                            })
                            .map(|entry| {
                                entry
                                    .after
                                    .map(|row| selected_change_ref(row, entry.kind))
                                    .ok_or_else(|| {
                                        LixError::new(
                                            LixError::CODE_INTERNAL_ERROR,
                                            format!(
                                                "working change for schema '{}' entity {:?} has no target row",
                                                entry.identity.schema_key, entry.identity.entity_pk
                                            ),
                                        )
                                    })
                            })
                            .collect::<Result<Vec<_>, _>>()?
                    };

                    transaction
                        .stage_write(TransactionWrite::Rows {
                            mode: TransactionWriteMode::Replace,
                            rows: vec![checkpoint_marker_stage_row(&branch_id)],
                        })
                        .await?;
                    let commit_id = transaction.stage_checkpoint_commit(
                        branch_id,
                        previous_checkpoint_commit_id,
                        head_commit_id,
                        selected_changes,
                    )?;
                    Ok(CreateCheckpointReceipt { commit_id })
                })
            })
            .await?;
        self.collect_checkpoint_garbage_best_effort().await;
        Ok(receipt)
    }
}

fn selected_change_ref(
    row: TrackedStateDiffRow,
    kind: TrackedStateDiffKind,
) -> StagedCommitChangeRef {
    // A changelog change durably stores one timestamp, which rebuild uses for
    // both timestamps when the entity is absent from the parent checkpoint.
    // Canonicalize newly added rows to that representation so checkpoint roots
    // remain content-equivalent after their compacted auto-commits are swept.
    let created_at = match kind {
        TrackedStateDiffKind::Added => row.updated_at,
        TrackedStateDiffKind::Modified | TrackedStateDiffKind::Removed => row.created_at,
    };
    StagedCommitChangeRef {
        schema_key: row.schema_key,
        file_id: row.file_id,
        entity_pk: row.entity_pk,
        change_id: row.change_id,
        deleted: row.deleted,
        created_at,
        updated_at: row.updated_at,
    }
}
