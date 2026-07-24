use crate::LixError;
use crate::branch::{BranchLifecycle, BranchOperation, BranchReferenceRole};
use crate::checkpoint::{
    CHECKPOINT_MARKER_SCHEMA_KEY, checkpoint_history_from_head, checkpoint_marker_stage_row,
    latest_checkpoint_at_head,
};
use crate::gc::CheckpointGcState;
use crate::storage_adapter::Storage;
use crate::tracked_state::{TrackedStateDiffKind, TrackedStateDiffRequest, TrackedStateDiffRow};
use crate::transaction::types::{StagedCommitChangeRef, TransactionWrite, TransactionWriteMode};

use super::context::SessionContext;

/// Receipt returned after compacting the active working interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateCheckpointReceipt {
    pub commit_id: String,
}

const CHECKPOINT_GC_MIN_AGE: u64 = 64;
// Once history is mature, each successful sweep grows the next interval in
// proportion to retained checkpoint history. Full-sweep positions therefore
// grow geometrically instead of producing fixed-cadence quadratic work.
const CHECKPOINT_GC_HISTORY_FRACTION: u64 = 8;

struct CreateCheckpointOutcome {
    receipt: CreateCheckpointReceipt,
    gc_due: bool,
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
            .with_write_transaction(|transaction| {
                Box::pin(async move {
                    let branch_id = transaction.active_branch_id().to_string();
                    let (previous_recovery, mut gc_state) = transaction
                        .checkpoint_publication_state(&branch_id)
                        .await?;
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
                    let direct_checkpoint = {
                        let mut tracked = transaction.tracked_state_reader().await;
                        latest_checkpoint_at_head(&mut tracked, &head_commit_id, &branch_id).await?
                    };
                    let previous_checkpoint_commit_id = match direct_checkpoint {
                        Some(commit_id) => commit_id,
                        None => {
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
                        }
                    };
                    let interval_has_commits =
                        head_commit_id != previous_checkpoint_commit_id;
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
                    gc_state.checkpoint_sequence = gc_state
                        .checkpoint_sequence
                        .checked_add(1)
                        .ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "checkpoint sequence overflow",
                            )
                    })?;
                    if let Some(previous_recovery) = previous_recovery {
                        gc_state
                            .add_collectible_interval(previous_recovery.interval_has_commits);
                    }
                    let gc_due = checkpoint_gc_due(gc_state)?;

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
                        interval_has_commits,
                        gc_state,
                        selected_changes,
                    )?;
                    Ok(CreateCheckpointOutcome {
                        receipt: CreateCheckpointReceipt { commit_id },
                        gc_due,
                    })
                })
        })
            .await?;
        if outcome.gc_due {
            // GC debt is durable in the checkpoint transaction. The sweep is
            // therefore safely retryable and does not need to delay the user
            // checkpoint. A clone shares the same storage and collaboration
            // write gate; concurrent schedules serialize, and only the first
            // one that still sees debt performs work.
            let gc_session = self.clone();
            tokio::spawn(async move {
                gc_session.collect_checkpoint_garbage_best_effort().await;
            });
        }
        Ok(outcome.receipt)
    }
}

pub(crate) fn checkpoint_gc_due(state: CheckpointGcState) -> Result<bool, LixError> {
    if !state.has_collectible_debt() {
        return Ok(false);
    }
    let checkpoint_age = state
        .checkpoint_sequence
        .checked_sub(state.last_gc_sequence)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "checkpoint GC sequence is ahead of checkpoint sequence",
            )
        })?;
    let age_limit =
        CHECKPOINT_GC_MIN_AGE.max(state.last_gc_sequence / CHECKPOINT_GC_HISTORY_FRACTION);
    Ok(checkpoint_age >= age_limit)
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

#[cfg(test)]
mod tests {
    use super::{CHECKPOINT_GC_MIN_AGE, checkpoint_gc_due};
    use crate::gc::CheckpointGcState;

    fn state(sequence: u64, last_gc_sequence: u64) -> CheckpointGcState {
        CheckpointGcState {
            checkpoint_sequence: sequence,
            last_gc_sequence,
            collectible_interval_count: 1,
        }
    }

    #[test]
    fn sparse_gc_cadence_grows_with_checkpoint_history() {
        let mut early = state(CHECKPOINT_GC_MIN_AGE - 1, 0);
        assert!(!checkpoint_gc_due(early).expect("early GC state should be valid"));
        early.checkpoint_sequence = CHECKPOINT_GC_MIN_AGE;
        assert!(checkpoint_gc_due(early).expect("initial GC state should be due"));

        let last_gc_sequence = 8_000;
        let scaled_age = last_gc_sequence / 8;
        let mut mature = state(last_gc_sequence + scaled_age - 1, last_gc_sequence);
        assert!(!checkpoint_gc_due(mature).expect("mature GC state should be valid"));
        mature.checkpoint_sequence += 1;
        assert!(checkpoint_gc_due(mature).expect("scaled GC state should be due"));
    }

    #[test]
    fn sweep_count_stays_sublinear_through_ten_thousand_checkpoints() {
        let mut state = CheckpointGcState::default();
        let mut sweep_count = 0;
        for sequence in 1..=10_000 {
            state.checkpoint_sequence = sequence;
            state.add_collectible_interval(true);
            if checkpoint_gc_due(state).expect("simulated GC state should be valid") {
                sweep_count += 1;
                state.mark_collected();
            }
        }
        assert!(
            sweep_count < 50,
            "geometric cadence unexpectedly scheduled {sweep_count} sweeps"
        );
    }

    #[test]
    fn empty_debt_never_schedules_a_sweep() {
        let state = CheckpointGcState {
            checkpoint_sequence: u64::MAX,
            last_gc_sequence: 0,
            ..CheckpointGcState::default()
        };
        assert!(!checkpoint_gc_due(state).expect("empty GC state should be valid"));
    }

    #[test]
    fn invalid_gc_sequence_is_rejected() {
        let state = state(2, 3);
        assert!(checkpoint_gc_due(state).is_err());
    }
}
