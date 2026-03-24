use crate::live_state::tracked::{TrackedWriteOperation, TrackedWriteRow};
use crate::live_state::untracked::{UntrackedWriteOperation, UntrackedWriteRow};
use crate::{LixError, LixTransaction};

use super::contracts::CommitOutcome;
use super::write_plan::{TxnMaterializationPlan, TxnMaterializationUnit};

pub(crate) async fn run_materialization_plan(
    transaction: &mut dyn LixTransaction,
    plan: &TxnMaterializationPlan,
) -> Result<CommitOutcome, LixError> {
    let mut outcome = CommitOutcome::default();

    for unit in &plan.units {
        match unit {
            TxnMaterializationUnit::ApplyTracked { writes } => {
                crate::live_state::tracked::TrackedWriteParticipant::apply_write_batch(
                    transaction,
                    writes,
                )
                .await?;
                outcome.merge(CommitOutcome::from_tracked_writes(writes));
            }
            TxnMaterializationUnit::ApplyUntracked { writes } => {
                crate::live_state::untracked::UntrackedWriteParticipant::apply_write_batch(
                    transaction,
                    writes,
                )
                .await?;
                outcome.merge(CommitOutcome::from_untracked_writes(writes));
            }
        }
    }

    Ok(outcome)
}

#[allow(dead_code)]
fn tracked_writes_summary(writes: &[TrackedWriteRow]) -> (usize, usize) {
    let mut upserts = 0;
    let mut tombstones = 0;
    for write in writes {
        match write.operation {
            TrackedWriteOperation::Upsert => upserts += 1,
            TrackedWriteOperation::Tombstone => tombstones += 1,
        }
    }
    (upserts, tombstones)
}

#[allow(dead_code)]
fn untracked_writes_summary(writes: &[UntrackedWriteRow]) -> (usize, usize) {
    let mut upserts = 0;
    let mut deletes = 0;
    for write in writes {
        match write.operation {
            UntrackedWriteOperation::Upsert => upserts += 1,
            UntrackedWriteOperation::Delete => deletes += 1,
        }
    }
    (upserts, deletes)
}
