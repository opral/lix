use crate::contracts::artifacts::{
    TrackedWriteOperation, TrackedWriteRow, UntrackedWriteOperation, UntrackedWriteRow,
};
use crate::contracts::traits::{TrackedWriteParticipant, UntrackedWriteParticipant};
use crate::write_runtime::CommitOutcome;
use crate::{LixBackendTransaction, LixError};

use super::write_plan::{WritePlan, WriteUnit};

pub(crate) async fn apply_write_plan(
    transaction: &mut dyn LixBackendTransaction,
    plan: &WritePlan,
) -> Result<CommitOutcome, LixError> {
    let mut outcome = CommitOutcome::default();

    for unit in &plan.units {
        match unit {
            WriteUnit::ApplyTracked { writes } => {
                transaction.apply_tracked_write_batch(writes).await?;
                outcome.merge(CommitOutcome::from_tracked_writes(writes));
            }
            WriteUnit::ApplyUntracked { writes } => {
                transaction.apply_untracked_write_batch(writes).await?;
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
