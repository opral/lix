use crate::execution::write::CommitOutcome;
use crate::live_state::{write_live_rows, LiveRow};
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
                let live_rows = writes
                    .iter()
                    .map(live_row_from_tracked_write)
                    .collect::<Vec<_>>();
                write_live_rows(transaction, &live_rows).await?;
                outcome.merge(CommitOutcome::from_tracked_writes(writes));
            }
            WriteUnit::ApplyUntracked { writes } => {
                let live_rows = writes
                    .iter()
                    .map(live_row_from_untracked_write)
                    .collect::<Vec<_>>();
                write_live_rows(transaction, &live_rows).await?;
                outcome.merge(CommitOutcome::from_untracked_writes(writes));
            }
        }
    }

    Ok(outcome)
}

#[allow(dead_code)]
fn tracked_writes_summary(
    writes: &[crate::contracts::artifacts::TrackedWriteRow],
) -> (usize, usize) {
    let mut upserts = 0;
    let mut tombstones = 0;
    for write in writes {
        match write.operation {
            crate::contracts::artifacts::TrackedWriteOperation::Upsert => upserts += 1,
            crate::contracts::artifacts::TrackedWriteOperation::Tombstone => tombstones += 1,
        }
    }
    (upserts, tombstones)
}

#[allow(dead_code)]
fn untracked_writes_summary(
    writes: &[crate::contracts::artifacts::UntrackedWriteRow],
) -> (usize, usize) {
    let mut upserts = 0;
    let mut deletes = 0;
    for write in writes {
        match write.operation {
            crate::contracts::artifacts::UntrackedWriteOperation::Upsert => upserts += 1,
            crate::contracts::artifacts::UntrackedWriteOperation::Delete => deletes += 1,
        }
    }
    (upserts, deletes)
}

fn live_row_from_tracked_write(write: &crate::contracts::artifacts::TrackedWriteRow) -> LiveRow {
    LiveRow {
        entity_id: write.entity_id.clone(),
        file_id: write.file_id.clone(),
        schema_key: write.schema_key.clone(),
        schema_version: write.schema_version.clone(),
        version_id: write.version_id.clone(),
        plugin_key: write.plugin_key.clone(),
        metadata: write.metadata.clone(),
        change_id: Some(write.change_id.clone()),
        writer_key: write.writer_key.clone(),
        global: write.global,
        untracked: false,
        created_at: write.created_at.clone(),
        updated_at: Some(write.updated_at.clone()),
        snapshot_content: write.snapshot_content.clone(),
    }
}

fn live_row_from_untracked_write(
    write: &crate::contracts::artifacts::UntrackedWriteRow,
) -> LiveRow {
    LiveRow {
        entity_id: write.entity_id.clone(),
        file_id: write.file_id.clone(),
        schema_key: write.schema_key.clone(),
        schema_version: write.schema_version.clone(),
        version_id: write.version_id.clone(),
        plugin_key: write.plugin_key.clone(),
        metadata: write.metadata.clone(),
        change_id: None,
        writer_key: write.writer_key.clone(),
        global: write.global,
        untracked: true,
        created_at: write.created_at.clone(),
        updated_at: Some(write.updated_at.clone()),
        snapshot_content: write.snapshot_content.clone(),
    }
}
