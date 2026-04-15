use crate::canonical::{append_changes, CanonicalChangeWrite};
use crate::functions::SystemFunctionProvider;
use crate::live_state::{write_live_rows, LiveRow, LiveWriteOperation, LiveWriteRow};
use crate::transaction::CommitOutcome;
use crate::{LixBackendTransaction, LixError};

use super::write_plan::WritePlan;

pub(crate) async fn apply_write_plan(
    transaction: &mut dyn LixBackendTransaction,
    plan: &WritePlan,
) -> Result<CommitOutcome, LixError> {
    let (tracked_writes, untracked_writes): (Vec<_>, Vec<_>) =
        plan.writes.iter().partition(|write| !write.untracked);

    if !tracked_writes.is_empty() {
        let live_rows = tracked_writes
            .into_iter()
            .map(live_row_from_write)
            .collect::<Vec<_>>();
        write_live_rows(transaction, &live_rows).await?;
    }

    if !untracked_writes.is_empty() {
        let mut functions = SystemFunctionProvider;
        let canonical_changes = canonical_changes_from_untracked_writes(&untracked_writes)?;
        append_changes(transaction, &canonical_changes, &mut functions).await?;
        let live_rows = untracked_writes
            .into_iter()
            .zip(canonical_changes.iter())
            .map(|(write, change)| live_row_from_untracked_write(write, change))
            .collect::<Vec<_>>();
        write_live_rows(transaction, &live_rows).await?;
    }

    Ok(CommitOutcome::from_write_plan(plan))
}

#[allow(dead_code)]
fn tracked_writes_summary(writes: &[LiveWriteRow]) -> (usize, usize) {
    let mut upserts = 0;
    let mut tombstones = 0;
    for write in writes {
        if write.untracked {
            continue;
        }
        match write.operation {
            LiveWriteOperation::Upsert => upserts += 1,
            LiveWriteOperation::Tombstone => tombstones += 1,
            LiveWriteOperation::Delete => {}
        }
    }
    (upserts, tombstones)
}

#[allow(dead_code)]
fn untracked_writes_summary(writes: &[LiveWriteRow]) -> (usize, usize) {
    let mut upserts = 0;
    let mut deletes = 0;
    for write in writes {
        if !write.untracked {
            continue;
        }
        match write.operation {
            LiveWriteOperation::Upsert => upserts += 1,
            LiveWriteOperation::Delete => deletes += 1,
            LiveWriteOperation::Tombstone => {}
        }
    }
    (upserts, deletes)
}

fn live_row_from_write(write: &LiveWriteRow) -> LiveRow {
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
        untracked: write.untracked,
        created_at: write.created_at.clone(),
        updated_at: Some(write.updated_at.clone()),
        snapshot_content: write.snapshot_content.clone(),
    }
}

fn live_row_from_untracked_write(write: &LiveWriteRow, change: &CanonicalChangeWrite) -> LiveRow {
    LiveRow {
        entity_id: write.entity_id.clone(),
        file_id: write.file_id.clone(),
        schema_key: write.schema_key.clone(),
        schema_version: write.schema_version.clone(),
        version_id: write.version_id.clone(),
        plugin_key: write.plugin_key.clone(),
        metadata: write.metadata.clone(),
        change_id: Some(change.id.clone()),
        writer_key: write.writer_key.clone(),
        global: write.global,
        untracked: true,
        created_at: Some(change.created_at.clone()),
        updated_at: Some(change.created_at.clone()),
        snapshot_content: write.snapshot_content.clone(),
    }
}

fn canonical_changes_from_untracked_writes(
    writes: &[&LiveWriteRow],
) -> Result<Vec<CanonicalChangeWrite>, LixError> {
    writes
        .iter()
        .map(|write| {
            Ok(CanonicalChangeWrite {
                id: write.change_id.clone(),
                entity_id: write.entity_id.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid untracked write entity_id '{}'", write.entity_id),
                    )
                })?,
                schema_key: write.schema_key.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid untracked write schema_key '{}'", write.schema_key),
                    )
                })?,
                schema_version: write.schema_version.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "invalid untracked write schema_version '{}'",
                            write.schema_version
                        ),
                    )
                })?,
                file_id: write.file_id.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid untracked write file_id '{}'", write.file_id),
                    )
                })?,
                plugin_key: write.plugin_key.clone().try_into().map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("invalid untracked write plugin_key '{}'", write.plugin_key),
                    )
                })?,
                snapshot_content: write
                    .snapshot_content
                    .clone()
                    .map(crate::canonical::CanonicalJson::from_text)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "invalid untracked write snapshot_content for '{}': {}",
                                write.schema_key, error.description
                            ),
                        )
                    })?,
                metadata: write
                    .metadata
                    .clone()
                    .map(crate::canonical::CanonicalJson::from_text)
                    .transpose()
                    .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "invalid untracked write metadata for '{}': {}",
                                write.schema_key, error.description
                            ),
                        )
                    })?,
                untracked: true,
                created_at: write
                    .created_at
                    .clone()
                    .unwrap_or_else(|| write.updated_at.clone()),
            })
        })
        .collect()
}
