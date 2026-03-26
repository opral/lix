use crate::live_state::system::version_ref_write_row;
use crate::live_state::tracked::{
    TrackedWriteBatch, TrackedWriteOperation, TrackedWriteParticipant, TrackedWriteRow,
};
use crate::live_state::untracked::{UntrackedWriteBatch, UntrackedWriteParticipant};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError};

use super::types::{DerivedCommitApplyInput, MaterializedStateRow, VersionRefUpdate};

pub(crate) async fn apply_derived_live_state_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    derived_apply_input: &DerivedCommitApplyInput,
) -> Result<(), LixError> {
    let batch = tracked_write_batch_from_derived_apply_input(derived_apply_input);
    TrackedWriteParticipant::apply_write_batch(transaction, &batch).await?;
    let version_ref_batch = version_ref_write_batch_from_derived_apply_input(derived_apply_input);
    UntrackedWriteParticipant::apply_write_batch(transaction, &version_ref_batch).await
}

pub(crate) fn tracked_write_batch_from_derived_apply_input(
    derived_apply_input: &DerivedCommitApplyInput,
) -> TrackedWriteBatch {
    derived_apply_input
        .live_state_rows
        .iter()
        .map(tracked_write_row_from_materialized)
        .collect()
}

pub(crate) fn version_ref_write_batch_from_derived_apply_input(
    derived_apply_input: &DerivedCommitApplyInput,
) -> UntrackedWriteBatch {
    derived_apply_input
        .version_ref_updates
        .iter()
        .map(version_ref_write_row_from_update)
        .collect()
}

fn tracked_write_row_from_materialized(row: &MaterializedStateRow) -> TrackedWriteRow {
    TrackedWriteRow {
        entity_id: row.entity_id.to_string(),
        schema_key: row.schema_key.to_string(),
        schema_version: row.schema_version.to_string(),
        file_id: row.file_id.to_string(),
        version_id: row.lixcol_version_id.to_string(),
        global: row.lixcol_version_id.as_str() == GLOBAL_VERSION_ID,
        plugin_key: row.plugin_key.to_string(),
        metadata: row
            .metadata
            .as_ref()
            .map(|value| value.as_str().to_string()),
        change_id: row.id.clone(),
        writer_key: row.writer_key.clone(),
        snapshot_content: row
            .snapshot_content
            .as_ref()
            .map(|value| value.as_str().to_string()),
        created_at: Some(row.created_at.clone()),
        updated_at: row.created_at.clone(),
        operation: if row.snapshot_content.is_some() {
            TrackedWriteOperation::Upsert
        } else {
            TrackedWriteOperation::Tombstone
        },
    }
}

fn version_ref_write_row_from_update(
    update: &VersionRefUpdate,
) -> crate::live_state::untracked::UntrackedWriteRow {
    version_ref_write_row(
        update.version_id.as_str(),
        &update.commit_id,
        &update.created_at,
    )
}
