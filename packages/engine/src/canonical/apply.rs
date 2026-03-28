use crate::live_state::tracked::{
    TrackedWriteBatch, TrackedWriteOperation, TrackedWriteParticipant, TrackedWriteRow,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackendTransaction, LixError};

use super::receipt::CanonicalWatermark;
use super::types::{DerivedCommitApplyInput, MaterializedStateRow};

pub(crate) async fn apply_projected_live_state_rows_best_effort_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    derived_apply_input: &DerivedCommitApplyInput,
    canonical_watermark: &CanonicalWatermark,
) -> Result<(), LixError> {
    if derived_apply_input.live_state_rows.is_empty() {
        return Ok(());
    }

    if crate::live_state::require_ready_in_transaction(transaction)
        .await
        .is_err()
    {
        crate::live_state::mark_needs_rebuild_at_canonical_watermark_in_transaction(
            transaction,
            canonical_watermark,
        )
        .await?;
        return Ok(());
    }

    if let Err(_projection_error) =
        apply_projected_live_state_rows_in_transaction(transaction, derived_apply_input).await
    {
        crate::live_state::mark_needs_rebuild_at_canonical_watermark_in_transaction(
            transaction,
            canonical_watermark,
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn apply_projected_live_state_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    derived_apply_input: &DerivedCommitApplyInput,
) -> Result<(), LixError> {
    let batch = tracked_write_batch_from_derived_apply_input(derived_apply_input);
    TrackedWriteParticipant::apply_write_batch(transaction, &batch).await?;
    Ok(())
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
