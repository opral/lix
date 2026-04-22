use crate::live_state::store::LiveStateTransactionRef;
use crate::live_state::{LiveWriteOperation, LiveWriteRow};
use crate::LixError;

pub(crate) async fn apply_write_batch_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    batch: &[LiveWriteRow],
) -> Result<(), LixError> {
    if batch.is_empty() {
        return Ok(());
    }

    for row in batch {
        if row.untracked {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "tracked live-state writer received untracked row '{}' '{}'",
                    row.schema_key, row.entity_id
                ),
            ));
        }
        match row.operation {
            LiveWriteOperation::Upsert => {
                let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        &format!(
                            "tracked upsert for schema '{}' entity '{}' requires snapshot_content",
                            row.schema_key, row.entity_id
                        ),
                    )
                })?;
                apply_materialized_row_in_transaction(
                    transaction,
                    row,
                    Some(snapshot_content),
                    false,
                )
                .await?;
            }
            LiveWriteOperation::Tombstone => {
                apply_materialized_row_in_transaction(transaction, row, None, true).await?;
            }
            LiveWriteOperation::Delete => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "tracked live-state writer cannot apply delete for '{}' '{}'",
                        row.schema_key, row.entity_id
                    ),
                ));
            }
        }
    }

    Ok(())
}

async fn apply_materialized_row_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    row: &LiveWriteRow,
    snapshot_content: Option<&str>,
    is_tombstone: bool,
) -> Result<(), LixError> {
    crate::live_state::storage::upsert_tracked_live_row_in_transaction(
        transaction,
        row,
        snapshot_content,
        is_tombstone,
    )
    .await
}
