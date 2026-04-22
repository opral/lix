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
        if !row.untracked {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "untracked live-state writer received tracked row '{}' '{}'",
                    row.schema_key, row.entity_id
                ),
            ));
        }
        match row.operation {
            LiveWriteOperation::Upsert => apply_upsert_in_transaction(transaction, row).await?,
            LiveWriteOperation::Delete => apply_delete_in_transaction(transaction, row).await?,
            LiveWriteOperation::Tombstone => {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "untracked live-state writer cannot apply tombstone for '{}' '{}'",
                        row.schema_key, row.entity_id
                    ),
                ));
            }
        }
    }

    Ok(())
}

async fn apply_upsert_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    row: &LiveWriteRow,
) -> Result<(), LixError> {
    crate::live_state::storage::upsert_untracked_live_row_in_transaction(transaction, row).await
}

async fn apply_delete_in_transaction(
    transaction: LiveStateTransactionRef<'_>,
    row: &LiveWriteRow,
) -> Result<(), LixError> {
    crate::live_state::storage::delete_untracked_live_row_in_transaction(transaction, row).await
}
