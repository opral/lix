use crate::binary_cas::BinaryBlobWrite;
use crate::engine2::transaction::staging::{StagedStateRowOverlay, StagedWriteSet};
use crate::{LixBackendTransaction, LixError};

/// Flushes transaction-staged state rows into live_state.
///
/// This is the first engine2 commit seam: providers decode DataFusion DML into
/// `StateRow`s, the transaction owns those rows, and this temporary MVP commit
/// adapter writes them to durable live_state inside the backend transaction.
///
/// TODO(engine2): replace this naive live_state flush with canonical commit
/// generation. The future path should create commit graph rows first, then let
/// live_state catch up from canonical state.
pub(crate) async fn commit_staged_writes(
    transaction: &mut dyn LixBackendTransaction,
    staged_writes: StagedWriteSet,
) -> Result<(), LixError> {
    if !staged_writes.file_data_writes.is_empty() {
        let blob_writes = staged_writes
            .file_data_writes
            .iter()
            .map(|write| BinaryBlobWrite {
                file_id: &write.file_id,
                version_id: &write.version_id,
                data: &write.data,
            })
            .collect::<Vec<_>>();
        crate::binary_cas::persist_blob_writes_in_transaction(transaction, &blob_writes).await?;
    }

    let live_rows = StagedStateRowOverlay::into_live_rows(staged_writes.state_rows)?;
    if live_rows.is_empty() {
        return Ok(());
    }

    crate::live_state::write_live_rows(transaction, &live_rows).await
}
