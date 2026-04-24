use crate::engine2::transaction::staging::StagedStateRowOverlay;
use crate::sql2::StateRow;
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
    state_rows: Vec<StateRow>,
) -> Result<(), LixError> {
    let live_rows = StagedStateRowOverlay::into_live_rows(state_rows)?;
    if live_rows.is_empty() {
        return Ok(());
    }

    crate::live_state::write_live_rows(transaction, &live_rows).await
}
