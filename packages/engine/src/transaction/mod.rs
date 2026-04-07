//! Isolated transaction lifecycle over effective, tracked, and untracked state.

#[cfg(test)]
mod tests;

use crate::contracts::artifacts::FilesystemProjectionScope;
use crate::paths::filesystem::NormalizedDirectoryPath;
use crate::{LixBackendTransaction, LixError};

pub use crate::write_runtime::{
    CommitOutcome, ReadContext, TransactionCommitOutcome, TransactionDelta, TransactionJournal,
    WriteTransaction,
};

pub(crate) async fn lookup_directory_id_by_path_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
    path: &NormalizedDirectoryPath,
    scope: FilesystemProjectionScope,
) -> Result<Option<String>, LixError> {
    let backend = crate::backend::TransactionBackendAdapter::new(transaction);
    crate::write_runtime::filesystem::query::lookup_directory_id_by_path(
        &backend, version_id, path, scope,
    )
    .await
    .map_err(|error| LixError::new("LIX_ERROR_UNKNOWN", error.message))
}
