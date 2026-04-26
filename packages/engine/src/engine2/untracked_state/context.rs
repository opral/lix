use std::sync::Arc;

use crate::engine2::untracked_state::{
    UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::{LixBackend, LixBackendTransaction, LixError};

/// Durable local overlay excluded from changelog and commit membership.
///
/// Untracked state is not change-controlled, but it is still durable local
/// state. It is read alongside tracked live state and can override tracked rows
/// with the same identity.
pub(crate) struct UntrackedStateContext {
    backend: Arc<dyn LixBackend + Send + Sync>,
}

impl UntrackedStateContext {
    pub(crate) fn new(backend: Arc<dyn LixBackend + Send + Sync>) -> Self {
        Self { backend }
    }

    pub(crate) async fn scan_rows(
        &self,
        request: &UntrackedStateScanRequest,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        crate::engine2::untracked_state::storage::scan_rows(self.backend.as_ref(), request).await
    }

    pub(crate) async fn load_row(
        &self,
        request: &UntrackedStateRowRequest,
    ) -> Result<Option<UntrackedStateRow>, LixError> {
        crate::engine2::untracked_state::storage::load_row(self.backend.as_ref(), request).await
    }

    pub(crate) async fn load_row_in_transaction(
        &self,
        tx: &mut dyn LixBackendTransaction,
        request: &UntrackedStateRowRequest,
    ) -> Result<Option<UntrackedStateRow>, LixError> {
        crate::engine2::untracked_state::storage::load_row_in_transaction(tx, request).await
    }

    /// Creates a transaction-scoped writer for untracked overlay rows.
    ///
    /// The context never opens its own transaction. Callers pass the active
    /// backend transaction so untracked updates commit or roll back with the
    /// surrounding engine operation.
    pub(crate) fn writer<'a>(
        &'a self,
        tx: &'a mut dyn LixBackendTransaction,
    ) -> UntrackedStateWriter<'a> {
        UntrackedStateWriter { tx }
    }
}

/// Transaction-scoped untracked-state writer.
pub(crate) struct UntrackedStateWriter<'a> {
    tx: &'a mut dyn LixBackendTransaction,
}

impl UntrackedStateWriter<'_> {
    /// Writes the latest untracked rows for their identities.
    ///
    /// A row with `snapshot_content = None` is treated as removal because
    /// untracked state keeps only the current local value, not tombstones.
    pub(crate) async fn write_rows(&mut self, rows: &[UntrackedStateRow]) -> Result<(), LixError> {
        crate::engine2::untracked_state::storage::write_rows(self.tx, rows).await
    }

    /// Removes untracked rows by exact identity.
    pub(crate) async fn delete_rows(
        &mut self,
        identities: &[UntrackedStateIdentity],
    ) -> Result<(), LixError> {
        crate::engine2::untracked_state::storage::delete_rows(self.tx, identities).await
    }
}
