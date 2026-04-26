use std::sync::Arc;

use crate::engine2::changelog::{CanonicalChange, ChangelogScanRequest};
use crate::{LixBackend, LixBackendTransaction, LixError};

/// Durable append-only ledger for Lix changes.
///
/// This layer only records already-generated change facts. Transaction commit
/// code is responsible for producing user changes plus normal `lix_commit`
/// rows before appending them here.
pub(crate) struct ChangelogContext {
    backend: Arc<dyn LixBackend + Send + Sync>,
}

impl ChangelogContext {
    pub(crate) fn new(backend: Arc<dyn LixBackend + Send + Sync>) -> Self {
        Self { backend }
    }

    pub(crate) async fn load_change(
        &self,
        change_id: &str,
    ) -> Result<Option<CanonicalChange>, LixError> {
        crate::engine2::changelog::storage::load_change(self.backend.as_ref(), change_id).await
    }

    pub(crate) async fn scan_changes(
        &self,
        request: ChangelogScanRequest,
    ) -> Result<Vec<CanonicalChange>, LixError> {
        crate::engine2::changelog::storage::scan_changes(self.backend.as_ref(), &request).await
    }

    pub(crate) fn writer<'a>(
        &'a self,
        tx: &'a mut dyn LixBackendTransaction,
    ) -> ChangelogWriter<'a> {
        ChangelogWriter {
            _backend: Arc::clone(&self.backend),
            tx,
        }
    }
}

/// Transaction-scoped changelog writer.
pub(crate) struct ChangelogWriter<'a> {
    _backend: Arc<dyn LixBackend + Send + Sync>,
    tx: &'a mut dyn LixBackendTransaction,
}

impl ChangelogWriter<'_> {
    pub(crate) async fn append_changes(
        &mut self,
        changes: &[CanonicalChange],
    ) -> Result<(), LixError> {
        crate::engine2::changelog::storage::append_changes(self.tx, changes).await
    }
}
