use crate::backend::{KvStore, KvWriter};
use crate::engine2::changelog::{CanonicalChange, ChangelogScanRequest};
use crate::LixError;

/// Durable append-only ledger for Lix changes.
///
/// This layer only records already-generated change facts. Transaction commit
/// code is responsible for producing user changes plus normal `lix_commit`
/// rows before appending them here.
pub(crate) struct ChangelogContext;

impl ChangelogContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a changelog reader over a caller-provided KV store.
    ///
    /// The caller decides which KV store supplies visibility for the read.
    pub(crate) fn reader<S>(&self, store: S) -> ChangelogReader<S>
    where
        S: KvStore,
    {
        ChangelogReader { store }
    }

    /// Creates a changelog writer over a caller-provided KV writer.
    pub(crate) fn writer<S>(&self, store: S) -> ChangelogWriter<S>
    where
        S: KvWriter,
    {
        ChangelogWriter { store }
    }
}

/// Reader for durable changelog facts.
pub(crate) struct ChangelogReader<S> {
    store: S,
}

impl<S> ChangelogReader<S>
where
    S: KvStore,
{
    pub(crate) async fn load_change(
        &mut self,
        change_id: &str,
    ) -> Result<Option<CanonicalChange>, LixError> {
        crate::engine2::changelog::storage::load_change(&mut self.store, change_id).await
    }

    pub(crate) async fn scan_changes(
        &mut self,
        request: &ChangelogScanRequest,
    ) -> Result<Vec<CanonicalChange>, LixError> {
        crate::engine2::changelog::storage::scan_changes(&mut self.store, request).await
    }
}

/// Changelog writer over a caller-provided KV writer.
pub(crate) struct ChangelogWriter<S> {
    store: S,
}

impl<S> ChangelogWriter<S>
where
    S: KvWriter,
{
    pub(crate) async fn append_changes(
        &mut self,
        changes: &[CanonicalChange],
    ) -> Result<(), LixError> {
        crate::engine2::changelog::storage::append_changes(&mut self.store, changes).await
    }
}
