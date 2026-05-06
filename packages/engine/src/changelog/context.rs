use crate::changelog::{
    CanonicalChange, CanonicalChangeRef, ChangelogReader, ChangelogScanRequest,
};
use crate::storage::{StorageReader, StorageWriteSet};
use crate::LixError;
use tokio::sync::Mutex;

/// Durable append-only ledger for Lix changes.
///
/// This layer only records already-generated change facts. Transaction commit
/// code is responsible for producing user changes plus normal `lix_commit`
/// rows before appending them here.
#[derive(Clone, Copy)]
pub(crate) struct ChangelogContext;

impl ChangelogContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a changelog reader over a caller-provided KV store.
    ///
    /// The caller decides which KV store supplies visibility for the read.
    pub(crate) fn reader<S>(&self, store: S) -> ChangelogStoreReader<S>
    where
        S: StorageReader,
    {
        ChangelogStoreReader {
            store: Mutex::new(store),
        }
    }

    /// Creates a changelog writer over a transaction-local storage write set.
    pub(crate) fn writer<'a>(&self, writes: &'a mut StorageWriteSet) -> ChangelogWriter<'a> {
        ChangelogWriter { writes }
    }
}

/// KV-backed changelog reader created by `ChangelogContext`.
pub(crate) struct ChangelogStoreReader<S> {
    store: Mutex<S>,
}

impl<S> ChangelogStoreReader<S>
where
    S: StorageReader,
{
    pub(crate) async fn load_changes(
        &self,
        change_ids: &[String],
    ) -> Result<Vec<Option<CanonicalChange>>, LixError> {
        let mut store = self.store.lock().await;
        crate::changelog::storage::load_changes(&mut *store, change_ids).await
    }

    #[allow(dead_code)]
    pub(crate) async fn scan_changes(
        &self,
        request: &ChangelogScanRequest,
    ) -> Result<Vec<CanonicalChange>, LixError> {
        let mut store = self.store.lock().await;
        crate::changelog::storage::scan_changes(&mut *store, request).await
    }
}

#[async_trait::async_trait]
impl<S> ChangelogReader for ChangelogStoreReader<S>
where
    S: StorageReader,
{
    async fn load_changes(
        &self,
        change_ids: &[String],
    ) -> Result<Vec<Option<CanonicalChange>>, LixError> {
        ChangelogStoreReader::load_changes(self, change_ids).await
    }

    async fn scan_changes(
        &self,
        request: &ChangelogScanRequest,
    ) -> Result<Vec<CanonicalChange>, LixError> {
        ChangelogStoreReader::scan_changes(self, request).await
    }
}

/// Changelog writer over a transaction-local storage write set.
pub(crate) struct ChangelogWriter<'a> {
    writes: &'a mut StorageWriteSet,
}

impl ChangelogWriter<'_> {
    #[allow(dead_code)]
    pub(crate) fn stage_changes<'a, I>(&mut self, changes: I) -> Result<(), LixError>
    where
        I: IntoIterator<Item = CanonicalChangeRef<'a>>,
    {
        crate::changelog::storage::stage_changes(self.writes, changes)
    }
}
