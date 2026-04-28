use crate::backend::{KvStore, KvWriter};
use crate::engine2::commit_graph::CommitGraphContext;
use crate::engine2::tracked_state::rebuild::TrackedStateRebuildReport;
use crate::engine2::tracked_state::{
    TrackedStateDeleteRequest, TrackedStateRow, TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::LixError;

/// Factory for rebuildable tracked-state readers and writers.
///
/// Tracked state is the rebuildable projection of changelog facts.
#[derive(Clone, Copy)]
pub(crate) struct TrackedStateContext;

impl TrackedStateContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a tracked-state reader over a caller-provided KV store.
    pub(crate) fn reader<S>(&self, store: S) -> TrackedStateStoreReader<S>
    where
        S: KvStore,
    {
        TrackedStateStoreReader { store }
    }

    /// Creates a tracked-state writer over a caller-provided KV writer.
    pub(crate) fn writer<S>(&self, store: S) -> TrackedStateWriter<S>
    where
        S: KvWriter,
    {
        TrackedStateWriter { store }
    }

    /// Rebuilds one version's tracked projection from a commit graph head.
    ///
    /// The commit graph determines the effective canonical entities at the
    /// requested head. Tracked state owns replacing its serving projection with
    /// those entities.
    pub(crate) async fn rebuild_version_state<R, W>(
        &self,
        commit_graph: &CommitGraphContext,
        read_store: R,
        write_store: W,
        version_id: &str,
        head_commit_id: &str,
    ) -> Result<TrackedStateRebuildReport, LixError>
    where
        R: KvStore,
        W: KvWriter,
    {
        crate::engine2::tracked_state::rebuild::rebuild_version_state(
            self,
            commit_graph,
            read_store,
            write_store,
            version_id,
            head_commit_id,
        )
        .await
    }
}

/// Read side for rebuildable tracked-state rows.
#[async_trait::async_trait]
pub(crate) trait TrackedStateReader {
    async fn scan_rows(
        &mut self,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<TrackedStateRow>, LixError>;

    async fn load_row(
        &mut self,
        request: &TrackedStateRowRequest,
    ) -> Result<Option<TrackedStateRow>, LixError>;
}

/// Store-backed tracked-state reader created by `TrackedStateContext`.
pub(crate) struct TrackedStateStoreReader<S> {
    store: S,
}

impl<S> TrackedStateStoreReader<S>
where
    S: KvStore,
{
    pub(crate) async fn scan_rows(
        &mut self,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<TrackedStateRow>, LixError> {
        crate::engine2::tracked_state::storage::scan_rows(&mut self.store, request).await
    }

    pub(crate) async fn load_row(
        &mut self,
        request: &TrackedStateRowRequest,
    ) -> Result<Option<TrackedStateRow>, LixError> {
        crate::engine2::tracked_state::storage::load_row(&mut self.store, request).await
    }
}

#[async_trait::async_trait]
impl<S> TrackedStateReader for TrackedStateStoreReader<S>
where
    S: KvStore + Send,
{
    async fn scan_rows(
        &mut self,
        request: &TrackedStateScanRequest,
    ) -> Result<Vec<TrackedStateRow>, LixError> {
        TrackedStateStoreReader::scan_rows(self, request).await
    }

    async fn load_row(
        &mut self,
        request: &TrackedStateRowRequest,
    ) -> Result<Option<TrackedStateRow>, LixError> {
        TrackedStateStoreReader::load_row(self, request).await
    }
}

/// Writer for rebuildable tracked-state rows.
pub(crate) struct TrackedStateWriter<S> {
    store: S,
}

impl<S> TrackedStateWriter<S>
where
    S: KvWriter,
{
    pub(crate) async fn write_rows(&mut self, rows: &[TrackedStateRow]) -> Result<(), LixError> {
        crate::engine2::tracked_state::storage::write_rows(&mut self.store, rows).await
    }

    pub(crate) async fn delete_rows(
        &mut self,
        request: &TrackedStateDeleteRequest,
    ) -> Result<usize, LixError> {
        crate::engine2::tracked_state::storage::delete_rows(&mut self.store, request).await
    }
}
