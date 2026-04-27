use crate::backend::{KvStore, KvWriter};
use crate::engine2::tracked_state::{
    TrackedStateRow, TrackedStateRowRequest, TrackedStateScanRequest,
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
    pub(crate) fn reader<S>(&self, store: S) -> TrackedStateReader<S>
    where
        S: KvStore,
    {
        TrackedStateReader { store }
    }

    /// Creates a tracked-state writer over a caller-provided KV writer.
    pub(crate) fn writer<S>(&self, store: S) -> TrackedStateWriter<S>
    where
        S: KvWriter,
    {
        TrackedStateWriter { store }
    }
}

/// Reader for rebuildable tracked-state rows.
pub(crate) struct TrackedStateReader<S> {
    store: S,
}

impl<S> TrackedStateReader<S>
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
}
