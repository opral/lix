use crate::backend::{KvStore, KvWriter};
use crate::engine2::untracked_state::{
    UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::LixError;

/// Durable local overlay excluded from changelog and commit membership.
///
/// Untracked state is not change-controlled, but it is still durable local
/// state. It is read alongside tracked live state and can override tracked rows
/// with the same identity.
pub(crate) struct UntrackedStateContext;

impl UntrackedStateContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a reader over a caller-provided KV store.
    ///
    /// The caller decides which KV store supplies visibility for the read.
    pub(crate) fn reader<S>(&self, store: S) -> UntrackedStateReader<S>
    where
        S: KvStore,
    {
        UntrackedStateReader { store }
    }

    /// Creates a writer over a caller-provided KV writer.
    ///
    /// The context never opens its own transaction; caller-provided writer
    /// ownership controls commit or rollback behavior.
    pub(crate) fn writer<S>(&self, store: S) -> UntrackedStateWriter<S>
    where
        S: KvWriter,
    {
        UntrackedStateWriter { store }
    }
}

/// Reader for durable local untracked overlay rows.
pub(crate) struct UntrackedStateReader<S> {
    store: S,
}

impl<S> UntrackedStateReader<S>
where
    S: KvStore,
{
    pub(crate) async fn scan_rows(
        &mut self,
        request: &UntrackedStateScanRequest,
    ) -> Result<Vec<UntrackedStateRow>, LixError> {
        crate::engine2::untracked_state::storage::scan_rows(&mut self.store, request).await
    }

    pub(crate) async fn load_row(
        &mut self,
        request: &UntrackedStateRowRequest,
    ) -> Result<Option<UntrackedStateRow>, LixError> {
        crate::engine2::untracked_state::storage::load_row(&mut self.store, request).await
    }
}

/// Untracked-state writer over a caller-provided KV writer.
pub(crate) struct UntrackedStateWriter<S> {
    store: S,
}

impl<S> UntrackedStateWriter<S>
where
    S: KvWriter,
{
    /// Writes the latest untracked rows for their identities.
    ///
    /// A row with `snapshot_content = None` is treated as removal because
    /// untracked state keeps only the current local value, not tombstones.
    pub(crate) async fn write_rows(&mut self, rows: &[UntrackedStateRow]) -> Result<(), LixError> {
        crate::engine2::untracked_state::storage::write_rows(&mut self.store, rows).await
    }

    /// Removes untracked rows by exact identity.
    pub(crate) async fn delete_rows(
        &mut self,
        identities: &[UntrackedStateIdentity],
    ) -> Result<(), LixError> {
        crate::engine2::untracked_state::storage::delete_rows(&mut self.store, identities).await
    }
}
