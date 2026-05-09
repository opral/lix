use crate::storage::{StorageReader, StorageWriteSet};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateIdentity, UntrackedStateIdentityRef,
    UntrackedStateRowRef, UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::LixError;

/// Durable local overlay excluded from changelog and commit membership.
///
/// Untracked state is not change-controlled, but it is still durable local
/// state. It is read alongside tracked live state and can override tracked rows
/// with the same identity.
#[derive(Clone, Copy)]
pub(crate) struct UntrackedStateContext;

impl UntrackedStateContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a reader over a caller-provided KV store.
    ///
    /// The caller decides which KV store supplies visibility for the read.
    pub(crate) fn reader<S>(&self, store: S) -> UntrackedStateStoreReader<S>
    where
        S: StorageReader,
    {
        UntrackedStateStoreReader { store }
    }

    /// Creates a writer over a transaction-local storage write set.
    ///
    /// The context never opens its own transaction; the caller applies the
    /// write set to choose the durable commit or rollback boundary.
    pub(crate) fn writer<'a>(&self, writes: &'a mut StorageWriteSet) -> UntrackedStateWriter<'a> {
        UntrackedStateWriter { writes }
    }
}

/// Store-backed untracked-state reader created by `UntrackedStateContext`.
pub(crate) struct UntrackedStateStoreReader<S> {
    store: S,
}

impl<S> UntrackedStateStoreReader<S>
where
    S: StorageReader,
{
    pub(crate) async fn scan_rows(
        &mut self,
        request: &UntrackedStateScanRequest,
    ) -> Result<Vec<MaterializedUntrackedStateRow>, LixError> {
        crate::untracked_state::storage::scan_rows(&mut self.store, request).await
    }

    pub(crate) async fn load_row(
        &mut self,
        request: &UntrackedStateRowRequest,
    ) -> Result<Option<MaterializedUntrackedStateRow>, LixError> {
        crate::untracked_state::storage::load_row(&mut self.store, request).await
    }

    pub(crate) async fn existing_identities<'a, I>(
        &mut self,
        identities: I,
    ) -> Result<Vec<UntrackedStateIdentity>, LixError>
    where
        I: IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
    {
        crate::untracked_state::storage::existing_identities(&mut self.store, identities).await
    }
}

/// Untracked-state writer over a transaction-local storage write set.
pub(crate) struct UntrackedStateWriter<'a> {
    writes: &'a mut StorageWriteSet,
}

impl UntrackedStateWriter<'_> {
    /// Stages the latest untracked rows for their identities.
    ///
    /// A row with `snapshot_content = None` is treated as removal because
    /// untracked state keeps only the current local value, not tombstones.
    pub(crate) fn stage_rows<'a, I>(&mut self, rows: I) -> Result<(), LixError>
    where
        I: IntoIterator<Item = UntrackedStateRowRef<'a>>,
    {
        crate::untracked_state::storage::stage_rows(self.writes, rows)
    }

    /// Removes untracked rows by exact identity.
    pub(crate) fn stage_delete_rows<'a, I>(&mut self, identities: I)
    where
        I: IntoIterator<Item = UntrackedStateIdentityRef<'a>>,
    {
        crate::untracked_state::storage::stage_delete_rows(self.writes, identities)
    }
}
