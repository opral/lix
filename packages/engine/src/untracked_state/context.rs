use crate::storage::{StorageReader, StorageWriteSet};
use crate::untracked_state::{
    UntrackedStateGetManyRequest, UntrackedStateGetManyResponse, UntrackedStateIdentityRef,
    UntrackedStateRowRef, UntrackedStateScanRequest, UntrackedStateScanResponse,
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
    pub(crate) async fn get_many(
        &mut self,
        request: UntrackedStateGetManyRequest,
    ) -> Result<UntrackedStateGetManyResponse, LixError> {
        crate::untracked_state::storage::get_many(&mut self.store, request).await
    }

    pub(crate) async fn scan(
        &mut self,
        request: UntrackedStateScanRequest,
    ) -> Result<UntrackedStateScanResponse, LixError> {
        crate::untracked_state::storage::scan(&mut self.store, request).await
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
