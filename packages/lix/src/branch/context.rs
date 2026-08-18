use crate::storage_adapter::StorageAdapterRead;

use super::BranchRefReader;
use super::refs::BranchRefStoreReader;

/// Aggregate entrypoint for branch-domain services.
///
/// Today this owns the moving-ref subsystem. Descriptor helpers are re-exported
/// by `branch`; future branch APIs can grow here without making session or
/// SQL code depend directly on ref storage details.
pub(crate) struct BranchContext;

impl BranchContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a branch-ref reader over a caller-provided KV store.
    pub(crate) fn ref_reader<S>(&self, store: S) -> impl BranchRefReader + use<S>
    where
        S: StorageAdapterRead,
    {
        BranchRefStoreReader::new(store)
    }
}
