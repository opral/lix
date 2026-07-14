use std::sync::Arc;

use crate::storage_adapter::StorageAdapterRead;

use super::BranchRefReader;
use super::refs::BranchRefContext;

/// Aggregate entrypoint for branch-domain services.
///
/// Today this owns the moving-ref subsystem. Descriptor helpers are re-exported
/// by `branch`; future branch APIs can grow here without making session or
/// SQL code depend directly on ref storage details.
pub(crate) struct BranchContext {
    refs: Arc<BranchRefContext>,
}

impl BranchContext {
    pub(crate) fn new() -> Self {
        Self {
            refs: Arc::new(BranchRefContext::new()),
        }
    }

    /// Creates a branch-ref reader over a caller-provided KV store.
    pub(crate) fn ref_reader<S>(&self, store: S) -> impl BranchRefReader + use<S>
    where
        S: StorageAdapterRead,
    {
        self.refs.reader(store)
    }
}
