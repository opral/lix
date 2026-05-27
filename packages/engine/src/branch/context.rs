use std::sync::Arc;

use crate::storage::{StorageRead, StorageWriteSet};
use crate::untracked_state::{UntrackedStateContext, UntrackedStateRow};

use super::refs::BranchRefContext;
use super::BranchRefReader;

/// Aggregate entrypoint for branch-domain services.
///
/// Today this owns the moving-ref subsystem. Descriptor helpers are re-exported
/// by `branch`; future branch APIs can grow here without making session or
/// SQL code depend directly on ref storage details.
pub(crate) struct BranchContext {
    refs: Arc<BranchRefContext>,
}

impl BranchContext {
    pub(crate) fn new(untracked_state: Arc<UntrackedStateContext>) -> Self {
        Self {
            refs: Arc::new(BranchRefContext::new(untracked_state)),
        }
    }

    /// Creates a branch-ref reader over a caller-provided KV store.
    pub(crate) fn ref_reader<S>(&self, store: S) -> impl BranchRefReader
    where
        S: StorageRead + Send + Sync,
    {
        self.refs.reader(store)
    }

    pub(crate) fn stage_canonical_ref_rows(
        &self,
        writes: &mut StorageWriteSet,
        rows: &[UntrackedStateRow],
    ) -> Result<(), crate::LixError> {
        self.refs.writer(writes).stage_rows(rows)
    }
}
