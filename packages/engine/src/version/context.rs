use std::sync::Arc;

use crate::storage::{StorageRead, StorageWriteSet};
use crate::untracked_state::{UntrackedStateContext, UntrackedStateRow};

use super::refs::VersionRefContext;
use super::VersionRefReader;

/// Aggregate entrypoint for version-domain services.
///
/// Today this owns the moving-ref subsystem. Descriptor helpers are re-exported
/// by `version`; future version APIs can grow here without making session or
/// SQL code depend directly on ref storage details.
pub(crate) struct VersionContext {
    refs: Arc<VersionRefContext>,
}

impl VersionContext {
    pub(crate) fn new(untracked_state: Arc<UntrackedStateContext>) -> Self {
        Self {
            refs: Arc::new(VersionRefContext::new(untracked_state)),
        }
    }

    /// Creates a version-ref reader over a caller-provided KV store.
    pub(crate) fn ref_reader<S>(&self, store: S) -> impl VersionRefReader
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
