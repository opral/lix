use crate::filesystem::FilesystemPathIndexCache;
use crate::live_state::MaterializedLiveStateRow;

/// Commit-scoped cache coordinator retained for transaction invalidation.
///
/// Current-state reads are owned by the caller's `ForkTreeReadFacade`; this
/// context no longer constructs a second live-state reader or storage view.
pub(crate) struct LiveStateContext {
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

/// Transaction branch-control cache retained independently of the removed
/// public current-state reader. The branch-control owner still uses this
/// marker for its transaction-local lifecycle fence.
#[derive(Default)]
pub(crate) struct BranchHeadControlCache;

impl LiveStateContext {
    pub(crate) fn new() -> Self {
        Self {
            filesystem_path_index_cache: std::sync::Arc::new(FilesystemPathIndexCache::default()),
        }
    }

    pub(crate) fn advance_filesystem_path_indexes(
        &self,
        previous_revision: Option<&[u8]>,
        next_revision: Option<&[u8]>,
        rows: &[MaterializedLiveStateRow],
    ) {
        self.filesystem_path_index_cache
            .advance_committed(previous_revision, next_revision, rows);
    }

    pub(crate) fn clear_filesystem_path_indexes(&self) {
        self.filesystem_path_index_cache.clear();
    }
}
