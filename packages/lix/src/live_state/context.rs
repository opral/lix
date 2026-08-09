use crate::filesystem::FilesystemPathIndexCache;

/// Commit-scoped cache coordinator retained for transaction invalidation.
///
/// Current-state reads are owned by the caller's `ForkTreeReadFacade`; this
/// context no longer constructs a second live-state reader or storage view.
pub(crate) struct LiveStateContext {
    filesystem_path_index_cache: std::sync::Arc<FilesystemPathIndexCache>,
}

impl LiveStateContext {
    pub(crate) fn new() -> Self {
        Self {
            filesystem_path_index_cache: std::sync::Arc::new(FilesystemPathIndexCache::default()),
        }
    }

    pub(crate) fn clear_filesystem_path_indexes(&self) {
        self.filesystem_path_index_cache.clear();
    }
}
