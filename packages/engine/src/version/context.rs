use std::sync::Arc;

use crate::storage::{StorageReader, StorageWriter};
use crate::untracked_state::UntrackedStateContext;

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
        S: StorageReader + Send,
    {
        self.refs.reader(store)
    }

    /// Advances a version ref in a caller-provided KV writer.
    pub(crate) async fn advance_ref<S>(
        &self,
        store: S,
        version_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<(), crate::LixError>
    where
        S: StorageWriter,
    {
        self.refs
            .writer(store)
            .advance_head(version_id, commit_id, timestamp)
            .await
    }
}
