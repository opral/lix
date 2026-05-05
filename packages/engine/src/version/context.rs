use std::sync::Arc;

use crate::json_store::JsonStoreWriter;
use crate::storage::{StorageReader, StorageWriteSet};
use crate::untracked_state::{UntrackedStateContext, UntrackedStateRow};

use super::refs::{canonical_version_ref_row, VersionRefContext};
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

    pub(crate) fn canonical_ref_row(
        &self,
        json_writer: &mut JsonStoreWriter<'_>,
        version_id: &str,
        commit_id: &str,
        timestamp: &str,
    ) -> Result<UntrackedStateRow, crate::LixError> {
        canonical_version_ref_row(json_writer, version_id, commit_id, timestamp)
    }

    pub(crate) fn stage_canonical_ref_rows(
        &self,
        writes: &mut StorageWriteSet,
        rows: &[UntrackedStateRow],
    ) -> Result<(), crate::LixError> {
        self.refs.writer(writes).write_rows(rows)
    }
}
