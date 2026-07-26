//! Direct tracked-head snapshot serving for broad SQL entity scans.
//!
//! The generic live-state reader intentionally exposes fully materialized
//! engine rows. The committed tracked-head has a narrower, private capability:
//! it can serve durable snapshot bytes directly. Arrow providers and native
//! public-result reads consume those same bytes, keeping visibility proof in
//! one place and leaving every unsupported shape on the established row path.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::LixError;
use crate::live_state::{LiveStateContext, LiveStateRowFilter, LiveStateScanRequest};
use crate::storage_adapter::StorageAdapterRead;

/// Optional private capability supplied only by committed read sessions.
///
/// Returning `None` is the normal conservative answer: generic contexts,
/// transaction overlays, untracked state, exact reads, and unsupported
/// SQL shapes all retain the existing materialized-row implementation. A
/// successful result has one entry per live tracked member, ordered by the
/// logical entity primary key ascending. File-backed members sharing a
/// primary key remain separate adjacent entries; a caller that relies on a
/// stronger tie order must retain the general SQL path.
#[async_trait]
pub(crate) trait EntitySnapshotReader: Send + Sync {
    async fn scan_entity_snapshots(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError>;
}

pub(crate) struct TrackedEntitySnapshotReader<S> {
    live_state: Arc<LiveStateContext>,
    store: S,
}

impl<S> TrackedEntitySnapshotReader<S> {
    pub(crate) fn new(live_state: Arc<LiveStateContext>, store: S) -> Self {
        Self { live_state, store }
    }
}

#[async_trait]
impl<S> EntitySnapshotReader for TrackedEntitySnapshotReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn scan_entity_snapshots(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        if !direct_entity_snapshot_request(&request) {
            return Ok(None);
        }
        self.live_state
            .reader(self.store.clone())
            .scan_direct_entity_snapshots(&request)
            .await
    }
}

/// The raw snapshot plane is deliberately narrower than the general
/// live-state reader. It has no identity/filter evaluator, so only a broad
/// no-tombstone request can use it. Both Arrow and public-result consumers add
/// their own output-shape checks above this shared serving boundary.
fn direct_entity_snapshot_request(request: &LiveStateScanRequest) -> bool {
    matches!(request.filter.rows, LiveStateRowFilter::All)
        && !request.filter.include_tombstones
        && request.filter.entity_pks.is_empty()
        && request.filter.file_ids.is_empty()
        && request.filter.constraints.is_empty()
}
