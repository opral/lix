//! Direct current-state snapshot serving for current SQL entity scans.
//!
//! The generic live-state reader intentionally exposes fully materialized
//! engine rows. The committed current-state index has a narrower, private capability:
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
/// transaction-local staged state, retention-scoped reads, and unsupported
/// SQL shapes all retain the existing materialized-row implementation. A
/// successful result has one entry per live current-state member, ordered by
/// logical entity primary key ascending. File-backed members sharing a
/// primary key remain separate adjacent entries; a caller that relies on a
/// stronger tie order must retain the general SQL path.
#[async_trait]
pub(crate) trait EntitySnapshotReader: Send + Sync {
    /// Returns the exact cardinality of a broad current-state entity scan
    /// without materializing its snapshots. `None` preserves the normal scan
    /// when this reader cannot prove that one collection control owns the
    /// complete visible result.
    async fn count_entity_snapshots(
        &self,
        _request: LiveStateScanRequest,
    ) -> Result<Option<u64>, LixError> {
        Ok(None)
    }

    async fn scan_entity_snapshots(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError>;

    async fn scan_entity_snapshots_by_string_field(
        &self,
        _request: LiveStateScanRequest,
        _column: &str,
        _values: &[String],
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        Ok(None)
    }
}

pub(crate) struct CurrentEntitySnapshotReader<S> {
    live_state: Arc<LiveStateContext>,
    store: S,
}

impl<S> CurrentEntitySnapshotReader<S> {
    pub(crate) fn new(live_state: Arc<LiveStateContext>, store: S) -> Self {
        Self { live_state, store }
    }
}

#[async_trait]
impl<S> EntitySnapshotReader for CurrentEntitySnapshotReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn count_entity_snapshots(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<u64>, LixError> {
        if !direct_entity_snapshot_request(&request) || !request.filter.entity_pks.is_empty() {
            return Ok(None);
        }
        self.live_state
            .reader(self.store.clone())
            .count_direct_entity_snapshots(&request)
            .await
    }

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

    async fn scan_entity_snapshots_by_string_field(
        &self,
        request: LiveStateScanRequest,
        column: &str,
        values: &[String],
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        if !direct_entity_snapshot_request(&request) || !request.filter.entity_pks.is_empty() {
            return Ok(None);
        }
        self.live_state
            .reader(self.store.clone())
            .scan_direct_entity_snapshots_by_string_field(&request, column, values)
            .await
    }
}

/// The raw snapshot plane is deliberately narrower than the general
/// live-state reader. It has no identity/filter evaluator beyond exact entity
/// PKs, so only a no-tombstone request without file or residual constraints
/// can use it. Both Arrow and public-result consumers add their own
/// output-shape checks above this shared serving boundary.
fn direct_entity_snapshot_request(request: &LiveStateScanRequest) -> bool {
    matches!(request.filter.rows, LiveStateRowFilter::All)
        && !request.filter.include_tombstones
        && request.filter.untracked.is_none()
        && request.filter.file_ids.is_empty()
        && request.filter.constraints.is_empty()
}
