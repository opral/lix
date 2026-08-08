use async_trait::async_trait;

use crate::LixError;
#[cfg(test)]
use crate::live_state::MaterializedLiveStateBatchBuilder;
use crate::live_state::{LiveStateExactBatchRequest, LiveStateScanRequest};
use crate::live_state::{MaterializedLiveStateBatch, MaterializedLiveStateExactBatch};

/// Selects the authenticated serving domain for an internal read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LiveStateReadDomain {
    Combined,
    Tracked,
    Untracked,
}

/// Minimal engine read model for transaction planning and SQL providers.
///
/// Engine only needs visible state-row reads here. Changelog freshness/catch-up
/// should be added at this boundary later instead of leaking projection internals
/// into sessions or SQL providers.
#[async_trait]
pub(crate) trait LiveStateReader: Send + Sync {
    /// Columnar scan lane used by live-state composition and SQL providers.
    ///
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError>;

    /// Scans committed rows for constraint validation into one shared owner.
    ///
    /// Durable readers can override this lane to use publication metadata for
    /// a conservative empty-schema proof. Other readers preserve correctness
    /// by falling back to their ordinary columnar scan implementation.
    async fn scan_constraint_batch(
        &self,
        request: &LiveStateScanRequest,
        tracked_only: bool,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        if tracked_only {
            self.scan_tracked_batch(request).await
        } else {
            self.scan_batch(request).await
        }
    }

    /// Reads one explicit authenticated serving domain.  Ordinary visibility
    /// callers use `Combined`; internal historical/entity validation and
    /// current-only durable callers select their domain explicitly.
    async fn scan_domain_batch(
        &self,
        request: &LiveStateScanRequest,
        domain: LiveStateReadDomain,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        match domain {
            LiveStateReadDomain::Combined => self.scan_constraint_batch(request, false).await,
            LiveStateReadDomain::Tracked => {
                let mut request = request.clone();
                request.filter.untracked = Some(false);
                self.scan_constraint_batch(&request, true).await
            }
            LiveStateReadDomain::Untracked => {
                let mut request = request.clone();
                request.filter.untracked = Some(true);
                self.scan_constraint_batch(&request, false).await
            }
        }
    }

    /// Scans the immutable tracked head selected by the current branch ref.
    ///
    /// Every reader must choose and implement its tracked source explicitly;
    /// silently lowering this request to the combined current-state scan would
    /// make a missing tracked owner look like valid data.
    async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError>;

    /// Loads concrete visible identities while preserving request alignment.
    ///
    /// Readers must provide the correlated batch path directly.
    /// There is no scan-based default because silently lowering this operation
    /// to one scan per row would reintroduce the amplification this API exists
    /// to prevent.
    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError>;

    /// Loads collection control from this reader's coherent storage snapshot.
    ///
    /// Readers without a published tracked-head projection conservatively
    /// decline the proof so callers retain their ordinary identity scans.
    async fn collection_generation(
        &self,
        _branch_id: &str,
        _scope: crate::collection_generation::CollectionScopeRef<'_>,
    ) -> Result<Option<crate::collection_generation::CollectionGeneration>, LixError> {
        Ok(None)
    }
}

#[cfg(test)]
pub(crate) async fn load_exact_batch_via_scan_for_test<R>(
    reader: &R,
    request: &LiveStateExactBatchRequest,
) -> Result<MaterializedLiveStateExactBatch, LixError>
where
    R: LiveStateReader + ?Sized,
{
    let mut rows = MaterializedLiveStateBatchBuilder::with_capacity(request.rows.len());
    let mut slots = Vec::with_capacity(request.rows.len());
    for row in &request.rows {
        let scanned = reader.scan_batch(&request.row_scan_request(row)).await?;
        slots.push(
            scanned
                .get(0)
                .map(|row| u32::try_from(rows.push_ref(row, None)))
                .transpose()
                .map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "test exact live-state result exceeds u32 rows",
                    )
                })?,
        );
    }
    MaterializedLiveStateExactBatch::new(rows.finish(), slots)
}

#[cfg(test)]
pub(crate) async fn scan_tracked_batch_via_scan<R>(
    reader: &R,
    request: &LiveStateScanRequest,
) -> Result<MaterializedLiveStateBatch, LixError>
where
    R: LiveStateReader + ?Sized,
{
    let mut request = request.clone();
    request.filter.untracked = Some(false);
    reader.scan_batch(&request).await
}
