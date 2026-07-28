use async_trait::async_trait;

use crate::LixError;
#[cfg(test)]
use crate::live_state::MaterializedLiveStateBatchBuilder;
use crate::live_state::{LiveStateExactBatchRequest, LiveStateRowRequest, LiveStateScanRequest};
use crate::live_state::{
    MaterializedLiveStateBatch, MaterializedLiveStateExactBatch, MaterializedLiveStateRow,
};

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

    /// Scans the immutable tracked head selected by the current branch ref.
    ///
    /// Normal SQL reads use [`Self::scan_batch`] and therefore see exactly one
    /// canonical current row. Validation and schema planning use this explicit
    /// durability view when a tracked commit must not depend on untracked
    /// live state. Readers that wrap canonical current scans must override
    /// this method instead of relying on the fallback below.
    /// The default keeps the explicit tracked filter in the batch
    /// lane. Readers with a distinct immutable tracked source override this.
    async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let mut request = request.clone();
        request.filter.untracked = Some(false);
        self.scan_batch(&request).await
    }

    #[allow(dead_code)]
    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError>;

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
