use async_trait::async_trait;

use crate::LixError;
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
    /// Production readers must enter the shared batch pipeline directly.
    #[cfg(not(test))]
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError>;

    /// Test-only bridge for small row-oriented reader fakes.
    #[cfg(test)]
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_rows(request)
            .await
            .map(MaterializedLiveStateBatch::from_rows)
    }

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

    #[cfg(test)]
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_batch(request)
            .await
            .map(MaterializedLiveStateBatch::into_rows)
    }

    /// Scans the immutable tracked head selected by the current branch ref.
    ///
    /// Normal SQL reads use [`Self::scan_batch`] and therefore see exactly one
    /// canonical current row. Validation and schema planning use this explicit
    /// durability view when a tracked commit must not depend on untracked
    /// live state. Readers that wrap canonical current scans must override
    /// this method instead of relying on the fallback below.
    #[cfg(test)]
    async fn scan_tracked_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let mut request = request.clone();
        request.filter.untracked = Some(false);
        self.scan_rows(&request).await
    }

    /// Production default keeps the explicit tracked filter in the batch
    /// lane. Readers with a distinct immutable tracked source override this.
    #[cfg(not(test))]
    async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        let mut request = request.clone();
        request.filter.untracked = Some(false);
        self.scan_batch(&request).await
    }

    /// Test-only bridge for fakes that model canonical and tracked rows
    /// independently through the legacy row helper.
    #[cfg(test)]
    async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        self.scan_tracked_rows(request)
            .await
            .map(MaterializedLiveStateBatch::from_rows)
    }

    #[allow(dead_code)]
    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError>;

    /// Loads concrete visible identities while preserving request alignment.
    ///
    /// Production readers must provide the correlated batch path directly.
    /// There is no scan-based default because silently lowering this operation
    /// to one scan per row would reintroduce the amplification this API exists
    /// to prevent.
    #[cfg(not(test))]
    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError>;

    /// Test-only bridge for small row-oriented reader fakes.
    #[cfg(test)]
    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        self.load_exact_rows(request)
            .await
            .map(MaterializedLiveStateExactBatch::from_rows)
    }

    /// Explicit terminal DTO bridge for scalar consumers.
    ///
    /// Bulk production callers should retain the exact batch owner and borrow
    /// row views instead of materializing one owned structure per result.
    #[cfg(test)]
    async fn load_exact_rows(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
        self.load_exact_batch(request)
            .await
            .map(MaterializedLiveStateExactBatch::into_rows)
    }
}

#[cfg(test)]
pub(crate) async fn load_exact_rows_via_scan_for_test<R>(
    reader: &R,
    request: &LiveStateExactBatchRequest,
) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError>
where
    R: LiveStateReader + ?Sized,
{
    let mut rows = Vec::with_capacity(request.rows.len());
    for row in &request.rows {
        rows.push(
            reader
                .scan_rows(&request.row_scan_request(row))
                .await?
                .into_iter()
                .next(),
        );
    }
    Ok(rows)
}
