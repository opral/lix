use async_trait::async_trait;

use crate::LixError;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateExactBatchRequest, LiveStateRowRequest, LiveStateScanRequest};

/// Minimal engine read model for transaction planning and SQL providers.
///
/// Engine only needs visible state-row reads here. Changelog freshness/catch-up
/// should be added at this boundary later instead of leaking projection internals
/// into sessions or SQL providers.
#[async_trait]
pub(crate) trait LiveStateReader: Send + Sync {
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError>;

    /// Scans the immutable tracked head selected by the current branch ref.
    ///
    /// Normal SQL reads use [`Self::scan_rows`] and therefore see exactly one
    /// canonical current row. Validation and schema planning use this explicit
    /// durability view when a tracked commit must not depend on untracked
    /// live state. Readers that wrap canonical current scans must override
    /// this method instead of relying on the fallback below.
    async fn scan_tracked_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        let mut request = request.clone();
        request.filter.untracked = Some(false);
        self.scan_rows(&request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError>;

    /// Loads concrete visible identities while preserving request alignment.
    ///
    /// Implementations must provide a correlated batch path. There is no
    /// scan-based default because silently lowering this operation to one scan
    /// per row would reintroduce the amplification this API exists to prevent.
    async fn load_exact_rows(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError>;
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
