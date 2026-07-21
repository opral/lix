use async_trait::async_trait;

use crate::LixError;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateFileScanRequest, LiveStateRowRequest, LiveStateScanRequest};

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

    #[cfg_attr(not(test), allow(dead_code))]
    async fn scan_file_rows(
        &self,
        request: &LiveStateFileScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        self.scan_rows(&request.to_scan_request()).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError>;
}
