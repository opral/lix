use async_trait::async_trait;

use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{LiveStateRowRequest, LiveStateScanRequest};
use crate::LixError;

/// Minimal engine2 read model for transaction planning and SQL providers.
///
/// Engine2 only needs visible state-row reads here. Changelog freshness/catch-up
/// should be added at this boundary later instead of leaking projection internals
/// into sessions or SQL providers.
#[async_trait]
pub(crate) trait LiveStateContext: Send + Sync {
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError>;

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError>;
}
