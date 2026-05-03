use async_trait::async_trait;

use crate::changelog::{CanonicalChange, ChangelogScanRequest};
use crate::LixError;

/// Read side for immutable changelog facts.
///
/// SQL providers and commit-graph readers depend on this role instead of
/// knowing which KV store backs the changelog for the current execution.
#[async_trait]
pub(crate) trait ChangelogReader: Send + Sync {
    #[allow(dead_code)]
    async fn load_change(&self, change_id: &str) -> Result<Option<CanonicalChange>, LixError>;

    #[allow(dead_code)]
    async fn scan_changes(
        &self,
        request: &ChangelogScanRequest,
    ) -> Result<Vec<CanonicalChange>, LixError>;
}
