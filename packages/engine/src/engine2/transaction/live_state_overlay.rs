use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{LiveStateContext, LiveStateRowRequest, LiveStateScanRequest};
use crate::engine2::transaction::staging::{
    StagedExactRow, StagedStateRowIdentity, StagedStateRowOverlay,
};
use crate::LixError;

/// Live-state view for one engine2 write transaction.
///
/// Reads see staged rows first. Committed rows with the same identity are
/// hidden so staged updates and deletes behave like transaction-local state.
pub(crate) struct TransactionLiveStateContext {
    committed: Arc<dyn LiveStateContext>,
    staged: StagedStateRowOverlay,
}

impl TransactionLiveStateContext {
    /// Composes committed live state with the transaction-local staging overlay.
    pub(crate) fn new(committed: Arc<dyn LiveStateContext>, staged: StagedStateRowOverlay) -> Self {
        Self { committed, staged }
    }
}

#[async_trait]
impl LiveStateContext for TransactionLiveStateContext {
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        let mut rows = self.staged.scan(request);
        let hidden_identities = self.staged.identities_matching_scan(request);
        let mut visible_identities = rows
            .iter()
            .map(StagedStateRowIdentity::from)
            .collect::<BTreeSet<_>>();

        for row in self.committed.scan_rows(request).await? {
            let identity = StagedStateRowIdentity::from(&row);
            if hidden_identities.contains(&identity) {
                continue;
            }
            if visible_identities.insert(identity) {
                rows.push(row);
            }
        }

        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        match self.staged.load_exact(request) {
            Some(StagedExactRow::Row(row)) => Ok(Some(row)),
            Some(StagedExactRow::Tombstone) => Ok(None),
            None => self.committed.load_row(request).await,
        }
    }
}
