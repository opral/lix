use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::live_state::LiveStateRow;
use crate::live_state::{LiveStateReader, LiveStateRowRequest, LiveStateScanRequest};
use crate::transaction::staging::{StagedExactRow, StagedStateRowIdentity, StagedStateRowOverlay};
use crate::LixError;

/// Live-state view for one engine2 write transaction.
///
/// Reads see staged rows first. Base rows with the same identity are
/// hidden so staged updates and deletes behave like transaction-local state.
pub(crate) struct TransactionLiveStateContext {
    base: Arc<dyn LiveStateReader>,
    staged: StagedStateRowOverlay,
}

impl TransactionLiveStateContext {
    /// Composes base live state with the transaction-local staging overlay.
    pub(crate) fn new(base: Arc<dyn LiveStateReader>, staged: StagedStateRowOverlay) -> Self {
        Self { base, staged }
    }
}

#[async_trait]
impl LiveStateReader for TransactionLiveStateContext {
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        overlay_scan_rows(self.base.as_ref(), &self.staged, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        match self.staged.load_exact(request) {
            Some(StagedExactRow::Row(row)) => Ok(Some(row)),
            Some(StagedExactRow::Tombstone) => Ok(None),
            None => self.base.load_row(request).await,
        }
    }
}

pub(crate) async fn overlay_scan_rows(
    base: &dyn LiveStateReader,
    staged: &StagedStateRowOverlay,
    request: &LiveStateScanRequest,
) -> Result<Vec<LiveStateRow>, LixError> {
    let mut rows = staged.scan(request);
    let hidden_identities = staged.identities_matching_scan(request);
    let mut visible_identities = rows
        .iter()
        .map(StagedStateRowIdentity::from)
        .collect::<BTreeSet<_>>();

    for row in base.scan_rows(request).await? {
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
