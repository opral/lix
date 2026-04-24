use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::engine2::transaction::staging::{
    StagedExactRow, StagedStateRowIdentity, StagedStateRowOverlay,
};
use crate::live_state::{ExactRowRequest, LiveRow, LiveStateContext, LiveStateScanRequest};
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
    async fn scan(&self, request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
        let mut rows = self.staged.scan(request);
        let hidden_identities = self.staged.identities_matching_scan(request);
        let mut visible_identities = rows
            .iter()
            .map(staged_identity_from_live_row)
            .collect::<BTreeSet<_>>();

        for row in self.committed.scan(request).await? {
            let identity = staged_identity_from_live_row(&row);
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

    async fn load_exact(&self, request: &ExactRowRequest) -> Result<Option<LiveRow>, LixError> {
        match self.staged.load_exact(request) {
            Some(StagedExactRow::Row(row)) => Ok(Some(row)),
            Some(StagedExactRow::Tombstone) => Ok(None),
            None => self.committed.load_exact(request).await,
        }
    }
}

fn staged_identity_from_live_row(row: &LiveRow) -> StagedStateRowIdentity {
    (
        row.untracked,
        row.schema_key.clone(),
        row.entity_id.clone(),
        row.file_id.clone(),
        row.version_id.clone(),
        row.plugin_key.clone(),
        row.schema_version.clone(),
    )
}
