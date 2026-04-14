use crate::canonical::CanonicalCommitReceipt;

use super::ReplayCursor;

/// Operational receipt used to advance local derived projections after a
/// canonical commit succeeds.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CanonicalCommitProjectionReceipt {
    pub canonical_receipt: CanonicalCommitReceipt,
    pub replay_cursor: ReplayCursor,
}

impl CanonicalCommitProjectionReceipt {
    pub(crate) fn new(
        canonical_receipt: CanonicalCommitReceipt,
        replay_cursor: ReplayCursor,
    ) -> Self {
        Self {
            canonical_receipt,
            replay_cursor,
        }
    }
}
