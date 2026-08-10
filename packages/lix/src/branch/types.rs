use crate::changelog::{ChangeId, CommitId};
use crate::common::LixTimestamp;

/// Current changelog head for a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchHead {
    pub(crate) branch_id: String,
    pub(crate) commit_id: CommitId,
}

/// Authenticated metadata for the RefChange currently named by a branch
/// selector. This is deliberately separate from the head commit projection:
/// both fields come from the same immutable RefChange object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchRefMetadata {
    pub(crate) change_id: ChangeId,
    pub(crate) updated_at: LixTimestamp,
}

/// Typed reader for moving branch heads.
#[async_trait::async_trait]
pub(crate) trait BranchRefReader: Send + Sync {
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, crate::LixError>;

    async fn load_head_commit_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<CommitId>, crate::LixError> {
        Ok(self.load_head(branch_id).await?.map(|head| head.commit_id))
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, crate::LixError>;

    /// Returns branch heads and authenticated RefChange metadata from one
    /// operation-owned batch. ForkTree-backed readers override this method;
    /// head-only readers do not expose this projection.
    async fn scan_head_metadata(
        &self,
    ) -> Result<Vec<(BranchHead, BranchRefMetadata)>, crate::LixError> {
        Err(crate::LixError::new(
            crate::LixError::CODE_STORAGE_ERROR,
            "authenticated branch metadata batch is unavailable",
        ))
    }

    /// Returns authenticated branch metadata for exactly the requested
    /// selectors in one operation-owned batch. Missing selectors are omitted
    /// from the result so callers can preserve requested order while
    /// fail-closing on any absent branch; implementations must not widen this
    /// into a scan-all or a sequence of point reads.
    async fn load_head_metadata_batch(
        &self,
        _branch_ids: &[String],
    ) -> Result<Vec<(BranchHead, BranchRefMetadata)>, crate::LixError> {
        Err(crate::LixError::new(
            crate::LixError::CODE_STORAGE_ERROR,
            "requested authenticated branch metadata batch is unavailable",
        ))
    }
}
