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

    /// Returns the authenticated RefChange metadata for a branch. Head-only
    /// readers intentionally return no metadata; production branch-ref
    /// projection rejects that absence instead of manufacturing values.
    async fn load_head_metadata(
        &self,
        _branch_id: &str,
    ) -> Result<Option<BranchRefMetadata>, crate::LixError> {
        Ok(None)
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, crate::LixError>;
}
