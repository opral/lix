/// Current changelog head for a branch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BranchHead {
    pub(crate) branch_id: String,
    pub(crate) commit_id: String,
}

/// Typed reader for moving branch heads.
#[async_trait::async_trait]
pub(crate) trait BranchRefReader: Send + Sync {
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, crate::LixError>;

    async fn load_head_commit_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<String>, crate::LixError> {
        Ok(self.load_head(branch_id).await?.map(|head| head.commit_id))
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, crate::LixError>;
}
