use crate::LixError;
use crate::branch::{BranchHead, BranchRefMetadata, BranchRefReader};
use crate::changelog::CommitId;
use crate::storage_adapter::StorageAdapterRead;

/// Typed access to moving branch heads stored in the direct control plane.
///
/// The control record is deliberately below live-state visibility, keeping
/// the dependency acyclic: `branch-control -> tracked-head -> live-state`.
pub(super) struct BranchRefContext {}

impl BranchRefContext {
    pub(super) fn new() -> Self {
        Self {}
    }

    /// Creates a branch-ref reader over a caller-provided KV store.
    #[expect(clippy::unused_self)]
    pub(super) fn reader<S>(&self, store: S) -> BranchRefStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        BranchRefStoreReader { store }
    }
}

/// Read side for branch heads.
pub(super) struct BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    store: S,
}

impl<S> BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        let requested = [branch_id.to_string()];
        Ok(
            crate::forktree::load_branch_heads_with_metadata(&self.store, Some(&requested))
                .await?
                .into_iter()
                .next()
                .map(|row| BranchHead {
                    branch_id: row.branch_id,
                    commit_id: row.head_commit_id,
                }),
        )
    }

    pub(crate) async fn load_head_commit_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<CommitId>, LixError> {
        Ok(self.load_head(branch_id).await?.map(|head| head.commit_id))
    }

    pub(crate) async fn load_head_change_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<crate::changelog::ChangeId>, LixError> {
        let requested = [branch_id.to_string()];
        Ok(
            crate::forktree::load_branch_heads_with_metadata(&self.store, Some(&requested))
                .await?
                .into_iter()
                .next()
                .map(|row| row.change_id),
        )
    }

    pub(crate) async fn load_head_metadata(
        &self,
        branch_id: &str,
    ) -> Result<Option<BranchRefMetadata>, LixError> {
        let requested = [branch_id.to_string()];
        let row = crate::forktree::load_branch_heads_with_metadata(&self.store, Some(&requested))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "requested branch selector is absent",
                )
            })?;
        Ok(Some(BranchRefMetadata {
            change_id: row.change_id,
            updated_at: row.updated_at,
        }))
    }

    pub(crate) async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Ok(
            crate::forktree::load_branch_heads_with_metadata(&self.store, None)
                .await?
                .into_iter()
                .map(|row| BranchHead {
                    branch_id: row.branch_id,
                    commit_id: row.head_commit_id,
                })
                .collect(),
        )
    }
}

#[async_trait::async_trait]
impl<S> BranchRefReader for BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
        Self::load_head(self, branch_id).await
    }

    async fn load_head_commit_id(&self, branch_id: &str) -> Result<Option<CommitId>, LixError> {
        Self::load_head_commit_id(self, branch_id).await
    }

    async fn load_head_change_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<crate::changelog::ChangeId>, LixError> {
        Self::load_head_change_id(self, branch_id).await
    }

    async fn load_head_metadata(
        &self,
        branch_id: &str,
    ) -> Result<Option<BranchRefMetadata>, LixError> {
        Self::load_head_metadata(self, branch_id).await
    }

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Self::scan_heads(self).await
    }
}
