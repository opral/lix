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
        Ok(crate::forktree::load_branch_head(&self.store, branch_id)
            .await?
            .map(|commit_id| BranchHead {
                branch_id: branch_id.to_string(),
                commit_id,
            }))
    }

    pub(crate) async fn load_head_commit_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<CommitId>, LixError> {
        Ok(self.load_head(branch_id).await?.map(|head| head.commit_id))
    }

    pub(crate) async fn load_head_metadata(
        &self,
        branch_id: &str,
    ) -> Result<Option<BranchRefMetadata>, LixError> {
        Ok(Some(
            crate::forktree::load_branch_ref_metadata(&self.store, branch_id).await?,
        ))
    }

    pub(crate) async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Ok(crate::forktree::scan_branch_heads(&self.store)
            .await?
            .into_iter()
            .map(|(branch_id, commit_id)| BranchHead {
                branch_id,
                commit_id,
            })
            .collect())
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
