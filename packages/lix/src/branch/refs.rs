use crate::LixError;
use crate::branch::{BranchHead, BranchRefMetadata, BranchRefReader};
use crate::changelog::CommitId;
use crate::storage_adapter::StorageAdapterRead;

/// Read side for branch heads.
pub(crate) struct BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    store: S,
}

impl<S> BranchRefStoreReader<S>
where
    S: StorageAdapterRead,
{
    pub(crate) fn new(store: S) -> Self {
        Self { store }
    }

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

    async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
        Self::scan_heads(self).await
    }

    async fn scan_head_metadata(&self) -> Result<Vec<(BranchHead, BranchRefMetadata)>, LixError> {
        Ok(
            crate::forktree::load_branch_heads_with_metadata(&self.store, None)
                .await?
                .into_iter()
                .map(|row| {
                    (
                        BranchHead {
                            branch_id: row.branch_id,
                            commit_id: row.head_commit_id,
                        },
                        BranchRefMetadata {
                            change_id: row.change_id,
                            updated_at: row.updated_at,
                        },
                    )
                })
                .collect(),
        )
    }

    async fn load_head_metadata_batch(
        &self,
        branch_ids: &[String],
    ) -> Result<Vec<(BranchHead, BranchRefMetadata)>, LixError> {
        Ok(
            crate::forktree::load_branch_heads_with_metadata(&self.store, Some(branch_ids))
                .await?
                .into_iter()
                .map(|row| {
                    (
                        BranchHead {
                            branch_id: row.branch_id,
                            commit_id: row.head_commit_id,
                        },
                        BranchRefMetadata {
                            change_id: row.change_id,
                            updated_at: row.updated_at,
                        },
                    )
                })
                .collect(),
        )
    }
}
