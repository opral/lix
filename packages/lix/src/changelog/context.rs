use async_trait::async_trait;

use super::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, CommitId, CommitLoadBatch, CommitLoadRequest, CommitScanBatch,
    CommitScanRequest,
};
use crate::LixError;
use crate::storage_adapter::StorageAdapterRead;

const CATALOG_PAGE_LIMIT: usize = 1024;

/// Public changelog semantics served by the single authenticated ForkTree
/// CommitCatalog and ChangeCatalog. The context owns no storage namespace and
/// creates no secondary identity/order index.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChangelogContext;

impl ChangelogContext {
    pub(crate) const fn new() -> Self {
        Self
    }

    pub(crate) fn reader<S>(&self, read: S) -> ChangelogStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        ChangelogStoreReader { read }
    }
}

pub(crate) struct ChangelogStoreReader<S> {
    read: S,
}

#[async_trait]
pub(crate) trait ChangelogReader {
    async fn load_commits<'a>(
        &mut self,
        request: CommitLoadRequest<'a>,
    ) -> Result<CommitLoadBatch<'a>, LixError>;

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError>;

    async fn load_changes<'a>(
        &mut self,
        request: ChangeLoadRequest<'a>,
    ) -> Result<ChangeLoadBatch<'a>, LixError>;

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError>;
}

/// Writer capability intentionally has no implementation during the reader
/// wave. The writer-last compiler wave lowers semantic append/retirement into
/// one PreparedPublication consumed by the existing atomic transaction commit.
#[async_trait]
pub(crate) trait ChangelogWriter {
    async fn stage_append(&mut self, append: ChangelogAppend) -> Result<(), LixError>;

    async fn stage_delete_standalone_changes(
        &mut self,
        change_ids: &[ChangeId],
    ) -> Result<(), LixError>;
}

#[async_trait]
impl<S> ChangelogReader for ChangelogStoreReader<S>
where
    S: StorageAdapterRead,
{
    async fn load_commits<'a>(
        &mut self,
        request: CommitLoadRequest<'a>,
    ) -> Result<CommitLoadBatch<'a>, LixError> {
        let values = crate::forktree::load_commit_records(&self.read, request.commit_ids).await?;
        CommitLoadBatch::try_new("ForkTree CommitCatalog", request.commit_ids, values)
    }

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError> {
        let start_after = request
            .start_after
            .map(|value| CommitId::parse_lix(value, "commit scan start_after"))
            .transpose()?;
        let limit = request.limit.unwrap_or(CATALOG_PAGE_LIMIT);
        let entries = crate::forktree::scan_commit_records(&self.read, start_after, limit).await?;
        let next_start_after = (entries.len() == limit && limit != 0)
            .then(|| entries.last().map(|record| record.commit_id))
            .flatten();
        Ok(CommitScanBatch {
            entries,
            next_start_after,
        })
    }

    async fn load_changes<'a>(
        &mut self,
        request: ChangeLoadRequest<'a>,
    ) -> Result<ChangeLoadBatch<'a>, LixError> {
        let values = crate::forktree::load_change_records(&self.read, request.change_ids).await?;
        ChangeLoadBatch::try_new("ForkTree ChangeCatalog", request.change_ids, values)
    }

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError> {
        let start_after = request
            .start_after
            .map(|value| ChangeId::parse_lix(value, "change scan start_after"))
            .transpose()?;
        let limit = request.limit.unwrap_or(CATALOG_PAGE_LIMIT);
        let entries = crate::forktree::scan_change_records(&self.read, start_after, limit).await?;
        let next_start_after = (entries.len() == limit && limit != 0)
            .then(|| entries.last().map(|record| record.change_id))
            .flatten();
        Ok(ChangeScanBatch {
            entries,
            next_start_after,
        })
    }
}
