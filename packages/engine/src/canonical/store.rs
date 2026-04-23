#![allow(dead_code)]

use async_trait::async_trait;

use crate::functions::LixFunctionProvider;
use crate::LixError;

use super::{
    CanonicalAppendSummary, CanonicalChange, CanonicalChangeWrite, CanonicalCommit,
    CanonicalHistoryRequest, CanonicalHistoryRow, CanonicalUntrackedVisibilityWrite,
    CanonicalVisibleStateRequest, CanonicalVisibleStateRow,
};

pub(crate) type CanonicalBackendRef<'a> = &'a (dyn crate::LixBackend + 'a);
pub(crate) type CanonicalExecutorRef<'a> = &'a mut (dyn crate::QueryExecutor + 'a);
pub(crate) type CanonicalTransactionRef<'a> = &'a mut (dyn crate::LixBackendTransaction + 'a);
pub(crate) type CanonicalPreparedBatch = crate::PreparedBatch;
pub(crate) type CanonicalPreparedStatement = crate::PreparedStatement;
pub(crate) type CanonicalCommitQueryExecutor<'a> = dyn crate::QueryExecutor + 'a;

/// Owner-facing read surface for canonical committed-history persistence.
#[async_trait]
pub(crate) trait CanonicalReadStore: Send {
    async fn load_commit(&mut self, commit_id: &str) -> Result<Option<CanonicalCommit>, LixError>;

    async fn load_change(&mut self, change_id: &str) -> Result<Option<CanonicalChange>, LixError>;

    async fn load_history(
        &mut self,
        request: &CanonicalHistoryRequest,
    ) -> Result<Vec<CanonicalHistoryRow>, LixError>;

    async fn load_visible_state(
        &mut self,
        request: &CanonicalVisibleStateRequest,
    ) -> Result<Vec<CanonicalVisibleStateRow>, LixError>;

    async fn resolve_merge_base(
        &mut self,
        left_head_commit_id: &str,
        right_head_commit_id: &str,
    ) -> Result<Option<String>, LixError>;
}

/// Owner-facing write surface for canonical committed-history persistence.
#[async_trait]
pub(crate) trait CanonicalWriteStore: Send {
    async fn append_changes(
        &mut self,
        changes: &[CanonicalChangeWrite],
        functions: &mut dyn LixFunctionProvider,
    ) -> Result<CanonicalAppendSummary, LixError>;

    async fn append_untracked_change_visibility_rows(
        &mut self,
        visibility_rows: &[CanonicalUntrackedVisibilityWrite],
    ) -> Result<(), LixError>;

    async fn replace_snapshot_content(
        &mut self,
        snapshot_id: &str,
        snapshot_content: &str,
    ) -> Result<(), LixError>;
}
