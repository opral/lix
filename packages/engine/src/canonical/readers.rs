use crate::{LixBackend, LixError};

use super::roots::load_committed_version_head_commit_id as roots_load_committed_version_head_commit_id;
use super::state_source::{
    load_canonical_change_row_by_id as state_source_load_canonical_change_row_by_id,
    load_commit_lineage_entry_by_id as state_source_load_commit_lineage_entry_by_id,
    load_exact_committed_state_row_at_version_head as state_source_load_exact_committed_state_row_at_version_head,
    load_exact_committed_state_row_from_commit_with_executor as state_source_load_exact_committed_state_row_from_commit_with_executor,
};
pub(crate) use super::state_source::{
    CommitLineageEntry, CommitQueryExecutor, CommittedCanonicalChangeRow, ExactCommittedStateRow,
    ExactCommittedStateRowRequest,
};

pub(crate) async fn load_committed_version_head_commit_id(
    executor: &mut dyn CommitQueryExecutor,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    roots_load_committed_version_head_commit_id(executor, version_id).await
}

pub(crate) async fn load_exact_committed_state_row_at_version_head(
    backend: &dyn LixBackend,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    state_source_load_exact_committed_state_row_at_version_head(backend, request).await
}

pub(crate) async fn load_exact_committed_state_row_from_commit_with_executor(
    executor: &mut dyn CommitQueryExecutor,
    commit_id: &str,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    state_source_load_exact_committed_state_row_from_commit_with_executor(
        executor, commit_id, request,
    )
    .await
}

pub(crate) async fn load_commit_lineage_entry_by_id(
    executor: &mut dyn CommitQueryExecutor,
    commit_id: &str,
) -> Result<Option<CommitLineageEntry>, LixError> {
    state_source_load_commit_lineage_entry_by_id(executor, commit_id).await
}

pub(crate) async fn load_canonical_change_row_by_id(
    executor: &mut dyn CommitQueryExecutor,
    change_id: &str,
) -> Result<Option<CommittedCanonicalChangeRow>, LixError> {
    state_source_load_canonical_change_row_by_id(executor, change_id).await
}
