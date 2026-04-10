use async_trait::async_trait;

mod bootstrap;
pub(crate) mod commit;
pub(crate) mod committed_state;
pub(crate) mod context;
mod create;
pub(crate) mod descriptors;
mod frontier;
mod history_roots;
mod merge;
mod state_history;
pub(crate) mod undo_redo;

pub(crate) use bootstrap::{init, seed_bootstrap};
pub(crate) use committed_state::{
    load_exact_canonical_row_at_version_head_with_executor,
    load_version_head_commit_id_with_executor, load_version_info_for_versions,
};
pub(crate) use committed_state::{VersionInfo, VersionSnapshot};
pub(crate) use create::create_version_in_session;
pub use create::{CreateVersionOptions, CreateVersionResult};
pub(crate) use frontier::{
    load_all_version_head_commit_ids_with_executor,
    load_current_committed_version_frontier_with_backend,
    load_current_committed_version_frontier_with_executor,
    load_version_head_commit_map_with_executor,
};
pub(crate) use merge::merge_version_in_session;
pub use merge::{ExpectedVersionHeads, MergeOutcome, MergeVersionOptions, MergeVersionResult};
pub use undo_redo::{RedoOptions, RedoResult, UndoOptions, UndoResult};

#[async_trait(?Send)]
impl crate::contracts::CommittedStateHistoryReader for dyn crate::LixBackend + '_ {
    async fn load_committed_state_history_rows(
        &self,
        request: &crate::contracts::StateHistoryRequest,
    ) -> Result<Vec<crate::contracts::StateHistoryRow>, crate::LixError> {
        state_history::load_state_history_rows(self, request).await
    }
}
