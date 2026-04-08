use async_trait::async_trait;

pub(crate) mod committed_state;
pub(crate) mod context;
mod bootstrap;
mod create;
pub(crate) mod descriptors;
mod merge;
mod state_history;
pub(crate) mod undo_redo;

pub(crate) use bootstrap::{init, seed_bootstrap};
pub(crate) use committed_state::{
    load_exact_committed_state_row_at_version_head,
    load_exact_committed_state_row_at_version_head_with_executor, load_version_info_for_versions,
};
pub(crate) use create::create_version_in_session;
pub(crate) use merge::merge_version_in_session;
pub(crate) use committed_state::{VersionInfo, VersionSnapshot};
pub use create::{CreateVersionOptions, CreateVersionResult};
pub use merge::{
    ExpectedVersionHeads, MergeOutcome, MergeVersionOptions, MergeVersionResult,
};
pub use undo_redo::{RedoOptions, RedoResult, UndoOptions, UndoResult};

#[async_trait(?Send)]
impl crate::contracts::traits::CommittedStateHistoryReader for dyn crate::LixBackend + '_ {
    async fn load_committed_state_history_rows(
        &self,
        request: &crate::contracts::artifacts::StateHistoryRequest,
    ) -> Result<Vec<crate::contracts::artifacts::StateHistoryRow>, crate::LixError> {
        state_history::load_state_history_rows(self, request).await
    }
}
