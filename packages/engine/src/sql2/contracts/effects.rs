use crate::state_commit_stream::StateCommitStreamChange;

#[derive(Debug, Clone, Default)]
pub(crate) struct PlanEffects {
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) next_active_version_id: Option<String>,
}
