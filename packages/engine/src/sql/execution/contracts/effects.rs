use std::collections::BTreeSet;

use crate::state::stream::StateCommitStreamChange;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemPayloadDomainChange {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PlanEffects {
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) next_active_version_id: Option<String>,
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}
