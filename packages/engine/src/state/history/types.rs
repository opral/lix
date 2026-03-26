#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryContentMode {
    MetadataOnly,
    #[default]
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryOrder {
    #[default]
    EntityFileSchemaDepthAsc,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryRootScope {
    #[default]
    AllRoots,
    RequestedRoots(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryLineageScope {
    #[default]
    Standard,
    ActiveVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum StateHistoryVersionScope {
    #[default]
    Any,
    RequestedVersions(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct StateHistoryRequest {
    pub(crate) root_scope: StateHistoryRootScope,
    pub(crate) lineage_scope: StateHistoryLineageScope,
    pub(crate) active_version_id: Option<String>,
    pub(crate) version_scope: StateHistoryVersionScope,
    pub(crate) entity_ids: Vec<String>,
    pub(crate) file_ids: Vec<String>,
    pub(crate) schema_keys: Vec<String>,
    pub(crate) plugin_keys: Vec<String>,
    pub(crate) min_depth: Option<i64>,
    pub(crate) max_depth: Option<i64>,
    pub(crate) content_mode: StateHistoryContentMode,
    pub(crate) order: StateHistoryOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StateHistoryRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
    pub(crate) commit_created_at: String,
    pub(crate) root_commit_id: String,
    pub(crate) depth: i64,
    pub(crate) version_id: String,
}
