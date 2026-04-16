#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateHistoryContentMode {
    MetadataOnly,
    #[default]
    IncludeSnapshotContent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateHistoryOrder {
    #[default]
    EntityFileSchemaDepthAsc,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StateHistoryRootScope {
    #[default]
    AllRoots,
    RequestedRoots(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StateHistoryLineageScope {
    #[default]
    Standard,
    ActiveVersion,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum StateHistoryVersionScope {
    #[default]
    Any,
    RequestedVersions(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StateHistoryRequest {
    pub root_scope: StateHistoryRootScope,
    pub lineage_scope: StateHistoryLineageScope,
    pub lineage_version_id: Option<String>,
    pub version_scope: StateHistoryVersionScope,
    pub entity_ids: Vec<String>,
    pub file_ids: Vec<String>,
    pub schema_keys: Vec<String>,
    pub plugin_keys: Vec<String>,
    pub min_depth: Option<i64>,
    pub max_depth: Option<i64>,
    pub content_mode: StateHistoryContentMode,
    pub order: StateHistoryOrder,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateHistoryRow {
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: Option<String>,
    pub plugin_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub schema_version: String,
    pub change_id: String,
    pub commit_id: String,
    pub commit_created_at: String,
    pub root_commit_id: String,
    pub depth: i64,
    pub version_id: String,
}
