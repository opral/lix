use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainChangeInput {
    pub id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub created_at: String,
    pub version_id: String,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSnapshot {
    pub id: String,
    pub working_commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionInfo {
    pub parent_commit_ids: Vec<String>,
    pub snapshot: VersionSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitArgs {
    pub timestamp: String,
    pub active_accounts: Vec<String>,
    pub changes: Vec<DomainChangeInput>,
    pub versions: BTreeMap<String, VersionInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeRow {
    pub id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedStateRow {
    pub id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub created_at: String,
    pub lixcol_version_id: String,
    pub lixcol_commit_id: String,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitResult {
    pub changes: Vec<ChangeRow>,
    pub materialized_state: Vec<MaterializedStateRow>,
}
