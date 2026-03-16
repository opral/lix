use std::collections::BTreeMap;

use crate::CanonicalJson;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProposedDomainChange {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: Option<String>,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) version_id: String,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainChangeInput {
    pub id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
    pub version_id: String,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSnapshot {
    pub id: String,
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
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
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
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
    pub lixcol_version_id: String,
    pub lixcol_commit_id: String,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalCommitOutput {
    pub changes: Vec<ChangeRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedCommitApplyInput {
    pub live_state_rows: Vec<MaterializedStateRow>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitResult {
    pub canonical_output: CanonicalCommitOutput,
    pub derived_apply_input: DerivedCommitApplyInput,
}
