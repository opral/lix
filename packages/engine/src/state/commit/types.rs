use std::collections::BTreeMap;

use crate::schema::live_layout::LiveTableLayout;
use crate::{
    CanonicalJson, CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId,
    FileId, VersionId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProposedDomainChange {
    pub(crate) entity_id: EntityId,
    pub(crate) schema_key: CanonicalSchemaKey,
    pub(crate) schema_version: Option<CanonicalSchemaVersion>,
    pub(crate) file_id: Option<FileId>,
    pub(crate) plugin_key: Option<CanonicalPluginKey>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) version_id: VersionId,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainChangeInput {
    pub id: String,
    pub entity_id: EntityId,
    pub schema_key: CanonicalSchemaKey,
    pub schema_version: CanonicalSchemaVersion,
    pub file_id: FileId,
    pub plugin_key: CanonicalPluginKey,
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
    pub version_id: VersionId,
    pub writer_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionSnapshot {
    pub id: VersionId,
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
    pub entity_id: EntityId,
    pub schema_key: CanonicalSchemaKey,
    pub schema_version: CanonicalSchemaVersion,
    pub file_id: FileId,
    pub plugin_key: CanonicalPluginKey,
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedStateRow {
    pub id: String,
    pub entity_id: EntityId,
    pub schema_key: CanonicalSchemaKey,
    pub schema_version: CanonicalSchemaVersion,
    pub file_id: FileId,
    pub plugin_key: CanonicalPluginKey,
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
    pub created_at: String,
    pub lixcol_version_id: VersionId,
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
    pub live_layouts: BTreeMap<String, LiveTableLayout>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GenerateCommitResult {
    pub canonical_output: CanonicalCommitOutput,
    pub derived_apply_input: DerivedCommitApplyInput,
}
