use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::NullableKeyFilter;

/// Durable local row excluded from changelog and commit membership.
///
/// This is the canonical physical shape: identity/header fields are stored
/// directly, while JSON payloads live in json_store and are referenced by hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) version_id: String,
}

/// Hydrated boundary shape for callers that still work with JSON payloads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedUntrackedStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) version_id: String,
}

/// Stable identity for one local untracked overlay row.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct UntrackedStateIdentity {
    pub(crate) version_id: String,
    pub(crate) schema_key: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) file_id: Option<String>,
}

impl UntrackedStateIdentity {
    pub(crate) fn from_row(row: &UntrackedStateRow) -> Self {
        Self {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }
}

/// Identity-centered filter for untracked local overlay scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct UntrackedStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_ids: Vec<EntityIdentity>,
    #[serde(default)]
    pub(crate) version_ids: Vec<String>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
}

/// Requested property set for an untracked-state scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct UntrackedStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// Scan request for local untracked overlay rows.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct UntrackedStateScanRequest {
    #[serde(default)]
    pub(crate) filter: UntrackedStateFilter,
    #[serde(default)]
    pub(crate) projection: UntrackedStateProjection,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Point lookup request for one untracked local overlay row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) file_id: NullableKeyFilter<String>,
}
