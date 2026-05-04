use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::RowMetadata;

/// Immutable canonical change fact stored in the changelog.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CanonicalChange {
    pub(crate) id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
}

/// Boundary shape for callers that still work with materialized JSON payloads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedCanonicalChange {
    pub(crate) id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<RowMetadata>,
    pub(crate) created_at: String,
}

/// Minimal changelog scan request.
///
/// TODO(engine2): add filters and append-order cursors once changelog storage
/// has real append sequence keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChangelogScanRequest {
    pub(crate) limit: Option<usize>,
}
