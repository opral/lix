use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;

/// Immutable canonical change fact stored in the changelog.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct CanonicalChange {
    pub(crate) id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) created_at: String,
}

impl CanonicalChange {
    pub(crate) fn as_ref(&self) -> CanonicalChangeRef<'_> {
        CanonicalChangeRef {
            id: &self.id,
            entity_id: &self.entity_id,
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            snapshot_ref: self.snapshot_ref.as_ref(),
            metadata_ref: self.metadata_ref.as_ref(),
            created_at: &self.created_at,
        }
    }
}

/// Borrowed changelog write row.
///
/// Changelog owns this storage-facing shape; transaction code may adapt into it
/// but changelog never depends on transaction/commit types.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CanonicalChangeRef<'a> {
    pub(crate) id: &'a str,
    pub(crate) entity_id: &'a EntityIdentity,
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) snapshot_ref: Option<&'a JsonRef>,
    pub(crate) metadata_ref: Option<&'a JsonRef>,
    pub(crate) created_at: &'a str,
}

/// Boundary shape for callers that still work with materialized JSON payloads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedCanonicalChange {
    pub(crate) id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
}

/// Minimal changelog scan request.
///
/// TODO(engine): add filters and append-order cursors once changelog storage
/// has real append sequence keys.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ChangelogScanRequest {
    pub(crate) limit: Option<usize>,
}
