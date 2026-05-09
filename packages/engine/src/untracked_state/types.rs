use crate::entity_identity::EntityIdentity;
use crate::NullableKeyFilter;

/// Durable local row excluded from changelog and commit membership.
///
/// This is the canonical physical shape: identity/header fields are stored
/// directly, and mutable JSON payloads are stored inline in the sidecar row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) version_id: String,
}

impl UntrackedStateRow {
    pub(crate) fn as_ref(&self) -> UntrackedStateRowRef<'_> {
        UntrackedStateRowRef {
            entity_id: &self.entity_id,
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            snapshot_content: self.snapshot_content.as_deref(),
            metadata: self.metadata.as_deref(),
            created_at: &self.created_at,
            updated_at: &self.updated_at,
            global: self.global,
            version_id: &self.version_id,
        }
    }
}

/// Zero-copy view of untracked-state write row.
///
/// Untracked state owns this storage-facing write shape. Callers adapt into it
/// without making untracked_state depend on transaction or live-state types.
#[derive(Debug, Clone, Copy)]
pub(crate) struct UntrackedStateRowRef<'a> {
    pub(crate) entity_id: &'a EntityIdentity,
    pub(crate) schema_key: &'a str,
    pub(crate) file_id: Option<&'a str>,
    pub(crate) snapshot_content: Option<&'a str>,
    pub(crate) metadata: Option<&'a str>,
    pub(crate) created_at: &'a str,
    pub(crate) updated_at: &'a str,
    pub(crate) global: bool,
    pub(crate) version_id: &'a str,
}

/// Hydrated boundary shape for callers that still work with JSON payloads.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedUntrackedStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) deleted: bool,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UntrackedStateIdentityRef<'a> {
    pub(crate) version_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) entity_id: &'a EntityIdentity,
    pub(crate) file_id: Option<&'a str>,
}

impl UntrackedStateIdentity {
    pub(crate) fn as_ref(&self) -> UntrackedStateIdentityRef<'_> {
        UntrackedStateIdentityRef {
            version_id: &self.version_id,
            schema_key: &self.schema_key,
            entity_id: &self.entity_id,
            file_id: self.file_id.as_deref(),
        }
    }
}

impl<'a> From<UntrackedStateRowRef<'a>> for UntrackedStateIdentityRef<'a> {
    fn from(row: UntrackedStateRowRef<'a>) -> Self {
        Self {
            version_id: row.version_id,
            schema_key: row.schema_key,
            entity_id: row.entity_id,
            file_id: row.file_id,
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
