use crate::entity_identity::EntityIdentity;
use crate::LixError;
use crate::NullableKeyFilter;

/// Durable local row excluded from changelog and commit membership.
///
/// This is the canonical storage row shape after joining the physical key with
/// the value payload. Identity fields live in the key; mutable JSON payloads
/// and scalar row state live in the value.
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

    pub(crate) fn from_exact_row_request(request: &UntrackedStateRowRequest) -> Option<Self> {
        let file_id = match &request.file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value.clone()),
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            version_id: request.version_id.clone(),
            schema_key: request.schema_key.clone(),
            entity_id: request.entity_id.clone(),
            file_id,
        })
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

/// Semantic property set for untracked-state reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum UntrackedStateProjection {
    Identity,
    Header,
    Payload,
    Full,
}

impl Default for UntrackedStateProjection {
    fn default() -> Self {
        Self::Full
    }
}

impl UntrackedStateProjection {
    pub(crate) fn from_column_names(columns: &[String]) -> Self {
        if columns.is_empty() {
            return Self::Full;
        }
        let wants_payload = columns
            .iter()
            .any(|column| matches!(column.as_str(), "snapshot_content" | "metadata"));
        let wants_header = columns.iter().any(|column| {
            matches!(
                column.as_str(),
                "created_at" | "updated_at" | "global" | "deleted"
            )
        });
        match (wants_header, wants_payload) {
            (false, false) => Self::Identity,
            (true, false) => Self::Header,
            (false, true) => Self::Payload,
            (true, true) => Self::Full,
        }
    }
}

/// Owned projected row for untracked reads.
///
/// `identity` is always present because it is encoded in the physical key. The
/// remaining fields are present only when requested by the projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateProjectedRow {
    pub(crate) identity: UntrackedStateIdentity,
    pub(crate) created_at: Option<String>,
    pub(crate) updated_at: Option<String>,
    pub(crate) global: Option<bool>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) deleted: Option<bool>,
}

impl UntrackedStateProjectedRow {
    pub(crate) fn into_materialized_full(self) -> Result<MaterializedUntrackedStateRow, LixError> {
        Ok(MaterializedUntrackedStateRow {
            entity_id: self.identity.entity_id,
            schema_key: self.identity.schema_key,
            file_id: self.identity.file_id,
            snapshot_content: self.snapshot_content,
            metadata: self.metadata,
            deleted: self.deleted.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "untracked projected row missing deleted field for materialization",
                )
            })?,
            created_at: self.created_at.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "untracked projected row missing created_at for materialization",
                )
            })?,
            updated_at: self.updated_at.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "untracked projected row missing updated_at for materialization",
                )
            })?,
            global: self.global.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "untracked projected row missing global for materialization",
                )
            })?,
            version_id: self.identity.version_id,
        })
    }
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
    #[serde(default)]
    pub(crate) after: Option<Vec<u8>>,
    #[serde(default)]
    pub(crate) batch_size: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateGetManyRequest {
    pub(crate) identities: Vec<UntrackedStateIdentity>,
    pub(crate) projection: UntrackedStateProjection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateGetManyResponse {
    pub(crate) rows: Vec<Option<UntrackedStateProjectedRow>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateScanResponse {
    pub(crate) rows: Vec<UntrackedStateProjectedRow>,
    pub(crate) resume_after: Option<Vec<u8>>,
}

/// Point lookup request for one untracked local overlay row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UntrackedStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) file_id: NullableKeyFilter<String>,
}

impl From<UntrackedStateIdentity> for UntrackedStateRowRequest {
    fn from(identity: UntrackedStateIdentity) -> Self {
        Self {
            schema_key: identity.schema_key,
            version_id: identity.version_id,
            entity_id: identity.entity_id,
            file_id: identity
                .file_id
                .map_or(NullableKeyFilter::Null, NullableKeyFilter::Value),
        }
    }
}
