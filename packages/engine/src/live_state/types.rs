use crate::changelog::MaterializedCanonicalChange;
use crate::entity_identity::EntityIdentity;
use crate::json_store::JsonRef;
use crate::tracked_state::{MaterializedTrackedStateRow, TrackedStateRow};
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateFilter, UntrackedStateRow,
    UntrackedStateRowRequest,
};
use crate::{NullableKeyFilter, RowMetadata, Value};

/// Durable row visible through live_state reads.
///
/// Unlike provider write rows, live-state rows are fully hydrated facts. Missing
/// generated fields should be caught before this type is constructed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedLiveStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<RowMetadata>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

/// Ref-backed row accepted by live-state write boundaries.
///
/// The transaction layer owns materialized JSON -> JsonRef preparation. Live
/// state only routes already-canonical rows into tracked and untracked stores.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateRow {
    pub(crate) entity_id: EntityIdentity,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_ref: Option<JsonRef>,
    pub(crate) metadata_ref: Option<JsonRef>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: Option<String>,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

/// Ref-backed live-state write batch.
///
/// Live-state reads are materialized, but writes should already carry JsonRefs
/// prepared by the transaction boundary. This keeps json_store ownership out of
/// live_state and makes root ancestry explicit instead of deriving it from
/// staged commit JSON.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateWriteBatch {
    pub(crate) untracked_rows: Vec<LiveStateRow>,
    pub(crate) tracked_roots: Vec<LiveStateTrackedRootWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiveStateTrackedRootWrite {
    pub(crate) commit_id: String,
    pub(crate) parent_commit_id: Option<String>,
    pub(crate) rows: Vec<LiveStateRow>,
}

impl TryFrom<LiveStateRow> for UntrackedStateRow {
    type Error = crate::LixError;

    fn try_from(row: LiveStateRow) -> Result<Self, Self::Error> {
        if !row.untracked {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "untracked_state cannot store tracked live-state rows",
            ));
        }
        Ok(UntrackedStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_ref: row.snapshot_ref,
            metadata_ref: row.metadata_ref,
            schema_version: row.schema_version,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            version_id: row.version_id,
        })
    }
}

impl TryFrom<LiveStateRow> for TrackedStateRow {
    type Error = crate::LixError;

    fn try_from(row: LiveStateRow) -> Result<Self, Self::Error> {
        if row.untracked {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state cannot store untracked live-state rows",
            ));
        }
        let Some(change_id) = row.change_id else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked live-state rows require change_id",
            ));
        };
        let Some(commit_id) = row.commit_id else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked live-state rows require commit_id",
            ));
        };
        Ok(TrackedStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_ref: row.snapshot_ref,
            metadata_ref: row.metadata_ref,
            schema_version: row.schema_version,
            created_at: row.created_at,
            updated_at: row.updated_at,
            change_id,
            commit_id,
        })
    }
}

impl From<MaterializedLiveStateRow> for MaterializedCanonicalChange {
    fn from(row: MaterializedLiveStateRow) -> Self {
        MaterializedCanonicalChange {
            id: row
                .change_id
                .expect("tracked live-state rows must carry change_id"),
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            schema_version: row.schema_version,
            file_id: row.file_id,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            created_at: row.created_at,
        }
    }
}

impl From<MaterializedUntrackedStateRow> for MaterializedLiveStateRow {
    fn from(row: MaterializedUntrackedStateRow) -> Self {
        MaterializedLiveStateRow {
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            schema_version: row.schema_version,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global: row.global,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: row.version_id,
        }
    }
}

impl TryFrom<&MaterializedLiveStateRow> for MaterializedTrackedStateRow {
    type Error = crate::LixError;

    fn try_from(row: &MaterializedLiveStateRow) -> Result<Self, Self::Error> {
        if row.untracked {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state cannot store untracked live-state rows",
            ));
        }
        let Some(change_id) = row.change_id.clone() else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state rows require change_id",
            ));
        };
        let Some(commit_id) = row.commit_id.clone() else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state rows require commit_id",
            ));
        };

        Ok(MaterializedTrackedStateRow {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot_content.clone(),
            metadata: row.metadata.clone(),
            schema_version: row.schema_version.clone(),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            change_id,
            commit_id,
        })
    }
}

impl From<&MaterializedLiveStateRow> for MaterializedUntrackedStateRow {
    fn from(row: &MaterializedLiveStateRow) -> Self {
        MaterializedUntrackedStateRow {
            entity_id: row.entity_id.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot_content.clone(),
            metadata: row.metadata.clone(),
            schema_version: row.schema_version.clone(),
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            global: row.global,
            version_id: row.version_id.clone(),
        }
    }
}

/// Which indexed field a live-state scan constraint applies to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ScanField {
    EntityId,
    FileId,
    SchemaVersion,
}

/// Inclusive or exclusive range bound.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct Bound {
    pub(crate) value: Value,
    pub(crate) inclusive: bool,
}

/// SQL-free structured scan constraint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) struct ScanConstraint {
    pub(crate) field: ScanField,
    pub(crate) operator: ScanOperator,
}

/// Structured scan operator aligned with the current planner/storage split.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ScanOperator {
    Eq(Value),
    In(Vec<Value>),
    Range {
        lower: Option<Bound>,
        upper: Option<Bound>,
    },
}

/// Identity-centered filter for visible live entities.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_ids: Vec<EntityIdentity>,
    #[serde(default)]
    pub(crate) version_ids: Vec<String>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

impl From<LiveStateFilter> for UntrackedStateFilter {
    fn from(filter: LiveStateFilter) -> Self {
        Self {
            schema_keys: filter.schema_keys,
            entity_ids: filter.entity_ids,
            version_ids: filter.version_ids,
            file_ids: filter.file_ids,
        }
    }
}

/// Requested property set for a live-state scan.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// First-principles scan request for engine2-owned reads.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateScanRequest {
    #[serde(default)]
    pub(crate) filter: LiveStateFilter,
    #[serde(default)]
    pub(crate) projection: LiveStateProjection,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Point lookup request for one visible live-state row.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LiveStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) file_id: NullableKeyFilter<String>,
}

impl From<&LiveStateRowRequest> for UntrackedStateRowRequest {
    fn from(request: &LiveStateRowRequest) -> Self {
        Self {
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            entity_id: request.entity_id.clone(),
            file_id: request.file_id.clone(),
        }
    }
}

/// Stable visible-row identity used for overlay composition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveStateRowIdentity {
    pub(crate) version_id: String,
    pub(crate) schema_key: String,
    pub(crate) entity_id: EntityIdentity,
    pub(crate) file_id: Option<String>,
}

impl LiveStateRowIdentity {
    pub(crate) fn from_row(row: &MaterializedLiveStateRow) -> Self {
        Self {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }
}
