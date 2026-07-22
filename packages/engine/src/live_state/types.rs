use crate::changelog::{ChangeId, CommitId};
use crate::entity_pk::EntityPk;
use crate::live_state::index::MaterializedLiveStateIndexRow;
use crate::tracked_state::MaterializedTrackedStateRow;
use crate::{NullableKeyFilter, Value};

/// Durable row visible through live_state reads.
///
/// Unlike provider write rows, live-state rows are fully hydrated facts. Missing
/// generated fields should be caught before this type is constructed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct MaterializedLiveStateRow {
    pub(crate) entity_pk: EntityPk,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) deleted: bool,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: Option<ChangeId>,
    pub(crate) commit_id: Option<CommitId>,
    pub(crate) untracked: bool,
    pub(crate) branch_id: String,
}

impl From<MaterializedLiveStateIndexRow> for MaterializedLiveStateRow {
    fn from(row: MaterializedLiveStateIndexRow) -> Self {
        let global = row.branch_id == crate::GLOBAL_BRANCH_ID;
        Self {
            entity_pk: row.entity_pk,
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            deleted: false,
            created_at: row.created_at,
            updated_at: row.updated_at,
            global,
            change_id: Some(row.change_id),
            commit_id: None,
            untracked: true,
            branch_id: row.branch_id,
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
        let Some(change_id) = row.change_id else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state rows require change_id",
            ));
        };
        let Some(commit_id) = row.commit_id else {
            return Err(crate::LixError::new(
                "LIX_ERROR_UNKNOWN",
                "tracked_state rows require commit_id",
            ));
        };

        Ok(Self {
            entity_pk: row.entity_pk.clone(),
            schema_key: row.schema_key.clone(),
            file_id: row.file_id.clone(),
            snapshot_content: row.snapshot_content.clone(),
            metadata: row.metadata.clone(),
            deleted: row.deleted,
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            change_id,
            commit_id,
        })
    }
}

/// Which indexed field a live-state scan constraint applies to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ScanField {
    EntityPk,
    FileId,
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
    pub(crate) rows: LiveStateRowFilter,
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_pks: Vec<EntityPk>,
    #[serde(default)]
    pub(crate) branch_ids: Vec<String>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) untracked: Option<bool>,
    #[serde(default)]
    pub(crate) constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) enum LiveStateRowFilter {
    #[default]
    All,
    None,
}

/// Requested property set for a live-state scan.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct LiveStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// First-principles scan request for engine-owned reads.
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
    pub(crate) branch_id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: NullableKeyFilter<String>,
}

/// One concrete visible-row identity in an exact batch read.
///
/// Unlike [`LiveStateFilter`], the identity fields in this request are
/// correlated. Implementations must never expand multiple requests into the
/// Cartesian product of their schema, entity, and file dimensions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveStateExactRowRequest {
    pub(crate) schema_key: String,
    pub(crate) branch_id: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: Option<String>,
}

/// Aligned point-read request for visible live-state rows.
///
/// Results preserve `rows` order and cardinality: duplicate identities produce
/// duplicate result slots and missing or tombstoned identities produce `None`
/// unless tombstones are explicitly requested.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct LiveStateExactBatchRequest {
    pub(crate) rows: Vec<LiveStateExactRowRequest>,
    pub(crate) projection: LiveStateProjection,
    pub(crate) untracked: Option<bool>,
    pub(crate) include_tombstones: bool,
}

impl LiveStateExactBatchRequest {
    pub(crate) fn row_scan_request(&self, row: &LiveStateExactRowRequest) -> LiveStateScanRequest {
        LiveStateScanRequest {
            filter: LiveStateFilter {
                schema_keys: vec![row.schema_key.clone()],
                entity_pks: vec![row.entity_pk.clone()],
                branch_ids: vec![row.branch_id.clone()],
                file_ids: vec![
                    row.file_id
                        .as_ref()
                        .map_or(NullableKeyFilter::Null, |file_id| {
                            NullableKeyFilter::Value(file_id.clone())
                        }),
                ],
                untracked: self.untracked,
                include_tombstones: self.include_tombstones,
                ..LiveStateFilter::default()
            },
            projection: self.projection.clone(),
            limit: Some(1),
        }
    }
}

/// Stable visible-row identity used for overlay composition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LiveStateRowIdentity {
    pub(crate) branch_id: String,
    pub(crate) schema_key: String,
    pub(crate) entity_pk: EntityPk,
    pub(crate) file_id: Option<String>,
}

impl LiveStateRowIdentity {
    pub(crate) fn from_row(row: &MaterializedLiveStateRow) -> Self {
        Self {
            branch_id: row.branch_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_pk: row.entity_pk.clone(),
            file_id: row.file_id.clone(),
        }
    }
}
