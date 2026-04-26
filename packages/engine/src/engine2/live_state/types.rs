use crate::engine2::changelog::CanonicalChange;
use crate::{NullableKeyFilter, Value};

/// Durable row visible through live_state reads.
///
/// Unlike provider write rows, live-state rows are fully hydrated facts. Missing
/// generated fields should be caught before this type is constructed.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct LiveStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) global: bool,
    pub(crate) change_id: String,
    pub(crate) commit_id: Option<String>,
    pub(crate) untracked: bool,
    pub(crate) version_id: String,
}

impl From<LiveStateRow> for CanonicalChange {
    fn from(row: LiveStateRow) -> Self {
        CanonicalChange {
            id: row.change_id,
            entity_id: row.entity_id,
            schema_key: row.schema_key,
            schema_version: row.schema_version,
            file_id: row.file_id,
            plugin_key: row.plugin_key,
            snapshot_content: row.snapshot_content,
            metadata: row.metadata,
            created_at: row.created_at,
        }
    }
}

/// Which indexed field a live-state scan constraint applies to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub(crate) enum ScanField {
    EntityId,
    FileId,
    PluginKey,
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
    pub(crate) entity_ids: Vec<String>,
    #[serde(default)]
    pub(crate) version_ids: Vec<String>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) plugin_keys: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) constraints: Vec<ScanConstraint>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
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
    pub(crate) entity_id: String,
    pub(crate) file_id: NullableKeyFilter<String>,
    pub(crate) untracked: bool,
}
