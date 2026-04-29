use crate::NullableKeyFilter;

/// Rebuildable tracked state row.
///
/// Tracked rows are the projection that can be rebuilt from changelog facts.
/// They intentionally do not carry an `untracked` flag: untracked local overlay
/// data belongs to `untracked_state`, and the serving `live_state` facade is
/// responsible for combining both sources.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct TrackedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) plugin_key: Option<String>,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) schema_version: String,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
    pub(crate) change_id: String,
    pub(crate) commit_id: String,
}

/// Identity-centered filter for tracked-state scans.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateFilter {
    #[serde(default)]
    pub(crate) schema_keys: Vec<String>,
    #[serde(default)]
    pub(crate) entity_ids: Vec<String>,
    #[serde(default)]
    pub(crate) file_ids: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) plugin_keys: Vec<NullableKeyFilter<String>>,
    #[serde(default)]
    pub(crate) include_tombstones: bool,
}

/// Requested property set for a tracked-state scan.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateProjection {
    #[serde(default)]
    pub(crate) columns: Vec<String>,
}

/// Scan request for the rebuildable tracked-state projection.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub(crate) struct TrackedStateScanRequest {
    #[serde(default)]
    pub(crate) filter: TrackedStateFilter,
    #[serde(default)]
    pub(crate) projection: TrackedStateProjection,
    #[serde(default)]
    pub(crate) limit: Option<usize>,
}

/// Point lookup request for one tracked-state row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrackedStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) entity_id: String,
    pub(crate) file_id: NullableKeyFilter<String>,
}
