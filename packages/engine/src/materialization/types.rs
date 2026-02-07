use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MaterializationScope {
    Full,
    Versions(BTreeSet<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MaterializationDebugMode {
    Off,
    Summary,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaterializationRequest {
    pub scope: MaterializationScope,
    pub debug: MaterializationDebugMode,
    pub debug_row_limit: usize,
}

impl Default for MaterializationRequest {
    fn default() -> Self {
        Self {
            scope: MaterializationScope::Full,
            debug: MaterializationDebugMode::Summary,
            debug_row_limit: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum MaterializationWriteOp {
    Upsert,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaterializationWrite {
    pub schema_key: String,
    pub entity_id: String,
    pub file_id: String,
    pub version_id: String,
    pub inherited_from_version_id: Option<String>,
    pub op: MaterializationWriteOp,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub schema_version: String,
    pub plugin_key: String,
    pub change_id: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StageStat {
    pub stage: String,
    pub input_rows: usize,
    pub output_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaterializationWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionPointerDebugRow {
    pub version_id: String,
    pub tip_commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraversedCommitDebugRow {
    pub version_id: String,
    pub commit_id: String,
    pub depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TraversedEdgeDebugRow {
    pub version_id: String,
    pub parent_id: String,
    pub child_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionAncestryDebugRow {
    pub version_id: String,
    pub ancestor_version_id: String,
    pub inheritance_depth: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LatestVisibleWinnerDebugRow {
    pub version_id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: String,
    pub commit_id: String,
    pub change_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InheritanceWinnerDebugRow {
    pub version_id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: String,
    pub inherited_from_version_id: Option<String>,
    pub change_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaterializationDebugTrace {
    pub tips_by_version: Vec<VersionPointerDebugRow>,
    pub traversed_commits: Vec<TraversedCommitDebugRow>,
    pub traversed_edges: Vec<TraversedEdgeDebugRow>,
    pub version_ancestry: Vec<VersionAncestryDebugRow>,
    pub latest_visible_winners: Vec<LatestVisibleWinnerDebugRow>,
    pub inheritance_winners: Vec<InheritanceWinnerDebugRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaterializationPlan {
    pub run_id: String,
    pub scope: MaterializationScope,
    pub stats: Vec<StageStat>,
    pub writes: Vec<MaterializationWrite>,
    pub warnings: Vec<MaterializationWarning>,
    pub debug: Option<MaterializationDebugTrace>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaterializationApplyReport {
    pub run_id: String,
    pub rows_written: usize,
    pub rows_deleted: usize,
    pub tables_touched: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MaterializationReport {
    pub plan: MaterializationPlan,
    pub apply: MaterializationApplyReport,
}
