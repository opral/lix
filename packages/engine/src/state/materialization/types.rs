use std::collections::BTreeSet;

use crate::CanonicalJson;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LiveStateRebuildScope {
    Full,
    Versions(BTreeSet<String>),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LiveStateRebuildDebugMode {
    Off,
    Summary,
    Full,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveStateRebuildRequest {
    pub scope: LiveStateRebuildScope,
    pub debug: LiveStateRebuildDebugMode,
    pub debug_row_limit: usize,
}

impl Default for LiveStateRebuildRequest {
    fn default() -> Self {
        Self {
            scope: LiveStateRebuildScope::Full,
            debug: LiveStateRebuildDebugMode::Summary,
            debug_row_limit: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LiveStateWriteOp {
    Upsert,
    Tombstone,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveStateWrite {
    pub schema_key: String,
    pub entity_id: String,
    pub file_id: String,
    pub version_id: String,
    pub global: bool,
    pub op: LiveStateWriteOp,
    pub snapshot_content: Option<CanonicalJson>,
    pub metadata: Option<CanonicalJson>,
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
pub struct LiveStateRebuildWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct VersionHeadDebugRow {
    pub version_id: String,
    pub commit_id: String,
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
pub struct ScopeWinnerDebugRow {
    pub version_id: String,
    pub entity_id: String,
    pub schema_key: String,
    pub file_id: String,
    pub global: bool,
    pub change_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveStateRebuildDebugTrace {
    pub heads_by_version: Vec<VersionHeadDebugRow>,
    pub traversed_commits: Vec<TraversedCommitDebugRow>,
    pub traversed_edges: Vec<TraversedEdgeDebugRow>,
    pub version_ancestry: Vec<VersionAncestryDebugRow>,
    pub latest_visible_winners: Vec<LatestVisibleWinnerDebugRow>,
    pub scope_winners: Vec<ScopeWinnerDebugRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveStateRebuildPlan {
    pub run_id: String,
    pub scope: LiveStateRebuildScope,
    pub stats: Vec<StageStat>,
    pub writes: Vec<LiveStateWrite>,
    pub warnings: Vec<LiveStateRebuildWarning>,
    pub debug: Option<LiveStateRebuildDebugTrace>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveStateApplyReport {
    pub run_id: String,
    pub rows_written: usize,
    pub rows_deleted: usize,
    pub tables_touched: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LiveStateRebuildReport {
    pub plan: LiveStateRebuildPlan,
    pub apply: LiveStateApplyReport,
}
