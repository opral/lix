use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PendingCommitLane {
    Version(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PendingCommitState {
    pub lane: PendingCommitLane,
    pub commit_id: String,
    pub commit_change_snapshot_id: String,
    pub commit_snapshot: JsonValue,
}
