#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct UndoOptions {
    /// Target `lix_version.id`. If omitted, uses the active `version_id`.
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct RedoOptions {
    /// Target `lix_version.id`. If omitted, uses the active `version_id`.
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UndoResult {
    pub version_id: String,
    pub target_commit_id: String,
    pub inverse_commit_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RedoResult {
    pub version_id: String,
    pub target_commit_id: String,
    pub replay_commit_id: String,
}
