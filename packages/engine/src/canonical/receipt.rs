use crate::live_state::ReplayCursor;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpdatedVersionRef {
    pub version_id: crate::VersionId,
    pub commit_id: String,
    pub created_at: String,
}

/// Durable output of a canonical commit.
///
/// `commit_id`, `updated_version_refs`, and `affected_versions` describe the
/// semantic outcome. `replay_cursor` is included so local derived projections
/// can catch up without becoming canonical truth.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CanonicalCommitReceipt {
    pub commit_id: String,
    pub replay_cursor: ReplayCursor,
    pub updated_version_refs: Vec<UpdatedVersionRef>,
    pub affected_versions: Vec<String>,
}
