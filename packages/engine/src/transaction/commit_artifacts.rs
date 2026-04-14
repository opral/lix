use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::common::LixError;
use crate::contracts::ReplayCursor;

/// Semantic frontier for committed state selected by replica-local version
/// heads.
///
/// The commit DAG remains canonical, but this mapping records which committed
/// head each local engine instance currently chooses for each version id.
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct CommittedVersionFrontier {
    pub version_heads: BTreeMap<String, String>,
}

impl CommittedVersionFrontier {
    pub fn is_empty(&self) -> bool {
        self.version_heads.is_empty()
    }

    pub fn describe(&self) -> String {
        if self.version_heads.is_empty() {
            return "(empty)".to_string();
        }

        self.version_heads
            .iter()
            .map(|(version_id, commit_id)| format!("{version_id}={commit_id}"))
            .collect::<Vec<_>>()
            .join(", ")
    }

    pub fn to_json_string(&self) -> String {
        serde_json::to_string(self).expect("committed frontier serialization should succeed")
    }

    pub fn from_json_str(value: &str) -> Result<Self, LixError> {
        serde_json::from_str(value).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("invalid committed frontier json: {error}"),
            )
        })
    }
}

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
