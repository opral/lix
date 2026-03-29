use crate::live_state::ReplayCursor;
use crate::VersionId;

use super::types::ChangeRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatedVersionRef {
    pub version_id: VersionId,
    pub commit_id: String,
    pub created_at: String,
}

/// Durable output of a canonical commit.
///
/// `commit_id`, `updated_version_refs`, and `affected_versions` describe the
/// semantic outcome. `replay_cursor` is included so local derived projections
/// can catch up without becoming canonical truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalCommitReceipt {
    pub commit_id: String,
    pub replay_cursor: ReplayCursor,
    pub updated_version_refs: Vec<UpdatedVersionRef>,
    pub affected_versions: Vec<String>,
}

pub(crate) fn latest_replay_cursor_from_change_rows(changes: &[ChangeRow]) -> Option<ReplayCursor> {
    changes
        .iter()
        .map(|change| ReplayCursor::new(change.id.clone(), change.created_at.clone()))
        .max()
}
