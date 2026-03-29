use crate::VersionId;

use super::types::ChangeRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalWatermark {
    pub change_ordinal: i64,
    pub change_id: String,
    pub created_at: String,
}

impl CanonicalWatermark {
    pub(crate) fn is_newer_than(&self, other: &Self) -> bool {
        self.change_ordinal > other.change_ordinal
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdatedVersionRef {
    pub version_id: VersionId,
    pub commit_id: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalCommitReceipt {
    pub commit_id: String,
    pub canonical_watermark: CanonicalWatermark,
    pub updated_version_refs: Vec<UpdatedVersionRef>,
    pub affected_versions: Vec<String>,
}

pub(crate) fn latest_canonical_watermark_from_change_rows(
    changes: &[ChangeRow],
    starting_change_ordinal: i64,
) -> Option<CanonicalWatermark> {
    changes.last().map(|change| CanonicalWatermark {
        change_ordinal: starting_change_ordinal + (changes.len() as i64) - 1,
        change_id: change.id.clone(),
        created_at: change.created_at.clone(),
    })
}
