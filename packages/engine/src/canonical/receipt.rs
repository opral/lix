use crate::VersionId;

use super::types::ChangeRow;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalWatermark {
    pub change_id: String,
    pub created_at: String,
}

impl CanonicalWatermark {
    pub(crate) fn is_newer_than(&self, other: &Self) -> bool {
        (self.created_at.as_str(), self.change_id.as_str())
            > (other.created_at.as_str(), other.change_id.as_str())
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
) -> Option<CanonicalWatermark> {
    changes
        .iter()
        .max_by(|left, right| {
            (left.created_at.as_str(), left.id.as_str())
                .cmp(&(right.created_at.as_str(), right.id.as_str()))
        })
        .map(|change| CanonicalWatermark {
            change_id: change.id.clone(),
            created_at: change.created_at.clone(),
        })
}
