#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpdatedVersionRef {
    pub version_id: crate::VersionId,
    pub commit_id: String,
    pub created_at: String,
}

/// Durable output of a canonical commit.
///
/// This receipt carries only canonical semantic facts. Local replay cursors and
/// projection catch-up boundaries belong to higher runtime owners.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CanonicalCommitReceipt {
    pub commit_id: String,
    pub updated_version_refs: Vec<UpdatedVersionRef>,
    pub affected_versions: Vec<String>,
}
