use std::collections::BTreeMap;

use crate::canonical::refs::VersionRefRow;
use crate::LixError;

/// Semantic frontier for committed state selected by canonical refs.
///
/// This frontier is replica-independent committed meaning. It records the head
/// commit currently selected for each version id.
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

    pub(crate) fn from_version_ref_rows(rows: Vec<VersionRefRow>) -> Self {
        Self {
            version_heads: rows
                .into_iter()
                .map(|row| (row.version_id, row.commit_id))
                .collect(),
        }
    }

    pub(crate) fn to_json_string(&self) -> String {
        serde_json::to_string(self).expect("committed frontier serialization should succeed")
    }

    pub(crate) fn from_json_str(value: &str) -> Result<Self, LixError> {
        serde_json::from_str(value).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("invalid committed frontier json: {error}"),
            )
        })
    }
}
