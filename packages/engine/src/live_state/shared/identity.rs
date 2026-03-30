use crate::live_state::tracked::{TrackedRow, TrackedTombstoneMarker, TrackedWriteRow};
use crate::live_state::untracked::{UntrackedRow, UntrackedWriteRow};

use super::query::{BatchRowRequest, ExactRowRequest, ScanRequest};

/// Logical live-state row key shared across tracked and untracked lanes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RowIdentity {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
}

impl RowIdentity {
    pub fn from_tracked_write(row: &TrackedWriteRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_untracked_write(row: &UntrackedWriteRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_tracked_row(row: &TrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_untracked_row(row: &UntrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn from_tombstone(row: &TrackedTombstoneMarker) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub fn matches_exact(&self, request: &ExactRowRequest) -> bool {
        self.schema_key == request.schema_key
            && self.version_id == request.version_id
            && self.entity_id == request.entity_id
            && request
                .file_id
                .as_ref()
                .is_none_or(|file_id| self.file_id == *file_id)
    }

    pub fn matches_batch(&self, request: &BatchRowRequest) -> bool {
        self.schema_key == request.schema_key
            && self.version_id == request.version_id
            && request.entity_ids.contains(&self.entity_id)
            && request
                .file_id
                .as_ref()
                .is_none_or(|file_id| self.file_id == *file_id)
    }

    pub fn matches_scan_partition(&self, request: &ScanRequest) -> bool {
        self.schema_key == request.schema_key && self.version_id == request.version_id
    }
}
