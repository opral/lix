use crate::live_state::tracked::{
    TrackedRow, TrackedTombstoneMarker, TrackedWriteRow,
};
use crate::live_state::untracked::{UntrackedRow, UntrackedWriteRow};

use super::query::{BatchRowRequest, ExactRowRequest, ScanRequest};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct RowIdentity {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) entity_id: String,
    pub(crate) file_id: String,
}

impl RowIdentity {
    pub(crate) fn from_tracked_write(row: &TrackedWriteRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub(crate) fn from_untracked_write(row: &UntrackedWriteRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub(crate) fn from_tracked_row(row: &TrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub(crate) fn from_untracked_row(row: &UntrackedRow) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub(crate) fn from_tombstone(row: &TrackedTombstoneMarker) -> Self {
        Self {
            schema_key: row.schema_key.clone(),
            version_id: row.version_id.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    pub(crate) fn matches_exact(&self, request: &ExactRowRequest) -> bool {
        self.schema_key == request.schema_key
            && self.version_id == request.version_id
            && self.entity_id == request.entity_id
            && request
                .file_id
                .as_ref()
                .is_none_or(|file_id| self.file_id == *file_id)
    }

    pub(crate) fn matches_batch(&self, request: &BatchRowRequest) -> bool {
        self.schema_key == request.schema_key
            && self.version_id == request.version_id
            && request.entity_ids.contains(&self.entity_id)
            && request
                .file_id
                .as_ref()
                .is_none_or(|file_id| self.file_id == *file_id)
    }

    pub(crate) fn matches_scan_partition(&self, request: &ScanRequest) -> bool {
        self.schema_key == request.schema_key && self.version_id == request.version_id
    }
}
