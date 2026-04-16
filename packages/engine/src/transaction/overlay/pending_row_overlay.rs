use std::collections::{BTreeMap, BTreeSet};

use crate::live_state::{
    values_from_snapshot_content, LiveWriteOperation, LiveWriteRow, RowIdentity, TrackedRow,
    TrackedTombstoneMarker, UntrackedRow,
};
use crate::transaction::TransactionDelta;
use crate::LixError;

/// Session-local pending-row overlay.
///
/// This structure is disposable bookkeeping for uncommitted transaction state.
/// Dropping or rebuilding it must not change committed meaning or canonical
/// ref state.
#[derive(Debug, Clone, Default)]
pub(crate) struct PendingRowOverlay {
    tracked_rows: BTreeMap<RowIdentity, TrackedRow>,
    tracked_tombstones: BTreeMap<RowIdentity, TrackedTombstoneMarker>,
    untracked_rows: BTreeMap<RowIdentity, UntrackedRow>,
    untracked_deletes: BTreeSet<RowIdentity>,
}

impl PendingRowOverlay {
    pub(crate) fn from_delta(delta: &TransactionDelta) -> Result<Self, LixError> {
        let mut participants = Self::default();
        for row in &delta.writes {
            participants.apply_write(row)?;
        }
        Ok(participants)
    }

    pub(crate) fn merge(&mut self, incoming: PendingRowOverlay) {
        for (identity, row) in incoming.tracked_rows {
            self.tracked_tombstones.remove(&identity);
            self.tracked_rows.insert(identity, row);
        }
        for (identity, tombstone) in incoming.tracked_tombstones {
            self.tracked_rows.remove(&identity);
            self.tracked_tombstones.insert(identity, tombstone);
        }
        for (identity, row) in incoming.untracked_rows {
            self.untracked_deletes.remove(&identity);
            self.untracked_rows.insert(identity, row);
        }
        for identity in incoming.untracked_deletes {
            self.untracked_rows.remove(&identity);
            self.untracked_deletes.insert(identity);
        }
    }

    pub(crate) fn tracked_rows(&self) -> &BTreeMap<RowIdentity, TrackedRow> {
        &self.tracked_rows
    }

    pub(crate) fn tracked_tombstones(&self) -> &BTreeMap<RowIdentity, TrackedTombstoneMarker> {
        &self.tracked_tombstones
    }

    pub(crate) fn untracked_rows(&self) -> &BTreeMap<RowIdentity, UntrackedRow> {
        &self.untracked_rows
    }

    pub(crate) fn untracked_deletes(&self) -> &BTreeSet<RowIdentity> {
        &self.untracked_deletes
    }

    pub(crate) fn tracked_identities(&self) -> BTreeSet<RowIdentity> {
        self.tracked_rows
            .keys()
            .chain(self.tracked_tombstones.keys())
            .cloned()
            .collect()
    }

    pub(crate) fn untracked_identities(&self) -> BTreeSet<RowIdentity> {
        self.untracked_rows
            .keys()
            .chain(self.untracked_deletes.iter())
            .cloned()
            .collect()
    }

    pub(crate) fn has_tombstones(&self) -> bool {
        !self.tracked_tombstones.is_empty()
    }

    fn apply_write(&mut self, row: &LiveWriteRow) -> Result<(), LixError> {
        let identity = RowIdentity::from_live_write(row);
        if row.untracked {
            match row.operation {
                LiveWriteOperation::Upsert => {
                    self.untracked_deletes.remove(&identity);
                    self.untracked_rows
                        .insert(identity, untracked_row_from_write(row)?);
                }
                LiveWriteOperation::Delete => {
                    self.untracked_rows.remove(&identity);
                    self.untracked_deletes.insert(identity);
                }
                LiveWriteOperation::Tombstone => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "pending row overlay cannot apply untracked tombstone for '{}' '{}'",
                            row.schema_key, row.entity_id
                        ),
                    ));
                }
            }
        } else {
            match row.operation {
                LiveWriteOperation::Upsert => {
                    self.tracked_tombstones.remove(&identity);
                    self.tracked_rows
                        .insert(identity, tracked_row_from_write(row)?);
                }
                LiveWriteOperation::Tombstone => {
                    self.tracked_rows.remove(&identity);
                    self.tracked_tombstones
                        .insert(identity, tracked_tombstone_from_write(row));
                }
                LiveWriteOperation::Delete => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "pending row overlay cannot apply tracked delete for '{}' '{}'",
                            row.schema_key, row.entity_id
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

fn tracked_row_from_write(row: &LiveWriteRow) -> Result<TrackedRow, LixError> {
    Ok(TrackedRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        global: row.global,
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
        change_id: Some(row.change_id.clone()),
        writer_key: row.writer_key.clone(),
        created_at: row
            .created_at
            .clone()
            .unwrap_or_else(|| row.updated_at.clone()),
        updated_at: row.updated_at.clone(),
        values: values_from_snapshot_content(row.snapshot_content.as_deref())?,
    })
}

fn tracked_tombstone_from_write(row: &LiveWriteRow) -> TrackedTombstoneMarker {
    TrackedTombstoneMarker {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        global: row.global,
        schema_version: Some(row.schema_version.clone()),
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
        writer_key: row.writer_key.clone(),
        created_at: row.created_at.clone(),
        updated_at: Some(row.updated_at.clone()),
        change_id: Some(row.change_id.clone()),
    }
}

fn untracked_row_from_write(row: &LiveWriteRow) -> Result<UntrackedRow, LixError> {
    Ok(UntrackedRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        global: row.global,
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
        change_id: row.change_id.clone(),
        writer_key: row.writer_key.clone(),
        created_at: row
            .created_at
            .clone()
            .unwrap_or_else(|| row.updated_at.clone()),
        updated_at: row.updated_at.clone(),
        values: values_from_snapshot_content(row.snapshot_content.as_deref())?,
    })
}
