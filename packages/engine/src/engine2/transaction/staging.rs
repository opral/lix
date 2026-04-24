use std::collections::BTreeSet;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::live_state::{ExactRowRequest, LiveRow, LiveStateScanRequest};
use crate::sql2::{SqlWriteIntent, SqlWriteOutcome, SqlWriteStager, StateWriteRow};
use crate::{LixError, NullableKeyFilter};

/// Transaction-local writes decoded by DataFusion provider hooks.
///
/// This is the engine2 seam between SQL execution and transaction ownership:
/// providers stage `StateWriteRow`s here, reads build a `StagedStateRowOverlay`
/// from the staged rows, and commit later drains the same writes.
#[derive(Default)]
pub(crate) struct TransactionStagedWrites {
    writes: Mutex<Vec<SqlWriteIntent>>,
}

impl TransactionStagedWrites {
    /// Drains staged writes for commit.
    pub(crate) fn drain(&self) -> Result<Vec<SqlWriteIntent>, LixError> {
        let mut guard = self.writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        Ok(std::mem::take(&mut *guard))
    }

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(&self) -> Result<StagedStateRowOverlay, LixError> {
        let guard = self.writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        StagedStateRowOverlay::from_write_intents(&guard)
    }
}

#[async_trait]
impl SqlWriteStager for TransactionStagedWrites {
    async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
        let count = match &write {
            SqlWriteIntent::InsertRows { rows } | SqlWriteIntent::DeleteRows { rows } => {
                rows.len() as u64
            }
            SqlWriteIntent::InsertRowsWithFileData { count, .. } => *count,
        };
        let mut guard = self.writes.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        guard.push(write);
        Ok(SqlWriteOutcome { count })
    }
}

/// Read overlay derived from staged transaction writes.
pub(crate) struct StagedStateRowOverlay {
    rows: Vec<StagedStateRow>,
}

impl StagedStateRowOverlay {
    fn from_write_intents(writes: &[SqlWriteIntent]) -> Result<Self, LixError> {
        let mut rows = Vec::new();
        for write in writes {
            match write {
                SqlWriteIntent::InsertRows { rows: write_rows } => {
                    push_write_rows(&mut rows, write_rows, false)?;
                }
                SqlWriteIntent::DeleteRows { rows: write_rows } => {
                    push_write_rows(&mut rows, write_rows, true)?;
                }
                SqlWriteIntent::InsertRowsWithFileData {
                    rows: write_rows, ..
                } => {
                    // TODO(engine2): model staged file payload visibility for
                    // same-transaction file data reads. The state row overlay
                    // is enough for the first key-value smoke path.
                    push_write_rows(&mut rows, write_rows, false)?;
                }
            }
        }
        Ok(Self { rows })
    }

    /// Returns staged rows visible for a scan request.
    pub(crate) fn scan(&self, request: &LiveStateScanRequest) -> Vec<LiveRow> {
        self.rows
            .iter()
            .filter(|row| staged_row_matches_scan(row, request))
            .filter(|row| !row.tombstone || request.filter.include_tombstones)
            .cloned()
            .map(LiveRow::from)
            .collect()
    }

    /// Returns staged identities that should suppress committed rows.
    ///
    /// Tombstones also suppress committed rows, even when the caller is not
    /// asking to see tombstone rows.
    pub(crate) fn identities_matching_scan(
        &self,
        request: &LiveStateScanRequest,
    ) -> BTreeSet<StagedStateRowIdentity> {
        self.rows
            .iter()
            .filter(|row| staged_row_identity_matches_scan(row, request))
            .map(StagedStateRowIdentity::from)
            .collect()
    }

    /// Returns a staged exact-row answer, if this transaction has one.
    pub(crate) fn load_exact(&self, request: &ExactRowRequest) -> Option<StagedExactRow> {
        self.rows
            .iter()
            .find(|row| staged_row_matches_exact(row, request))
            .map(|row| {
                if row.tombstone {
                    StagedExactRow::Tombstone
                } else {
                    StagedExactRow::Row(LiveRow::from(row.clone()))
                }
            })
    }
}

pub(crate) enum StagedExactRow {
    Row(LiveRow),
    Tombstone,
}

#[derive(Clone)]
struct StagedStateRow {
    untracked: bool,
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: Option<String>,
    version_id: String,
    plugin_key: Option<String>,
    change_id: Option<String>,
    commit_id: Option<String>,
    global: bool,
    created_at: Option<String>,
    updated_at: Option<String>,
    snapshot_content: Option<String>,
    metadata: Option<String>,
    tombstone: bool,
}

pub(crate) type StagedStateRowIdentity = (
    bool,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    String,
);

impl From<&StagedStateRow> for StagedStateRowIdentity {
    fn from(row: &StagedStateRow) -> Self {
        (
            row.untracked,
            row.schema_key.clone(),
            row.entity_id.clone(),
            row.file_id.clone(),
            row.version_id.clone(),
            row.plugin_key.clone(),
            row.schema_version.clone(),
        )
    }
}

impl From<StagedStateRow> for LiveRow {
    fn from(row: StagedStateRow) -> Self {
        LiveRow {
            entity_id: row.entity_id,
            file_id: row.file_id,
            schema_key: row.schema_key,
            schema_version: row.schema_version,
            version_id: row.version_id,
            plugin_key: row.plugin_key,
            metadata: row.metadata,
            change_id: row.change_id,
            commit_id: row.commit_id,
            global: row.global,
            untracked: row.untracked,
            created_at: row.created_at,
            updated_at: row.updated_at,
            snapshot_content: row.snapshot_content,
        }
    }
}

fn push_write_rows(
    staged_rows: &mut Vec<StagedStateRow>,
    write_rows: &[StateWriteRow],
    tombstone: bool,
) -> Result<(), LixError> {
    for row in write_rows {
        staged_rows.push(staged_row_from_write_row(row, tombstone)?);
    }
    Ok(())
}

fn staged_row_from_write_row(
    row: &StateWriteRow,
    tombstone: bool,
) -> Result<StagedStateRow, LixError> {
    let schema_version = row.schema_version.clone().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 staged write requires schema_version for staging overlay",
        )
    })?;
    Ok(StagedStateRow {
        untracked: row.untracked,
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version,
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        plugin_key: row.plugin_key.clone(),
        change_id: row.change_id.clone(),
        commit_id: row.commit_id.clone(),
        global: row.global,
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        snapshot_content: if tombstone {
            None
        } else {
            row.snapshot_content.clone()
        },
        metadata: row.metadata.clone(),
        tombstone,
    })
}

fn staged_row_matches_scan(row: &StagedStateRow, request: &LiveStateScanRequest) -> bool {
    staged_row_identity_matches_scan(row, request)
}

fn staged_row_identity_matches_scan(row: &StagedStateRow, request: &LiveStateScanRequest) -> bool {
    if !request.filter.schema_keys.is_empty()
        && !request.filter.schema_keys.contains(&row.schema_key)
    {
        return false;
    }
    if !request.filter.entity_ids.is_empty() && !request.filter.entity_ids.contains(&row.entity_id)
    {
        return false;
    }
    if !request.filter.version_ids.is_empty()
        && !request.filter.version_ids.contains(&row.version_id)
    {
        return false;
    }
    nullable_key_matches_filters(&row.file_id, &request.filter.file_ids)
        && nullable_key_matches_filters(&row.plugin_key, &request.filter.plugin_keys)
}

fn staged_row_matches_exact(row: &StagedStateRow, request: &ExactRowRequest) -> bool {
    row.schema_key == request.schema_key
        && row.entity_id == request.entity_id
        && row.version_id == request.version_id
        && nullable_key_matches_filter(&row.file_id, &request.file_id)
}

fn nullable_key_matches_filters(
    value: &Option<String>,
    filters: &[NullableKeyFilter<String>],
) -> bool {
    filters.is_empty()
        || filters
            .iter()
            .any(|filter| nullable_key_matches_filter(value, filter))
}

fn nullable_key_matches_filter(value: &Option<String>, filter: &NullableKeyFilter<String>) -> bool {
    match filter {
        NullableKeyFilter::Any => true,
        NullableKeyFilter::Null => value.is_none(),
        NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
    }
}
