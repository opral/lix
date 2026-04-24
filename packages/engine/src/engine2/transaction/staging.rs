use std::collections::BTreeSet;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::functions::{LixFunctionProvider, SystemFunctionProvider};
use crate::live_state::{ExactRowRequest, LiveRow, LiveStateScanRequest};
use crate::sql2::{SqlWriteIntent, SqlWriteOutcome, SqlWriteStager, StateRow};
use crate::{LixError, NullableKeyFilter};

/// Transaction-local writes decoded by DataFusion provider hooks.
///
/// This is the engine2 seam between SQL execution and transaction ownership:
/// providers stage SQL write intents here, the transaction normalizes them into
/// stable `StateRow`s, reads build a `StagedStateRowOverlay` from those rows,
/// and commit later drains the same rows.
#[derive(Default)]
pub(crate) struct TransactionStagedWrites {
    rows: Mutex<Vec<StateRow>>,
}

impl TransactionStagedWrites {
    /// Drains staged writes for commit.
    pub(crate) fn drain(&self) -> Result<Vec<StateRow>, LixError> {
        let mut guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        Ok(std::mem::take(&mut *guard))
    }

    /// Builds the transaction-local read overlay from currently staged writes.
    pub(crate) fn staging_overlay(&self) -> Result<StagedStateRowOverlay, LixError> {
        let guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        Ok(StagedStateRowOverlay::new(guard.clone()))
    }
}

#[async_trait]
impl SqlWriteStager for TransactionStagedWrites {
    async fn stage_write(&self, write: SqlWriteIntent) -> Result<SqlWriteOutcome, LixError> {
        let mut functions = SystemFunctionProvider;
        let count = match &write {
            SqlWriteIntent::WriteRows { rows } => rows.len() as u64,
            SqlWriteIntent::WriteRowsWithFileData { count, .. } => *count,
        };
        let mut rows = state_rows_from_write_intent(write, &mut functions)?;
        let mut guard = self.rows.lock().map_err(|_| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "failed to acquire transaction staged writes lock",
            )
        })?;
        guard.append(&mut rows);
        Ok(SqlWriteOutcome { count })
    }
}

/// Read overlay derived from staged transaction writes.
pub(crate) struct StagedStateRowOverlay {
    rows: Vec<StateRow>,
}

impl StagedStateRowOverlay {
    fn new(rows: Vec<StateRow>) -> Self {
        Self { rows }
    }

    /// Returns staged rows visible for a scan request.
    pub(crate) fn scan(&self, request: &LiveStateScanRequest) -> Vec<LiveRow> {
        self.rows
            .iter()
            .filter(|row| staged_row_matches_scan(row, request))
            .map(|row| live_row_from_state_row_ref(row))
            .collect::<Result<Vec<_>, _>>()
            .expect("engine2 staged rows should already be normalized")
    }

    /// Converts staged rows for commit into the live_state adapter shape.
    pub(crate) fn into_live_rows(rows: Vec<StateRow>) -> Result<Vec<LiveRow>, LixError> {
        rows.into_iter().map(live_row_from_state_row).collect()
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
                if row.snapshot_content.is_none() {
                    StagedExactRow::Tombstone
                } else {
                    StagedExactRow::Row(
                        live_row_from_state_row_ref(row)
                            .expect("engine2 staged rows should already be normalized"),
                    )
                }
            })
    }
}

pub(crate) enum StagedExactRow {
    Row(LiveRow),
    Tombstone,
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

impl From<&StateRow> for StagedStateRowIdentity {
    fn from(row: &StateRow) -> Self {
        (
            row.untracked,
            row.schema_key.clone(),
            row.entity_id.clone(),
            row.file_id.clone(),
            row.version_id.clone(),
            row.plugin_key.clone(),
            row.schema_version
                .clone()
                .expect("engine2 staged rows should already be normalized"),
        )
    }
}

fn state_rows_from_write_intent(
    write: SqlWriteIntent,
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<StateRow>, LixError> {
    let mut state_rows = Vec::new();
    match write {
        SqlWriteIntent::WriteRows { rows } => {
            push_state_rows(&mut state_rows, rows, functions)?;
        }
        SqlWriteIntent::WriteRowsWithFileData {
            rows, file_data, ..
        } => {
            // TODO(engine2): persist staged file payloads alongside the state
            // rows when file writes move to the native commit path.
            let _ = file_data;
            push_state_rows(&mut state_rows, rows, functions)?;
        }
    }
    Ok(state_rows)
}

fn push_state_rows(
    state_rows: &mut Vec<StateRow>,
    rows: Vec<StateRow>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<(), LixError> {
    state_rows.reserve(rows.len());
    for row in rows {
        state_rows.push(normalize_state_row(row, functions)?);
    }
    Ok(())
}

fn normalize_state_row(
    mut row: StateRow,
    functions: &mut dyn LixFunctionProvider,
) -> Result<StateRow, LixError> {
    if row.schema_version.is_none() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 staged write requires schema_version for staging overlay",
        ));
    }
    let updated_at = row.updated_at.unwrap_or_else(|| functions.timestamp());
    row.created_at = row.created_at.or_else(|| Some(updated_at.clone()));
    row.updated_at = Some(updated_at);
    row.change_id = row.change_id.or_else(|| Some(functions.uuid_v7()));
    Ok(row)
}

pub(crate) fn live_row_from_state_row(row: StateRow) -> Result<LiveRow, LixError> {
    let schema_version = row.schema_version.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 staged write requires schema_version for staging overlay",
        )
    })?;

    Ok(LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version,
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
    })
}

fn live_row_from_state_row_ref(row: &StateRow) -> Result<LiveRow, LixError> {
    live_row_from_state_row(row.clone())
}

fn staged_row_matches_scan(row: &StateRow, request: &LiveStateScanRequest) -> bool {
    staged_row_identity_matches_scan(row, request)
        && (row.snapshot_content.is_some() || request.filter.include_tombstones)
}

fn staged_row_identity_matches_scan(row: &StateRow, request: &LiveStateScanRequest) -> bool {
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

fn staged_row_matches_exact(row: &StateRow, request: &ExactRowRequest) -> bool {
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
