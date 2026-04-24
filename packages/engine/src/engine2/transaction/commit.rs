use crate::functions::{LixFunctionProvider, SystemFunctionProvider};
use crate::live_state::LiveRow;
use crate::sql2::{SqlWriteIntent, StateWriteRow};
use crate::{LixBackendTransaction, LixError};

/// Flushes SQL-decoded staged writes directly into live_state.
///
/// This is the first engine2 commit seam: providers decode DataFusion DML into
/// `StateWriteRow`s, the transaction owns those staged rows, and commit writes
/// them to durable live_state inside the backend transaction.
///
/// TODO(engine2): replace this naive live_state flush with canonical commit
/// generation. The future path should create commit graph rows first, then let
/// live_state catch up from canonical state.
pub(crate) async fn commit_staged_writes(
    transaction: &mut dyn LixBackendTransaction,
    staged_writes: Vec<SqlWriteIntent>,
) -> Result<(), LixError> {
    let mut functions = SystemFunctionProvider;
    let live_rows = live_rows_from_staged_writes(staged_writes, &mut functions)?;
    if live_rows.is_empty() {
        return Ok(());
    }

    crate::live_state::write_live_rows(transaction, &live_rows).await
}

fn live_rows_from_staged_writes(
    staged_writes: Vec<SqlWriteIntent>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<LiveRow>, LixError> {
    let mut live_rows = Vec::new();
    for write in staged_writes {
        match write {
            SqlWriteIntent::InsertRows { rows } => {
                push_live_rows(&mut live_rows, rows, false, functions)?;
            }
            SqlWriteIntent::DeleteRows { rows } => {
                push_live_rows(&mut live_rows, rows, true, functions)?;
            }
            SqlWriteIntent::InsertRowsWithFileData {
                rows, file_data, ..
            } => {
                // TODO(engine2): persist staged file payloads alongside the
                // state rows when file writes move to the native commit path.
                let _ = file_data;
                push_live_rows(&mut live_rows, rows, false, functions)?;
            }
        }
    }
    Ok(live_rows)
}

fn push_live_rows(
    live_rows: &mut Vec<LiveRow>,
    rows: Vec<StateWriteRow>,
    tombstone: bool,
    functions: &mut dyn LixFunctionProvider,
) -> Result<(), LixError> {
    for row in rows {
        live_rows.push(live_row_from_state_write_row(row, tombstone, functions)?);
    }
    Ok(())
}

fn live_row_from_state_write_row(
    row: StateWriteRow,
    tombstone: bool,
    functions: &mut dyn LixFunctionProvider,
) -> Result<LiveRow, LixError> {
    let schema_version = row.schema_version.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "engine2 commit requires schema_version for staged state row",
        )
    })?;
    let updated_at = row.updated_at.unwrap_or_else(|| functions.timestamp());
    let created_at = row.created_at.or_else(|| Some(updated_at.clone()));
    let change_id = row.change_id.or_else(|| Some(functions.uuid_v7()));

    Ok(LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id,
        commit_id: row.commit_id,
        global: row.global,
        untracked: row.untracked,
        created_at,
        updated_at: Some(updated_at),
        snapshot_content: if tombstone {
            None
        } else {
            row.snapshot_content
        },
    })
}
