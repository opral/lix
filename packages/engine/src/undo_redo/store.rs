use crate::sql::common::text::escape_sql_string;
use crate::{LixBackendTransaction, LixError, Value};

use super::types::{UndoRedoOperationKind, UndoRedoOperationRecord};

const UNDO_REDO_OPERATION_TABLE: &str = "lix_internal_undo_redo_operation";

pub(crate) async fn insert_undo_redo_operation_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    record: &UndoRedoOperationRecord,
) -> Result<(), LixError> {
    let sql = format!(
        "INSERT INTO {table} (\
         version_id, operation_commit_id, operation_kind, target_commit_id, created_at\
         ) VALUES ('{version_id}', '{operation_commit_id}', '{operation_kind}', '{target_commit_id}', '{created_at}')",
        table = UNDO_REDO_OPERATION_TABLE,
        version_id = escape_sql_string(&record.version_id),
        operation_commit_id = escape_sql_string(&record.operation_commit_id),
        operation_kind = escape_sql_string(record.operation_kind.as_str()),
        target_commit_id = escape_sql_string(&record.target_commit_id),
        created_at = escape_sql_string(&record.created_at),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

pub(crate) async fn load_undo_redo_operations_for_version_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
) -> Result<Vec<UndoRedoOperationRecord>, LixError> {
    let sql = format!(
        "SELECT version_id, operation_commit_id, operation_kind, target_commit_id, created_at \
         FROM {table} \
         WHERE version_id = $1 \
         ORDER BY created_at ASC, operation_commit_id ASC",
        table = UNDO_REDO_OPERATION_TABLE,
    );
    let result = transaction
        .execute(&sql, &[Value::Text(version_id.to_string())])
        .await?;

    result
        .rows
        .iter()
        .map(|row| parse_undo_redo_operation_record(row))
        .collect()
}

fn parse_undo_redo_operation_record(row: &[Value]) -> Result<UndoRedoOperationRecord, LixError> {
    let version_id = required_text(row, 0, "version_id")?;
    let operation_commit_id = required_text(row, 1, "operation_commit_id")?;
    let operation_kind_raw = required_text(row, 2, "operation_kind")?;
    let target_commit_id = required_text(row, 3, "target_commit_id")?;
    let created_at = required_text(row, 4, "created_at")?;
    let operation_kind = UndoRedoOperationKind::parse(&operation_kind_raw).ok_or_else(|| {
        LixError::unknown(format!(
            "unknown undo/redo operation kind '{}'",
            operation_kind_raw
        ))
    })?;

    Ok(UndoRedoOperationRecord {
        version_id,
        operation_commit_id,
        operation_kind,
        target_commit_id,
        created_at,
    })
}

fn required_text(row: &[Value], index: usize, field: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(LixError::unknown(format!("{field} is empty"))),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(LixError::unknown(format!(
            "expected text-like value for {field}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!("missing {field}"))),
    }
}
