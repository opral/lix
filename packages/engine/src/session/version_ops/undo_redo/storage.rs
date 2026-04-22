use crate::{LixBackend, LixBackendTransaction, LixError, Value};

pub(crate) async fn init_undo_redo_operation_storage(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<(), LixError> {
    let statements = [
        format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             version_id TEXT NOT NULL,\
             operation_commit_id TEXT PRIMARY KEY,\
             operation_kind TEXT NOT NULL,\
             target_commit_id TEXT NOT NULL,\
             created_at TEXT NOT NULL\
             )",
            table_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_undo_redo_operation_version_created \
             ON {} (version_id, created_at)",
            table_name
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS idx_lix_internal_undo_redo_operation_target \
             ON {} (target_commit_id)",
            table_name
        ),
    ];
    let statement_refs = statements.iter().map(String::as_str).collect::<Vec<_>>();
    crate::backend::execute_ddl_batch(backend, "version.undo_redo", &statement_refs).await
}

pub(crate) async fn load_undo_redo_operation_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
) -> Result<Vec<Vec<Value>>, LixError> {
    let result = transaction
        .execute(
            "SELECT version_id, operation_commit_id, operation_kind, target_commit_id, created_at \
             FROM lix_internal_undo_redo_operation \
             WHERE version_id = $1 \
             ORDER BY created_at ASC, operation_commit_id ASC",
            &[Value::Text(version_id.to_string())],
        )
        .await?;
    Ok(result.rows)
}

pub(crate) async fn insert_undo_redo_operation_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
    operation_commit_id: &str,
    operation_kind: &str,
    target_commit_id: &str,
    created_at: &str,
) -> Result<(), LixError> {
    transaction
        .execute(
            "INSERT INTO lix_internal_undo_redo_operation (\
             version_id, operation_commit_id, operation_kind, target_commit_id, created_at\
             ) VALUES ($1, $2, $3, $4, $5)",
            &[
                Value::Text(version_id.to_string()),
                Value::Text(operation_commit_id.to_string()),
                Value::Text(operation_kind.to_string()),
                Value::Text(target_commit_id.to_string()),
                Value::Text(created_at.to_string()),
            ],
        )
        .await?;
    Ok(())
}
