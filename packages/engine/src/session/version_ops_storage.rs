use crate::session::version_ops::{VersionOpsBackendRef, VersionOpsTransactionRef};
use crate::{LixError, Value};

pub(crate) async fn execute_ddl_batch_with_backend(
    backend: VersionOpsBackendRef<'_>,
    batch_name: &str,
    statements: &[&str],
) -> Result<(), LixError> {
    crate::backend::execute_ddl_batch(backend, batch_name, statements).await
}

pub(crate) async fn load_undo_redo_operation_rows_in_transaction(
    transaction: VersionOpsTransactionRef<'_>,
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
    transaction: VersionOpsTransactionRef<'_>,
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
