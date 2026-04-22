use crate::session::version_ops::VersionOpsTransactionRef;
use crate::{LixError, Value};

pub(crate) async fn load_undo_redo_operation_rows_in_transaction(
    transaction: VersionOpsTransactionRef<'_>,
    version_id: &str,
) -> Result<Vec<Vec<Value>>, LixError> {
    crate::session::version_ops::undo_redo::storage::load_undo_redo_operation_rows_in_transaction(
        transaction,
        version_id,
    )
    .await
}

pub(crate) async fn insert_undo_redo_operation_in_transaction(
    transaction: VersionOpsTransactionRef<'_>,
    version_id: &str,
    operation_commit_id: &str,
    operation_kind: &str,
    target_commit_id: &str,
    created_at: &str,
) -> Result<(), LixError> {
    crate::session::version_ops::undo_redo::storage::insert_undo_redo_operation_in_transaction(
        transaction,
        version_id,
        operation_commit_id,
        operation_kind,
        target_commit_id,
        created_at,
    )
    .await
}
