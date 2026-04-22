use crate::session::version_ops::VersionOpsBackendRef;
use crate::LixError;

pub(crate) async fn init(backend: VersionOpsBackendRef<'_>) -> Result<(), LixError> {
    crate::session::version_ops::undo_redo::storage::init_undo_redo_operation_storage(
        backend,
        super::UNDO_REDO_OPERATION_TABLE,
    )
    .await
}
