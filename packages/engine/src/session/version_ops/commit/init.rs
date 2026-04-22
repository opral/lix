use crate::session::version_ops::VersionOpsBackendRef;
use crate::LixError;

pub(crate) async fn init(backend: VersionOpsBackendRef<'_>) -> Result<(), LixError> {
    crate::transaction::init_commit_idempotency_storage(backend, super::COMMIT_IDEMPOTENCY_TABLE)
        .await
}
