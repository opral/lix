use crate::session::version_ops::VersionOpsBackendRef;
use crate::LixError;

pub(crate) async fn init(backend: VersionOpsBackendRef<'_>) -> Result<(), LixError> {
    super::commit::init(backend).await?;
    super::undo_redo::init(backend).await
}
