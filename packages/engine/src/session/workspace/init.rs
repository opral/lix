use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    crate::session::workspace::storage::init_workspace_metadata_storage(backend).await
}
