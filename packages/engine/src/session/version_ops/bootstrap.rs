use crate::{LixBackend, LixError};

pub(crate) async fn init(backend: &dyn LixBackend) -> Result<(), LixError> {
    super::commit::init(backend).await?;
    super::undo_redo::init(backend).await
}
