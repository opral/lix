pub(crate) mod cache;
mod create;
pub(crate) mod storage;

pub(crate) async fn init(backend: &dyn crate::LixBackend) -> Result<(), crate::LixError> {
    cache::init(backend).await
}

pub(crate) use create::create_checkpoint_in_session;
pub use create::CreateCheckpointResult;
pub(crate) use storage::{
    clear_last_checkpoint_rows_in_transaction, insert_last_checkpoint_for_version_in_transaction,
};
