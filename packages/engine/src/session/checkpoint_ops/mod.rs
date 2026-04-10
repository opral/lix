pub(crate) mod cache;
mod create;

pub(crate) async fn init(backend: &dyn crate::LixBackend) -> Result<(), crate::LixError> {
    cache::init(backend).await
}

pub(crate) use create::create_checkpoint_in_session;
pub use create::CreateCheckpointResult;
