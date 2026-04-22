use crate::live_state::lifecycle;
use crate::live_state::register_schema;
use crate::live_state::storage::SqlLiveStateStore;
use crate::live_state::store::LiveStateBackendRef;
use crate::LixError;

pub async fn init(backend: LiveStateBackendRef<'_>) -> Result<(), LixError> {
    lifecycle::init(&SqlLiveStateStore::from_backend(backend)).await?;
    crate::live_state::storage::init_storage(backend).await?;
    register_schema(backend, "lix_registered_schema").await?;
    Ok(())
}
