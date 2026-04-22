use crate::live_state::store::LiveStateBackendRef;
use crate::LixError;

pub(crate) async fn load_file_payload_cache_data(
    backend: LiveStateBackendRef<'_>,
    file_id: &str,
    version_id: &str,
) -> Result<Vec<u8>, LixError> {
    crate::live_state::storage::load_file_payload_cache_data(backend, file_id, version_id).await
}

pub(crate) async fn upsert_file_payload_cache_data(
    backend: LiveStateBackendRef<'_>,
    file_id: &str,
    version_id: &str,
    data: &[u8],
) -> Result<(), LixError> {
    crate::live_state::storage::upsert_file_payload_cache_data(backend, file_id, version_id, data)
        .await
}

pub(crate) async fn delete_file_payload_cache_data(
    backend: LiveStateBackendRef<'_>,
    file_id: &str,
    version_id: &str,
) -> Result<(), LixError> {
    crate::live_state::storage::delete_file_payload_cache_data(backend, file_id, version_id).await
}
