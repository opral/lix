use crate::binary_cas::store::BinaryCasBackendRef;
use async_trait::async_trait;

use crate::LixError;

#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError>;
}

pub(crate) async fn blob_exists(
    backend: BinaryCasBackendRef<'_>,
    blob_hash: &str,
) -> Result<bool, LixError> {
    crate::binary_cas::storage::blob_exists(backend, blob_hash).await
}

pub(crate) async fn load_binary_blob_data_by_hash(
    backend: BinaryCasBackendRef<'_>,
    blob_hash: &str,
) -> Result<Option<Vec<u8>>, LixError> {
    crate::binary_cas::storage::load_blob_data_by_hash(backend, blob_hash).await
}
