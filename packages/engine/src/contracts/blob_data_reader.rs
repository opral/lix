use async_trait::async_trait;

use crate::common::LixError;

#[async_trait(?Send)]
pub trait BlobDataReader {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError>;
}
