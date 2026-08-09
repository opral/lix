#[cfg(test)]
use async_trait::async_trait;

#[cfg(test)]
use crate::LixError;
#[cfg(test)]
use crate::binary_cas::BlobId;

#[cfg(test)]
#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_bytes_many(
        &self,
        hashes: &[BlobId],
    ) -> Result<crate::binary_cas::BlobBytesBatch, LixError>;
}
