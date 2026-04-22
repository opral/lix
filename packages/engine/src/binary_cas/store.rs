#![allow(dead_code)]

use async_trait::async_trait;

use crate::LixError;

use super::BinaryBlobWrite;

pub(crate) type BinaryCasBackendRef<'a> = &'a (dyn crate::LixBackend + 'a);
pub(crate) type BinaryCasTransactionRef<'a> = &'a mut (dyn crate::LixBackendTransaction + 'a);

#[async_trait(?Send)]
impl super::read::BlobDataReader for dyn crate::LixBackend + '_ {
    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError> {
        super::read::load_binary_blob_data_by_hash(self, blob_hash).await
    }
}

/// Owner-facing read surface for binary CAS persistence.
#[async_trait(?Send)]
pub(crate) trait BinaryCasReadStore {
    async fn blob_exists(&self, blob_hash: &str) -> Result<bool, LixError>;

    async fn load_blob_data_by_hash(&self, blob_hash: &str) -> Result<Option<Vec<u8>>, LixError>;
}

/// Owner-facing write surface for binary CAS persistence.
#[async_trait(?Send)]
pub(crate) trait BinaryCasWriteStore {
    async fn persist_blob_writes(&mut self, writes: &[BinaryBlobWrite<'_>])
        -> Result<(), LixError>;

    async fn garbage_collect_unreachable(&mut self) -> Result<(), LixError>;
}
