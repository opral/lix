use async_trait::async_trait;

use crate::binary_cas::{
    BlobBytesBatch, BlobExistsBatch, BlobHash, BlobMetadataBatch, BlobWrite, BlobWriteReceipt,
};
use crate::storage::{KvWriteBatch, StorageReader, StorageWriter};
use crate::LixError;
use std::collections::HashSet;

#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError>;
}

/// Long-lived Binary CAS context factory.
///
/// The context does not own storage. Callers explicitly provide a KV store via
/// `reader(...)` or `writer(...)`, keeping backend and transaction ownership at
/// the execution layer.
pub(crate) struct BinaryCasContext;

impl BinaryCasContext {
    pub(crate) fn new() -> Self {
        Self
    }

    /// Creates a Binary CAS reader over any storage reader.
    ///
    /// The reader can be a read transaction or the active write transaction
    /// when reads must participate in transaction-local visibility.
    pub(crate) fn reader<S>(&self, store: S) -> BinaryCasStoreReader<S>
    where
        S: StorageReader,
    {
        BinaryCasStoreReader { store }
    }

    pub(crate) fn writer(&self) -> BinaryCasWriter {
        BinaryCasWriter::new()
    }
}

#[async_trait]
impl<S> BlobDataReader for BinaryCasStoreReader<S>
where
    S: StorageReader + Clone + Send + Sync,
{
    async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
        let mut reader = BinaryCasStoreReader {
            store: self.store.clone(),
        };
        BinaryCasStoreReader::load_bytes_many(&mut reader, hashes).await
    }
}

/// Binary CAS reader over a caller-supplied KV store.
pub(crate) struct BinaryCasStoreReader<S> {
    store: S,
}

impl<S> BinaryCasStoreReader<S>
where
    S: StorageReader,
{
    #[allow(dead_code)]
    pub(crate) async fn exists_many(
        &mut self,
        hashes: &[BlobHash],
    ) -> Result<BlobExistsBatch, LixError> {
        crate::binary_cas::kv::exists_many(&mut self.store, hashes).await
    }

    #[allow(dead_code)]
    pub(crate) async fn load_metadata_many(
        &mut self,
        hashes: &[BlobHash],
    ) -> Result<BlobMetadataBatch, LixError> {
        crate::binary_cas::kv::load_metadata_many(&mut self.store, hashes).await
    }

    pub(crate) async fn load_bytes_many(
        &mut self,
        hashes: &[BlobHash],
    ) -> Result<BlobBytesBatch, LixError> {
        crate::binary_cas::kv::load_bytes_many(&mut self.store, hashes).await
    }

    #[cfg(feature = "storage-benches")]
    pub(crate) async fn count_blob_manifests(&mut self) -> Result<usize, LixError> {
        crate::binary_cas::kv::count_manifests(&mut self.store).await
    }
}

/// Transaction-scoped Binary CAS writer.
///
/// This type does not begin, commit, or roll back transactions. It only writes
/// CAS data into the transaction supplied by the caller.
pub(crate) struct BinaryCasWriter {
    batch: KvWriteBatch,
    blob_hashes: HashSet<[u8; 32]>,
    chunk_keys: HashSet<Vec<u8>>,
}

impl BinaryCasWriter {
    fn new() -> Self {
        Self {
            batch: KvWriteBatch::new(),
            blob_hashes: HashSet::new(),
            chunk_keys: HashSet::new(),
        }
    }

    pub(crate) fn stage_bytes(&mut self, bytes: &[u8]) -> Result<BlobWriteReceipt, LixError> {
        crate::binary_cas::kv::stage_blob_write(
            &mut self.batch,
            &mut self.blob_hashes,
            &mut self.chunk_keys,
            &BlobWrite { bytes },
        )
    }

    #[allow(dead_code)]
    pub(crate) fn stage_many(
        &mut self,
        writes: &[BlobWrite<'_>],
    ) -> Result<Vec<BlobWriteReceipt>, LixError> {
        writes
            .iter()
            .map(|write| {
                crate::binary_cas::kv::stage_blob_write(
                    &mut self.batch,
                    &mut self.blob_hashes,
                    &mut self.chunk_keys,
                    write,
                )
            })
            .collect()
    }

    pub(crate) async fn flush(
        self,
        store: &mut (impl StorageWriter + ?Sized),
    ) -> Result<(), LixError> {
        if !self.batch.is_empty() {
            store.write_kv_batch(self.batch).await?;
        }
        Ok(())
    }
}
