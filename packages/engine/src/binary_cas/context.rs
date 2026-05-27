use async_trait::async_trait;

use crate::binary_cas::{BlobBytesBatch, BlobHash, BlobWrite, BlobWriteReceipt};
use crate::storage::{StorageRead, StorageWriteSet};
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
        S: StorageRead,
    {
        BinaryCasStoreReader { store }
    }

    pub(crate) fn writer<'a>(&self, writes: &'a mut StorageWriteSet) -> BinaryCasWriter<'a> {
        BinaryCasWriter::new(writes)
    }
}

#[async_trait]
impl<S> BlobDataReader for BinaryCasStoreReader<S>
where
    S: StorageRead + Clone + Send + Sync,
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
    S: StorageRead,
{
    pub(crate) async fn load_bytes_many(
        &mut self,
        hashes: &[BlobHash],
    ) -> Result<BlobBytesBatch, LixError> {
        crate::binary_cas::kv::load_bytes_many(&self.store, hashes).await
    }
}

/// Transaction-scoped Binary CAS writer.
///
/// This type does not begin, commit, or roll back transactions. It only writes
/// CAS data into the transaction supplied by the caller.
pub(crate) struct BinaryCasWriter<'a> {
    writes: &'a mut StorageWriteSet,
    blob_hashes: HashSet<[u8; 32]>,
    chunk_keys: HashSet<Vec<u8>>,
}

impl<'a> BinaryCasWriter<'a> {
    fn new(writes: &'a mut StorageWriteSet) -> Self {
        Self {
            writes,
            blob_hashes: HashSet::new(),
            chunk_keys: HashSet::new(),
        }
    }

    pub(crate) fn stage_bytes(&mut self, bytes: &[u8]) -> Result<BlobWriteReceipt, LixError> {
        crate::binary_cas::kv::stage_blob_write(
            self.writes,
            &mut self.blob_hashes,
            &mut self.chunk_keys,
            &BlobWrite { bytes },
        )
    }
}
