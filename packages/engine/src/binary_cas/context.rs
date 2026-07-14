use async_trait::async_trait;

use crate::LixError;
use crate::binary_cas::BinaryCasChunking;
use crate::binary_cas::{BlobBytesBatch, BlobHash, BlobPayload, BlobWriteReceipt};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use std::collections::HashSet;

#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError>;
}

/// Long-lived Binary CAS context factory.
///
/// The context does not own storage. Callers explicitly provide a KV store via
/// `reader(...)` or `writer(...)`, keeping storage and transaction ownership at
/// the execution layer.
pub(crate) struct BinaryCasContext {
    chunking: BinaryCasChunking,
}

impl BinaryCasContext {
    pub(crate) fn new() -> Self {
        Self {
            chunking: BinaryCasChunking::default(),
        }
    }

    /// Creates a Binary CAS reader over any storage reader.
    ///
    /// The reader can be a read transaction or the active write transaction
    /// when reads must participate in transaction-local visibility.
    #[expect(clippy::unused_self)]
    pub(crate) fn reader<S>(&self, store: S) -> BinaryCasStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        BinaryCasStoreReader { store }
    }

    #[allow(dead_code)]
    pub(crate) fn writer<'a>(&self, writes: &'a mut StorageWriteSet) -> BinaryCasWriter<'a> {
        BinaryCasWriter::new(writes, self.chunking)
    }

    pub(crate) fn writer_skipping_existing_chunks<'a, S>(
        &self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> ExistingChunkAwareBinaryCasWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        ExistingChunkAwareBinaryCasWriter::new(store, writes, self.chunking)
    }
}

#[async_trait]
impl<S> BlobDataReader for BinaryCasStoreReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError> {
        let mut reader = Self {
            store: self.store.clone(),
        };
        Self::load_bytes_many(&mut reader, hashes).await
    }
}

/// Binary CAS reader over a caller-supplied KV store.
pub(crate) struct BinaryCasStoreReader<S> {
    store: S,
}

impl<S> BinaryCasStoreReader<S>
where
    S: StorageAdapterRead,
{
    #[expect(clippy::needless_pass_by_ref_mut)]
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
#[allow(dead_code)]
pub(crate) struct BinaryCasWriter<'a> {
    writes: &'a mut StorageWriteSet,
    chunking: BinaryCasChunking,
    blob_hashes: HashSet<[u8; 32]>,
    chunk_keys: HashSet<Vec<u8>>,
}

/// Binary CAS writer that avoids re-putting chunk payload rows already present
/// in the backing store.
pub(crate) struct ExistingChunkAwareBinaryCasWriter<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    store: &'a S,
    writes: &'a mut StorageWriteSet,
    chunking: BinaryCasChunking,
    blob_hashes: HashSet<[u8; 32]>,
    chunk_keys: HashSet<Vec<u8>>,
}

#[allow(dead_code)]
impl<'a> BinaryCasWriter<'a> {
    fn new(writes: &'a mut StorageWriteSet, chunking: BinaryCasChunking) -> Self {
        Self {
            writes,
            chunking,
            blob_hashes: HashSet::new(),
            chunk_keys: HashSet::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn stage_bytes(&mut self, bytes: &[u8]) -> Result<BlobWriteReceipt, LixError> {
        crate::binary_cas::kv::stage_blob_write(
            self.chunking,
            self.writes,
            &mut self.blob_hashes,
            &mut self.chunk_keys,
            bytes,
            None,
        )
    }

    pub(crate) fn stage_payload(
        &mut self,
        payload: &BlobPayload,
    ) -> Result<BlobWriteReceipt, LixError> {
        crate::binary_cas::kv::stage_blob_write(
            self.chunking,
            self.writes,
            &mut self.blob_hashes,
            &mut self.chunk_keys,
            payload.bytes(),
            payload.hash(),
        )
    }
}

impl<'a, S> ExistingChunkAwareBinaryCasWriter<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    fn new(store: &'a S, writes: &'a mut StorageWriteSet, chunking: BinaryCasChunking) -> Self {
        Self {
            store,
            writes,
            chunking,
            blob_hashes: HashSet::new(),
            chunk_keys: HashSet::new(),
        }
    }

    pub(crate) async fn stage_payload(
        &mut self,
        payload: &BlobPayload,
    ) -> Result<BlobWriteReceipt, LixError> {
        crate::binary_cas::kv::stage_blob_write_skipping_existing_chunks(
            self.chunking,
            self.store,
            self.writes,
            &mut self.blob_hashes,
            &mut self.chunk_keys,
            payload.bytes(),
            payload.hash(),
        )
        .await
    }
}
