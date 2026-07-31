use async_trait::async_trait;

use crate::LixError;
use crate::binary_cas::BinaryCasChunking;
use crate::binary_cas::{
    BlobBytesBatch, BlobHash, BlobPayload, BlobSameLengthSplice, BlobWriteReceipt,
};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use std::collections::HashSet;

#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_bytes_many(&self, hashes: &[BlobHash]) -> Result<BlobBytesBatch, LixError>;
}

/// Long-lived Binary CAS context factory.
///
/// The context does not own storage. Callers explicitly provide a KV store via
/// `reader(...)` or `writer_skipping_existing_chunks(...)`, keeping storage and
/// transaction ownership at the execution layer.
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
    dictionary_sample_slots: HashSet<u8>,
    dictionary: Option<Option<crate::binary_cas::kv::BinaryCasDictionary>>,
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
            dictionary_sample_slots: HashSet::new(),
            dictionary: None,
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

    /// Stages a normal file payload, opportunistically retaining unchanged
    /// manifest chunks for one host-verified same-length splice. Any
    /// ineligible or unavailable base falls through to the canonical full
    /// rechunking path.
    pub(crate) async fn stage_file_payload(
        &mut self,
        payload: &BlobPayload,
        base_blob_hash: Option<BlobHash>,
        same_length_splice: Option<BlobSameLengthSplice>,
    ) -> Result<(), LixError> {
        if self.dictionary.is_none() {
            self.dictionary = Some(
                crate::binary_cas::kv::load_or_train_dictionary(self.store, self.writes).await?,
            );
        }
        let dictionary = self.dictionary.as_ref().and_then(Option::as_ref).cloned();
        if let Some(splice) = same_length_splice
            && crate::binary_cas::kv::try_stage_blob_write_reusing_same_length_splice(
                self.store,
                self.writes,
                &mut self.blob_hashes,
                &mut self.chunk_keys,
                payload.bytes(),
                payload.hash(),
                splice,
            )
            .await?
        {
            if dictionary.is_none() {
                crate::binary_cas::kv::stage_dictionary_sample(
                    self.writes,
                    &mut self.dictionary_sample_slots,
                    payload.bytes(),
                    payload.hash(),
                );
            }
            return Ok(());
        }
        if let Some(base_blob_hash) = base_blob_hash
            && crate::binary_cas::kv::try_stage_inline_delta(
                self.store,
                self.writes,
                &mut self.blob_hashes,
                payload.bytes(),
                payload.hash(),
                base_blob_hash,
                dictionary.as_ref(),
            )
            .await?
        {
            if dictionary.is_none() {
                crate::binary_cas::kv::stage_dictionary_sample(
                    self.writes,
                    &mut self.dictionary_sample_slots,
                    payload.bytes(),
                    payload.hash(),
                );
            }
            return Ok(());
        }
        if crate::binary_cas::kv::try_stage_inline_dictionary(
            self.store,
            self.writes,
            &mut self.blob_hashes,
            payload.bytes(),
            payload.hash(),
            dictionary.as_ref(),
        )
        .await?
        {
            if dictionary.is_none() {
                crate::binary_cas::kv::stage_dictionary_sample(
                    self.writes,
                    &mut self.dictionary_sample_slots,
                    payload.bytes(),
                    payload.hash(),
                );
            }
            return Ok(());
        }
        self.stage_payload(payload).await?;
        if dictionary.is_none() {
            crate::binary_cas::kv::stage_dictionary_sample(
                self.writes,
                &mut self.dictionary_sample_slots,
                payload.bytes(),
                payload.hash(),
            );
        }
        Ok(())
    }
}
