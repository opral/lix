use async_trait::async_trait;

use crate::LixError;
use crate::binary_cas::{
    BlobBytesBatch, BlobChunkReceipt, BlobEditSplice, BlobId, BlobPayload, BlobRangeBytes,
    BlobRangeBytesBatch, BlobSameLengthSplice, BlobWriteReceipt,
};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use std::collections::HashSet;

#[async_trait]
pub(crate) trait BlobDataReader: Send + Sync {
    async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError>;

    async fn load_ranges_many(
        &self,
        requests: &[(BlobId, std::ops::Range<u64>)],
    ) -> Result<BlobRangeBytesBatch, LixError> {
        let hashes = requests.iter().map(|(hash, _)| *hash).collect::<Vec<_>>();
        let values = self.load_bytes_many(&hashes).await?.into_vec();
        let entries = values
            .into_iter()
            .zip(requests)
            .map(|(value, (_, requested))| {
                value
                    .map(|bytes| materialize_blob_range(bytes, requested.clone()))
                    .transpose()
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(BlobRangeBytesBatch::new(entries))
    }
}

fn materialize_blob_range(
    bytes: Vec<u8>,
    requested: std::ops::Range<u64>,
) -> Result<BlobRangeBytes, LixError> {
    let total_size = u64::try_from(bytes.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "binary CAS blob size exceeds u64",
        )
    })?;
    if requested.start >= requested.end || requested.start >= total_size {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS range is not satisfiable",
        ));
    }
    let range = requested.start..requested.end.min(total_size);
    let start = usize::try_from(range.start).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS range is too large",
        )
    })?;
    let end = usize::try_from(range.end).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS range is too large",
        )
    })?;
    Ok(BlobRangeBytes {
        bytes: bytes[start..end].to_vec(),
        total_size,
        range,
    })
}

/// Long-lived Binary CAS context factory.
///
/// The context does not own storage. Callers explicitly provide a KV store via
/// `reader(...)` or `writer_skipping_existing_chunks(...)`, keeping storage and
/// transaction ownership at the execution layer.
pub(crate) struct BinaryCasContext;

impl BinaryCasContext {
    pub(crate) fn new() -> Self {
        Self
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
        ExistingChunkAwareBinaryCasWriter::new(store, writes)
    }
}

#[async_trait]
impl<S> BlobDataReader for BinaryCasStoreReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync,
{
    async fn load_bytes_many(&self, hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
        let mut reader = Self {
            store: self.store.clone(),
        };
        Self::load_bytes_many(&mut reader, hashes).await
    }

    async fn load_ranges_many(
        &self,
        requests: &[(BlobId, std::ops::Range<u64>)],
    ) -> Result<BlobRangeBytesBatch, LixError> {
        crate::binary_cas::kv::load_ranges_many(&self.store, requests).await
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
        hashes: &[BlobId],
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
    blob_hashes: HashSet<[u8; 32]>,
    chunk_keys: HashSet<Vec<u8>>,
}

impl<'a, S> ExistingChunkAwareBinaryCasWriter<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    fn new(store: &'a S, writes: &'a mut StorageWriteSet) -> Self {
        Self {
            store,
            writes,
            blob_hashes: HashSet::new(),
            chunk_keys: HashSet::new(),
        }
    }

    pub(crate) async fn stage_payload(
        &mut self,
        payload: &BlobPayload,
    ) -> Result<BlobWriteReceipt, LixError> {
        crate::binary_cas::kv::stage_blob_write_skipping_existing_chunks(
            self.store,
            self.writes,
            &mut self.blob_hashes,
            &mut self.chunk_keys,
            payload,
        )
        .await
    }

    pub(crate) async fn stage_upload_part(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<BlobChunkReceipt>, LixError> {
        crate::binary_cas::kv::stage_upload_part_skipping_existing(
            self.store,
            self.writes,
            &mut self.chunk_keys,
            bytes,
        )
        .await
    }

    pub(crate) fn stage_upload_manifest(
        &mut self,
        chunks: &[BlobChunkReceipt],
    ) -> Result<BlobWriteReceipt, LixError> {
        crate::binary_cas::kv::stage_upload_manifest(self.writes, chunks)
    }

    /// Stages a normal file payload, opportunistically retaining unchanged
    /// manifest chunks for one host-verified same-length splice. Any
    /// ineligible or unavailable base falls through to the canonical full
    /// rechunking path.
    pub(crate) async fn stage_file_payload(
        &mut self,
        payload: &BlobPayload,
        same_length_splice: Option<BlobSameLengthSplice>,
        edit_splice: Option<BlobEditSplice>,
    ) -> Result<(), LixError> {
        if let Some(splice) = edit_splice
            && crate::binary_cas::kv::try_stage_blob_write_as_flat_delta(
                self.store,
                self.writes,
                &mut self.blob_hashes,
                payload.bytes(),
                payload.hash(),
                splice,
            )
            .await?
        {
            return Ok(());
        }
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
            return Ok(());
        }
        self.stage_payload(payload).await?;
        Ok(())
    }
}
