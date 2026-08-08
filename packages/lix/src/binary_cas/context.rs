use async_trait::async_trait;

use crate::LixError;
use crate::binary_cas::{
    BlobBytesBatch, BlobChunkReceipt, BlobEditSplice, BlobId, BlobPayload, BlobRangeBytes,
    BlobRangeBytesBatch, BlobSameLengthSplice, BlobWriteReceipt,
};
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};

fn blob_owner_not_lowered() -> LixError {
    LixError::new(
        LixError::CODE_UNSUPPORTED_SQL,
        "BlobId-only binary-CAS access is not lowered; resolve an authenticated ForkTree BlobRef",
    )
}

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

/// Internal migration boundary for the removed BlobId-only owner.
///
/// The context deliberately owns no storage and contains no physical-space
/// knowledge. W4 replaces these operations with the caller-owned ForkTree
/// BlobRef/manifest path; until then every old operation fails closed.
pub(crate) struct BinaryCasContext;

impl BinaryCasContext {
    pub(crate) fn new() -> Self {
        Self
    }

    pub(crate) fn prepared_manifest_is_staged(
        &self,
        _writes: &StorageWriteSet,
        _blob_id: BlobId,
    ) -> bool {
        false
    }

    pub(crate) fn reader<S>(&self, store: S) -> BinaryCasStoreReader<S>
    where
        S: StorageAdapterRead,
    {
        BinaryCasStoreReader { _store: store }
    }

    pub(crate) fn writer_skipping_existing_chunks<'a, S>(
        &self,
        store: &'a S,
        writes: &'a mut StorageWriteSet,
    ) -> ExistingChunkAwareBinaryCasWriter<'a, S>
    where
        S: StorageAdapterRead + ?Sized,
    {
        ExistingChunkAwareBinaryCasWriter {
            _store: store,
            _writes: writes,
        }
    }
}

pub(crate) struct BinaryCasStoreReader<S> {
    _store: S,
}

#[async_trait]
impl<S> BlobDataReader for BinaryCasStoreReader<S>
where
    S: StorageAdapterRead + Send + Sync,
{
    async fn load_bytes_many(&self, _hashes: &[BlobId]) -> Result<BlobBytesBatch, LixError> {
        Err(blob_owner_not_lowered())
    }
}

pub(crate) struct ExistingChunkAwareBinaryCasWriter<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    _store: &'a S,
    _writes: &'a mut StorageWriteSet,
}

impl<'a, S> ExistingChunkAwareBinaryCasWriter<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) async fn stage_payload(
        &mut self,
        _payload: &BlobPayload,
    ) -> Result<BlobWriteReceipt, LixError> {
        Err(blob_owner_not_lowered())
    }

    pub(crate) async fn stage_fixed_part(
        &mut self,
        _bytes: &[u8],
    ) -> Result<Vec<BlobChunkReceipt>, LixError> {
        Err(blob_owner_not_lowered())
    }

    pub(crate) fn stage_fixed_manifest(
        &mut self,
        _chunks: &[BlobChunkReceipt],
    ) -> Result<BlobWriteReceipt, LixError> {
        Err(blob_owner_not_lowered())
    }

    pub(crate) async fn stage_file_payload(
        &mut self,
        _payload: &BlobPayload,
        _same_length_splice: Option<BlobSameLengthSplice>,
        _edit_splice: Option<BlobEditSplice>,
    ) -> Result<(), LixError> {
        Err(blob_owner_not_lowered())
    }
}
