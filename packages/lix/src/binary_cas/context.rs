use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::LixError;
use crate::binary_cas::{
    BlobBytesBatch, BlobChunkReceipt, BlobId, BlobLayout, BlobPayload, BlobRangeBytes,
    BlobRangeBytesBatch, BlobWriteReceipt,
};
use crate::forktree::{BlobChunkV1, BlobManifestV1, OBJECT_SPACE, ObjectId};
use crate::storage::{CoreProjection, GetManyRequest, GetOptions, Key, ProjectedValue};
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
            store,
            writes,
            staged_objects: BTreeMap::new(),
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
    store: &'a S,
    writes: &'a mut StorageWriteSet,
    staged_objects: BTreeMap<[u8; 32], Vec<u8>>,
}

impl<'a, S> ExistingChunkAwareBinaryCasWriter<'a, S>
where
    S: StorageAdapterRead + ?Sized,
{
    pub(crate) async fn stage_payload(
        &mut self,
        payload: &BlobPayload,
    ) -> Result<BlobWriteReceipt, LixError> {
        let chunks = self.stage_fixed_part(payload.bytes()).await?;
        self.stage_fixed_manifest(&chunks).await
    }

    pub(crate) async fn stage_fixed_part(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<BlobChunkReceipt>, LixError> {
        let mut receipts = Vec::with_capacity(bytes.len().div_ceil(1024 * 1024));
        for chunk in bytes.chunks(1024 * 1024) {
            let value = BlobChunkV1 {
                bytes: bytes::Bytes::copy_from_slice(chunk),
            };
            let (object_id, encoded) = value.encode().map_err(LixError::from)?;
            let object_key = object_id.as_bytes().to_vec();
            if !self.chunk_exists(object_id).await?
                && !self.writes.has_put(OBJECT_SPACE, &object_key)
            {
                self.writes.put(OBJECT_SPACE, object_key, encoded.to_vec());
                self.staged_objects
                    .insert(*object_id.as_bytes(), encoded.to_vec());
            }
            receipts.push(BlobChunkReceipt {
                hash: crate::binary_cas::ChunkHash::from_content(chunk),
                size_bytes: chunk.len() as u64,
                object_id: *object_id.as_bytes(),
            });
        }
        Ok(receipts)
    }

    pub(crate) async fn stage_fixed_manifest(
        &mut self,
        chunks: &[BlobChunkReceipt],
    ) -> Result<BlobWriteReceipt, LixError> {
        let mut ordered_chunks = Vec::with_capacity(chunks.len());
        let mut content_digest = blake3::Hasher::new();
        let mut total_size = 0_u64;
        for receipt in chunks {
            let object_id = ObjectId::from_bytes(receipt.object_id);
            let encoded = match self.staged_objects.get(object_id.as_bytes()) {
                Some(encoded) => encoded.clone(),
                None => self.load_object(object_id).await?,
            };
            let chunk = BlobChunkV1::decode(object_id, &encoded).map_err(LixError::from)?;
            if crate::binary_cas::ChunkHash::from_content(&chunk.bytes) != receipt.hash
                || chunk.bytes.len() as u64 != receipt.size_bytes
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "ForkTree upload receipt does not authenticate its chunk object",
                ));
            }
            total_size = total_size.checked_add(receipt.size_bytes).ok_or_else(|| {
                LixError::new(LixError::CODE_INVALID_PARAM, "blob size overflows u64")
            })?;
            content_digest.update(&chunk.bytes);
            ordered_chunks.push(crate::forktree::BlobChunkRefV1 {
                chunk_object_id: object_id,
                declared_len: receipt.size_bytes,
            });
        }
        let canonical_blob_id = if total_size <= 1024 * 1024 {
            let hash = chunks
                .first()
                .map(|chunk| chunk.hash)
                .unwrap_or_else(|| crate::binary_cas::ChunkHash::from_content(&[]));
            BlobId::from_single_chunk(hash)
        } else {
            BlobId::from_chunks(
                total_size,
                chunks.iter().map(|chunk| (chunk.hash, chunk.size_bytes)),
            )
        };
        let manifest = BlobManifestV1::from_authenticated_chunks(
            total_size,
            ordered_chunks,
            canonical_blob_id,
            *content_digest.finalize().as_bytes(),
        );
        let (manifest_object_id, encoded) = manifest.encode().map_err(LixError::from)?;
        if !self.object_exists(manifest_object_id).await?
            && !self
                .writes
                .has_put(OBJECT_SPACE, manifest_object_id.as_bytes())
        {
            self.writes.put(
                OBJECT_SPACE,
                manifest_object_id.as_bytes().to_vec(),
                encoded.to_vec(),
            );
            self.staged_objects
                .insert(*manifest_object_id.as_bytes(), encoded.to_vec());
        }
        let layout = if total_size <= 1024 * 1024 {
            BlobLayout::SingleChunk {
                chunk_hash: chunks
                    .first()
                    .map(|chunk| chunk.hash)
                    .unwrap_or_else(|| crate::binary_cas::ChunkHash::from_content(&[])),
            }
        } else {
            BlobLayout::Chunked {
                chunk_count: u32::try_from(chunks.len()).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "blob has too many fixed chunks",
                    )
                })?,
            }
        };
        Ok(BlobWriteReceipt {
            hash: canonical_blob_id,
            size_bytes: total_size,
            layout,
            manifest_object_id: *manifest_object_id.as_bytes(),
        })
    }

    async fn load_object(&self, object_id: ObjectId) -> Result<Vec<u8>, LixError> {
        let key = [Key(object_id.as_bytes().to_vec().into())];
        let loaded = self
            .store
            .get_many(&[GetManyRequest {
                space: OBJECT_SPACE,
                keys: &key,
                opts: GetOptions {
                    projection: CoreProjection::FullValue,
                },
            }])
            .await?;
        match loaded.values.as_slice() {
            [Some(ProjectedValue::FullValue(bytes))] => Ok(bytes.to_vec()),
            [Some(ProjectedValue::KeyOnly)] => Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "ForkTree upload object read returned key-only data",
            )),
            [None] => Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "ForkTree upload object is missing",
            )),
            _ => Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "ForkTree upload object read returned invalid cardinality",
            )),
        }
    }

    async fn object_exists(&self, object_id: ObjectId) -> Result<bool, LixError> {
        let key = [Key(object_id.as_bytes().to_vec().into())];
        let loaded = self
            .store
            .get_many(&[GetManyRequest {
                space: OBJECT_SPACE,
                keys: &key,
                opts: GetOptions {
                    projection: CoreProjection::KeyOnly,
                },
            }])
            .await?;
        Ok(matches!(
            loaded.values.as_slice(),
            [Some(ProjectedValue::KeyOnly) | Some(ProjectedValue::FullValue(_))]
        ))
    }

    async fn chunk_exists(&self, object_id: ObjectId) -> Result<bool, LixError> {
        self.object_exists(object_id).await
    }
}
