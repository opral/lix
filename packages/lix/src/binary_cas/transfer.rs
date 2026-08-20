//! Storage-neutral transfer boundary for the binary CAS.
//!
//! The durable CAS has several physical layouts, including a bounded delta
//! representation. None of those layouts belong on a sync wire. This module
//! presents every blob as one canonical ordered FastCDC manifest and raw
//! BLAKE3-addressed chunks, and validates that representation before it can be
//! published into the durable CAS.

use bytes::Bytes;

use crate::LixError;
use crate::binary_cas::chunking::{CHUNK_ANCHOR_BYTES, MAX_BINARY_CAS_CHUNK_BYTES, chunk_ranges};
use crate::binary_cas::codec::{BinaryChunkCodec, decode_binary_cas_chunk};
use crate::binary_cas::{
    BINARY_CAS_CHUNK_PRESENCE_SPACE, BINARY_CAS_CHUNK_SPACE, BlobChunkReceipt, BlobId, BlobPayload,
    BlobWriteReceipt, ChunkHash,
};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageCoreProjection, StorageGetOptions, StorageKey,
    StoragePrecondition, StorageWriteSet,
};

/// Couples a transfer publication to the ordinary CAS reclamation fence.
pub(crate) async fn stage_transfer_publication_fence(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    crate::binary_cas::kv::stage_publication_fence(store, writes, preconditions).await
}

/// Canonical, wire-neutral description of one blob.
///
/// This is always a flat ordered list, even when the durable source blob is
/// stored as a delta. Chunk offsets are deliberately omitted because they are
/// the prefix sums of `size_bytes` and therefore cannot disagree with them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalBlobManifest {
    pub(crate) blob_id: BlobId,
    pub(crate) size_bytes: u64,
    pub(crate) chunks: Vec<BlobChunkReceipt>,
}

impl CanonicalBlobManifest {
    /// Derives the canonical transfer manifest from exact blob bytes.
    pub(crate) fn from_bytes(bytes: &[u8]) -> Self {
        let payload = BlobPayload::from_bytes(bytes.to_vec());
        let blob_id = payload.hash().unwrap_or_else(|| BlobId::from_content(&[]));
        Self {
            blob_id,
            size_bytes: bytes.len() as u64,
            chunks: payload.chunks().to_vec(),
        }
    }
}

/// One canonical raw transfer chunk paired with its authenticated receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CanonicalBlobChunk {
    pub(crate) receipt: BlobChunkReceipt,
    pub(crate) bytes: Vec<u8>,
}

/// Loads a blob through the authenticated serving path and flattens any
/// physical delta layout into the canonical FastCDC transfer manifest.
pub(crate) async fn load_canonical_blob_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    blob_id: BlobId,
) -> Result<Option<CanonicalBlobManifest>, LixError> {
    let Some(bytes) = load_blob_bytes(store, blob_id).await? else {
        return Ok(None);
    };
    let manifest = CanonicalBlobManifest::from_bytes(&bytes);
    if manifest.blob_id != blob_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "binary CAS blob '{}' materialized as '{}'",
                blob_id.to_hex(),
                manifest.blob_id.to_hex()
            ),
        ));
    }
    Ok(Some(manifest))
}

/// Loads all canonical chunks of a blob.
///
/// This is the fallback needed for a delta-backed blob whose canonical flat
/// chunks need not exist as physical chunk rows. Ordinary chunk-addressed
/// transfer should use [`load_verified_chunk`]; a caller serving an older
/// delta can use this operation once and optionally publish the returned flat
/// representation locally.
pub(crate) async fn load_canonical_blob_chunks(
    store: &(impl StorageAdapterRead + ?Sized),
    blob_id: BlobId,
) -> Result<Option<Vec<CanonicalBlobChunk>>, LixError> {
    let Some(bytes) = load_blob_bytes(store, blob_id).await? else {
        return Ok(None);
    };
    let manifest = CanonicalBlobManifest::from_bytes(&bytes);
    if manifest.blob_id != blob_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "binary CAS blob '{}' materialized as '{}'",
                blob_id.to_hex(),
                manifest.blob_id.to_hex()
            ),
        ));
    }
    let mut offset = 0usize;
    let chunks = manifest
        .chunks
        .into_iter()
        .map(|receipt| {
            let size = usize::try_from(receipt.size_bytes).map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "binary CAS canonical chunk size exceeds this runtime",
                )
            })?;
            let end = offset.checked_add(size).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "binary CAS canonical chunk offset overflowed",
                )
            })?;
            let chunk = CanonicalBlobChunk {
                receipt,
                bytes: bytes[offset..end].to_vec(),
            };
            offset = end;
            Ok(chunk)
        })
        .collect::<Result<Vec<_>, LixError>>()?;
    debug_assert_eq!(offset, bytes.len());
    Ok(Some(chunks))
}

async fn load_blob_bytes(
    store: &(impl StorageAdapterRead + ?Sized),
    blob_id: BlobId,
) -> Result<Option<Vec<u8>>, LixError> {
    Ok(crate::binary_cas::load_bytes_many(store, &[blob_id])
        .await?
        .into_vec()
        .into_iter()
        .next()
        .flatten())
}

/// Tests chunk marker presence without loading payload bytes.
///
/// A positive result means the atomic chunk publication marker exists. Use
/// [`load_verified_chunk`] before consuming bytes; that operation also checks
/// the payload row and its BLAKE3 identity.
pub(crate) async fn chunk_presence_many(
    store: &(impl StorageAdapterRead + ?Sized),
    chunk_ids: &[ChunkHash],
) -> Result<Vec<bool>, LixError> {
    if chunk_ids.is_empty() {
        return Ok(Vec::new());
    }
    let keys = chunk_ids
        .iter()
        .map(|chunk_id| StorageKey(Bytes::copy_from_slice(chunk_id.as_bytes())))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(BINARY_CAS_CHUNK_PRESENCE_SPACE, &keys)
        .materialize(
            store,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?;
    Ok(result
        .value
        .into_iter()
        .map(|value| value.is_some())
        .collect())
}

/// Reads and authenticates one physical raw chunk.
pub(crate) async fn load_verified_chunk(
    store: &(impl StorageAdapterRead + ?Sized),
    chunk_id: ChunkHash,
) -> Result<Option<Vec<u8>>, LixError> {
    let keys = [StorageKey(Bytes::copy_from_slice(chunk_id.as_bytes()))];
    let result = PointReadPlan::new(BINARY_CAS_CHUNK_SPACE, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    let Some(value) = result.value.into_iter().next().flatten() else {
        return Ok(None);
    };
    let crate::storage_adapter::StorageProjectedValue::FullValue(encoded) = value else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "binary CAS chunk read omitted its value",
        ));
    };
    let (codec, uncompressed_len, payload) = decode_binary_cas_chunk(&encoded)?;
    if codec != BinaryChunkCodec::Raw {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "binary CAS transfer only supports raw chunk payloads",
        ));
    }
    if uncompressed_len == 0 || uncompressed_len > MAX_BINARY_CAS_CHUNK_BYTES as u64 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "binary CAS chunk '{}' has invalid size {}",
                chunk_id.to_hex(),
                uncompressed_len
            ),
        ));
    }
    if payload.len() as u64 != uncompressed_len || ChunkHash::from_content(payload) != chunk_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "binary CAS chunk '{}' failed content-address verification",
                chunk_id.to_hex()
            ),
        ));
    }
    Ok(Some(payload.to_vec()))
}

/// Stages one raw chunk only after verifying its declared BLAKE3 identity.
///
/// The caller owns the normal CAS publication fence and storage commit. Empty
/// blobs have no chunk and must be registered through an empty manifest.
pub(crate) fn stage_verified_raw_chunk(
    writes: &mut StorageWriteSet,
    chunk_id: ChunkHash,
    bytes: &[u8],
) -> Result<BlobChunkReceipt, LixError> {
    if bytes.is_empty() || bytes.len() > MAX_BINARY_CAS_CHUNK_BYTES {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "binary CAS transfer chunks must contain 1 through {} bytes",
                MAX_BINARY_CAS_CHUNK_BYTES
            ),
        ));
    }
    if ChunkHash::from_content(bytes) != chunk_id {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "binary CAS chunk '{}' does not match its BLAKE3 payload",
                chunk_id.to_hex()
            ),
        ));
    }
    crate::binary_cas::kv::stage_chunk(
        writes,
        chunk_id,
        BinaryChunkCodec::Raw,
        bytes.len() as u64,
        bytes,
    );
    Ok(BlobChunkReceipt {
        hash: chunk_id,
        size_bytes: bytes.len() as u64,
    })
}

/// Verifies and stages an empty or one-chunk canonical blob carried inline
/// with its manifest.
pub(crate) fn stage_verified_inline_canonical_blob(
    writes: &mut StorageWriteSet,
    manifest: &CanonicalBlobManifest,
    bytes: &[u8],
) -> Result<BlobWriteReceipt, LixError> {
    validate_manifest_receipts(manifest)?;
    let chunk = match manifest.chunks.as_slice() {
        [] if bytes.is_empty() => {
            let receipt = crate::binary_cas::kv::stage_upload_manifest(writes, &[])?;
            if receipt.hash != manifest.blob_id || receipt.size_bytes != manifest.size_bytes {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "binary CAS inline empty manifest staging changed its identity",
                ));
            }
            return Ok(receipt);
        }
        [chunk] => chunk,
        _ => {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "binary CAS inline manifest must contain zero or one chunk",
            ));
        }
    };
    if bytes.len() as u64 != manifest.size_bytes || bytes.len() as u64 != chunk.size_bytes {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS inline payload size does not match its manifest",
        ));
    }
    stage_verified_raw_chunk(writes, chunk.hash, bytes)?;
    verify_anchor(bytes, &manifest.chunks)?;
    let receipt = crate::binary_cas::kv::stage_upload_manifest(writes, &manifest.chunks)?;
    if receipt.hash != manifest.blob_id || receipt.size_bytes != manifest.size_bytes {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "binary CAS inline manifest staging changed its identity",
        ));
    }
    Ok(receipt)
}

/// Stages a canonical flat manifest without requiring its payload chunks.
///
/// Receipt shape, total size, and ordered manifest identity are validated;
/// payload hashes, receipt sizes against bytes, and FastCDC boundaries remain
/// deferred until the named chunks are hydrated. Every absent chunk receives
/// a durable demand marker in the same write set, while already-present chunks
/// have stale markers cleared.
pub(crate) async fn stage_deferred_canonical_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    manifest: &CanonicalBlobManifest,
) -> Result<Vec<ChunkHash>, LixError> {
    validate_manifest_receipts(manifest)?;
    let presence = chunk_presence_many(
        store,
        &manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect::<Vec<_>>(),
    )
    .await?;
    let receipt = crate::binary_cas::kv::stage_upload_manifest(writes, &manifest.chunks)?;
    if receipt.hash != manifest.blob_id || receipt.size_bytes != manifest.size_bytes {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "binary CAS deferred manifest staging changed its identity",
        ));
    }
    let availability = manifest
        .chunks
        .iter()
        .zip(presence)
        .map(|(chunk, present)| (chunk.hash, present))
        .collect::<std::collections::BTreeMap<_, _>>();
    let mut missing = Vec::new();
    for (chunk_hash, present) in availability {
        if present {
            crate::binary_cas::kv::stage_chunk_available(writes, chunk_hash);
        } else {
            crate::binary_cas::kv::stage_chunk_demand(writes, chunk_hash);
            missing.push(chunk_hash);
        }
    }
    Ok(missing)
}

/// Verifies and stages one canonical flat manifest.
///
/// Every named chunk must already be committed. Verification is bounded to
/// one 16 MiB forced FastCDC anchor at a time: chunks are authenticated as
/// they are read, their ordered sizes must tile each anchor exactly, and a
/// fresh boundary search must reproduce the supplied receipts. Only after all
/// anchors and the final `BlobId` agree are manifest rows staged.
pub(crate) async fn stage_verified_canonical_manifest(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    manifest: &CanonicalBlobManifest,
) -> Result<BlobWriteReceipt, LixError> {
    validate_manifest_receipts(manifest)?;

    let mut anchor_bytes = Vec::with_capacity(CHUNK_ANCHOR_BYTES);
    let mut anchor_receipts = Vec::new();
    for receipt in &manifest.chunks {
        let bytes = load_verified_chunk(store, receipt.hash)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "binary CAS manifest references missing chunk '{}'",
                        receipt.hash.to_hex()
                    ),
                )
            })?;
        if bytes.len() as u64 != receipt.size_bytes {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                format!(
                    "binary CAS chunk '{}' has size {}, manifest says {}",
                    receipt.hash.to_hex(),
                    bytes.len(),
                    receipt.size_bytes
                ),
            ));
        }
        if anchor_bytes.len().saturating_add(bytes.len()) > CHUNK_ANCHOR_BYTES {
            return Err(noncanonical_manifest_error());
        }
        anchor_bytes.extend_from_slice(&bytes);
        anchor_receipts.push(*receipt);
        if anchor_bytes.len() == CHUNK_ANCHOR_BYTES {
            verify_anchor(&anchor_bytes, &anchor_receipts)?;
            anchor_bytes.clear();
            anchor_receipts.clear();
        }
    }
    if !anchor_bytes.is_empty() {
        verify_anchor(&anchor_bytes, &anchor_receipts)?;
    }
    let receipt = crate::binary_cas::kv::stage_upload_manifest(writes, &manifest.chunks)?;
    if receipt.hash != manifest.blob_id || receipt.size_bytes != manifest.size_bytes {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "binary CAS canonical manifest staging changed its identity",
        ));
    }
    for chunk_hash in manifest
        .chunks
        .iter()
        .map(|chunk| chunk.hash)
        .collect::<std::collections::BTreeSet<_>>()
    {
        crate::binary_cas::kv::stage_chunk_available(writes, chunk_hash);
    }
    Ok(receipt)
}

fn validate_manifest_receipts(manifest: &CanonicalBlobManifest) -> Result<(), LixError> {
    validate_manifest_shape(manifest)?;
    let observed_size = manifest.chunks.iter().try_fold(0u64, |total, chunk| {
        total.checked_add(chunk.size_bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "binary CAS manifest size exceeds u64",
            )
        })
    })?;
    if observed_size != manifest.size_bytes {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "binary CAS manifest size is {}, chunk sum is {}",
                manifest.size_bytes, observed_size
            ),
        ));
    }
    let expected_blob_id = blob_id_for_manifest(manifest.size_bytes, &manifest.chunks);
    if expected_blob_id != manifest.blob_id {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            format!(
                "binary CAS manifest declares blob '{}', canonical chunks derive '{}'",
                manifest.blob_id.to_hex(),
                expected_blob_id.to_hex()
            ),
        ));
    }
    Ok(())
}

fn validate_manifest_shape(manifest: &CanonicalBlobManifest) -> Result<(), LixError> {
    if manifest.chunks.is_empty() {
        if manifest.size_bytes != 0 || manifest.blob_id != BlobId::from_content(&[]) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "binary CAS empty manifest has invalid size or identity",
            ));
        }
        return Ok(());
    }
    if manifest.size_bytes == 0 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS non-empty manifest declares zero bytes",
        ));
    }
    for receipt in &manifest.chunks {
        if receipt.size_bytes == 0 || receipt.size_bytes > MAX_BINARY_CAS_CHUNK_BYTES as u64 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "binary CAS manifest contains an invalid chunk size",
            ));
        }
    }
    Ok(())
}

fn verify_anchor(bytes: &[u8], receipts: &[BlobChunkReceipt]) -> Result<(), LixError> {
    let ranges = chunk_ranges(bytes);
    if ranges.len() != receipts.len() {
        return Err(noncanonical_manifest_error());
    }
    for ((start, end), receipt) in ranges.into_iter().zip(receipts) {
        if (end - start) as u64 != receipt.size_bytes
            || ChunkHash::from_content(&bytes[start..end]) != receipt.hash
        {
            return Err(noncanonical_manifest_error());
        }
    }
    Ok(())
}

fn blob_id_for_manifest(size_bytes: u64, chunks: &[BlobChunkReceipt]) -> BlobId {
    match chunks {
        [] => BlobId::from_content(&[]),
        [chunk] => BlobId::from_single_chunk(chunk.hash),
        chunks => BlobId::from_chunks(
            size_bytes,
            chunks.iter().map(|chunk| (chunk.hash, chunk.size_bytes)),
        ),
    }
}

fn noncanonical_manifest_error() -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        "binary CAS manifest does not match canonical FastCDC boundaries",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binary_cas::codec::{
        BinaryCasManifest, StorageBinaryCasDeltaBaseLayout, StorageBinaryCasDeltaSegment,
    };
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions, StorageWriteOptions};

    fn structured_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut bytes = vec![0; len];
        let mut state = seed ^ 0x9e37_79b9_7f4a_7c15;
        for chunk in bytes.chunks_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes()[..chunk.len()]);
        }
        bytes
    }

    async fn stage_chunks(
        storage: &StorageAdapter<Memory>,
        bytes: &[u8],
        manifest: &CanonicalBlobManifest,
    ) {
        let mut writes = storage.new_write_set();
        let mut offset = 0usize;
        for receipt in &manifest.chunks {
            let end = offset + receipt.size_bytes as usize;
            stage_verified_raw_chunk(&mut writes, receipt.hash, &bytes[offset..end])
                .expect("canonical chunk should stage");
            offset = end;
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("chunks should commit");
    }

    #[tokio::test]
    async fn canonical_transfer_round_trips_a_chunked_blob() {
        let storage = StorageAdapter::new(Memory::new());
        let bytes = structured_bytes(5 * 1024 * 1024 + 19, 7);
        let manifest = CanonicalBlobManifest::from_bytes(&bytes);
        assert!(manifest.chunks.len() > 1);
        stage_chunks(&storage, &bytes, &manifest).await;

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert!(
            chunk_presence_many(&read, &[manifest.chunks[0].hash])
                .await
                .expect("presence should load")[0]
        );
        let mut writes = storage.new_write_set();
        stage_verified_canonical_manifest(&read, &mut writes, &manifest)
            .await
            .expect("canonical manifest should verify");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("manifest should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        assert_eq!(
            load_canonical_blob_manifest(&read, manifest.blob_id)
                .await
                .expect("manifest should load"),
            Some(manifest)
        );
    }

    #[tokio::test]
    async fn empty_manifest_needs_no_chunks() {
        let storage = StorageAdapter::new(Memory::new());
        let manifest = CanonicalBlobManifest::from_bytes(&[]);
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        stage_verified_canonical_manifest(&read, &mut writes, &manifest)
            .await
            .expect("empty manifest should verify");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("empty manifest should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        assert_eq!(
            load_canonical_blob_manifest(&read, manifest.blob_id)
                .await
                .expect("empty manifest should load"),
            Some(manifest)
        );
    }

    #[tokio::test]
    async fn deferred_manifest_classifies_full_and_range_reads_by_missing_chunk_id() {
        let storage = StorageAdapter::new(Memory::new());
        let bytes = structured_bytes(5 * 1024 * 1024 + 19, 29);
        let manifest = CanonicalBlobManifest::from_bytes(&bytes);
        assert!(manifest.chunks.len() > 1);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        stage_deferred_canonical_manifest(&read, &mut writes, &manifest)
            .await
            .expect("manifest-only staging should succeed");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("manifest-only rows should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        for chunk in &manifest.chunks {
            assert!(
                load_verified_chunk(&read, chunk.hash)
                    .await
                    .expect("chunk lookup should succeed")
                    .is_none(),
                "manifest-only staging must not write payload rows"
            );
        }

        let full_error = crate::binary_cas::load_bytes_many(&read, &[manifest.blob_id])
            .await
            .expect_err("full reads must request absent chunks");
        assert_eq!(full_error.code, "LIX_SYNC_CHUNKS_REQUIRED");
        let expected_all = manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash.to_hex())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(
            full_error.details.unwrap()["chunkIds"],
            serde_json::json!(expected_all),
        );

        let first = manifest.chunks[0];
        let range_error = crate::binary_cas::kv::load_ranges_many(
            &read,
            &[(manifest.blob_id, 0..first.size_bytes)],
        )
        .await
        .expect_err("range reads must request only selected absent chunks");
        assert_eq!(range_error.code, "LIX_SYNC_CHUNKS_REQUIRED");
        assert_eq!(
            range_error.details.unwrap()["chunkIds"],
            serde_json::json!([first.hash.to_hex()])
        );
    }

    #[tokio::test]
    async fn deferred_manifest_rejects_false_size_and_identity() {
        let storage = StorageAdapter::new(Memory::new());
        let manifest = CanonicalBlobManifest::from_bytes(&structured_bytes(128 * 1024, 31));
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");

        let mut false_size = manifest.clone();
        false_size.size_bytes += 1;
        let mut writes = storage.new_write_set();
        let error = stage_deferred_canonical_manifest(&read, &mut writes, &false_size)
            .await
            .expect_err("false manifest size must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(writes.is_empty());

        let mut false_identity = manifest.clone();
        false_identity.blob_id = BlobId::from_content(b"not this manifest");
        let mut writes = storage.new_write_set();
        let error = stage_deferred_canonical_manifest(&read, &mut writes, &false_identity)
            .await
            .expect_err("false manifest identity must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        assert!(writes.is_empty());
    }

    #[tokio::test]
    async fn deferred_manifest_deduplicates_repeated_chunk_demand() {
        let storage = StorageAdapter::new(Memory::new());
        let chunk = BlobChunkReceipt {
            hash: ChunkHash::from_content(b"repeated canonical receipt"),
            size_bytes: 26,
        };
        let chunks = vec![chunk, chunk];
        let size_bytes = chunks.iter().map(|chunk| chunk.size_bytes).sum();
        let manifest = CanonicalBlobManifest {
            blob_id: BlobId::from_chunks(
                size_bytes,
                chunks.iter().map(|chunk| (chunk.hash, chunk.size_bytes)),
            ),
            size_bytes,
            chunks,
        };
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let missing = stage_deferred_canonical_manifest(&read, &mut writes, &manifest)
            .await
            .expect("repeated receipts should stage one demand marker");
        assert_eq!(missing, vec![chunk.hash]);
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("deduplicated demand should commit");
    }

    #[test]
    fn raw_chunk_rejects_a_false_identity() {
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let error = stage_verified_raw_chunk(
            &mut writes,
            ChunkHash::from_content(b"different"),
            b"payload",
        )
        .expect_err("wrong hash must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn manifest_rejects_noncanonical_chunk_boundaries() {
        let storage = StorageAdapter::new(Memory::new());
        let bytes = structured_bytes(100 * 1024, 11);
        let halves = [&bytes[..50 * 1024], &bytes[50 * 1024..]];
        let receipts = halves
            .iter()
            .map(|bytes| BlobChunkReceipt {
                hash: ChunkHash::from_content(bytes),
                size_bytes: bytes.len() as u64,
            })
            .collect::<Vec<_>>();
        let manifest = CanonicalBlobManifest {
            blob_id: BlobId::from_chunks(
                bytes.len() as u64,
                receipts.iter().map(|chunk| (chunk.hash, chunk.size_bytes)),
            ),
            size_bytes: bytes.len() as u64,
            chunks: receipts.clone(),
        };
        let mut chunk_writes = storage.new_write_set();
        for (receipt, bytes) in receipts.iter().zip(halves) {
            stage_verified_raw_chunk(&mut chunk_writes, receipt.hash, bytes)
                .expect("chunk should stage");
        }
        storage
            .commit_write_set(chunk_writes, StorageWriteOptions::default())
            .await
            .expect("chunks should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let error = stage_verified_canonical_manifest(&read, &mut writes, &manifest)
            .await
            .expect_err("noncanonical boundaries must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn delta_backing_flattens_to_canonical_transfer_chunks() {
        let storage = StorageAdapter::new(Memory::new());
        let base = structured_bytes(1024 * 1024, 17);
        let base_manifest = CanonicalBlobManifest::from_bytes(&base);
        stage_chunks(&storage, &base, &base_manifest).await;
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut base_writes = storage.new_write_set();
        let base_receipt =
            stage_verified_canonical_manifest(&read, &mut base_writes, &base_manifest)
                .await
                .expect("base manifest should verify");
        drop(read);
        storage
            .commit_write_set(base_writes, StorageWriteOptions::default())
            .await
            .expect("base manifest should commit");

        let offset = 32 * 1024;
        let replacement = b"sixteen-new-byte";
        assert_eq!(replacement.len(), 16);
        let mut result = base.clone();
        result[offset..offset + replacement.len()].copy_from_slice(replacement);
        let result_id = BlobId::from_content(&result);
        let base_layout = match base_receipt.layout {
            crate::binary_cas::BlobLayout::SingleChunk { chunk_hash } => {
                StorageBinaryCasDeltaBaseLayout::SingleChunk {
                    chunk_hash: chunk_hash.into_bytes(),
                }
            }
            crate::binary_cas::BlobLayout::Chunked { chunk_count } => {
                StorageBinaryCasDeltaBaseLayout::Chunked { chunk_count }
            }
            other => panic!("unexpected base layout: {other:?}"),
        };
        let mut delta_writes = storage.new_write_set();
        crate::binary_cas::kv::stage_manifest(
            &mut delta_writes,
            result_id,
            &BinaryCasManifest::Delta {
                size_bytes: result.len() as u64,
                base_blob_hash: base_manifest.blob_id.into_bytes(),
                base_size_bytes: base.len() as u64,
                base_layout,
                segments: vec![
                    StorageBinaryCasDeltaSegment::Copy {
                        offset: 0,
                        length: offset as u64,
                    },
                    StorageBinaryCasDeltaSegment::Insert {
                        bytes: replacement.to_vec(),
                    },
                    StorageBinaryCasDeltaSegment::Copy {
                        offset: (offset + replacement.len()) as u64,
                        length: (base.len() - offset - replacement.len()) as u64,
                    },
                ],
            },
        );
        storage
            .commit_write_set(delta_writes, StorageWriteOptions::default())
            .await
            .expect("delta manifest should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should reopen");
        let expected = CanonicalBlobManifest::from_bytes(&result);
        assert_eq!(
            load_canonical_blob_manifest(&read, result_id)
                .await
                .expect("delta should flatten"),
            Some(expected.clone())
        );
        let chunks = load_canonical_blob_chunks(&read, result_id)
            .await
            .expect("delta chunks should flatten")
            .expect("delta blob should exist");
        assert_eq!(
            chunks
                .iter()
                .flat_map(|chunk| chunk.bytes.iter().copied())
                .collect::<Vec<_>>(),
            result
        );
        assert_eq!(
            chunks.iter().map(|chunk| chunk.receipt).collect::<Vec<_>>(),
            expected.chunks
        );
    }
}
