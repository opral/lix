#![allow(clippy::cast_sign_loss)]

use crate::LixError;
use crate::binary_cas::chunking::{
    INLINE_BINARY_CAS_MAX_BYTES, MAX_BINARY_CAS_CHUNK_BYTES, fastcdc_chunk_ranges_with_chunking,
};
use crate::binary_cas::codec::{
    BinaryCasManifest, BinaryChunkCodec, decode_binary_cas_chunk, decode_binary_cas_manifest,
    decode_binary_cas_manifest_chunk, encode_binary_cas_chunk, encode_binary_cas_manifest,
    encode_binary_cas_manifest_chunk, encode_inline_binary_cas_manifest,
};
use crate::binary_cas::compression::{decode_zstd_chunk, encode_chunk_payload};
use crate::binary_cas::{
    BinaryCasChunking, BlobBytesBatch, BlobHash, BlobLayout, BlobMetadata, BlobMetadataBatch,
    BlobWriteReceipt, InlineBlob,
};
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageSpace, StorageWriteSet,
};
use crate::storage_adapter::{
    StorageCoreProjection, StorageGetOptions, StorageKey, StoragePrefix, StorageProjectedValue,
    StorageScanOptions, StorageSpaceId, StorageValue,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use web_time::Instant;

// Keep independent manifest scans bounded so large blob batches do not create
// unbounded backend pressure. Eight matches the engine's other remote scan
// fan-out and is enough to hide storage latency without a large request burst.
const MANIFEST_SCAN_CONCURRENCY: usize = 8;

pub(crate) const BINARY_CAS_MANIFEST_NAMESPACE: &str = "binary_cas.manifest";
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_NAMESPACE: &str = "binary_cas.manifest_chunk";
pub(crate) const BINARY_CAS_CHUNK_NAMESPACE: &str = "binary_cas.chunk";
pub(crate) const BINARY_CAS_CHUNK_PRESENCE_NAMESPACE: &str = "binary_cas.chunk_presence";
pub(crate) const BINARY_CAS_MANIFEST_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0005_0001), BINARY_CAS_MANIFEST_NAMESPACE);
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0005_0002),
    BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
);
pub(crate) const BINARY_CAS_CHUNK_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0005_0003), BINARY_CAS_CHUNK_NAMESPACE);
pub(crate) const BINARY_CAS_CHUNK_PRESENCE_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0005_0004),
    BINARY_CAS_CHUNK_PRESENCE_NAMESPACE,
);

#[derive(Debug)]
struct BlobWritePlan {
    blob_hash: BlobHash,
    chunk_ranges: Vec<(usize, usize)>,
    layout: BlobLayout,
    receipt: BlobWriteReceipt,
}

#[derive(Debug, Clone, Copy)]
struct PreparedChunk {
    start: usize,
    end: usize,
    hash: BlobHash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvBlobManifestChunk {
    pub(crate) chunk_hash: [u8; 32],
    pub(crate) chunk_size: u64,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KvChunk {
    pub(crate) codec: BinaryChunkCodec,
    pub(crate) uncompressed_len: u64,
    pub(crate) data: Vec<u8>,
}

#[cfg(test)]
async fn load_manifest(
    store: &impl StorageAdapterRead,
    blob_hash: BlobHash,
) -> Result<Option<BinaryCasManifest>, LixError> {
    let Some(bytes) = get_one(store, BINARY_CAS_MANIFEST_SPACE, manifest_key(blob_hash)).await?
    else {
        return Ok(None);
    };
    decode_binary_cas_manifest(&bytes).map(Some)
}

pub(crate) fn stage_manifest(
    writes: &mut StorageWriteSet,
    blob_hash: BlobHash,
    manifest: &BinaryCasManifest,
) {
    writes.put(
        BINARY_CAS_MANIFEST_SPACE,
        key(manifest_key(blob_hash)),
        value(encode_binary_cas_manifest(manifest)),
    );
}

pub(crate) async fn scan_manifest_chunks(
    store: &impl StorageAdapterRead,
    blob_hash: BlobHash,
) -> Result<Vec<KvBlobManifestChunk>, LixError> {
    scan_all_values(
        store,
        BINARY_CAS_MANIFEST_CHUNK_SPACE,
        manifest_chunk_prefix(blob_hash),
    )
    .await?
    .into_iter()
    .map(|value| {
        let (chunk_hash, chunk_size) = decode_binary_cas_manifest_chunk(&value)?;
        Ok(KvBlobManifestChunk {
            chunk_hash,
            chunk_size,
        })
    })
    .collect()
}

pub(crate) fn stage_manifest_chunk(
    writes: &mut StorageWriteSet,
    blob_hash: BlobHash,
    chunk_index: u64,
    chunk: &KvBlobManifestChunk,
) {
    writes.put(
        BINARY_CAS_MANIFEST_CHUNK_SPACE,
        key(manifest_chunk_key(blob_hash, chunk_index)),
        value(encode_binary_cas_manifest_chunk(
            &chunk.chunk_hash,
            chunk.chunk_size,
        )),
    );
}

#[cfg(test)]
async fn load_chunk(
    store: &impl StorageAdapterRead,
    chunk_hash: BlobHash,
) -> Result<Option<KvChunk>, LixError> {
    let Some(bytes) = get_one(store, BINARY_CAS_CHUNK_SPACE, chunk_key(chunk_hash)).await? else {
        return Ok(None);
    };
    let (codec, uncompressed_len, payload) = decode_binary_cas_chunk(&bytes)?;
    Ok(Some(KvChunk {
        codec,
        uncompressed_len,
        data: payload.to_vec(),
    }))
}

pub(crate) fn stage_chunk(
    writes: &mut StorageWriteSet,
    chunk_hash: BlobHash,
    codec: BinaryChunkCodec,
    uncompressed_len: u64,
    payload: &[u8],
) {
    // The storage API's key-only projection still has to materialize a value
    // on backends without an exact exists primitive. Keep an empty marker in
    // a separate space so content-addressed dedupe never reads chunk payloads
    // merely to prove that their hash is present. The marker and payload are
    // staged in the same canonical write set and become visible atomically.
    writes.put(
        BINARY_CAS_CHUNK_PRESENCE_SPACE,
        key(chunk_key(chunk_hash)),
        value(Vec::new()),
    );
    writes.put(
        BINARY_CAS_CHUNK_SPACE,
        key(chunk_key(chunk_hash)),
        value(encode_binary_cas_chunk(codec, uncompressed_len, payload)),
    );
}

fn stage_content_chunk(
    writes: &mut StorageWriteSet,
    chunk_hash: BlobHash,
    chunk_data: &[u8],
) -> Result<(), LixError> {
    let encoded = encode_chunk_payload(chunk_hash, chunk_data)?;
    stage_chunk(
        writes,
        chunk_hash,
        encoded.codec,
        chunk_data.len() as u64,
        &encoded.data,
    );
    Ok(())
}

#[cfg(test)]
async fn get_one(
    store: &impl StorageAdapterRead,
    space: StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = PointReadPlan::new(space, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value)
        .map(|bytes| bytes.to_vec()))
}

async fn scan_all_values(
    store: &impl StorageAdapterRead,
    space: StorageSpace,
    prefix: Vec<u8>,
) -> Result<Vec<Vec<u8>>, LixError> {
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::from(prefix),
        },
    );
    let mut values = Vec::new();
    let mut resume_after = None;
    loop {
        let page = plan
            .collect(
                store,
                StorageScanOptions {
                    resume_after: resume_after.clone(),
                    ..StorageScanOptions::default()
                },
            )
            .await?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        values.extend(
            page.value
                .entries
                .into_iter()
                .filter_map(|entry| full_value(entry.value))
                .map(|bytes| bytes.to_vec()),
        );
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(values)
}

pub(crate) async fn load_metadata_many(
    store: &impl StorageAdapterRead,
    hashes: &[BlobHash],
) -> Result<BlobMetadataBatch, LixError> {
    if hashes.is_empty() {
        return Ok(BlobMetadataBatch::new(Vec::new()));
    }
    let rows = point_values(
        store,
        BINARY_CAS_MANIFEST_SPACE,
        hashes.iter().map(|hash| manifest_key(*hash)).collect(),
    )
    .await?;
    if rows.len() != hashes.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS metadata read expected {} rows, got {}",
                hashes.len(),
                rows.len()
            ),
        ));
    }
    let entries = rows
        .into_iter()
        .zip(hashes.iter().copied())
        .map(|(row, hash)| {
            row.map(|bytes| {
                let manifest = decode_binary_cas_manifest(&bytes)?;
                metadata_from_manifest(hash, manifest)
            })
            .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BlobMetadataBatch::new(entries))
}

pub(crate) async fn load_bytes_many(
    store: &impl StorageAdapterRead,
    hashes: &[BlobHash],
) -> Result<BlobBytesBatch, LixError> {
    let metadata = load_metadata_many(store, hashes).await?.into_vec();
    let mut seen_manifest_hashes = HashSet::new();
    let chunked_blobs = metadata
        .iter()
        .filter_map(|metadata| {
            let metadata = metadata.as_ref()?;
            let BlobLayout::Chunked { chunk_count } = &metadata.layout else {
                return None;
            };
            seen_manifest_hashes
                .insert(metadata.hash)
                .then_some((metadata.hash, *chunk_count))
        })
        .collect::<Vec<_>>();
    let scan_count = chunked_blobs.len();
    // Consume completions out of order so a slow early scan does not prevent
    // the bounded window from refilling. Results cross the gate below only in
    // first-request order, preserving deterministic error selection.
    let mut scans = stream::iter(chunked_blobs.into_iter().enumerate())
        .map(|(order, (blob_hash, chunk_count))| async move {
            let result = async {
                let manifest_chunks = scan_manifest_chunks(store, blob_hash).await?;
                if manifest_chunks.len() != chunk_count as usize {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS blob '{}' expected {} chunks, found {}",
                            blob_hash.to_hex(),
                            chunk_count,
                            manifest_chunks.len()
                        ),
                    ));
                }
                Ok(manifest_chunks)
            }
            .await;
            (order, blob_hash, result)
        })
        .buffer_unordered(MANIFEST_SCAN_CONCURRENCY);
    let mut completed = Vec::with_capacity(scan_count);
    completed.resize_with(scan_count, || None);
    let mut next_order = 0;
    let mut chunked_manifests_by_hash = HashMap::with_capacity(scan_count);
    while let Some((order, blob_hash, result)) = scans.next().await {
        completed[order] = Some((blob_hash, result));
        while next_order < completed.len() {
            let Some((blob_hash, result)) = completed[next_order].take() else {
                break;
            };
            chunked_manifests_by_hash.insert(blob_hash, result?);
            next_order += 1;
        }
    }
    debug_assert_eq!(next_order, scan_count);
    let mut requested_chunks = Vec::new();
    let mut seen_chunks = HashSet::new();

    for metadata in &metadata {
        let Some(metadata) = metadata else {
            continue;
        };
        match &metadata.layout {
            BlobLayout::Empty | BlobLayout::Inline => {}
            BlobLayout::SingleChunk { chunk_hash } => {
                if seen_chunks.insert(*chunk_hash) {
                    requested_chunks.push(*chunk_hash);
                }
            }
            BlobLayout::Chunked { .. } => {
                let manifest_chunks =
                    chunked_manifests_by_hash
                        .get(&metadata.hash)
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "binary CAS blob '{}' missing chunk manifest",
                                    metadata.hash.to_hex()
                                ),
                            )
                        })?;
                for manifest_chunk in manifest_chunks {
                    let chunk_hash = BlobHash::from_bytes(manifest_chunk.chunk_hash);
                    if seen_chunks.insert(chunk_hash) {
                        requested_chunks.push(chunk_hash);
                    }
                }
            }
        }
    }

    let chunk_rows = load_chunk_rows(store, &requested_chunks).await?;
    let chunk_rows_by_hash = requested_chunks
        .into_iter()
        .zip(chunk_rows)
        .collect::<HashMap<_, _>>();

    let entries = metadata
        .into_iter()
        .map(|metadata| {
            metadata
                .map(|metadata| {
                    let hash = metadata.hash;
                    assemble_blob_bytes(
                        metadata,
                        &chunk_rows_by_hash,
                        chunked_manifests_by_hash.get(&hash),
                    )
                })
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BlobBytesBatch::new(entries))
}

async fn load_chunk_rows(
    store: &impl StorageAdapterRead,
    hashes: &[BlobHash],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    point_values(
        store,
        BINARY_CAS_CHUNK_SPACE,
        hashes.iter().map(|hash| chunk_key(*hash)).collect(),
    )
    .await
}

async fn point_values(
    store: &impl StorageAdapterRead,
    space: StorageSpace,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Bytes>>, LixError> {
    let keys = keys
        .into_iter()
        .map(|key| StorageKey(Bytes::from(key)))
        .collect::<Vec<_>>();
    let result = PointReadPlan::new(space, &keys)
        .materialize(store, StorageGetOptions::default())
        .await?;
    Ok(result
        .value
        .into_iter()
        .map(|value| value.and_then(full_value))
        .collect())
}

fn key(bytes: Vec<u8>) -> StorageKey {
    StorageKey(Bytes::from(bytes))
}

fn value(bytes: Vec<u8>) -> StorageValue {
    StorageValue {
        bytes: Bytes::from(bytes),
    }
}

fn full_value(value: StorageProjectedValue) -> Option<Bytes> {
    match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes),
        StorageProjectedValue::KeyOnly => None,
    }
}

fn assemble_blob_bytes(
    mut metadata: BlobMetadata,
    chunk_rows_by_hash: &HashMap<BlobHash, Option<Bytes>>,
    chunked_manifest: Option<&Vec<KvBlobManifestChunk>>,
) -> Result<Vec<u8>, LixError> {
    let expected_blob_size = persisted_size_to_usize(metadata.size_bytes, "binary CAS blob")?;
    let bytes = match &metadata.layout {
        BlobLayout::Empty => {
            if cfg!(debug_assertions) && metadata.hash != BlobHash::from_content(&[]) {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' failed content-address verification",
                        metadata.hash.to_hex()
                    ),
                ));
            }
            Vec::new()
        }
        BlobLayout::Inline => {
            let inline_blob = metadata.inline_blob.take().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS inline blob '{}' is missing its encoded bytes",
                        metadata.hash.to_hex()
                    ),
                )
            })?;
            decode_and_verify_payload(
                inline_blob.codec,
                metadata.size_bytes,
                Cow::Owned(inline_blob.payload),
                expected_blob_size,
                metadata.hash,
                metadata.hash,
            )?
            .into_owned()
        }
        BlobLayout::SingleChunk { chunk_hash } => {
            let chunk = decode_chunk_from_map(
                chunk_rows_by_hash,
                metadata.hash,
                *chunk_hash,
                expected_blob_size,
            )?;
            if cfg!(debug_assertions)
                && *chunk_hash != metadata.hash
                && BlobHash::from_content(&chunk) != metadata.hash
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' failed content-address verification",
                        metadata.hash.to_hex()
                    ),
                ));
            }
            chunk.into_owned()
        }
        BlobLayout::Chunked { chunk_count } => {
            let Some(manifest_chunks) = chunked_manifest else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' missing chunk manifest",
                        metadata.hash.to_hex()
                    ),
                ));
            };
            if manifest_chunks.len() != *chunk_count as usize {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' expected {} chunks, found {}",
                        metadata.hash.to_hex(),
                        chunk_count,
                        manifest_chunks.len()
                    ),
                ));
            }
            let mut out = Vec::with_capacity(expected_blob_size);
            for manifest_chunk in manifest_chunks {
                let chunk_hash = BlobHash::from_bytes(manifest_chunk.chunk_hash);
                let expected_chunk_size =
                    persisted_size_to_usize(manifest_chunk.chunk_size, "binary CAS chunk")?;
                let chunk = decode_chunk_from_map(
                    chunk_rows_by_hash,
                    metadata.hash,
                    chunk_hash,
                    expected_chunk_size,
                )?;
                out.extend_from_slice(&chunk);
            }
            if out.len() != expected_blob_size {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' expected {} bytes, decoded {} bytes",
                        metadata.hash.to_hex(),
                        expected_blob_size,
                        out.len()
                    ),
                ));
            }
            if cfg!(debug_assertions) && BlobHash::from_content(&out) != metadata.hash {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' failed content-address verification",
                        metadata.hash.to_hex()
                    ),
                ));
            }
            out
        }
    };
    Ok(bytes)
}

fn decode_chunk_from_map(
    chunk_rows_by_hash: &HashMap<BlobHash, Option<Bytes>>,
    blob_hash: BlobHash,
    chunk_hash: BlobHash,
    expected_chunk_size: usize,
) -> Result<Cow<'_, [u8]>, LixError> {
    let Some(Some(chunk_bytes)) = chunk_rows_by_hash.get(&chunk_hash) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' is missing for blob '{}'",
                chunk_hash.to_hex(),
                blob_hash.to_hex()
            ),
        ));
    };
    decode_and_verify_chunk(chunk_bytes, expected_chunk_size, blob_hash, chunk_hash)
}

fn decode_and_verify_chunk(
    chunk_bytes: &[u8],
    expected_chunk_size: usize,
    blob_hash: BlobHash,
    chunk_hash: BlobHash,
) -> Result<Cow<'_, [u8]>, LixError> {
    let (codec, uncompressed_len, chunk_payload) = decode_binary_cas_chunk(chunk_bytes)?;
    decode_and_verify_payload(
        codec,
        uncompressed_len,
        Cow::Borrowed(chunk_payload),
        expected_chunk_size,
        blob_hash,
        chunk_hash,
    )
}

fn decode_and_verify_payload(
    codec: BinaryChunkCodec,
    uncompressed_len: u64,
    chunk_payload: Cow<'_, [u8]>,
    expected_chunk_size: usize,
    blob_hash: BlobHash,
    chunk_hash: BlobHash,
) -> Result<Cow<'_, [u8]>, LixError> {
    if expected_chunk_size > MAX_BINARY_CAS_CHUNK_BYTES
        || uncompressed_len > MAX_BINARY_CAS_CHUNK_BYTES as u64
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' exceeds the {} byte format maximum",
                chunk_hash.to_hex(),
                blob_hash.to_hex(),
                MAX_BINARY_CAS_CHUNK_BYTES
            ),
        ));
    }
    if uncompressed_len != expected_chunk_size as u64 {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' expected {} uncompressed bytes, row says {}",
                chunk_hash.to_hex(),
                blob_hash.to_hex(),
                expected_chunk_size,
                uncompressed_len
            ),
        ));
    }
    let decoded = match codec {
        BinaryChunkCodec::Raw => chunk_payload,
        BinaryChunkCodec::Zstd => Cow::Owned(decode_zstd_chunk(
            chunk_hash,
            &chunk_payload,
            expected_chunk_size,
        )?),
    };
    if decoded.len() != expected_chunk_size {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' expected {} decoded bytes, got {}",
                chunk_hash.to_hex(),
                blob_hash.to_hex(),
                expected_chunk_size,
                decoded.len()
            ),
        ));
    }
    // Native zstd level 1 does not enable frame checksums. Always authenticate
    // decoded compressed bytes against the CAS key, including in release builds.
    if (matches!(codec, BinaryChunkCodec::Zstd) || cfg!(debug_assertions))
        && BlobHash::from_content(&decoded) != chunk_hash
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' failed content-address verification",
                chunk_hash.to_hex(),
                blob_hash.to_hex()
            ),
        ));
    }
    Ok(decoded)
}

pub(in crate::binary_cas) async fn stage_blob_write_skipping_existing_chunks<S>(
    chunking: BinaryCasChunking,
    store: &S,
    writes: &mut StorageWriteSet,
    blob_hashes: &mut HashSet<[u8; 32]>,
    chunk_keys: &mut HashSet<Vec<u8>>,
    bytes: &[u8],
    precomputed_hash: Option<BlobHash>,
) -> Result<BlobWriteReceipt, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let plan = prepare_blob_write(chunking, bytes, precomputed_hash)?;
    let receipt = plan.receipt.clone();
    if !blob_hashes.insert(plan.blob_hash.into_bytes()) {
        return Ok(receipt);
    }

    let chunks = prepare_chunks(bytes, &plan);
    let mut chunk_hashes_to_stage = missing_chunk_hashes(store, chunk_keys, &plan, &chunks).await?;
    stage_prepared_blob_write(writes, bytes, &plan, &chunks, |chunk_hash| {
        Ok(chunk_hashes_to_stage.remove(&chunk_hash))
    })?;
    Ok(receipt)
}

fn prepare_blob_write(
    chunking: BinaryCasChunking,
    bytes: &[u8],
    precomputed_hash: Option<BlobHash>,
) -> Result<BlobWritePlan, LixError> {
    let blob_hash = precomputed_hash.unwrap_or_else(|| BlobHash::from_content(bytes));
    if cfg!(debug_assertions)
        && precomputed_hash.is_some()
        && BlobHash::from_content(bytes) != blob_hash
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "binary CAS blob hash does not match blob contents".to_string(),
        ));
    }
    let (chunk_ranges, layout) = if bytes.is_empty() {
        (Vec::new(), BlobLayout::Empty)
    } else if bytes.len() <= INLINE_BINARY_CAS_MAX_BYTES {
        (Vec::new(), BlobLayout::Inline)
    } else {
        let chunk_ranges = fastcdc_chunk_ranges_with_chunking(bytes, chunking);
        let layout = match chunk_ranges.as_slice() {
            [] => unreachable!("non-empty blobs always have at least one chunk"),
            [(start, end)] => BlobLayout::SingleChunk {
                chunk_hash: if *start == 0 && *end == bytes.len() {
                    blob_hash
                } else {
                    BlobHash::from_content(&bytes[*start..*end])
                },
            },
            _ => BlobLayout::Chunked {
                chunk_count: u32::try_from(chunk_ranges.len()).map_err(|_| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "binary CAS blob has too many chunks for manifest".to_string(),
                    )
                })?,
            },
        };
        (chunk_ranges, layout)
    };
    let receipt = BlobWriteReceipt {
        hash: blob_hash,
        size_bytes: bytes.len() as u64,
        layout: layout.clone(),
    };

    Ok(BlobWritePlan {
        blob_hash,
        chunk_ranges,
        layout,
        receipt,
    })
}

fn prepare_chunks(bytes: &[u8], plan: &BlobWritePlan) -> Vec<PreparedChunk> {
    if !matches!(plan.layout, BlobLayout::Chunked { .. }) {
        return Vec::new();
    }

    plan.chunk_ranges
        .iter()
        .map(|&(start, end)| PreparedChunk {
            start,
            end,
            hash: if start == 0 && end == bytes.len() {
                plan.blob_hash
            } else {
                BlobHash::from_content(&bytes[start..end])
            },
        })
        .collect()
}

fn stage_prepared_blob_write(
    writes: &mut StorageWriteSet,
    bytes: &[u8],
    plan: &BlobWritePlan,
    chunks: &[PreparedChunk],
    mut should_stage_chunk: impl FnMut(BlobHash) -> Result<bool, LixError>,
) -> Result<(), LixError> {
    match &plan.layout {
        BlobLayout::Empty => {
            stage_manifest(
                writes,
                plan.blob_hash,
                &BinaryCasManifest::Empty { size_bytes: 0 },
            );
        }
        BlobLayout::Inline => {
            let encoded = encode_chunk_payload(plan.blob_hash, bytes)?;
            writes.put(
                BINARY_CAS_MANIFEST_SPACE,
                key(manifest_key(plan.blob_hash)),
                value(encode_inline_binary_cas_manifest(
                    bytes.len() as u64,
                    encoded.codec,
                    &encoded.data,
                )),
            );
        }
        BlobLayout::SingleChunk { chunk_hash } => {
            let chunk_hash = *chunk_hash;
            stage_manifest(
                writes,
                plan.blob_hash,
                &BinaryCasManifest::SingleChunk {
                    size_bytes: bytes.len() as u64,
                    chunk_hash: chunk_hash.into_bytes(),
                },
            );
            if should_stage_chunk(chunk_hash)? {
                stage_content_chunk(writes, chunk_hash, bytes)?;
            }
        }
        BlobLayout::Chunked { chunk_count } => {
            stage_manifest(
                writes,
                plan.blob_hash,
                &BinaryCasManifest::Chunked {
                    size_bytes: bytes.len() as u64,
                    chunk_count: *chunk_count,
                },
            );

            for (chunk_index, chunk) in chunks.iter().copied().enumerate() {
                let chunk_data = &bytes[chunk.start..chunk.end];
                let chunk_hash = chunk.hash;
                if should_stage_chunk(chunk_hash)? {
                    stage_content_chunk(writes, chunk_hash, chunk_data)?;
                }

                stage_manifest_chunk(
                    writes,
                    plan.blob_hash,
                    chunk_index as u64,
                    &KvBlobManifestChunk {
                        chunk_hash: *chunk_hash.as_bytes(),
                        chunk_size: chunk_data.len() as u64,
                    },
                );
            }
        }
    }
    Ok(())
}

async fn missing_chunk_hashes(
    store: &(impl StorageAdapterRead + ?Sized),
    transaction_chunk_keys: &mut HashSet<Vec<u8>>,
    plan: &BlobWritePlan,
    chunks: &[PreparedChunk],
) -> Result<HashSet<BlobHash>, LixError> {
    let mut candidates = Vec::<(BlobHash, StorageKey)>::new();
    match &plan.layout {
        BlobLayout::Empty | BlobLayout::Inline => {}
        BlobLayout::SingleChunk { chunk_hash } => {
            collect_chunk_lookup_candidate(*chunk_hash, transaction_chunk_keys, &mut candidates);
        }
        BlobLayout::Chunked { .. } => {
            for chunk in chunks {
                collect_chunk_lookup_candidate(chunk.hash, transaction_chunk_keys, &mut candidates);
            }
        }
    }

    if candidates.is_empty() {
        return Ok(HashSet::new());
    }

    let keys = candidates
        .iter()
        .map(|(_, key)| key.clone())
        .collect::<Vec<_>>();
    let existing = chunk_keys_exist(store, keys).await?;
    Ok(candidates
        .into_iter()
        .zip(existing)
        .filter_map(|((chunk_hash, _), exists)| (!exists).then_some(chunk_hash))
        .collect())
}

fn collect_chunk_lookup_candidate(
    chunk_hash: BlobHash,
    transaction_chunk_keys: &mut HashSet<Vec<u8>>,
    candidates: &mut Vec<(BlobHash, StorageKey)>,
) {
    let key = chunk_key(chunk_hash);
    if !transaction_chunk_keys.insert(key.clone()) {
        crate::binary_cas::metrics::record_binary_cas_transaction_duplicate_chunk();
        return;
    }
    candidates.push((chunk_hash, StorageKey(Bytes::from(key))));
}

async fn chunk_keys_exist(
    store: &(impl StorageAdapterRead + ?Sized),
    keys: Vec<StorageKey>,
) -> Result<Vec<bool>, LixError> {
    let started = Instant::now();
    let result = PointReadPlan::from_unique_keys(BINARY_CAS_CHUNK_PRESENCE_SPACE, keys)
        .materialize(
            store,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?;
    let exists = result
        .value
        .into_iter()
        .map(|value| value.is_some())
        .collect::<Vec<_>>();
    let hit_count = exists.iter().filter(|&&exists| exists).count() as u64;
    let miss_count = exists.len() as u64 - hit_count;
    crate::binary_cas::metrics::record_binary_cas_chunk_lookup_batch(
        hit_count,
        miss_count,
        started.elapsed(),
    );
    Ok(exists)
}

fn metadata_from_manifest(
    hash: BlobHash,
    manifest: BinaryCasManifest,
) -> Result<BlobMetadata, LixError> {
    let size_bytes = manifest.size_bytes();
    let (layout, inline_blob) = match manifest {
        BinaryCasManifest::Empty { size_bytes } => {
            if size_bytes != 0 {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS empty blob '{}' has nonzero size {size_bytes}",
                        hash.to_hex()
                    ),
                ));
            }
            (BlobLayout::Empty, None)
        }
        BinaryCasManifest::Inline {
            size_bytes,
            codec,
            payload,
        } => {
            if size_bytes == 0 || size_bytes > INLINE_BINARY_CAS_MAX_BYTES as u64 {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS inline blob '{}' has invalid size {size_bytes}",
                        hash.to_hex()
                    ),
                ));
            }
            (BlobLayout::Inline, Some(InlineBlob { codec, payload }))
        }
        BinaryCasManifest::SingleChunk { chunk_hash, .. } => (
            BlobLayout::SingleChunk {
                chunk_hash: BlobHash::from_bytes(chunk_hash),
            },
            None,
        ),
        BinaryCasManifest::Chunked { chunk_count, .. } => {
            (BlobLayout::Chunked { chunk_count }, None)
        }
    };
    Ok(BlobMetadata {
        hash,
        size_bytes,
        layout,
        inline_blob,
    })
}

fn manifest_key(blob_hash: BlobHash) -> Vec<u8> {
    blob_hash.as_bytes().to_vec()
}

fn manifest_chunk_prefix(blob_hash: BlobHash) -> Vec<u8> {
    blob_hash.as_bytes().to_vec()
}

fn manifest_chunk_key(blob_hash: BlobHash, chunk_index: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    out.extend_from_slice(blob_hash.as_bytes());
    out.extend_from_slice(&chunk_index.to_be_bytes());
    out
}

fn chunk_key(chunk_hash: BlobHash) -> Vec<u8> {
    chunk_hash.as_bytes().to_vec()
}

fn persisted_size_to_usize(size: u64, label: &str) -> Result<usize, LixError> {
    usize::try_from(size).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{label} size {size} does not fit in this runtime"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Bound;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::sync::Notify;

    fn definitely_multi_chunk_blob_bytes() -> Vec<u8> {
        (0..5_000_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>()
    }

    fn deterministic_high_entropy_bytes(len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len);
        let mut counter = 0u64;
        while out.len() < len {
            out.extend_from_slice(blake3::hash(&counter.to_le_bytes()).as_bytes());
            counter += 1;
        }
        out.truncate(len);
        out
    }

    use crate::binary_cas::BinaryCasContext;
    use crate::binary_cas::BlobPayload;
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{
        Memory, StorageError, StorageGetManyResult, StorageKeyRange, StorageReadOptions,
        StorageScanChunk, StorageWriteOptions, StorageWriteSet,
    };

    struct DelayedManifestScanRead<R> {
        inner: R,
        default_manifest_delay: Duration,
        blocked_manifest: Option<(BlobHash, BlobHash)>,
        blocked_manifest_release: Notify,
        active_manifest_scans: AtomicUsize,
        max_active_manifest_scans: AtomicUsize,
        manifest_scan_calls: AtomicUsize,
        chunk_get_many_calls: AtomicUsize,
        presence_get_many_calls: AtomicUsize,
        chunk_keys_requested: AtomicUsize,
        completed_manifest_hashes: Mutex<Vec<BlobHash>>,
    }

    impl<R> DelayedManifestScanRead<R> {
        fn new(inner: R, default_manifest_delay: Duration) -> Self {
            Self {
                inner,
                default_manifest_delay,
                blocked_manifest: None,
                blocked_manifest_release: Notify::new(),
                active_manifest_scans: AtomicUsize::new(0),
                max_active_manifest_scans: AtomicUsize::new(0),
                manifest_scan_calls: AtomicUsize::new(0),
                chunk_get_many_calls: AtomicUsize::new(0),
                presence_get_many_calls: AtomicUsize::new(0),
                chunk_keys_requested: AtomicUsize::new(0),
                completed_manifest_hashes: Mutex::new(Vec::new()),
            }
        }

        fn block_manifest_until(
            mut self,
            blocked_manifest: BlobHash,
            completed_manifest: BlobHash,
        ) -> Self {
            self.blocked_manifest = Some((blocked_manifest, completed_manifest));
            self
        }
    }

    impl<R> StorageAdapterRead for DelayedManifestScanRead<R>
    where
        R: StorageAdapterRead,
    {
        async fn get_many(
            &self,
            space: StorageSpaceId,
            keys: &[StorageKey],
            opts: StorageGetOptions,
        ) -> Result<StorageGetManyResult, StorageError> {
            if space == BINARY_CAS_CHUNK_SPACE.id {
                self.chunk_get_many_calls.fetch_add(1, Ordering::Relaxed);
                self.chunk_keys_requested
                    .fetch_add(keys.len(), Ordering::Relaxed);
            }
            if space == BINARY_CAS_CHUNK_PRESENCE_SPACE.id {
                self.presence_get_many_calls.fetch_add(1, Ordering::Relaxed);
            }
            self.inner.get_many(space, keys, opts).await
        }

        async fn scan(
            &self,
            space: StorageSpaceId,
            range: StorageKeyRange,
            opts: StorageScanOptions,
        ) -> Result<StorageScanChunk, StorageError> {
            let is_manifest_scan = space == BINARY_CAS_MANIFEST_CHUNK_SPACE.id;
            let manifest_hash = if is_manifest_scan {
                manifest_hash_from_range(&range)
            } else {
                None
            };
            if is_manifest_scan {
                self.manifest_scan_calls.fetch_add(1, Ordering::Relaxed);
                let active = self.active_manifest_scans.fetch_add(1, Ordering::Relaxed) + 1;
                self.max_active_manifest_scans
                    .fetch_max(active, Ordering::Relaxed);
                if self
                    .blocked_manifest
                    .as_ref()
                    .is_some_and(|(blocked, _)| Some(*blocked) == manifest_hash)
                {
                    self.blocked_manifest_release.notified().await;
                } else if !self.default_manifest_delay.is_zero() {
                    tokio::time::sleep(self.default_manifest_delay).await;
                }
            }
            let result = self.inner.scan(space, range, opts).await;
            if is_manifest_scan {
                self.active_manifest_scans.fetch_sub(1, Ordering::Relaxed);
                if let Some(manifest_hash) = manifest_hash {
                    self.completed_manifest_hashes
                        .lock()
                        .expect("completed manifest lock")
                        .push(manifest_hash);
                    if self
                        .blocked_manifest
                        .as_ref()
                        .is_some_and(|(_, completed)| *completed == manifest_hash)
                    {
                        self.blocked_manifest_release.notify_one();
                    }
                }
            }
            result
        }
    }

    fn manifest_hash_from_range(range: &StorageKeyRange) -> Option<BlobHash> {
        let Bound::Included(StorageKey(bytes)) = &range.lower else {
            return None;
        };
        let hash = <[u8; 32]>::try_from(bytes.get(..32)?).ok()?;
        Some(BlobHash::from_bytes(hash))
    }

    fn stage_two_chunk_blob(writes: &mut StorageWriteSet, ordinal: usize) -> (BlobHash, Vec<u8>) {
        let left = format!("blob-{ordinal}-left").into_bytes();
        let right = format!("blob-{ordinal}-right").into_bytes();
        let bytes = [left.as_slice(), right.as_slice()].concat();
        let blob_hash = BlobHash::from_content(&bytes);
        let chunks = [left, right];

        stage_manifest(
            writes,
            blob_hash,
            &BinaryCasManifest::Chunked {
                size_bytes: bytes.len() as u64,
                chunk_count: u32::try_from(chunks.len())
                    .expect("test chunk count should fit in u32"),
            },
        );
        for (index, chunk) in chunks.iter().enumerate() {
            let chunk_hash = BlobHash::from_content(chunk);
            stage_manifest_chunk(
                writes,
                blob_hash,
                index as u64,
                &KvBlobManifestChunk {
                    chunk_hash: chunk_hash.into_bytes(),
                    chunk_size: chunk.len() as u64,
                },
            );
            stage_chunk(
                writes,
                chunk_hash,
                BinaryChunkCodec::Raw,
                chunk.len() as u64,
                chunk,
            );
        }
        (blob_hash, bytes)
    }

    fn stage_incomplete_manifest(
        writes: &mut StorageWriteSet,
        label: &[u8],
        declared_chunk_count: u32,
    ) -> BlobHash {
        let blob_hash = BlobHash::from_content(label);
        let chunk_hash = BlobHash::from_content(label);
        stage_manifest(
            writes,
            blob_hash,
            &BinaryCasManifest::Chunked {
                size_bytes: label.len() as u64,
                chunk_count: declared_chunk_count,
            },
        );
        stage_manifest_chunk(
            writes,
            blob_hash,
            0,
            &KvBlobManifestChunk {
                chunk_hash: chunk_hash.into_bytes(),
                chunk_size: label.len() as u64,
            },
        );
        blob_hash
    }

    async fn stage_test_payload(
        storage: &StorageAdapter<Memory>,
        writes: &mut StorageWriteSet,
        payload: &BlobPayload,
    ) -> BlobWriteReceipt {
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("test blob read should open");
        BinaryCasContext::new()
            .writer_skipping_existing_chunks(&store, writes)
            .stage_payload(payload)
            .await
            .expect("test blob write should stage")
    }

    async fn stage_test_bytes(
        storage: &StorageAdapter<Memory>,
        writes: &mut StorageWriteSet,
        bytes: &[u8],
    ) -> BlobWriteReceipt {
        stage_test_payload(storage, writes, &BlobPayload::from_bytes(bytes.to_vec())).await
    }

    #[tokio::test]
    async fn stores_manifest_chunks_in_scan_order() {
        let storage = StorageAdapter::new(Memory::new());
        let blob_hash = BlobHash::from_content(b"blob-a");
        let chunk_a_hash = BlobHash::from_content(b"chunk-a").into_bytes();
        let chunk_b_hash = BlobHash::from_content(b"chunk-b").into_bytes();

        {
            let mut writes = storage.new_write_set();
            stage_manifest(
                &mut writes,
                blob_hash,
                &BinaryCasManifest::Chunked {
                    size_bytes: 12,
                    chunk_count: 2,
                },
            );
            stage_manifest_chunk(
                &mut writes,
                blob_hash,
                1,
                &KvBlobManifestChunk {
                    chunk_hash: chunk_b_hash,
                    chunk_size: 6,
                },
            );
            stage_manifest_chunk(
                &mut writes,
                blob_hash,
                0,
                &KvBlobManifestChunk {
                    chunk_hash: chunk_a_hash,
                    chunk_size: 6,
                },
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("manifest writes should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_manifest(&store, blob_hash)
                .await
                .expect("manifest should load"),
            Some(BinaryCasManifest::Chunked {
                size_bytes: 12,
                chunk_count: 2,
            })
        );
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            scan_manifest_chunks(&store, blob_hash)
                .await
                .expect("manifest chunks should scan"),
            vec![
                KvBlobManifestChunk {
                    chunk_hash: chunk_a_hash,
                    chunk_size: 6,
                },
                KvBlobManifestChunk {
                    chunk_hash: chunk_b_hash,
                    chunk_size: 6,
                },
            ]
        );
    }

    #[tokio::test]
    async fn batched_chunked_blob_reads_bound_and_overlap_manifest_scans() {
        let storage = StorageAdapter::new(Memory::new());
        let blob_count = MANIFEST_SCAN_CONCURRENCY + 3;
        let mut hashes = Vec::with_capacity(blob_count);
        let mut expected = Vec::with_capacity(blob_count);

        {
            let mut writes = storage.new_write_set();
            for ordinal in 0..blob_count {
                let (hash, bytes) = stage_two_chunk_blob(&mut writes, ordinal);
                hashes.push(hash);
                expected.push(Some(bytes));
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("chunked blob fixtures should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let delayed = DelayedManifestScanRead::new(store, Duration::from_millis(20));
        let actual = load_bytes_many(&delayed, &hashes)
            .await
            .expect("chunked blobs should load")
            .into_vec();

        assert_eq!(
            actual, expected,
            "batch results should retain request order"
        );
        assert_eq!(
            delayed.manifest_scan_calls.load(Ordering::Relaxed),
            blob_count
        );
        assert_eq!(
            delayed.max_active_manifest_scans.load(Ordering::Relaxed),
            MANIFEST_SCAN_CONCURRENCY,
            "the batch should fill, but never exceed, the manifest scan bound"
        );
        assert_eq!(
            delayed.chunk_get_many_calls.load(Ordering::Relaxed),
            1,
            "manifest fan-out should still feed one batched chunk point read"
        );
        assert_eq!(
            delayed.chunk_keys_requested.load(Ordering::Relaxed),
            blob_count * 2
        );
    }

    #[tokio::test]
    async fn duplicate_chunked_blob_requests_share_manifest_and_chunk_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let (blob_hash, bytes) = {
            let mut writes = storage.new_write_set();
            let fixture = stage_two_chunk_blob(&mut writes, 0);
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("chunked blob fixture should commit");
            fixture
        };
        let missing_hash = BlobHash::from_content(b"missing duplicate fixture");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let delayed = DelayedManifestScanRead::new(store, Duration::from_millis(5));
        let actual = load_bytes_many(&delayed, &[blob_hash, missing_hash, blob_hash])
            .await
            .expect("duplicate chunked blobs should load")
            .into_vec();

        assert_eq!(
            actual,
            vec![Some(bytes.clone()), None, Some(bytes)],
            "deduplication must retain every requested output slot"
        );
        assert_eq!(
            delayed.manifest_scan_calls.load(Ordering::Relaxed),
            1,
            "one chunked hash should issue one manifest scan"
        );
        assert_eq!(delayed.chunk_get_many_calls.load(Ordering::Relaxed), 1);
        assert_eq!(delayed.chunk_keys_requested.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn slow_first_manifest_does_not_block_scan_window_refill() {
        let storage = StorageAdapter::new(Memory::new());
        let blob_count = MANIFEST_SCAN_CONCURRENCY + 3;
        let mut hashes = Vec::with_capacity(blob_count);
        {
            let mut writes = storage.new_write_set();
            for ordinal in 0..blob_count {
                hashes.push(stage_two_chunk_blob(&mut writes, ordinal).0);
            }
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("chunked blob fixtures should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let release_after = *hashes.last().expect("fixture should not be empty");
        let delayed = DelayedManifestScanRead::new(store, Duration::ZERO)
            .block_manifest_until(hashes[0], release_after);
        tokio::time::timeout(Duration::from_secs(5), load_bytes_many(&delayed, &hashes))
            .await
            .expect("unordered scan window should refill before the timeout")
            .expect("skewed chunked blobs should load");

        let completed = delayed
            .completed_manifest_hashes
            .lock()
            .expect("completed manifest lock");
        let slow_position = completed
            .iter()
            .position(|hash| *hash == hashes[0])
            .expect("slow manifest should complete");
        for hash in &hashes[MANIFEST_SCAN_CONCURRENCY..] {
            let position = completed
                .iter()
                .position(|completed_hash| completed_hash == hash)
                .expect("refilled manifest should complete");
            assert!(
                position < slow_position,
                "a scan beyond the initial window should complete before the slow first scan"
            );
        }
        let max_active = delayed.max_active_manifest_scans.load(Ordering::Relaxed);
        assert!(
            (2..=MANIFEST_SCAN_CONCURRENCY).contains(&max_active),
            "skewed scans should overlap without exceeding the concurrency cap; observed {max_active}"
        );
    }

    #[tokio::test]
    async fn concurrent_manifest_scan_errors_follow_request_order() {
        let storage = StorageAdapter::new(Memory::new());
        let (first, second) = {
            let mut writes = storage.new_write_set();
            let first = stage_incomplete_manifest(&mut writes, b"first-invalid", 2);
            let second = stage_incomplete_manifest(&mut writes, b"second-invalid", 3);
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("invalid manifest fixtures should commit");
            (first, second)
        };

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let delayed =
            DelayedManifestScanRead::new(store, Duration::ZERO).block_manifest_until(first, second);
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            load_bytes_many(&delayed, &[first, second]),
        )
        .await
        .expect("the later manifest should release the first before timeout")
        .expect_err("the first requested malformed manifest should fail");

        assert_eq!(
            *delayed
                .completed_manifest_hashes
                .lock()
                .expect("completed manifest lock"),
            vec![second, first],
            "the later malformed manifest should complete first"
        );
        assert!(
            error.message.contains(&first.to_hex()),
            "later scan completion must not replace the first requested error: {error:?}"
        );
        assert!(error.message.contains("expected 2 chunks, found 1"));
        assert_eq!(
            delayed.max_active_manifest_scans.load(Ordering::Relaxed),
            2,
            "the later manifest should finish while the first scan is delayed"
        );
        assert_eq!(
            delayed.chunk_get_many_calls.load(Ordering::Relaxed),
            0,
            "manifest validation should still precede the batched chunk fetch"
        );
    }

    #[tokio::test]
    async fn stores_encoded_chunks_by_chunk_hash() {
        let storage = StorageAdapter::new(Memory::new());
        let chunk = KvChunk {
            codec: BinaryChunkCodec::Raw,
            uncompressed_len: 5,
            data: b"hello".to_vec(),
        };
        let chunk_hash = BlobHash::from_content(b"chunk-a");

        {
            let mut writes = storage.new_write_set();
            stage_chunk(
                &mut writes,
                chunk_hash,
                chunk.codec,
                chunk.uncompressed_len,
                &chunk.data,
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("chunk should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_chunk(&store, chunk_hash)
                .await
                .expect("chunk should load"),
            Some(chunk)
        );
    }

    #[test]
    fn binary_hash_keys_are_compact_and_manifest_chunks_sort_by_index() {
        let blob_hash = BlobHash::from_content(b"blob");
        let manifest_key = manifest_key(blob_hash);
        let chunk_key = chunk_key(BlobHash::from_content(b"chunk"));
        let first = manifest_chunk_key(blob_hash, 1);
        let second = manifest_chunk_key(blob_hash, 2);
        let later = manifest_chunk_key(blob_hash, 10);

        assert_eq!(manifest_key.len(), 32);
        assert_eq!(chunk_key.len(), 32);
        assert_eq!(first.len(), 40);
        assert!(first < second);
        assert!(second < later);
    }

    #[test]
    fn inline_layout_stops_at_the_32kib_boundary() {
        let at_boundary = vec![b'a'; INLINE_BINARY_CAS_MAX_BYTES];
        let above_boundary = vec![b'a'; INLINE_BINARY_CAS_MAX_BYTES + 1];

        let inline = prepare_blob_write(BinaryCasChunking::default(), &at_boundary, None)
            .expect("boundary blob should plan");
        let out_of_line = prepare_blob_write(BinaryCasChunking::default(), &above_boundary, None)
            .expect("above-boundary blob should plan");

        assert_eq!(inline.layout, BlobLayout::Inline);
        assert!(inline.chunk_ranges.is_empty());
        assert!(matches!(out_of_line.layout, BlobLayout::SingleChunk { .. }));
        assert_eq!(out_of_line.chunk_ranges, vec![(0, above_boundary.len())]);
    }

    #[test]
    fn inline_manifest_rejects_sizes_outside_the_format_boundary() {
        let hash = BlobHash::from_content(b"invalid inline");
        for size_bytes in [0, INLINE_BINARY_CAS_MAX_BYTES as u64 + 1] {
            let error = metadata_from_manifest(
                hash,
                BinaryCasManifest::Inline {
                    size_bytes,
                    codec: BinaryChunkCodec::Raw,
                    payload: Vec::new(),
                },
            )
            .expect_err("invalid inline size should be rejected");
            assert!(error.message.contains("invalid size"));
        }
    }

    #[tokio::test]
    async fn public_kv_api_roundtrips_blob_bytes() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"hello chunked kv cas";
        let blob_hash = BlobHash::from_content(data);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("blob should load")
                .into_vec(),
            vec![Some(data.to_vec())]
        );
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_manifest(&store, blob_hash)
                .await
                .expect("manifest should load"),
            Some(BinaryCasManifest::Inline {
                size_bytes: data.len() as u64,
                codec: BinaryChunkCodec::Raw,
                payload: data.to_vec(),
            })
        );
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            scan_manifest_chunks(&store, blob_hash)
                .await
                .expect("inline blob should not spill manifest chunks"),
            Vec::<KvBlobManifestChunk>::new()
        );
    }

    #[tokio::test]
    async fn public_kv_api_compresses_repetitive_inline_blob() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"component-section:function-signature\n".repeat(512);
        let blob_hash = BlobHash::from_content(&data);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let stored = load_manifest(&store, blob_hash)
            .await
            .expect("manifest should load")
            .expect("manifest should exist");
        let BinaryCasManifest::Inline {
            size_bytes,
            codec,
            payload,
        } = stored
        else {
            panic!("small blob should be inline");
        };
        assert_eq!(codec, BinaryChunkCodec::Zstd);
        assert_eq!(size_bytes, data.len() as u64);
        assert!(payload.len() < data.len() / 4);

        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("blob should load")
                .into_vec(),
            vec![Some(data)]
        );
    }

    #[tokio::test]
    async fn public_kv_api_keeps_high_entropy_inline_blob_raw() {
        let storage = StorageAdapter::new(Memory::new());
        let data = deterministic_high_entropy_bytes(32 * 1024);
        let blob_hash = BlobHash::from_content(&data);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let stored = load_manifest(&store, blob_hash)
            .await
            .expect("manifest should load")
            .expect("manifest should exist");
        assert_eq!(
            stored,
            BinaryCasManifest::Inline {
                size_bytes: data.len() as u64,
                codec: BinaryChunkCodec::Raw,
                payload: data,
            }
        );
    }

    #[tokio::test]
    async fn inline_writer_stages_one_row_without_chunk_presence_reads() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"hello chunked kv cas";
        let payload = BlobPayload::from_bytes(data.to_vec());
        let blob_hash = payload.hash().expect("payload should have a hash");

        {
            let mut writes = storage.new_write_set();
            stage_test_payload(&storage, &mut writes, &payload).await;
            assert_eq!(writes.stats().staged_puts, 1);
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("initial blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            get_one(
                &store,
                BINARY_CAS_CHUNK_PRESENCE_SPACE,
                chunk_key(BlobHash::from_content(data)),
            )
            .await
            .expect("chunk presence marker should load"),
            None
        );
        assert_eq!(
            get_one(
                &store,
                BINARY_CAS_CHUNK_SPACE,
                chunk_key(BlobHash::from_content(data)),
            )
            .await
            .expect("chunk row lookup should succeed"),
            None
        );
        let counted = DelayedManifestScanRead::new(store, Duration::ZERO);
        let mut writes = storage.new_write_set();
        let mut writer =
            BinaryCasContext::new().writer_skipping_existing_chunks(&counted, &mut writes);
        writer
            .stage_payload(&payload)
            .await
            .expect("repeat blob write should stage");

        assert_eq!(
            writes.stats().staged_puts,
            1,
            "an inline repeat write should deterministically replace one manifest"
        );
        assert_eq!(
            counted.presence_get_many_calls.load(Ordering::Relaxed),
            0,
            "inline blobs must not probe the out-of-line chunk presence space"
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repeat blob write should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let counted = DelayedManifestScanRead::new(store, Duration::ZERO);
        assert_eq!(
            load_bytes_many(&counted, &[blob_hash])
                .await
                .expect("blob should load")
                .into_vec(),
            vec![Some(data.to_vec())]
        );
        assert_eq!(
            counted.chunk_get_many_calls.load(Ordering::Relaxed),
            0,
            "inline blob reads must finish from the manifest point read"
        );
    }

    #[tokio::test]
    async fn existing_chunk_aware_writer_batches_persisted_chunk_checks() {
        let storage = StorageAdapter::new(Memory::new());
        let data = definitely_multi_chunk_blob_bytes();
        let payload = BlobPayload::from_bytes(data.clone());
        let blob_hash = payload.hash().expect("payload should have a hash");
        let chunk_ranges = crate::binary_cas::chunking::fastcdc_chunk_ranges(&data);
        assert!(chunk_ranges.len() > 1);
        let chunk_hashes = chunk_ranges
            .iter()
            .map(|(start, end)| BlobHash::from_content(&data[*start..*end]))
            .collect::<HashSet<_>>();

        {
            let mut writes = storage.new_write_set();
            stage_test_payload(&storage, &mut writes, &payload).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("initial blob write should commit");
        }

        crate::binary_cas::metrics::reset_binary_cas_write_metrics();
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer =
            BinaryCasContext::new().writer_skipping_existing_chunks(&store, &mut writes);
        writer
            .stage_payload(&payload)
            .await
            .expect("repeat blob write should stage");

        assert_eq!(
            writes.stats().staged_puts,
            1 + u64::try_from(chunk_ranges.len()).expect("chunk count should fit in u64")
        );
        let metrics = crate::binary_cas::metrics::binary_cas_write_metrics_snapshot();
        // These counters are process-global test metrics. Other tests in this
        // binary can run concurrently, so assert this test's contribution
        // instead of requiring exclusive ownership of the counters.
        assert!(metrics.chunk_lookup_count >= chunk_hashes.len() as u64);
        assert!(metrics.chunk_lookup_batch_count >= 1);
        assert!(metrics.chunk_lookup_hit_count >= chunk_hashes.len() as u64);
        assert!(
            metrics.transaction_duplicate_chunk_count
                >= (chunk_ranges.len() - chunk_hashes.len()) as u64
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("repeat blob write should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("blob should load")
                .into_vec(),
            vec![Some(data)]
        );
    }

    #[test]
    fn prepared_blob_write_stages_duplicate_chunk_payload_once() {
        let data = b"abcabc";
        let blob_hash = BlobHash::from_content(data);
        let chunk_hash = BlobHash::from_content(b"abc");
        let plan = BlobWritePlan {
            blob_hash,
            chunk_ranges: vec![(0, 3), (3, 6)],
            layout: BlobLayout::Chunked { chunk_count: 2 },
            receipt: BlobWriteReceipt {
                hash: blob_hash,
                size_bytes: data.len() as u64,
                layout: BlobLayout::Chunked { chunk_count: 2 },
            },
        };
        let chunks = vec![
            PreparedChunk {
                start: 0,
                end: 3,
                hash: chunk_hash,
            },
            PreparedChunk {
                start: 3,
                end: 6,
                hash: chunk_hash,
            },
        ];
        let mut writes = StorageWriteSet::new();
        let mut chunk_hashes_to_stage = HashSet::from([chunk_hash]);

        stage_prepared_blob_write(&mut writes, data, &plan, &chunks, |chunk_hash| {
            Ok(chunk_hashes_to_stage.remove(&chunk_hash))
        })
        .expect("duplicate chunk payload write should stage");

        assert_eq!(writes.stats().staged_puts, 5);
        writes
            .validate()
            .expect("duplicate chunk payload should be staged only once");
    }

    #[tokio::test]
    async fn public_kv_api_accepts_precomputed_blob_hash() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"hello precomputed hash";
        let payload = BlobPayload::from_bytes(data.to_vec());
        let blob_hash = payload
            .hash()
            .expect("non-empty payload should have blob hash");

        {
            let mut writes = storage.new_write_set();
            stage_test_payload(&storage, &mut writes, &payload).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("blob should load")
                .into_vec(),
            vec![Some(data.to_vec())]
        );
        assert_eq!(
            load_manifest(&store, blob_hash)
                .await
                .expect("manifest should load"),
            Some(BinaryCasManifest::Inline {
                size_bytes: data.len() as u64,
                codec: BinaryChunkCodec::Raw,
                payload: data.to_vec(),
            })
        );
    }

    #[tokio::test]
    async fn read_rejects_chunk_bytes_that_do_not_match_manifest_hash() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"same length";
        let corrupted = b"SAME length";
        let blob_hash = BlobHash::from_content(data);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        {
            let mut writes = storage.new_write_set();
            writes.put(
                BINARY_CAS_MANIFEST_SPACE,
                key(manifest_key(blob_hash)),
                value(encode_inline_binary_cas_manifest(
                    corrupted.len() as u64,
                    BinaryChunkCodec::Raw,
                    corrupted,
                )),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("corrupt manifest should overwrite");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = load_bytes_many(&store, &[blob_hash])
            .await
            .expect_err("corrupt chunk should be rejected");
        assert!(
            error
                .message
                .contains("failed content-address verification")
        );
    }

    #[tokio::test]
    async fn read_rejects_truncated_zstd_inline_payload() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"compressible binary CAS bytes".repeat(1024);
        let blob_hash = BlobHash::from_content(&data);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        let encoded = encode_chunk_payload(blob_hash, &data).expect("chunk should encode");
        assert_eq!(encoded.codec, BinaryChunkCodec::Zstd);
        let mut corrupted = encoded.data.into_owned();
        corrupted.truncate(corrupted.len() / 2);
        {
            let mut writes = storage.new_write_set();
            writes.put(
                BINARY_CAS_MANIFEST_SPACE,
                key(manifest_key(blob_hash)),
                value(encode_inline_binary_cas_manifest(
                    data.len() as u64,
                    BinaryChunkCodec::Zstd,
                    &corrupted,
                )),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("corrupt inline manifest should overwrite");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = load_bytes_many(&store, &[blob_hash])
            .await
            .expect_err("corrupt chunk should be rejected");
        assert!(error.message.contains("decompression failed"));
    }

    #[test]
    fn decode_rejects_same_length_valid_zstd_frame_for_wrong_hash() {
        let expected = vec![b'a'; 128 * 1024];
        let substituted = vec![b'b'; expected.len()];
        assert_eq!(expected.len(), substituted.len());
        let expected_hash = BlobHash::from_content(&expected);
        let substituted_hash = BlobHash::from_content(&substituted);
        let encoded = encode_chunk_payload(substituted_hash, &substituted)
            .expect("substituted chunk should encode");
        assert_eq!(encoded.codec, BinaryChunkCodec::Zstd);
        let row =
            encode_binary_cas_chunk(BinaryChunkCodec::Zstd, expected.len() as u64, &encoded.data);

        let error = decode_and_verify_chunk(&row, expected.len(), expected_hash, expected_hash)
            .expect_err("valid zstd frame for different bytes should be rejected");

        assert!(
            error
                .message
                .contains("failed content-address verification")
        );
    }

    #[test]
    fn decode_rejects_chunks_above_the_format_maximum_before_decompression() {
        let data = b"valid compressed content".repeat(4096);
        let chunk_hash = BlobHash::from_content(&data);
        let encoded = encode_chunk_payload(chunk_hash, &data).expect("chunk should encode");
        assert_eq!(encoded.codec, BinaryChunkCodec::Zstd);
        let oversized_len = MAX_BINARY_CAS_CHUNK_BYTES + 1;
        let row =
            encode_binary_cas_chunk(BinaryChunkCodec::Zstd, oversized_len as u64, &encoded.data);

        let error = decode_and_verify_chunk(&row, oversized_len, chunk_hash, chunk_hash)
            .expect_err("oversized chunk metadata should be rejected");

        assert!(error.message.contains("exceeds"));
        assert!(error.message.contains("format maximum"));
    }

    #[tokio::test]
    async fn read_rejects_manifest_that_assembles_wrong_blob_hash() {
        let storage = StorageAdapter::new(Memory::new());
        let expected = b"expected bytes";
        let substituted = b"different byte";
        assert_eq!(expected.len(), substituted.len());
        let expected_blob_hash = BlobHash::from_content(expected);
        let substituted_chunk_hash = BlobHash::from_content(substituted);

        {
            let mut writes = storage.new_write_set();
            stage_manifest(
                &mut writes,
                expected_blob_hash,
                &BinaryCasManifest::Chunked {
                    size_bytes: expected.len() as u64,
                    chunk_count: 1,
                },
            );
            stage_manifest_chunk(
                &mut writes,
                expected_blob_hash,
                0,
                &KvBlobManifestChunk {
                    chunk_hash: BlobHash::from_content(substituted).into_bytes(),
                    chunk_size: substituted.len() as u64,
                },
            );
            stage_chunk(
                &mut writes,
                substituted_chunk_hash,
                BinaryChunkCodec::Raw,
                substituted.len() as u64,
                substituted,
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("wrong manifest fixture should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let error = load_bytes_many(&store, &[expected_blob_hash])
            .await
            .expect_err("wrong assembled blob should be rejected");
        assert!(
            error
                .message
                .contains("failed content-address verification")
        );
    }

    #[tokio::test]
    async fn public_kv_api_roundtrips_empty_blob() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"";
        let blob_hash = BlobHash::from_content(data);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("empty blob should load")
                .into_vec(),
            vec![Some(Vec::new())]
        );
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            scan_manifest_chunks(&store, blob_hash)
                .await
                .expect("empty blob chunks should scan"),
            Vec::<KvBlobManifestChunk>::new()
        );
    }

    #[tokio::test]
    async fn public_kv_api_roundtrips_multi_chunk_blob() {
        let storage = StorageAdapter::new(Memory::new());
        let data = definitely_multi_chunk_blob_bytes();
        let blob_hash = BlobHash::from_content(&data);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("large blob should load")
                .into_vec(),
            vec![Some(data.clone())]
        );
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert!(
            scan_manifest_chunks(&store, blob_hash)
                .await
                .expect("large blob chunks should scan")
                .len()
                > 1
        );
    }
}
