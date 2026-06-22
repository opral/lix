#![allow(clippy::cast_sign_loss)]

use crate::LixError;
use crate::binary_cas::chunking::fastcdc_chunk_ranges_with_chunking;
use crate::binary_cas::codec::{
    BinaryCasManifest, BinaryChunkCodec, decode_binary_cas_chunk, decode_binary_cas_manifest,
    decode_binary_cas_manifest_chunk, encode_binary_cas_chunk, encode_binary_cas_manifest,
    encode_binary_cas_manifest_chunk,
};
use crate::binary_cas::{
    BinaryCasChunking, BlobBytesBatch, BlobHash, BlobLayout, BlobMetadata, BlobMetadataBatch,
    BlobWriteReceipt,
};
use crate::storage::{PointReadPlan, ScanPlan, StorageRead, StorageSpace, StorageWriteSet};
use crate::storage::{
    StorageCoreProjection, StorageGetOptions, StorageKey, StoragePrefix, StorageProjectedValue,
    StorageScanOptions, StorageSpaceId, StorageValue,
};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

pub(crate) const BINARY_CAS_MANIFEST_NAMESPACE: &str = "binary_cas.manifest";
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_NAMESPACE: &str = "binary_cas.manifest_chunk";
pub(crate) const BINARY_CAS_CHUNK_NAMESPACE: &str = "binary_cas.chunk";
pub(crate) const BINARY_CAS_MANIFEST_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0005_0001), BINARY_CAS_MANIFEST_NAMESPACE);
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0005_0002),
    BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
);
pub(crate) const BINARY_CAS_CHUNK_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0005_0003), BINARY_CAS_CHUNK_NAMESPACE);

#[derive(Debug)]
struct BlobWritePlan {
    blob_hash: BlobHash,
    chunk_ranges: Vec<(usize, usize)>,
    layout: BlobLayout,
    receipt: BlobWriteReceipt,
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
    store: &impl StorageRead,
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
    store: &impl StorageRead,
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
    store: &impl StorageRead,
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
    writes.put(
        BINARY_CAS_CHUNK_SPACE,
        key(chunk_key(chunk_hash)),
        value(encode_binary_cas_chunk(codec, uncompressed_len, payload)),
    );
}

#[cfg(test)]
async fn get_one(
    store: &impl StorageRead,
    space: StorageSpace,
    key: Vec<u8>,
) -> Result<Option<Vec<u8>>, LixError> {
    let result = PointReadPlan::new(space, &[StorageKey(Bytes::from(key))])
        .materialize(store, StorageGetOptions::default())?;
    Ok(result
        .value
        .into_iter()
        .next()
        .flatten()
        .and_then(full_value))
}

async fn scan_all_values(
    store: &impl StorageRead,
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
        let page = plan.collect(
            store,
            StorageScanOptions {
                resume_after: resume_after.as_ref(),
                ..StorageScanOptions::default()
            },
        )?;
        resume_after = page.value.entries.last().map(|entry| entry.key.clone());
        values.extend(
            page.value
                .entries
                .into_iter()
                .filter_map(|entry| full_value(entry.value)),
        );
        if !page.value.has_more || resume_after.is_none() {
            break;
        }
    }
    Ok(values)
}

pub(crate) async fn load_metadata_many(
    store: &impl StorageRead,
    hashes: &[BlobHash],
) -> Result<BlobMetadataBatch, LixError> {
    if hashes.is_empty() {
        return Ok(BlobMetadataBatch::new(Vec::new()));
    }
    let rows = point_values(
        store,
        BINARY_CAS_MANIFEST_SPACE,
        hashes.iter().map(|hash| manifest_key(*hash)).collect(),
    )?;
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
    store: &impl StorageRead,
    hashes: &[BlobHash],
) -> Result<BlobBytesBatch, LixError> {
    let metadata = load_metadata_many(store, hashes).await?.into_vec();
    let mut chunked_manifests = Vec::new();
    let mut requested_chunks = Vec::new();
    let mut seen_chunks = HashSet::new();

    for (index, metadata) in metadata.iter().enumerate() {
        let Some(metadata) = metadata else {
            continue;
        };
        match &metadata.layout {
            BlobLayout::Empty => {}
            BlobLayout::SingleChunk { chunk_hash } => {
                if seen_chunks.insert(*chunk_hash) {
                    requested_chunks.push(*chunk_hash);
                }
            }
            BlobLayout::Chunked { chunk_count } => {
                let manifest_chunks = scan_manifest_chunks(store, metadata.hash).await?;
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
                for manifest_chunk in &manifest_chunks {
                    let chunk_hash = BlobHash::from_bytes(manifest_chunk.chunk_hash);
                    if seen_chunks.insert(chunk_hash) {
                        requested_chunks.push(chunk_hash);
                    }
                }
                chunked_manifests.push((index, manifest_chunks));
            }
        }
    }

    let chunk_rows = load_chunk_rows(store, &requested_chunks).await?;
    let chunk_rows_by_hash = requested_chunks
        .into_iter()
        .zip(chunk_rows)
        .collect::<HashMap<_, _>>();
    let chunked_manifests_by_index = chunked_manifests
        .into_iter()
        .collect::<HashMap<usize, Vec<KvBlobManifestChunk>>>();

    let entries = metadata
        .into_iter()
        .enumerate()
        .map(|(index, metadata)| {
            metadata
                .map(|metadata| {
                    assemble_blob_bytes(
                        &metadata,
                        &chunk_rows_by_hash,
                        chunked_manifests_by_index.get(&index),
                    )
                })
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BlobBytesBatch::new(entries))
}

async fn load_chunk_rows(
    store: &impl StorageRead,
    hashes: &[BlobHash],
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    point_values(
        store,
        BINARY_CAS_CHUNK_SPACE,
        hashes.iter().map(|hash| chunk_key(*hash)).collect(),
    )
}

fn point_values(
    store: &impl StorageRead,
    space: StorageSpace,
    keys: Vec<Vec<u8>>,
) -> Result<Vec<Option<Vec<u8>>>, LixError> {
    let keys = keys
        .into_iter()
        .map(|key| StorageKey(Bytes::from(key)))
        .collect::<Vec<_>>();
    let result =
        PointReadPlan::new(space, &keys).materialize(store, StorageGetOptions::default())?;
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

fn full_value(value: StorageProjectedValue) -> Option<Vec<u8>> {
    match value {
        StorageProjectedValue::FullValue(bytes) => Some(bytes.to_vec()),
        StorageProjectedValue::KeyOnly => None,
    }
}

fn assemble_blob_bytes(
    metadata: &BlobMetadata,
    chunk_rows_by_hash: &HashMap<BlobHash, Option<Vec<u8>>>,
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
            chunk
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
    chunk_rows_by_hash: &HashMap<BlobHash, Option<Vec<u8>>>,
    blob_hash: BlobHash,
    chunk_hash: BlobHash,
    expected_chunk_size: usize,
) -> Result<Vec<u8>, LixError> {
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
) -> Result<Vec<u8>, LixError> {
    let (codec, uncompressed_len, chunk_payload) = decode_binary_cas_chunk(chunk_bytes)?;
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
    let BinaryChunkCodec::Raw = codec;
    if chunk_payload.len() != expected_chunk_size {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' expected {} decoded bytes, got {}",
                chunk_hash.to_hex(),
                blob_hash.to_hex(),
                expected_chunk_size,
                chunk_payload.len()
            ),
        ));
    }
    if cfg!(debug_assertions) && BlobHash::from_content(chunk_payload) != chunk_hash {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS chunk '{}' for blob '{}' failed content-address verification",
                chunk_hash.to_hex(),
                blob_hash.to_hex()
            ),
        ));
    }
    Ok(chunk_payload.to_vec())
}

#[allow(dead_code)]
pub(in crate::binary_cas) fn stage_blob_write(
    chunking: BinaryCasChunking,
    writes: &mut StorageWriteSet,
    blob_hashes: &mut HashSet<[u8; 32]>,
    chunk_keys: &mut HashSet<Vec<u8>>,
    bytes: &[u8],
    precomputed_hash: Option<BlobHash>,
) -> Result<BlobWriteReceipt, LixError> {
    stage_blob_write_with_chunk_filter(
        chunking,
        writes,
        blob_hashes,
        bytes,
        precomputed_hash,
        |chunk_hash| Ok(chunk_keys.insert(chunk_key(chunk_hash))),
    )
}

pub(in crate::binary_cas) fn stage_blob_write_skipping_existing_chunks<S>(
    chunking: BinaryCasChunking,
    store: &S,
    writes: &mut StorageWriteSet,
    blob_hashes: &mut HashSet<[u8; 32]>,
    chunk_keys: &mut HashSet<Vec<u8>>,
    bytes: &[u8],
    precomputed_hash: Option<BlobHash>,
) -> Result<BlobWriteReceipt, LixError>
where
    S: StorageRead + ?Sized,
{
    let plan = prepare_blob_write(chunking, bytes, precomputed_hash)?;
    let receipt = plan.receipt.clone();
    if !blob_hashes.insert(plan.blob_hash.into_bytes()) {
        return Ok(receipt);
    }

    let mut chunk_hashes_to_stage = missing_chunk_hashes(store, chunk_keys, bytes, &plan)?;
    stage_prepared_blob_write(writes, bytes, &plan, |chunk_hash| {
        Ok(chunk_hashes_to_stage.remove(&chunk_hash))
    })?;
    Ok(receipt)
}

fn stage_blob_write_with_chunk_filter(
    chunking: BinaryCasChunking,
    writes: &mut StorageWriteSet,
    blob_hashes: &mut HashSet<[u8; 32]>,
    bytes: &[u8],
    precomputed_hash: Option<BlobHash>,
    mut should_stage_chunk: impl FnMut(BlobHash) -> Result<bool, LixError>,
) -> Result<BlobWriteReceipt, LixError> {
    let plan = prepare_blob_write(chunking, bytes, precomputed_hash)?;
    let receipt = plan.receipt.clone();
    if !blob_hashes.insert(plan.blob_hash.into_bytes()) {
        return Ok(receipt);
    }

    stage_prepared_blob_write(writes, bytes, &plan, |chunk_hash| {
        should_stage_chunk(chunk_hash)
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
    let chunk_ranges = fastcdc_chunk_ranges_with_chunking(bytes, chunking);
    let layout = match chunk_ranges.as_slice() {
        [] => BlobLayout::Empty,
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

fn stage_prepared_blob_write(
    writes: &mut StorageWriteSet,
    bytes: &[u8],
    plan: &BlobWritePlan,
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
                stage_chunk(
                    writes,
                    chunk_hash,
                    BinaryChunkCodec::Raw,
                    bytes.len() as u64,
                    bytes,
                );
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

            for (chunk_index, (start, end)) in plan.chunk_ranges.iter().copied().enumerate() {
                let chunk_data = &bytes[start..end];
                let chunk_hash = BlobHash::from_content(chunk_data);
                if should_stage_chunk(chunk_hash)? {
                    stage_chunk(
                        writes,
                        chunk_hash,
                        BinaryChunkCodec::Raw,
                        chunk_data.len() as u64,
                        chunk_data,
                    );
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

fn missing_chunk_hashes(
    store: &(impl StorageRead + ?Sized),
    transaction_chunk_keys: &mut HashSet<Vec<u8>>,
    bytes: &[u8],
    plan: &BlobWritePlan,
) -> Result<HashSet<BlobHash>, LixError> {
    let mut candidates = Vec::<(BlobHash, StorageKey)>::new();
    match &plan.layout {
        BlobLayout::Empty => {}
        BlobLayout::SingleChunk { chunk_hash } => {
            collect_chunk_lookup_candidate(*chunk_hash, transaction_chunk_keys, &mut candidates);
        }
        BlobLayout::Chunked { .. } => {
            for (start, end) in plan.chunk_ranges.iter().copied() {
                let chunk_hash = BlobHash::from_content(&bytes[start..end]);
                collect_chunk_lookup_candidate(chunk_hash, transaction_chunk_keys, &mut candidates);
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
    let existing = chunk_keys_exist(store, keys)?;
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

fn chunk_keys_exist(
    store: &(impl StorageRead + ?Sized),
    keys: Vec<StorageKey>,
) -> Result<Vec<bool>, LixError> {
    let started = Instant::now();
    let result = PointReadPlan::from_unique_keys(BINARY_CAS_CHUNK_SPACE, keys).materialize(
        store,
        StorageGetOptions {
            projection: StorageCoreProjection::KeyOnly,
            ..StorageGetOptions::default()
        },
    )?;
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
    let layout = match manifest {
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
            BlobLayout::Empty
        }
        BinaryCasManifest::SingleChunk { chunk_hash, .. } => BlobLayout::SingleChunk {
            chunk_hash: BlobHash::from_bytes(chunk_hash),
        },
        BinaryCasManifest::Chunked { chunk_count, .. } => BlobLayout::Chunked { chunk_count },
    };
    Ok(BlobMetadata {
        hash,
        size_bytes,
        layout,
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

    fn definitely_multi_chunk_blob_bytes() -> Vec<u8> {
        (0..5_000_000)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>()
    }
    use crate::binary_cas::BinaryCasContext;
    use crate::binary_cas::BlobPayload;
    use crate::storage::StorageContext;
    use crate::storage::{
        InMemoryStorageBackend, StorageReadOptions, StorageWriteOptions, StorageWriteSet,
    };

    #[tokio::test]
    async fn stores_manifest_chunks_in_scan_order() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
                .expect("manifest writes should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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
    async fn stores_encoded_chunks_by_chunk_hash() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
                .expect("chunk should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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

    #[tokio::test]
    async fn public_kv_api_roundtrips_blob_bytes() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let data = b"hello chunked kv cas";
        let blob_hash = BlobHash::from_content(data);

        {
            let mut writes = storage.new_write_set();
            let mut writer = BinaryCasContext::new().writer(&mut writes);
            writer.stage_bytes(data).expect("blob write should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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
            .expect("read should open");
        assert_eq!(
            load_manifest(&store, blob_hash)
                .await
                .expect("manifest should load"),
            Some(BinaryCasManifest::SingleChunk {
                size_bytes: data.len() as u64,
                chunk_hash: BlobHash::from_content(data).into_bytes(),
            })
        );
        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        assert_eq!(
            scan_manifest_chunks(&store, blob_hash)
                .await
                .expect("single-chunk blob should not spill manifest chunks"),
            Vec::<KvBlobManifestChunk>::new()
        );
    }

    #[tokio::test]
    async fn existing_chunk_aware_writer_skips_persisted_chunk_payloads() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let data = b"hello chunked kv cas";
        let payload = BlobPayload::from_bytes(data.to_vec());
        let blob_hash = payload.hash().expect("payload should have a hash");

        {
            let mut writes = storage.new_write_set();
            let mut writer = BinaryCasContext::new().writer(&mut writes);
            writer
                .stage_payload(&payload)
                .expect("initial blob write should stage");
            assert_eq!(writes.stats().staged_puts, 2);
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("initial blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer =
            BinaryCasContext::new().writer_skipping_existing_chunks(&store, &mut writes);
        writer
            .stage_payload(&payload)
            .expect("repeat blob write should stage");

        assert_eq!(writes.stats().staged_puts, 1);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("repeat blob write should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("blob should load")
                .into_vec(),
            vec![Some(data.to_vec())]
        );
    }

    #[tokio::test]
    async fn existing_chunk_aware_writer_batches_persisted_chunk_checks() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
            let mut writer = BinaryCasContext::new().writer(&mut writes);
            writer
                .stage_payload(&payload)
                .expect("initial blob write should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("initial blob write should commit");
        }

        crate::binary_cas::reset_binary_cas_write_metrics();
        let store = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mut writer =
            BinaryCasContext::new().writer_skipping_existing_chunks(&store, &mut writes);
        writer
            .stage_payload(&payload)
            .expect("repeat blob write should stage");

        assert_eq!(
            writes.stats().staged_puts,
            1 + u64::try_from(chunk_ranges.len()).expect("chunk count should fit in u64")
        );
        let metrics = crate::binary_cas::binary_cas_write_metrics_snapshot();
        assert_eq!(metrics.chunk_lookup_count, chunk_hashes.len() as u64);
        assert_eq!(metrics.chunk_lookup_batch_count, 1);
        assert_eq!(metrics.chunk_lookup_hit_count, chunk_hashes.len() as u64);
        assert_eq!(metrics.chunk_lookup_miss_count, 0);
        assert_eq!(
            metrics.transaction_duplicate_chunk_count,
            (chunk_ranges.len() - chunk_hashes.len()) as u64
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .expect("repeat blob write should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
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
        let mut writes = StorageWriteSet::new();
        let mut chunk_hashes_to_stage = HashSet::from([chunk_hash]);

        stage_prepared_blob_write(&mut writes, data, &plan, |chunk_hash| {
            Ok(chunk_hashes_to_stage.remove(&chunk_hash))
        })
        .expect("duplicate chunk payload write should stage");

        assert_eq!(writes.stats().staged_puts, 4);
        writes
            .validate()
            .expect("duplicate chunk payload should be staged only once");
    }

    #[tokio::test]
    async fn public_kv_api_accepts_precomputed_blob_hash() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let data = b"hello precomputed hash";
        let payload = BlobPayload::from_bytes(data.to_vec());
        let blob_hash = payload
            .hash()
            .expect("non-empty payload should have blob hash");

        {
            let mut writes = storage.new_write_set();
            let mut writer = BinaryCasContext::new().writer(&mut writes);
            writer
                .stage_payload(&payload)
                .expect("blob write should stage with payload hash");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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
            Some(BinaryCasManifest::SingleChunk {
                size_bytes: data.len() as u64,
                chunk_hash: blob_hash.into_bytes(),
            })
        );
    }

    #[tokio::test]
    async fn read_rejects_chunk_bytes_that_do_not_match_manifest_hash() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let data = b"same length";
        let corrupted = b"SAME length";
        let blob_hash = BlobHash::from_content(data);

        {
            let mut writes = storage.new_write_set();
            let mut writer = BinaryCasContext::new().writer(&mut writes);
            writer.stage_bytes(data).expect("blob write should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("blob write should commit");
        }

        {
            let mut writes = storage.new_write_set();
            writes.put(
                BINARY_CAS_CHUNK_SPACE,
                key(chunk_key(blob_hash)),
                value(encode_binary_cas_chunk(
                    BinaryChunkCodec::Raw,
                    corrupted.len() as u64,
                    corrupted,
                )),
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("corrupt chunk should overwrite");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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
    async fn read_rejects_manifest_that_assembles_wrong_blob_hash() {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
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
                .expect("wrong manifest fixture should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let data = b"";
        let blob_hash = BlobHash::from_content(data);

        {
            let mut writes = storage.new_write_set();
            let mut writer = BinaryCasContext::new().writer(&mut writes);
            writer.stage_bytes(data).expect("blob write should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let data = definitely_multi_chunk_blob_bytes();
        let blob_hash = BlobHash::from_content(&data);

        {
            let mut writes = storage.new_write_set();
            let mut writer = BinaryCasContext::new().writer(&mut writes);
            writer.stage_bytes(&data).expect("blob write should stage");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .expect("blob write should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
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
