#![allow(clippy::cast_sign_loss)]

use crate::LixError;
use crate::binary_cas::chunking::{MAX_BINARY_CAS_CHUNK_BYTES, fastcdc_chunk_ranges_with_chunking};
use crate::binary_cas::codec::{
    BinaryCasManifest, BinaryChunkCodec, StorageBinaryCasDeltaBaseLayout,
    StorageBinaryCasDeltaSegment, decode_binary_cas_chunk, decode_binary_cas_manifest,
    decode_binary_cas_manifest_chunk, encode_binary_cas_chunk, encode_binary_cas_manifest,
    encode_binary_cas_manifest_chunk,
};
use crate::binary_cas::{
    BinaryCasChunking, BlobBytesBatch, BlobDeltaBaseLayout, BlobDeltaSegment, BlobEditSplice,
    BlobId, BlobLayout, BlobMetadata, BlobMetadataBatch, BlobRangeBytes, BlobRangeBytesBatch,
    BlobSameLengthSplice, BlobWriteReceipt, ChunkHash,
};
#[cfg(test)]
use crate::storage_adapter::StoragePrefix;
use crate::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapterRead, StorageSpace, StorageWriteSet,
};
use crate::storage_adapter::{
    StorageCoreProjection, StorageGetOptions, StorageKey, StorageKeyRange, StorageProjectedValue,
    StorageScanOptions, StorageSpaceId, StorageValue,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::ops::{Bound, Range};
use web_time::Instant;

// Keep independent manifest scans bounded so large blob batches do not create
// unbounded backend pressure. Eight matches the engine's other remote scan
// fan-out and is enough to hide storage latency without a large request burst.
const MANIFEST_SCAN_CONCURRENCY: usize = 8;
const MAX_DELTA_SEGMENTS: usize = 32;
const MAX_DELTA_INSERT_BYTES: usize = 64 * 1024;
const MAX_DELTA_INSERT_FRACTION_DIVISOR: usize = 8;

pub(crate) const BINARY_CAS_MANIFEST_NAMESPACE: &str = "binary_cas.manifest";
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_NAMESPACE: &str = "binary_cas.manifest_chunk";
pub(crate) const BINARY_CAS_CHUNK_NAMESPACE: &str = "binary_cas.chunk";
pub(crate) const BINARY_CAS_CHUNK_PRESENCE_NAMESPACE: &str = "binary_cas.chunk_presence";
pub(crate) const BINARY_CAS_MANIFEST_SPACE: StorageSpace =
    StorageSpace::mutable(StorageSpaceId(0x0005_0001), BINARY_CAS_MANIFEST_NAMESPACE);
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0005_0002),
    BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
);
pub(crate) const BINARY_CAS_CHUNK_SPACE: StorageSpace =
    StorageSpace::immutable(StorageSpaceId(0x0005_0003), BINARY_CAS_CHUNK_NAMESPACE);
pub(crate) const BINARY_CAS_CHUNK_PRESENCE_SPACE: StorageSpace = StorageSpace::mutable(
    StorageSpaceId(0x0005_0004),
    BINARY_CAS_CHUNK_PRESENCE_NAMESPACE,
);

#[derive(Debug)]
struct BlobWritePlan {
    blob_hash: BlobId,
    chunk_ranges: Vec<(usize, usize)>,
    layout: BlobLayout,
    receipt: BlobWriteReceipt,
}

#[derive(Debug, Clone, Copy)]
struct PreparedChunk {
    start: usize,
    end: usize,
    hash: ChunkHash,
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
    blob_hash: BlobId,
) -> Result<Option<BinaryCasManifest>, LixError> {
    let Some(bytes) = get_one(store, BINARY_CAS_MANIFEST_SPACE, manifest_key(blob_hash)).await?
    else {
        return Ok(None);
    };
    decode_binary_cas_manifest(&bytes).map(Some)
}

pub(crate) fn stage_manifest(
    writes: &mut StorageWriteSet,
    blob_hash: BlobId,
    manifest: &BinaryCasManifest,
) {
    writes.put(
        BINARY_CAS_MANIFEST_SPACE,
        key(manifest_key(blob_hash)),
        value(encode_binary_cas_manifest(manifest)),
    );
}

#[cfg(test)]
pub(crate) async fn scan_manifest_chunks(
    store: &impl StorageAdapterRead,
    blob_hash: BlobId,
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

/// Loads exactly the manifest rows declared by a chunked blob's metadata.
///
/// Blob roots are content-addressed by their complete bytes, while chunk rows
/// are a mutable physical representation. A later valid writer may select a
/// different layout for the same content hash. Restricting reads to the
/// declared ordinal range keeps stale suffix rows harmless; the caller still
/// rejects missing declared rows by comparing the resulting count.
async fn load_declared_manifest_chunks(
    store: &(impl StorageAdapterRead + ?Sized),
    blob_hash: BlobId,
    chunk_count: u32,
) -> Result<Vec<KvBlobManifestChunk>, LixError> {
    if chunk_count == 0 {
        return Ok(Vec::new());
    }
    let range = StorageKeyRange {
        lower: Bound::Included(StorageKey(Bytes::from(manifest_chunk_key(blob_hash, 0)))),
        upper: Bound::Excluded(StorageKey(Bytes::from(manifest_chunk_key(
            blob_hash,
            u64::from(chunk_count),
        )))),
    };
    let plan = ScanPlan::range(BINARY_CAS_MANIFEST_CHUNK_SPACE, range);
    scan_all_values_for_plan(store, &plan)
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

/// Loads one ordinal interval from a fixed-size media manifest.
async fn load_declared_manifest_chunk_range(
    store: &(impl StorageAdapterRead + ?Sized),
    blob_hash: BlobId,
    start_index: u64,
    end_index: u64,
) -> Result<Vec<KvBlobManifestChunk>, LixError> {
    if start_index >= end_index {
        return Ok(Vec::new());
    }
    let range = StorageKeyRange {
        lower: Bound::Included(StorageKey(Bytes::from(manifest_chunk_key(
            blob_hash,
            start_index,
        )))),
        upper: Bound::Excluded(StorageKey(Bytes::from(manifest_chunk_key(
            blob_hash, end_index,
        )))),
    };
    let plan = ScanPlan::range(BINARY_CAS_MANIFEST_CHUNK_SPACE, range);
    scan_all_values_for_plan(store, &plan)
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
    blob_hash: BlobId,
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
    chunk_hash: ChunkHash,
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
    chunk_hash: ChunkHash,
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
    chunk_hash: ChunkHash,
    chunk_data: &[u8],
) -> Result<(), LixError> {
    stage_chunk(
        writes,
        chunk_hash,
        BinaryChunkCodec::Raw,
        chunk_data.len() as u64,
        chunk_data,
    );
    Ok(())
}

pub(in crate::binary_cas) async fn stage_fixed_part_skipping_existing(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    transaction_chunk_keys: &mut HashSet<Vec<u8>>,
    bytes: &[u8],
) -> Result<Vec<crate::binary_cas::BlobChunkReceipt>, LixError> {
    if bytes.is_empty() || bytes.len() > 16 * 1024 * 1024 {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "resumable CAS parts must contain 1 byte through 16 MiB",
        ));
    }
    #[cfg(feature = "storage-benches")]
    crate::storage_bench::record_media_upload_chunk_payload_hash_bytes(bytes.len());
    let receipts = bytes
        .chunks(crate::binary_cas::chunking::MEDIA_CHUNK_BYTES)
        .map(|chunk| crate::binary_cas::BlobChunkReceipt {
            hash: ChunkHash::from_content(chunk),
            size_bytes: chunk.len() as u64,
        })
        .collect::<Vec<_>>();
    let mut candidates = Vec::with_capacity(receipts.len());
    for receipt in &receipts {
        collect_chunk_lookup_candidate(receipt.hash, transaction_chunk_keys, &mut candidates);
    }
    let keys = candidates
        .iter()
        .map(|(_, key)| key.clone())
        .collect::<Vec<_>>();
    let existing = chunk_keys_exist(store, keys).await?;
    let mut missing = candidates
        .into_iter()
        .zip(existing)
        .filter_map(|((hash, _), exists)| (!exists).then_some(hash))
        .collect::<HashSet<_>>();
    for (chunk, receipt) in bytes
        .chunks(crate::binary_cas::chunking::MEDIA_CHUNK_BYTES)
        .zip(&receipts)
    {
        if missing.remove(&receipt.hash) {
            stage_content_chunk(writes, receipt.hash, chunk)?;
        }
    }
    Ok(receipts)
}

pub(in crate::binary_cas) fn stage_fixed_manifest(
    writes: &mut StorageWriteSet,
    chunks: &[crate::binary_cas::BlobChunkReceipt],
) -> Result<BlobWriteReceipt, LixError> {
    let chunk_count = u32::try_from(chunks.len()).map_err(|_| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "resumable file has too many chunks",
        )
    })?;
    let mut size_bytes = 0u64;
    for (index, chunk) in chunks.iter().enumerate() {
        let is_last = index + 1 == chunks.len();
        if chunk.size_bytes == 0
            || chunk.size_bytes > crate::binary_cas::chunking::MEDIA_CHUNK_BYTES as u64
            || (!is_last
                && chunk.size_bytes != crate::binary_cas::chunking::MEDIA_CHUNK_BYTES as u64)
        {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "resumable file receipts are not canonical fixed chunks",
            ));
        }
        size_bytes = size_bytes.checked_add(chunk.size_bytes).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "resumable file size exceeds u64",
            )
        })?;
    }

    let (hash, layout) = match chunks {
        [] => (BlobId::from_content(&[]), BlobLayout::Empty),
        [chunk] => (
            BlobId::from_single_chunk(chunk.hash),
            BlobLayout::SingleChunk {
                chunk_hash: chunk.hash,
            },
        ),
        _ => (
            BlobId::from_chunks(
                size_bytes,
                chunks.iter().map(|chunk| (chunk.hash, chunk.size_bytes)),
            ),
            BlobLayout::Chunked { chunk_count },
        ),
    };
    match &layout {
        BlobLayout::Empty => stage_manifest(writes, hash, &BinaryCasManifest::Empty { size_bytes }),
        BlobLayout::SingleChunk { chunk_hash } => stage_manifest(
            writes,
            hash,
            &BinaryCasManifest::SingleChunk {
                size_bytes,
                chunk_hash: chunk_hash.into_bytes(),
            },
        ),
        BlobLayout::Chunked { chunk_count } => {
            stage_manifest(
                writes,
                hash,
                &BinaryCasManifest::Chunked {
                    size_bytes,
                    chunk_count: *chunk_count,
                },
            );
            for (index, chunk) in chunks.iter().enumerate() {
                stage_manifest_chunk(
                    writes,
                    hash,
                    index as u64,
                    &KvBlobManifestChunk {
                        chunk_hash: chunk.hash.into_bytes(),
                        chunk_size: chunk.size_bytes,
                    },
                );
            }
        }
        BlobLayout::Delta { .. } => unreachable!("fixed upload cannot produce delta layout"),
    }
    Ok(BlobWriteReceipt {
        hash,
        size_bytes,
        layout,
    })
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

#[cfg(test)]
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
    scan_all_values_for_plan(store, &plan).await
}

async fn scan_all_values_for_plan(
    store: &(impl StorageAdapterRead + ?Sized),
    plan: &ScanPlan,
) -> Result<Vec<Vec<u8>>, LixError> {
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
    store: &(impl StorageAdapterRead + ?Sized),
    hashes: &[BlobId],
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
    store: &(impl StorageAdapterRead + ?Sized),
    hashes: &[BlobId],
) -> Result<BlobBytesBatch, LixError> {
    let metadata = load_metadata_many(store, hashes).await?.into_vec();
    let mut delta_bases = HashMap::<BlobId, BlobMetadata>::new();
    let mut delta_base_order = Vec::<BlobId>::new();
    for entry in metadata.iter().flatten() {
        if let BlobLayout::Delta {
            base_blob_hash,
            base_size_bytes,
            base_layout,
            ..
        } = &entry.layout
        {
            let layout = match base_layout {
                BlobDeltaBaseLayout::SingleChunk { chunk_hash } => BlobLayout::SingleChunk {
                    chunk_hash: *chunk_hash,
                },
                BlobDeltaBaseLayout::Chunked { chunk_count } => BlobLayout::Chunked {
                    chunk_count: *chunk_count,
                },
            };
            let base = BlobMetadata {
                hash: *base_blob_hash,
                size_bytes: *base_size_bytes,
                layout,
            };
            match delta_bases.entry(*base_blob_hash) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    delta_base_order.push(*base_blob_hash);
                    entry.insert(base);
                }
                std::collections::hash_map::Entry::Occupied(entry) if entry.get() != &base => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS deltas disagree about base '{}' layout",
                            base_blob_hash.to_hex()
                        ),
                    ));
                }
                std::collections::hash_map::Entry::Occupied(_) => {}
            }
        }
    }
    let mut seen_physical_hashes = HashSet::new();
    let mut physical_metadata = metadata
        .iter()
        .flatten()
        .filter(|entry| {
            !matches!(entry.layout, BlobLayout::Delta { .. })
                && seen_physical_hashes.insert(entry.hash)
        })
        .cloned()
        .collect::<Vec<_>>();
    physical_metadata.extend(
        delta_base_order
            .into_iter()
            .filter(|hash| seen_physical_hashes.insert(*hash))
            .map(|hash| {
                delta_bases
                    .remove(&hash)
                    .expect("ordered delta base exists")
            }),
    );

    let mut seen_manifest_hashes = HashSet::new();
    let chunked_blobs = physical_metadata
        .iter()
        .filter_map(|metadata| {
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
                let manifest_chunks =
                    load_declared_manifest_chunks(store, blob_hash, chunk_count).await?;
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

    for metadata in &physical_metadata {
        match &metadata.layout {
            BlobLayout::Empty => {}
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
                    let chunk_hash = ChunkHash::from_bytes(manifest_chunk.chunk_hash);
                    if seen_chunks.insert(chunk_hash) {
                        requested_chunks.push(chunk_hash);
                    }
                }
            }
            BlobLayout::Delta { .. } => unreachable!("delta bases are flattened"),
        }
    }

    let chunk_rows = load_chunk_rows(store, &requested_chunks).await?;
    let chunk_rows_by_hash = requested_chunks
        .into_iter()
        .zip(chunk_rows)
        .collect::<HashMap<_, _>>();

    let mut full_bytes_by_hash = HashMap::<BlobId, Vec<u8>>::new();
    for metadata in physical_metadata {
        let hash = metadata.hash;
        let bytes = assemble_blob_bytes(
            metadata,
            &chunk_rows_by_hash,
            chunked_manifests_by_hash.get(&hash),
        )?;
        full_bytes_by_hash.insert(hash, bytes);
    }

    // Most reads request each full blob exactly once. Transfer that assembled
    // buffer into its output slot instead of cloning every payload after it
    // has already been authenticated and assembled. Repeated output slots
    // still need independent `Vec` ownership, and delta bases must remain
    // available until every dependent result has been reconstructed.
    let mut direct_output_counts = HashMap::<BlobId, usize>::new();
    let mut delta_base_hashes = HashSet::<BlobId>::new();
    for entry in metadata.iter().flatten() {
        match &entry.layout {
            BlobLayout::Delta { base_blob_hash, .. } => {
                delta_base_hashes.insert(*base_blob_hash);
            }
            _ => {
                *direct_output_counts.entry(entry.hash).or_default() += 1;
            }
        }
    }
    let movable_full_hashes = direct_output_counts
        .into_iter()
        .filter_map(|(hash, count)| {
            (count == 1 && !delta_base_hashes.contains(&hash)).then_some(hash)
        })
        .collect::<HashSet<_>>();

    let entries = metadata
        .into_iter()
        .map(|metadata| {
            metadata
                .map(|metadata| match metadata.layout {
                    BlobLayout::Delta {
                        base_blob_hash,
                        segments,
                        ..
                    } => apply_flat_delta(
                        metadata.hash,
                        metadata.size_bytes,
                        base_blob_hash,
                        &segments,
                        &full_bytes_by_hash,
                    ),
                    _ if movable_full_hashes.contains(&metadata.hash) => {
                        full_bytes_by_hash.remove(&metadata.hash).ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "binary CAS blob '{}' was not assembled",
                                    metadata.hash.to_hex()
                                ),
                            )
                        })
                    }
                    _ => full_bytes_by_hash
                        .get(&metadata.hash)
                        .cloned()
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!(
                                    "binary CAS blob '{}' was not assembled",
                                    metadata.hash.to_hex()
                                ),
                            )
                        }),
                })
                .transpose()
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BlobBytesBatch::new(entries))
}

pub(crate) async fn load_ranges_many(
    store: &(impl StorageAdapterRead + ?Sized),
    requests: &[(BlobId, Range<u64>)],
) -> Result<BlobRangeBytesBatch, LixError> {
    let hashes = requests.iter().map(|(hash, _)| *hash).collect::<Vec<_>>();
    let metadata = load_metadata_many(store, &hashes).await?.into_vec();
    let mut entries = Vec::with_capacity(requests.len());
    for (metadata, (_, requested)) in metadata.into_iter().zip(requests) {
        entries.push(match metadata {
            Some(metadata) => Some(load_blob_range(store, metadata, requested.clone()).await?),
            None => None,
        });
    }
    Ok(BlobRangeBytesBatch::new(entries))
}

async fn load_blob_range(
    store: &(impl StorageAdapterRead + ?Sized),
    metadata: BlobMetadata,
    requested: Range<u64>,
) -> Result<BlobRangeBytes, LixError> {
    if requested.start >= requested.end || requested.start >= metadata.size_bytes {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "binary CAS range is not satisfiable",
        ));
    }
    let range = requested.start..requested.end.min(metadata.size_bytes);
    let bytes = match &metadata.layout {
        BlobLayout::Empty => unreachable!("a non-empty range cannot select an empty blob"),
        BlobLayout::SingleChunk { chunk_hash } => {
            let rows = load_chunk_rows(store, &[*chunk_hash]).await?;
            let row = rows.into_iter().next().flatten().ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS chunk '{}' is missing for blob '{}'",
                        chunk_hash.to_hex(),
                        metadata.hash.to_hex()
                    ),
                )
            })?;
            let decoded = decode_and_verify_chunk(
                &row,
                persisted_size_to_usize(metadata.size_bytes, "binary CAS blob")?,
                metadata.hash,
                *chunk_hash,
            )?;
            let start = persisted_size_to_usize(range.start, "binary CAS range start")?;
            let end = persisted_size_to_usize(range.end, "binary CAS range end")?;
            decoded[start..end].to_vec()
        }
        BlobLayout::Chunked { chunk_count } => {
            let fixed_chunk_bytes = crate::binary_cas::chunking::MEDIA_CHUNK_BYTES as u64;
            let first_chunk_index = range.start / fixed_chunk_bytes;
            let end_chunk_index = range
                .end
                .div_ceil(fixed_chunk_bytes)
                .min(u64::from(*chunk_count));
            let manifest = load_declared_manifest_chunk_range(
                store,
                metadata.hash,
                first_chunk_index,
                end_chunk_index,
            )
            .await?;
            let expected_manifest_len = usize::try_from(end_chunk_index - first_chunk_index)
                .map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "binary CAS selected manifest range exceeds this runtime",
                    )
                })?;
            if manifest.len() != expected_manifest_len {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' expected {} selected chunks, found {}",
                        metadata.hash.to_hex(),
                        expected_manifest_len,
                        manifest.len()
                    ),
                ));
            }
            let mut selected = Vec::with_capacity(manifest.len());
            for (selected_index, chunk) in manifest.into_iter().enumerate() {
                let chunk_index = first_chunk_index
                    + u64::try_from(selected_index).map_err(|_| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "binary CAS selected chunk index exceeds u64",
                        )
                    })?;
                let chunk_start = chunk_index.checked_mul(fixed_chunk_bytes).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "binary CAS chunk offsets overflow u64")
                })?;
                let expected_chunk_size = if chunk_index + 1 == u64::from(*chunk_count) {
                    metadata
                        .size_bytes
                        .checked_sub(chunk_start)
                        .ok_or_else(|| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                "binary CAS final chunk starts beyond the declared blob size",
                            )
                        })?
                } else {
                    fixed_chunk_bytes
                };
                if chunk.chunk_size != expected_chunk_size {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS blob '{}' chunk {} has size {}, expected {}",
                            metadata.hash.to_hex(),
                            chunk_index,
                            chunk.chunk_size,
                            expected_chunk_size,
                        ),
                    ));
                }
                selected.push((chunk_start, chunk));
            }
            let hashes = selected
                .iter()
                .map(|(_, chunk)| ChunkHash::from_bytes(chunk.chunk_hash))
                .collect::<Vec<_>>();
            let rows = load_chunk_rows(store, &hashes).await?;
            let capacity =
                persisted_size_to_usize(range.end - range.start, "binary CAS selected range")?;
            let mut out = Vec::with_capacity(capacity);
            for (((chunk_start, chunk), chunk_hash), row) in
                selected.into_iter().zip(hashes).zip(rows)
            {
                let row = row.ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS chunk '{}' is missing for blob '{}'",
                            chunk_hash.to_hex(),
                            metadata.hash.to_hex()
                        ),
                    )
                })?;
                let expected_size = persisted_size_to_usize(chunk.chunk_size, "binary CAS chunk")?;
                let decoded =
                    decode_and_verify_chunk(&row, expected_size, metadata.hash, chunk_hash)?;
                let selected_start = range.start.saturating_sub(chunk_start);
                let selected_end = (range.end - chunk_start).min(chunk.chunk_size);
                let selected_start =
                    persisted_size_to_usize(selected_start, "binary CAS chunk range start")?;
                let selected_end =
                    persisted_size_to_usize(selected_end, "binary CAS chunk range end")?;
                out.extend_from_slice(&decoded[selected_start..selected_end]);
            }
            if out.len() != capacity {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "binary CAS range did not reconstruct its declared byte count",
                ));
            }
            out
        }
        BlobLayout::Delta { .. } => {
            // Flat deltas are optimized for localized document edits, not
            // immutable media. Keep their established reconstruction path
            // instead of complicating the media range reader.
            let full = load_bytes_many(store, &[metadata.hash])
                .await?
                .into_vec()
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "binary CAS delta base is missing")
                })?;
            let start = persisted_size_to_usize(range.start, "binary CAS range start")?;
            let end = persisted_size_to_usize(range.end, "binary CAS range end")?;
            full[start..end].to_vec()
        }
    };
    Ok(BlobRangeBytes {
        bytes,
        total_size: metadata.size_bytes,
        range,
    })
}

fn apply_flat_delta(
    blob_hash: BlobId,
    size_bytes: u64,
    base_blob_hash: BlobId,
    segments: &[BlobDeltaSegment],
    full_bytes_by_hash: &HashMap<BlobId, Vec<u8>>,
) -> Result<Vec<u8>, LixError> {
    let expected_size = persisted_size_to_usize(size_bytes, "binary CAS delta")?;
    let Some(base) = full_bytes_by_hash.get(&base_blob_hash) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' is missing base '{}'",
                blob_hash.to_hex(),
                base_blob_hash.to_hex()
            ),
        ));
    };
    let mut out = Vec::with_capacity(expected_size);
    for segment in segments {
        match segment {
            BlobDeltaSegment::Copy { offset, length } => {
                let start = persisted_size_to_usize(*offset, "binary CAS delta copy offset")?;
                let length = persisted_size_to_usize(*length, "binary CAS delta copy length")?;
                let Some(end) = start.checked_add(length) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS delta '{}' copy range overflowed",
                            blob_hash.to_hex()
                        ),
                    ));
                };
                let Some(slice) = base.get(start..end) else {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS delta '{}' has invalid copy ranges",
                            blob_hash.to_hex()
                        ),
                    ));
                };
                out.extend_from_slice(slice);
            }
            BlobDeltaSegment::Insert { bytes } => out.extend_from_slice(bytes),
        }
        if out.len() > expected_size {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "binary CAS delta '{}' exceeds its declared size",
                    blob_hash.to_hex()
                ),
            ));
        }
    }
    if out.len() != expected_size {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' expected {} bytes, decoded {}",
                blob_hash.to_hex(),
                expected_size,
                out.len()
            ),
        ));
    }
    if BlobId::from_content(&out) != blob_hash {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' failed content-address verification",
                blob_hash.to_hex()
            ),
        ));
    }
    Ok(out)
}

async fn load_chunk_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    hashes: &[ChunkHash],
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
    store: &(impl StorageAdapterRead + ?Sized),
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
    metadata: BlobMetadata,
    chunk_rows_by_hash: &HashMap<ChunkHash, Option<Bytes>>,
    chunked_manifest: Option<&Vec<KvBlobManifestChunk>>,
) -> Result<Vec<u8>, LixError> {
    let expected_blob_size = persisted_size_to_usize(metadata.size_bytes, "binary CAS blob")?;
    let bytes = match &metadata.layout {
        BlobLayout::Empty => {
            if cfg!(debug_assertions) && metadata.hash != BlobId::from_content(&[]) {
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
                && BlobId::from_single_chunk(*chunk_hash) != metadata.hash
                && BlobId::from_content(&chunk) != metadata.hash
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
                let chunk_hash = ChunkHash::from_bytes(manifest_chunk.chunk_hash);
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
            if cfg!(debug_assertions) && BlobId::from_content(&out) != metadata.hash {
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
        BlobLayout::Delta { .. } => {
            unreachable!("flat deltas are applied after their full bases are assembled")
        }
    };
    Ok(bytes)
}

fn decode_chunk_from_map(
    chunk_rows_by_hash: &HashMap<ChunkHash, Option<Bytes>>,
    blob_hash: BlobId,
    chunk_hash: ChunkHash,
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
    blob_hash: BlobId,
    chunk_hash: ChunkHash,
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
    blob_hash: BlobId,
    chunk_hash: ChunkHash,
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
    // The immutable sidecar and its range cache are not independently
    // authenticated. Verify raw and decoded compressed bytes against the CAS
    // key in every build before returning them.
    if ChunkHash::from_content(&decoded) != chunk_hash {
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
    precomputed_hash: Option<BlobId>,
) -> Result<BlobWriteReceipt, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if let Some(hash) = precomputed_hash
        && let Some(metadata) = load_metadata_many(store, &[hash])
            .await?
            .into_vec()
            .into_iter()
            .next()
            .flatten()
    {
        let size_bytes = u64::try_from(bytes.len())
            .map_err(|_| LixError::new(LixError::CODE_UNKNOWN, "binary CAS payload exceeds u64"))?;
        if metadata.size_bytes != size_bytes {
            return Err(LixError::new(
                LixError::CODE_UNKNOWN,
                "precomputed binary CAS hash names an existing blob with a different size",
            ));
        }
        blob_hashes.insert(hash.into_bytes());
        return Ok(BlobWriteReceipt {
            hash,
            size_bytes,
            layout: metadata.layout,
        });
    }
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

/// Attempts to stage a full replacement by retaining the base manifest's
/// unchanged chunk references around one host-verified fixed-width splice.
///
/// This is deliberately opportunistic. The caller still owns complete
/// replacement bytes and falls back to [`stage_blob_write_skipping_existing_chunks`]
/// for every missing, malformed, non-chunked, length-changing, or otherwise
/// ineligible base. A manifest is an ordered content-addressed chunk list;
/// readers do not require its boundaries to have been freshly produced by
/// FastCDC, so keeping valid existing boundaries is format-compatible.
pub(in crate::binary_cas) async fn try_stage_blob_write_reusing_same_length_splice<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    blob_hashes: &mut HashSet<[u8; 32]>,
    chunk_keys: &mut HashSet<Vec<u8>>,
    bytes: &[u8],
    precomputed_hash: Option<BlobId>,
    splice: BlobSameLengthSplice,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let Some(blob_hash) = precomputed_hash else {
        return Ok(false);
    };
    let Some(splice_end) = splice.end() else {
        return Ok(false);
    };
    if splice.length == 0 || splice_end > bytes.len() {
        return Ok(false);
    }
    if blob_hashes.contains(&blob_hash.into_bytes()) {
        return Ok(true);
    }

    let metadata = match load_metadata_many(&store, &[splice.base_blob_hash]).await {
        Ok(metadata) => metadata.into_vec().pop().flatten(),
        Err(_) => return Ok(false),
    };
    let Some(metadata) = metadata else {
        return Ok(false);
    };
    let BlobLayout::Chunked { chunk_count } = &metadata.layout else {
        return Ok(false);
    };
    let chunk_count = *chunk_count;
    if metadata.size_bytes != bytes.len() as u64 {
        return Ok(false);
    }

    let Ok(base_chunks) =
        load_declared_manifest_chunks(&store, splice.base_blob_hash, chunk_count).await
    else {
        return Ok(false);
    };
    if base_chunks.len() != chunk_count as usize {
        return Ok(false);
    }

    let mut cursor = 0usize;
    let mut chunks = Vec::with_capacity(base_chunks.len());
    let mut changed_chunks = Vec::new();
    for base_chunk in base_chunks {
        let Ok(chunk_len) = usize::try_from(base_chunk.chunk_size) else {
            return Ok(false);
        };
        if chunk_len == 0 || chunk_len > MAX_BINARY_CAS_CHUNK_BYTES {
            return Ok(false);
        }
        let Some(end) = cursor.checked_add(chunk_len) else {
            return Ok(false);
        };
        if end > bytes.len() {
            return Ok(false);
        }
        let changed = cursor < splice_end && splice.offset < end;
        let chunk = PreparedChunk {
            start: cursor,
            end,
            hash: if changed {
                ChunkHash::from_content(&bytes[cursor..end])
            } else {
                ChunkHash::from_bytes(base_chunk.chunk_hash)
            },
        };
        if changed {
            changed_chunks.push(chunk);
        }
        chunks.push((chunk, changed));
        cursor = end;
    }
    if cursor != bytes.len() || changed_chunks.is_empty() {
        return Ok(false);
    }

    let mut chunk_hashes_to_stage =
        missing_chunk_hashes_for_chunks(store, chunk_keys, &changed_chunks).await?;
    if !blob_hashes.insert(blob_hash.into_bytes()) {
        return Ok(true);
    }

    stage_manifest(
        writes,
        blob_hash,
        &BinaryCasManifest::Chunked {
            size_bytes: bytes.len() as u64,
            chunk_count,
        },
    );
    for (chunk_index, (chunk, changed)) in chunks.into_iter().enumerate() {
        let chunk_data = &bytes[chunk.start..chunk.end];
        if changed && chunk_hashes_to_stage.remove(&chunk.hash) {
            stage_content_chunk(writes, chunk.hash, chunk_data)?;
        }
        stage_manifest_chunk(
            writes,
            blob_hash,
            chunk_index as u64,
            &KvBlobManifestChunk {
                chunk_hash: *chunk.hash.as_bytes(),
                chunk_size: chunk_data.len() as u64,
            },
        );
    }
    Ok(true)
}

/// Stores a bounded, one-level edit delta against a canonical full blob.
///
/// A delta never references another delta: repeated edits compose into the
/// prior copy/insert program while retaining its full ancestor. This borrows
/// Git packfiles' delta-reuse idea without introducing read chains. Once the
/// segment or inserted-byte budget is exhausted, the caller falls back to a
/// normal full CAS write, which becomes the next delta base.
pub(in crate::binary_cas) async fn try_stage_blob_write_as_flat_delta<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    blob_hashes: &mut HashSet<[u8; 32]>,
    bytes: &[u8],
    precomputed_hash: Option<BlobId>,
    splice: BlobEditSplice,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let Some(blob_hash) = precomputed_hash else {
        return Ok(false);
    };
    let Some(delete_end) = splice.offset.checked_add(splice.delete_len) else {
        return Ok(false);
    };
    let Some(insert_end) = splice.offset.checked_add(splice.insert_len) else {
        return Ok(false);
    };
    if (splice.delete_len == 0 && splice.insert_len == 0)
        || insert_end > bytes.len()
        || bytes.len() < 512
    {
        return Ok(false);
    }
    if blob_hashes.contains(&blob_hash.into_bytes()) {
        return Ok(true);
    }

    let Some(base_metadata) = load_metadata_many(store, &[splice.base_blob_hash])
        .await?
        .into_vec()
        .into_iter()
        .next()
        .flatten()
    else {
        return Ok(false);
    };
    let Ok(current_size) = usize::try_from(base_metadata.size_bytes) else {
        return Ok(false);
    };
    if delete_end > current_size
        || current_size
            .checked_sub(splice.delete_len)
            .and_then(|size| size.checked_add(splice.insert_len))
            != Some(bytes.len())
    {
        return Ok(false);
    }

    let (full_base_hash, full_base_size, full_base_layout, segments) = match base_metadata.layout {
        BlobLayout::Empty => return Ok(false),
        BlobLayout::SingleChunk { chunk_hash } => (
            splice.base_blob_hash,
            base_metadata.size_bytes,
            BlobDeltaBaseLayout::SingleChunk { chunk_hash },
            vec![BlobDeltaSegment::Copy {
                offset: 0,
                length: base_metadata.size_bytes,
            }],
        ),
        BlobLayout::Chunked { chunk_count } => (
            splice.base_blob_hash,
            base_metadata.size_bytes,
            BlobDeltaBaseLayout::Chunked { chunk_count },
            vec![BlobDeltaSegment::Copy {
                offset: 0,
                length: base_metadata.size_bytes,
            }],
        ),
        BlobLayout::Delta {
            base_blob_hash,
            base_size_bytes,
            base_layout,
            segments,
        } => (base_blob_hash, base_size_bytes, base_layout, segments),
    };
    let Some(mut next_segments) = slice_delta_segments(&segments, 0, splice.offset) else {
        return Ok(false);
    };
    if splice.insert_len != 0 {
        next_segments.push(BlobDeltaSegment::Insert {
            bytes: bytes[splice.offset..insert_end].to_vec(),
        });
    }
    let Some(suffix) = slice_delta_segments(&segments, delete_end, current_size) else {
        return Ok(false);
    };
    next_segments.extend(suffix);
    let next_segments = normalize_delta_segments(next_segments);
    let insert_bytes = next_segments.iter().try_fold(0usize, |total, segment| {
        total.checked_add(match segment {
            BlobDeltaSegment::Copy { .. } => 0,
            BlobDeltaSegment::Insert { bytes } => bytes.len(),
        })
    });
    let Some(insert_bytes) = insert_bytes else {
        return Ok(false);
    };
    let relative_budget = bytes.len().div_ceil(MAX_DELTA_INSERT_FRACTION_DIVISOR);
    if next_segments.len() > MAX_DELTA_SEGMENTS
        || insert_bytes > MAX_DELTA_INSERT_BYTES.min(relative_budget)
    {
        return Ok(false);
    }

    let segments = next_segments
        .into_iter()
        .map(|segment| match segment {
            BlobDeltaSegment::Copy { offset, length } => {
                StorageBinaryCasDeltaSegment::Copy { offset, length }
            }
            BlobDeltaSegment::Insert { bytes } => StorageBinaryCasDeltaSegment::Insert { bytes },
        })
        .collect::<Vec<_>>();
    if !blob_hashes.insert(blob_hash.into_bytes()) {
        return Ok(true);
    }
    stage_manifest(
        writes,
        blob_hash,
        &BinaryCasManifest::Delta {
            size_bytes: bytes.len() as u64,
            base_blob_hash: full_base_hash.into_bytes(),
            base_size_bytes: full_base_size,
            base_layout: match full_base_layout {
                BlobDeltaBaseLayout::SingleChunk { chunk_hash } => {
                    StorageBinaryCasDeltaBaseLayout::SingleChunk {
                        chunk_hash: chunk_hash.into_bytes(),
                    }
                }
                BlobDeltaBaseLayout::Chunked { chunk_count } => {
                    StorageBinaryCasDeltaBaseLayout::Chunked { chunk_count }
                }
            },
            segments,
        },
    );
    Ok(true)
}

fn delta_segment_len(segment: &BlobDeltaSegment) -> Option<usize> {
    match segment {
        BlobDeltaSegment::Copy { length, .. } => usize::try_from(*length).ok(),
        BlobDeltaSegment::Insert { bytes } => Some(bytes.len()),
    }
}

fn slice_delta_segments(
    segments: &[BlobDeltaSegment],
    start: usize,
    end: usize,
) -> Option<Vec<BlobDeltaSegment>> {
    if start > end {
        return None;
    }
    let mut logical = 0usize;
    let mut out = Vec::new();
    for segment in segments {
        let length = delta_segment_len(segment)?;
        let segment_end = logical.checked_add(length)?;
        let take_start = start.max(logical);
        let take_end = end.min(segment_end);
        if take_start < take_end {
            let local_start = take_start - logical;
            let local_end = take_end - logical;
            out.push(match segment {
                BlobDeltaSegment::Copy { offset, .. } => BlobDeltaSegment::Copy {
                    offset: offset.checked_add(local_start as u64)?,
                    length: (local_end - local_start) as u64,
                },
                BlobDeltaSegment::Insert { bytes } => BlobDeltaSegment::Insert {
                    bytes: bytes[local_start..local_end].to_vec(),
                },
            });
        }
        logical = segment_end;
        if logical >= end {
            break;
        }
    }
    (logical >= end).then_some(out)
}

fn normalize_delta_segments(segments: Vec<BlobDeltaSegment>) -> Vec<BlobDeltaSegment> {
    let mut out = Vec::<BlobDeltaSegment>::with_capacity(segments.len());
    for segment in segments {
        if delta_segment_len(&segment) == Some(0) {
            continue;
        }
        match (out.last_mut(), segment) {
            (
                Some(BlobDeltaSegment::Copy { offset, length }),
                BlobDeltaSegment::Copy {
                    offset: next_offset,
                    length: next_length,
                },
            ) if offset.checked_add(*length) == Some(next_offset)
                && length.checked_add(next_length).is_some() =>
            {
                *length = length
                    .checked_add(next_length)
                    .expect("delta copy length was checked")
            }
            (
                Some(BlobDeltaSegment::Insert { bytes }),
                BlobDeltaSegment::Insert { bytes: next },
            ) => bytes.extend_from_slice(&next),
            (_, segment) => out.push(segment),
        }
    }
    out
}

fn prepare_blob_write(
    chunking: BinaryCasChunking,
    bytes: &[u8],
    precomputed_hash: Option<BlobId>,
) -> Result<BlobWritePlan, LixError> {
    let blob_hash = precomputed_hash.unwrap_or_else(|| BlobId::from_content(bytes));
    if cfg!(debug_assertions)
        && precomputed_hash.is_some()
        && BlobId::from_content(bytes) != blob_hash
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "binary CAS blob hash does not match blob contents".to_string(),
        ));
    }
    let (chunk_ranges, layout) = if bytes.is_empty() {
        (Vec::new(), BlobLayout::Empty)
    } else {
        let chunk_ranges = fastcdc_chunk_ranges_with_chunking(bytes, chunking);
        let layout = match chunk_ranges.as_slice() {
            [] => unreachable!("non-empty blobs always have at least one chunk"),
            [(start, end)] => BlobLayout::SingleChunk {
                chunk_hash: ChunkHash::from_content(&bytes[*start..*end]),
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
                ChunkHash::from_content(bytes)
            } else {
                ChunkHash::from_content(&bytes[start..end])
            },
        })
        .collect()
}

fn stage_prepared_blob_write(
    writes: &mut StorageWriteSet,
    bytes: &[u8],
    plan: &BlobWritePlan,
    chunks: &[PreparedChunk],
    mut should_stage_chunk: impl FnMut(ChunkHash) -> Result<bool, LixError>,
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
                stage_content_chunk(writes, chunk_hash, bytes)?;
            }
        }
        BlobLayout::Delta { .. } => {
            unreachable!("ordinary blob plans never construct delta layouts")
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
) -> Result<HashSet<ChunkHash>, LixError> {
    let mut candidates = Vec::<(ChunkHash, StorageKey)>::new();
    match &plan.layout {
        BlobLayout::Empty => {}
        BlobLayout::SingleChunk { chunk_hash } => {
            collect_chunk_lookup_candidate(*chunk_hash, transaction_chunk_keys, &mut candidates);
        }
        BlobLayout::Chunked { .. } => {
            for chunk in chunks {
                collect_chunk_lookup_candidate(chunk.hash, transaction_chunk_keys, &mut candidates);
            }
        }
        BlobLayout::Delta { .. } => {
            unreachable!("ordinary blob plans never construct delta layouts")
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

async fn missing_chunk_hashes_for_chunks(
    store: &(impl StorageAdapterRead + ?Sized),
    transaction_chunk_keys: &mut HashSet<Vec<u8>>,
    chunks: &[PreparedChunk],
) -> Result<HashSet<ChunkHash>, LixError> {
    let mut candidates = Vec::<(ChunkHash, StorageKey)>::new();
    for chunk in chunks {
        collect_chunk_lookup_candidate(chunk.hash, transaction_chunk_keys, &mut candidates);
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
    chunk_hash: ChunkHash,
    transaction_chunk_keys: &mut HashSet<Vec<u8>>,
    candidates: &mut Vec<(ChunkHash, StorageKey)>,
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
    hash: BlobId,
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
            chunk_hash: ChunkHash::from_bytes(chunk_hash),
        },
        BinaryCasManifest::Chunked { chunk_count, .. } => BlobLayout::Chunked { chunk_count },
        BinaryCasManifest::Delta {
            base_blob_hash,
            base_size_bytes,
            base_layout,
            segments,
            ..
        } => {
            validate_storage_delta_manifest(hash, size_bytes, base_size_bytes, &segments)?;
            BlobLayout::Delta {
                base_blob_hash: BlobId::from_bytes(base_blob_hash),
                base_size_bytes,
                base_layout: match base_layout {
                    StorageBinaryCasDeltaBaseLayout::SingleChunk { chunk_hash } => {
                        BlobDeltaBaseLayout::SingleChunk {
                            chunk_hash: ChunkHash::from_bytes(chunk_hash),
                        }
                    }
                    StorageBinaryCasDeltaBaseLayout::Chunked { chunk_count } => {
                        BlobDeltaBaseLayout::Chunked { chunk_count }
                    }
                },
                segments: segments
                    .into_iter()
                    .map(|segment| match segment {
                        StorageBinaryCasDeltaSegment::Copy { offset, length } => {
                            BlobDeltaSegment::Copy { offset, length }
                        }
                        StorageBinaryCasDeltaSegment::Insert { bytes } => {
                            BlobDeltaSegment::Insert { bytes }
                        }
                    })
                    .collect(),
            }
        }
    };
    Ok(BlobMetadata {
        hash,
        size_bytes,
        layout,
    })
}

fn validate_storage_delta_manifest(
    hash: BlobId,
    size_bytes: u64,
    base_size_bytes: u64,
    segments: &[StorageBinaryCasDeltaSegment],
) -> Result<(), LixError> {
    if base_size_bytes == 0 || segments.is_empty() || segments.len() > MAX_DELTA_SEGMENTS {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' has invalid segment metadata",
                hash.to_hex()
            ),
        ));
    }
    let mut output_size = 0u64;
    let mut inserted_bytes = 0u64;
    for segment in segments {
        let length = match segment {
            StorageBinaryCasDeltaSegment::Copy { offset, length } => {
                if *length == 0
                    || offset
                        .checked_add(*length)
                        .is_none_or(|end| end > base_size_bytes)
                {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS delta '{}' has invalid copy ranges",
                            hash.to_hex()
                        ),
                    ));
                }
                *length
            }
            StorageBinaryCasDeltaSegment::Insert { bytes } => {
                if bytes.is_empty() {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("binary CAS delta '{}' has an empty insert", hash.to_hex()),
                    ));
                }
                let length = u64::try_from(bytes.len()).map_err(|_| {
                    LixError::new("LIX_ERROR_UNKNOWN", "binary CAS delta insert is too large")
                })?;
                inserted_bytes = inserted_bytes.checked_add(length).ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "binary CAS delta insert size overflowed",
                    )
                })?;
                length
            }
        };
        output_size = output_size.checked_add(length).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "binary CAS delta output size overflowed",
            )
        })?;
    }
    let relative_budget = size_bytes.div_ceil(MAX_DELTA_INSERT_FRACTION_DIVISOR as u64);
    if output_size != size_bytes
        || inserted_bytes > (MAX_DELTA_INSERT_BYTES as u64).min(relative_budget)
    {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' exceeds its format budget",
                hash.to_hex()
            ),
        ));
    }
    Ok(())
}

fn manifest_key(blob_hash: BlobId) -> Vec<u8> {
    blob_hash.as_bytes().to_vec()
}

#[cfg(test)]
fn manifest_chunk_prefix(blob_hash: BlobId) -> Vec<u8> {
    blob_hash.as_bytes().to_vec()
}

fn manifest_chunk_key(blob_hash: BlobId, chunk_index: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    out.extend_from_slice(blob_hash.as_bytes());
    out.extend_from_slice(&chunk_index.to_be_bytes());
    out
}

fn chunk_key(chunk_hash: ChunkHash) -> Vec<u8> {
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
        blocked_manifest: Option<(BlobId, BlobId)>,
        blocked_manifest_release: Notify,
        active_manifest_scans: AtomicUsize,
        max_active_manifest_scans: AtomicUsize,
        manifest_scan_calls: AtomicUsize,
        manifest_get_many_calls: AtomicUsize,
        chunk_get_many_calls: AtomicUsize,
        presence_get_many_calls: AtomicUsize,
        chunk_keys_requested: AtomicUsize,
        completed_manifest_hashes: Mutex<Vec<BlobId>>,
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
                manifest_get_many_calls: AtomicUsize::new(0),
                chunk_get_many_calls: AtomicUsize::new(0),
                presence_get_many_calls: AtomicUsize::new(0),
                chunk_keys_requested: AtomicUsize::new(0),
                completed_manifest_hashes: Mutex::new(Vec::new()),
            }
        }

        fn block_manifest_until(
            mut self,
            blocked_manifest: BlobId,
            completed_manifest: BlobId,
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
            requests: &[crate::storage_adapter::StorageGetManyRequest<'_>],
        ) -> Result<StorageGetManyResult, StorageError> {
            for request in requests {
                if request.space == BINARY_CAS_MANIFEST_SPACE {
                    self.manifest_get_many_calls.fetch_add(1, Ordering::Relaxed);
                }
                if request.space == BINARY_CAS_CHUNK_SPACE {
                    self.chunk_get_many_calls.fetch_add(1, Ordering::Relaxed);
                    self.chunk_keys_requested
                        .fetch_add(request.keys.len(), Ordering::Relaxed);
                }
                if request.space == BINARY_CAS_CHUNK_PRESENCE_SPACE {
                    self.presence_get_many_calls.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.inner.get_many(requests).await
        }

        async fn scan(
            &self,
            space: StorageSpace,
            range: StorageKeyRange,
            opts: StorageScanOptions,
        ) -> Result<StorageScanChunk, StorageError> {
            let is_manifest_scan = space == BINARY_CAS_MANIFEST_CHUNK_SPACE;
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

    fn manifest_hash_from_range(range: &StorageKeyRange) -> Option<BlobId> {
        let Bound::Included(StorageKey(bytes)) = &range.lower else {
            return None;
        };
        let hash = <[u8; 32]>::try_from(bytes.get(..32)?).ok()?;
        Some(BlobId::from_bytes(hash))
    }

    fn stage_two_chunk_blob(writes: &mut StorageWriteSet, ordinal: usize) -> (BlobId, Vec<u8>) {
        let left = format!("blob-{ordinal}-left").into_bytes();
        let right = format!("blob-{ordinal}-right").into_bytes();
        let bytes = [left.as_slice(), right.as_slice()].concat();
        let blob_hash = BlobId::from_content(&bytes);
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
            let chunk_hash = ChunkHash::from_content(chunk);
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
    ) -> BlobId {
        let blob_hash = BlobId::from_content(label);
        let chunk_hash = BlobId::from_content(label);
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

    async fn stage_test_file_payload(
        storage: &StorageAdapter<Memory>,
        writes: &mut StorageWriteSet,
        payload: &BlobPayload,
        same_length_splice: Option<BlobSameLengthSplice>,
    ) {
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("test blob read should open");
        BinaryCasContext::new()
            .writer_skipping_existing_chunks(&store, writes)
            .stage_file_payload(payload, same_length_splice, None)
            .await
            .expect("test file payload should stage");
    }

    async fn stage_test_bytes(
        storage: &StorageAdapter<Memory>,
        writes: &mut StorageWriteSet,
        bytes: &[u8],
    ) -> BlobWriteReceipt {
        stage_test_payload(storage, writes, &BlobPayload::from_bytes(bytes.to_vec())).await
    }

    #[tokio::test]
    async fn prehashed_existing_blob_reuses_manifest_without_staging_chunks() {
        let storage = StorageAdapter::new(Memory::new());
        let bytes = definitely_multi_chunk_blob_bytes();
        let payload = BlobPayload::from_bytes(bytes.clone());
        let expected_hash = BlobId::from_content(&bytes);

        let mut initial = storage.new_write_set();
        let initial_receipt = stage_test_payload(&storage, &mut initial, &payload).await;
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("initial blob write should commit");

        let mut repeated = storage.new_write_set();
        let repeated_receipt = stage_test_payload(&storage, &mut repeated, &payload).await;

        assert_eq!(initial_receipt, repeated_receipt);
        assert_eq!(repeated_receipt.hash, expected_hash);
        assert!(
            repeated.is_empty(),
            "an existing prehashed blob must not restage its manifest or chunks"
        );
    }

    #[tokio::test]
    async fn stores_manifest_chunks_in_scan_order() {
        let storage = StorageAdapter::new(Memory::new());
        let blob_hash = BlobId::from_content(b"blob-a");
        let chunk_a_hash = BlobId::from_content(b"chunk-a").into_bytes();
        let chunk_b_hash = BlobId::from_content(b"chunk-b").into_bytes();

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
    async fn declared_manifest_reads_ignore_stale_suffix_rows() {
        let storage = StorageAdapter::new(Memory::new());
        let (blob_hash, expected) = {
            let mut writes = storage.new_write_set();
            let fixture = stage_two_chunk_blob(&mut writes, 0);
            stage_manifest_chunk(
                &mut writes,
                fixture.0,
                2,
                &KvBlobManifestChunk {
                    chunk_hash: ChunkHash::from_content(b"stale manifest suffix").into_bytes(),
                    chunk_size: 1,
                },
            );
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("manifest fixture should commit");
            fixture
        };

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            scan_manifest_chunks(&store, blob_hash)
                .await
                .expect("raw manifest rows should scan")
                .len(),
            3,
            "the physical suffix is intentionally present",
        );
        assert_eq!(
            load_bytes_many(&store, &[blob_hash])
                .await
                .expect("declared manifest rows should load")
                .into_vec(),
            vec![Some(expected)],
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
        let missing_hash = BlobId::from_content(b"missing duplicate fixture");

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
        let chunk_hash = ChunkHash::from_content(b"chunk-a");

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
        let blob_hash = BlobId::from_content(b"blob");
        let manifest_key = manifest_key(blob_hash);
        let chunk_key = chunk_key(ChunkHash::from_content(b"chunk"));
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
    fn delta_manifest_rejects_out_of_bounds_copy_program() {
        let hash = BlobId::from_content(b"invalid delta result");
        let error = metadata_from_manifest(
            hash,
            BinaryCasManifest::Delta {
                size_bytes: 10,
                base_blob_hash: BlobId::from_content(b"base").into_bytes(),
                base_size_bytes: 4,
                base_layout: StorageBinaryCasDeltaBaseLayout::SingleChunk {
                    chunk_hash: ChunkHash::from_content(b"base").into_bytes(),
                },
                segments: vec![StorageBinaryCasDeltaSegment::Copy {
                    offset: 0,
                    length: 10,
                }],
            },
        )
        .expect_err("out-of-bounds delta copy must fail");

        assert!(error.message.contains("invalid copy ranges"));
    }

    #[test]
    fn every_non_empty_blob_is_out_of_line() {
        for size in [1, 32 * 1024, 128 * 1024] {
            let bytes = vec![b'a'; size];
            let plan = prepare_blob_write(BinaryCasChunking::default(), &bytes, None)
                .expect("non-empty blob should plan");
            assert!(!plan.chunk_ranges.is_empty());
            assert!(matches!(
                plan.layout,
                BlobLayout::SingleChunk { .. } | BlobLayout::Chunked { .. }
            ));
        }
    }
    #[tokio::test]
    async fn public_kv_api_roundtrips_blob_bytes() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"hello chunked kv cas";
        let blob_hash = BlobId::from_content(data);

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
            Some(BinaryCasManifest::SingleChunk {
                size_bytes: data.len() as u64,
                chunk_hash: blob_hash.into_bytes(),
            })
        );
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        assert_eq!(
            scan_manifest_chunks(&store, blob_hash)
                .await
                .expect("single-chunk blob should not spill manifest chunks"),
            Vec::<KvBlobManifestChunk>::new()
        );
    }

    #[tokio::test]
    async fn ranged_kv_read_reconstructs_only_the_selected_chunk_span() {
        let storage = StorageAdapter::new(Memory::new());
        let data = (0..(3 * 1024 * 1024))
            .map(|index| ((index * 131 + 17) % 251) as u8)
            .collect::<Vec<_>>();
        let blob_hash = BlobId::from_content(&data);
        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &data).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("ranged blob fixture should commit");
        }

        let requested = 900_000..1_300_000;
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("ranged read should open");
        let actual = load_ranges_many(&store, &[(blob_hash, requested.clone())])
            .await
            .expect("blob range should load")
            .into_vec()
            .pop()
            .flatten()
            .expect("blob range should exist");

        assert_eq!(actual.total_size, data.len() as u64);
        assert_eq!(actual.range, requested.clone());
        assert_eq!(
            actual.bytes,
            data[requested.start as usize..requested.end as usize]
        );

        let mut writes = storage.new_write_set();
        writes.delete(
            BINARY_CAS_MANIFEST_CHUNK_SPACE,
            manifest_chunk_key(blob_hash, 2),
        );
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("remove manifest row outside selected range");
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("second ranged read should open");
        let selected = load_ranges_many(&store, &[(blob_hash, requested.clone())])
            .await
            .expect("range read must not visit unselected manifest rows")
            .into_vec()
            .pop()
            .flatten()
            .expect("selected range should exist");
        assert_eq!(
            selected.bytes,
            data[requested.start as usize..requested.end as usize]
        );
    }
    #[tokio::test]
    async fn existing_chunk_aware_writer_batches_persisted_chunk_checks_without_a_hash() {
        let storage = StorageAdapter::new(Memory::new());
        let data = definitely_multi_chunk_blob_bytes();
        let payload = BlobPayload::from_bytes(data.clone());
        let blob_hash = payload.hash().expect("payload should have a hash");
        let chunk_ranges = crate::binary_cas::chunking::fastcdc_chunk_ranges(&data);
        assert!(chunk_ranges.len() > 1);
        let chunk_hashes = chunk_ranges
            .iter()
            .map(|(start, end)| BlobId::from_content(&data[*start..*end]))
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
        stage_blob_write_skipping_existing_chunks(
            BinaryCasChunking::default(),
            &store,
            &mut writes,
            &mut HashSet::new(),
            &mut HashSet::new(),
            &data,
            None,
        )
        .await
        .expect("repeat blob write should stage");

        assert_eq!(
            writes.stats().staged_puts,
            1 + u64::try_from(chunk_ranges.len()).expect("chunk count should fit in u64")
        );
        let metrics = crate::binary_cas::metrics::binary_cas_write_metrics_snapshot();
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

    #[tokio::test]
    async fn same_length_splice_writer_reuses_unchanged_manifest_chunks_and_roundtrips() {
        let storage = StorageAdapter::new(Memory::new());
        let before = definitely_multi_chunk_blob_bytes();
        let base_blob_hash = BlobId::from_content(&before);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &before).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("base blob should commit");
        }

        let base_chunks = {
            let store = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("base manifest read should open");
            scan_manifest_chunks(&store, base_blob_hash)
                .await
                .expect("base manifest should scan")
        };
        assert!(base_chunks.len() > 1, "fixture must be chunked");
        let changed_chunk_index = base_chunks.len() / 2;
        let changed_chunk_start = base_chunks
            .iter()
            .take(changed_chunk_index)
            .map(|chunk| usize::try_from(chunk.chunk_size).expect("chunk size should fit"))
            .sum::<usize>();
        let changed_chunk_len =
            usize::try_from(base_chunks[changed_chunk_index].chunk_size).expect("chunk fits");
        let edit_offset = changed_chunk_start + changed_chunk_len / 2;
        let mut after = before.clone();
        after[edit_offset] ^= 0xff;
        let after_blob_hash = BlobId::from_content(&after);
        let after_payload = BlobPayload::from_bytes(after.clone());

        {
            let mut writes = storage.new_write_set();
            stage_test_file_payload(
                &storage,
                &mut writes,
                &after_payload,
                Some(BlobSameLengthSplice::new(base_blob_hash, edit_offset, 1)),
            )
            .await;
            assert_eq!(
                writes.stats().staged_puts,
                u64::try_from(base_chunks.len() + 3).expect("write count should fit"),
                "one changed chunk needs one presence marker and one payload; all other chunks are manifest references",
            );
            writes
                .validate()
                .expect("reused manifest writes should be canonical");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("same-length replacement should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("result read should open");
        let after_chunks = scan_manifest_chunks(&store, after_blob_hash)
            .await
            .expect("replacement manifest should scan");
        assert_eq!(after_chunks.len(), base_chunks.len());
        let changed_chunk_count = after_chunks
            .iter()
            .zip(&base_chunks)
            .filter(|(after_chunk, base_chunk)| after_chunk.chunk_hash != base_chunk.chunk_hash)
            .count();
        assert_eq!(
            changed_chunk_count, 1,
            "only the overlapping chunk may change"
        );
        assert_eq!(
            load_bytes_many(&store, &[after_blob_hash])
                .await
                .expect("replacement bytes should load")
                .into_vec(),
            vec![Some(after)],
        );
    }

    #[tokio::test]
    async fn flat_delta_writer_merges_edits_against_one_full_base_and_roundtrips() {
        let storage = StorageAdapter::new(Memory::new());
        let before = b"a representative text line with stable compression boundaries\n".repeat(512);
        let full_base_hash = BlobId::from_content(&before);
        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &before).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("full delta base should commit");
        }

        let first_offset = before.len() / 3;
        let mut first = before.clone();
        first[first_offset] ^= 1;
        let first_hash = BlobId::from_content(&first);
        {
            let payload = BlobPayload::from_bytes(first.clone());
            let store = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("first delta read should open");
            let mut writes = storage.new_write_set();
            BinaryCasContext::new()
                .writer_skipping_existing_chunks(&store, &mut writes)
                .stage_file_payload(
                    &payload,
                    Some(BlobSameLengthSplice::new(full_base_hash, first_offset, 1)),
                    Some(BlobEditSplice {
                        base_blob_hash: full_base_hash,
                        offset: first_offset,
                        delete_len: 1,
                        insert_len: 1,
                    }),
                )
                .await
                .expect("first delta should stage");
            assert_eq!(writes.stats().staged_puts, 1, "a delta is one manifest row");
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("first delta should commit");
        }

        let second_offset = before.len() * 2 / 3;
        let mut second = first.clone();
        second[second_offset] ^= 1;
        let second_hash = BlobId::from_content(&second);
        {
            let payload = BlobPayload::from_bytes(second.clone());
            let store = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("second delta read should open");
            let mut writes = storage.new_write_set();
            BinaryCasContext::new()
                .writer_skipping_existing_chunks(&store, &mut writes)
                .stage_file_payload(
                    &payload,
                    Some(BlobSameLengthSplice::new(first_hash, second_offset, 1)),
                    Some(BlobEditSplice {
                        base_blob_hash: first_hash,
                        offset: second_offset,
                        delete_len: 1,
                        insert_len: 1,
                    }),
                )
                .await
                .expect("second delta should stage");
            assert_eq!(writes.stats().staged_puts, 1);
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("second delta should commit");
        }

        let third_offset = before.len() / 2;
        let inserted = b"inserted";
        let mut third = second.clone();
        third.splice(third_offset..third_offset, inserted.iter().copied());
        let third_hash = BlobId::from_content(&third);
        {
            let payload = BlobPayload::from_bytes(third.clone());
            let store = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("length-changing delta read should open");
            let mut writes = storage.new_write_set();
            BinaryCasContext::new()
                .writer_skipping_existing_chunks(&store, &mut writes)
                .stage_file_payload(
                    &payload,
                    None,
                    Some(BlobEditSplice {
                        base_blob_hash: second_hash,
                        offset: third_offset,
                        delete_len: 0,
                        insert_len: inserted.len(),
                    }),
                )
                .await
                .expect("length-changing delta should stage");
            assert_eq!(writes.stats().staged_puts, 1);
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("length-changing delta should commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("delta result read should open");
        let metadata = load_metadata_many(&store, &[second_hash])
            .await
            .expect("delta metadata should load")
            .into_vec()
            .pop()
            .flatten()
            .expect("delta metadata should exist");
        let BlobLayout::Delta {
            base_blob_hash,
            segments,
            ..
        } = metadata.layout
        else {
            panic!("repeated edit should retain a flat delta")
        };
        assert_eq!(base_blob_hash, full_base_hash);
        assert_eq!(segments.len(), 5);
        assert_eq!(
            load_bytes_many(&store, &[first_hash, second_hash, third_hash, third_hash])
                .await
                .expect("flat deltas should load")
                .into_vec(),
            vec![Some(first), Some(second), Some(third.clone()), Some(third),],
        );
    }

    #[tokio::test]
    async fn same_length_splice_writer_falls_back_when_result_length_changes() {
        let storage = StorageAdapter::new(Memory::new());
        let before = definitely_multi_chunk_blob_bytes();
        let base_blob_hash = BlobId::from_content(&before);

        {
            let mut writes = storage.new_write_set();
            stage_test_bytes(&storage, &mut writes, &before).await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("base blob should commit");
        }

        let edit_offset = before.len() / 2;
        let mut after = before.clone();
        after.insert(edit_offset, b'!');
        let after_blob_hash = BlobId::from_content(&after);
        let after_payload = BlobPayload::from_bytes(after.clone());
        {
            let mut writes = storage.new_write_set();
            stage_test_file_payload(
                &storage,
                &mut writes,
                &after_payload,
                Some(BlobSameLengthSplice::new(base_blob_hash, edit_offset, 1)),
            )
            .await;
            storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("length-changing replacement should fall back and commit");
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("result read should open");
        let expected_hashes = crate::binary_cas::chunking::fastcdc_chunk_ranges(&after)
            .into_iter()
            .map(|(start, end)| BlobId::from_content(&after[start..end]).into_bytes())
            .collect::<Vec<_>>();
        let actual_hashes = scan_manifest_chunks(&store, after_blob_hash)
            .await
            .expect("fallback manifest should scan")
            .into_iter()
            .map(|chunk| chunk.chunk_hash)
            .collect::<Vec<_>>();
        assert_eq!(
            actual_hashes, expected_hashes,
            "fallback must retain FastCDC layout"
        );
        assert_eq!(
            load_bytes_many(&store, &[after_blob_hash])
                .await
                .expect("fallback bytes should load")
                .into_vec(),
            vec![Some(after)],
        );
    }

    #[test]
    fn prepared_blob_write_stages_duplicate_chunk_payload_once() {
        let data = b"abcabc";
        let blob_hash = BlobId::from_content(data);
        let chunk_hash = ChunkHash::from_content(b"abc");
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
            Some(BinaryCasManifest::SingleChunk {
                size_bytes: data.len() as u64,
                chunk_hash: blob_hash.into_bytes(),
            })
        );
    }

    #[tokio::test]
    async fn immutable_storage_rejects_chunk_bytes_that_do_not_match_identity() {
        let storage = StorageAdapter::new(Memory::new());
        let data = b"same length";
        let corrupted = b"SAME length";
        let blob_hash = BlobId::from_content(data);

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
            stage_chunk(
                &mut writes,
                ChunkHash::from_content(data),
                BinaryChunkCodec::Raw,
                corrupted.len() as u64,
                corrupted,
            );
            let error = storage
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect_err("corrupt immutable chunk must not overwrite");
            assert!(error.to_string().contains("immutable identity"));
        }

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let loaded = load_bytes_many(&store, &[blob_hash])
            .await
            .expect("original chunk remains readable");
        assert_eq!(loaded.into_vec(), vec![Some(data.to_vec())]);
    }
    #[test]
    fn decode_rejects_same_length_raw_bytes_for_wrong_hash() {
        let expected = vec![b'a'; 128 * 1024];
        let substituted = vec![b'b'; expected.len()];
        assert_eq!(expected.len(), substituted.len());
        let expected_hash = BlobId::from_content(&expected);
        let row =
            encode_binary_cas_chunk(BinaryChunkCodec::Raw, expected.len() as u64, &substituted);

        let error = decode_and_verify_chunk(
            &row,
            expected.len(),
            expected_hash,
            ChunkHash::from_content(&expected),
        )
        .expect_err("raw bytes for a different hash should be rejected");

        assert!(
            error
                .message
                .contains("failed content-address verification")
        );
    }

    #[test]
    fn decode_rejects_chunks_above_the_format_maximum_before_payload_validation() {
        let data = b"valid raw content".repeat(4096);
        let chunk_hash = ChunkHash::from_content(&data);
        let blob_hash = BlobId::from_single_chunk(chunk_hash);
        let oversized_len = MAX_BINARY_CAS_CHUNK_BYTES + 1;
        let row = encode_binary_cas_chunk(BinaryChunkCodec::Raw, oversized_len as u64, &data);

        let error = decode_and_verify_chunk(&row, oversized_len, blob_hash, chunk_hash)
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
        let expected_blob_hash = BlobId::from_content(expected);
        let substituted_chunk_hash = ChunkHash::from_content(substituted);

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
                    chunk_hash: substituted_chunk_hash.into_bytes(),
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
        let blob_hash = BlobId::from_content(data);

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
        let blob_hash = BlobId::from_content(&data);

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
