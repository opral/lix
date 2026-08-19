#![allow(clippy::cast_sign_loss)]

use crate::LixError;
use crate::binary_cas::chunking::{CHUNK_ANCHOR_BYTES, MAX_BINARY_CAS_CHUNK_BYTES, chunk_ranges};
use crate::binary_cas::codec::{
    BinaryCasManifest, BinaryChunkCodec, StorageBinaryCasDeltaBaseLayout,
    StorageBinaryCasDeltaSegment, decode_binary_cas_chunk, decode_binary_cas_manifest,
    decode_binary_cas_manifest_chunk, encode_binary_cas_chunk, encode_binary_cas_manifest,
    encode_binary_cas_manifest_chunk,
};
use crate::binary_cas::{
    BinaryCasGcSweep, BlobBytesBatch, BlobDeltaBaseLayout, BlobDeltaSegment, BlobEditSplice,
    BlobId, BlobLayout, BlobMetadata, BlobMetadataBatch, BlobRangeBytes, BlobRangeBytesBatch,
    BlobSameLengthSplice, BlobWriteReceipt, ChunkHash,
};
#[cfg(test)]
use crate::storage_adapter::StoragePrefix;
use crate::storage_adapter::{
    PointReadPlan, REVISION_KEY_BINARY_CAS_PUBLICATION, REVISION_KEY_BINARY_CAS_RECLAMATION,
    REVISION_SPACE, StorageAdapterRead, StorageSpace, StorageWriteSet, ValueSemantics,
    load_revision, load_revisions, revision_key,
};
use crate::storage_adapter::{
    StorageBeginScanOptions, StorageCoreProjection, StorageGetOptions, StorageKey, StorageKeyRange,
    StoragePrecondition, StorageProjectedValue, StorageSpaceId, StorageValue,
};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::{Bound, Range};
use web_time::Instant;

// Keep independent manifest scans bounded so large blob batches do not create
// unbounded backend pressure. Eight matches the engine's other remote scan
// fan-out and is enough to hide storage latency without a large request burst.
const MANIFEST_SCAN_CONCURRENCY: usize = 8;
const MAX_DELTA_SEGMENTS: usize = 32;
const MAX_DELTA_INSERT_BYTES: usize = 64 * 1024;
const MAX_DELTA_INSERT_FRACTION_DIVISOR: usize = 8;
// Every reclamation scan is bounded by rows because every reclamation scan now
// carries bounded per-row payloads: three of the four planes are scanned
// key-only (32- or 40-byte keys), and the manifest plane's values are a fixed
// header. The chunk plane deliberately does *not* project its value: chunk
// payloads are 256 KiB-4 MiB, so a row-bounded window over them would be
// unbounded in bytes, which is why this used to be paged one row at a time.
const CAS_RECLAIM_PAGE_ROWS: usize = 256;

pub(crate) const BINARY_CAS_MANIFEST_NAMESPACE: &str = "binary_cas.manifest";
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_NAMESPACE: &str = "binary_cas.manifest_chunk";
pub(crate) const BINARY_CAS_CHUNK_NAMESPACE: &str = "binary_cas.chunk";
pub(crate) const BINARY_CAS_CHUNK_PRESENCE_NAMESPACE: &str = "binary_cas.chunk_presence";
pub(crate) const BINARY_CAS_CHUNK_DEMAND_NAMESPACE: &str = "binary_cas.chunk_demand.v1";
pub(crate) const BINARY_CAS_MANIFEST_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0005_0001),
    BINARY_CAS_MANIFEST_NAMESPACE,
    ValueSemantics::Mutable,
);
pub(crate) const BINARY_CAS_MANIFEST_CHUNK_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0005_0002),
    BINARY_CAS_MANIFEST_CHUNK_NAMESPACE,
    ValueSemantics::Mutable,
);
/// The chunk payload plane, and the one space whose values authenticate
/// themselves.
///
/// Every row's key **is** the BLAKE3-256 digest of its own payload, and every
/// production full-value read of this space passes through
/// [`decode_and_verify_payload`], which recomputes that digest and compares it
/// before returning the bytes — unconditionally, in release builds too. The
/// two read sites are [`load_chunk_rows`] (which serves [`load_bytes_many`])
/// and [`verify_live_chunk_presence`]; the GC orphan scan projects
/// `KeyOnly` and never materializes a payload.
///
/// So a backend checksum over these same bytes is a strictly weaker duplicate
/// of a check the engine has already paid for, and the declaration below lets
/// a backend skip it. Measured on a 640 MiB read where RocksDB was on its
/// software CRC32C — the path aarch64 cannot leave — that duplicate was 33.8%
/// of all cycles.
///
/// **If you ever add a full-value read of this space that does not verify the
/// digest, this declaration becomes false** and must be reverted to
/// `StorageSpace::declare` in the same commit.
pub(crate) const BINARY_CAS_CHUNK_SPACE: StorageSpace = StorageSpace::declare_content_addressed(
    StorageSpaceId(0x0005_0003),
    BINARY_CAS_CHUNK_NAMESPACE,
    ValueSemantics::Immutable,
);
pub(crate) const BINARY_CAS_CHUNK_PRESENCE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0005_0004),
    BINARY_CAS_CHUNK_PRESENCE_NAMESPACE,
    ValueSemantics::Mutable,
);
pub(crate) const BINARY_CAS_CHUNK_DEMAND_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0005_0005),
    BINARY_CAS_CHUNK_DEMAND_NAMESPACE,
    ValueSemantics::Mutable,
);
fn fresh_revision_token() -> StorageValue {
    StorageValue {
        bytes: Bytes::copy_from_slice(uuid::Uuid::now_v7().as_bytes()),
    }
}

fn unchanged_revision_precondition(
    key: &'static [u8],
    token: Option<Bytes>,
) -> StoragePrecondition {
    let key = revision_key(key);
    match token {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: REVISION_SPACE,
            key,
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: REVISION_SPACE,
            key,
        },
    }
}

pub(in crate::binary_cas) async fn stage_publication_fence(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    let reclamation = load_revision(store, REVISION_KEY_BINARY_CAS_RECLAMATION).await?;
    // A fresh token, not a counter increment: two publishers planned from the
    // same snapshot must both be able to commit, so neither can hold a
    // compare-and-set on this row. Uniqueness is what a sweep's equality
    // precondition needs, and blind counter increments would let a second
    // publisher restore the value a sweep already observed.
    writes.put(
        REVISION_SPACE,
        revision_key(REVISION_KEY_BINARY_CAS_PUBLICATION),
        fresh_revision_token(),
    );
    preconditions.push(unchanged_revision_precondition(
        REVISION_KEY_BINARY_CAS_RECLAMATION,
        reclamation,
    ));
    Ok(())
}

pub(in crate::binary_cas) async fn stage_reclamation_fence(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    preconditions: &mut Vec<StoragePrecondition>,
) -> Result<(), LixError> {
    let [publication, reclamation] = load_revisions(
        store,
        [
            REVISION_KEY_BINARY_CAS_PUBLICATION,
            REVISION_KEY_BINARY_CAS_RECLAMATION,
        ],
    )
    .await?;
    writes.put(
        REVISION_SPACE,
        revision_key(REVISION_KEY_BINARY_CAS_RECLAMATION),
        fresh_revision_token(),
    );
    preconditions.push(unchanged_revision_precondition(
        REVISION_KEY_BINARY_CAS_PUBLICATION,
        publication,
    ));
    preconditions.push(unchanged_revision_precondition(
        REVISION_KEY_BINARY_CAS_RECLAMATION,
        reclamation,
    ));
    Ok(())
}

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

/// Stages deletion of binary-CAS rows not reachable from authenticated blob
/// roots and active upload receipts.
///
/// This is intentionally an explicit maintenance operation. Reachability is
/// decided entirely from keys and bounded manifest metadata, so no scan here
/// projects a chunk payload and no blob is ever reconstructed. Any malformed
/// live manifest or missing live chunk fails closed before the caller can
/// commit the write set.
pub(in crate::binary_cas) async fn stage_reclaim_unreachable_binary_cas(
    store: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    blob_roots: &BTreeSet<BlobId>,
    upload_chunks: &BTreeMap<ChunkHash, u64>,
) -> Result<BinaryCasGcSweep, LixError> {
    let mut live_blobs = BTreeSet::new();
    let mut live_chunks = BTreeMap::new();
    for (chunk_hash, expected_size) in upload_chunks {
        mark_live_chunk_expectation(
            &mut live_chunks,
            *chunk_hash,
            *expected_size,
            "active upload receipt",
        )?;
    }
    let mut live_manifest_sizes = BTreeMap::<BlobId, u64>::new();

    for root in blob_roots {
        mark_live_blob(
            store,
            *root,
            &mut live_blobs,
            &mut live_chunks,
            &mut live_manifest_sizes,
        )
        .await?;
    }

    let mut result = BinaryCasGcSweep {
        live_blob_count: live_blobs.len(),
        live_chunk_count: live_chunks.len(),
        ..BinaryCasGcSweep::default()
    };

    // Authenticate every declared live payload before staging any orphan
    // mutation. The caller commits one atomic write set, but keeping a failed
    // plan empty also prevents accidental reuse of partially staged sweep
    // work after corruption is reported.
    let live_demand_chunks = verify_live_chunk_presence(store, &live_chunks).await?;

    let mut manifest_cursor = store
        .begin_scan(
            BINARY_CAS_MANIFEST_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = manifest_cursor
            .next_page(CAS_RECLAIM_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let blob_id = BlobId::from_bytes(entry.key.0.as_ref().try_into().map_err(|_| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "binary CAS manifest key is not a 32-byte blob hash",
                )
            })?);
            let StorageProjectedValue::FullValue(bytes) = entry.value else {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "binary CAS manifest scan omitted its value",
                ));
            };
            let manifest = decode_binary_cas_manifest(&bytes)?;
            if !live_blobs.contains(&blob_id) {
                writes.delete(
                    BINARY_CAS_MANIFEST_SPACE,
                    StorageKey(Bytes::copy_from_slice(blob_id.as_bytes())),
                );
                result.reclaimed_manifest_rows += 1;
            } else {
                validate_live_manifest_identity(blob_id, &manifest)?;
            }
        }
        if !page_has_more {
            break;
        }
    }

    let mut manifest_chunk_cursor = store
        .begin_scan(
            BINARY_CAS_MANIFEST_CHUNK_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = manifest_chunk_cursor
            .next_page(CAS_RECLAIM_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let (blob_id, offset) = decode_manifest_chunk_key(&entry.key)?;
            let keep = live_manifest_sizes
                .get(&blob_id)
                .is_some_and(|size_bytes| offset < *size_bytes);
            if !keep {
                writes.delete(BINARY_CAS_MANIFEST_CHUNK_SPACE, entry.key);
                result.reclaimed_manifest_chunk_rows += 1;
            }
        }
        if !page_has_more {
            break;
        }
    }

    // Orphan selection is a key-set difference, so this scan must never
    // project chunk payloads. On SlateDB the chunk plane is
    // `ValueSemantics::Immutable`: its values live in object-store segments and
    // a full-value page pays one segment fetch round trip. The page is
    // hydrated before this loop can decide a row is live, so that round trip
    // was charged for every chunk in the repository, not only the reclaimed
    // ones. Key-only keeps the whole plane inside the LSM index.
    let mut chunk_cursor = store
        .begin_scan(
            BINARY_CAS_CHUNK_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = chunk_cursor
            .next_page(CAS_RECLAIM_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let chunk_hash =
                ChunkHash::from_bytes(entry.key.0.as_ref().try_into().map_err(|_| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "binary CAS chunk key is not a 32-byte chunk hash",
                    )
                })?);
            if live_chunks.contains_key(&chunk_hash) {
                continue;
            }
            writes.delete(BINARY_CAS_CHUNK_SPACE, entry.key);
            result.reclaimed_chunk_rows += 1;
        }
        if !page_has_more {
            break;
        }
    }

    let mut presence_cursor = store
        .begin_scan(
            BINARY_CAS_CHUNK_PRESENCE_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = presence_cursor
            .next_page(CAS_RECLAIM_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let chunk_hash =
                ChunkHash::from_bytes(entry.key.0.as_ref().try_into().map_err(|_| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "binary CAS chunk-presence key is not a 32-byte chunk hash",
                    )
                })?);
            if !live_chunks.contains_key(&chunk_hash) {
                writes.delete(BINARY_CAS_CHUNK_PRESENCE_SPACE, entry.key);
            }
        }
        if !page_has_more {
            break;
        }
    }

    let mut demand_cursor = store
        .begin_scan(
            BINARY_CAS_CHUNK_DEMAND_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    loop {
        let (page, page_has_more) = demand_cursor
            .next_page(CAS_RECLAIM_PAGE_ROWS)
            .await?
            .into_parts();
        for entry in page {
            let chunk_hash =
                ChunkHash::from_bytes(entry.key.0.as_ref().try_into().map_err(|_| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "binary CAS chunk-demand key is not a 32-byte chunk hash",
                    )
                })?);
            if !live_demand_chunks.contains(&chunk_hash) {
                writes.delete(BINARY_CAS_CHUNK_DEMAND_SPACE, entry.key);
                result.reclaimed_demand_marker_rows += 1;
            }
        }
        if !page_has_more {
            break;
        }
    }

    Ok(result)
}

async fn mark_live_blob(
    store: &(impl StorageAdapterRead + ?Sized),
    root_blob_id: BlobId,
    live_blobs: &mut BTreeSet<BlobId>,
    live_chunks: &mut BTreeMap<ChunkHash, u64>,
    live_manifest_sizes: &mut BTreeMap<BlobId, u64>,
) -> Result<(), LixError> {
    let mut pending = vec![root_blob_id];
    while let Some(blob_id) = pending.pop() {
        if !live_blobs.insert(blob_id) {
            continue;
        }
        let bytes = get_one(store, BINARY_CAS_MANIFEST_SPACE, manifest_key(blob_id))
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "live binary CAS blob '{}' has no manifest",
                        blob_id.to_hex()
                    ),
                )
            })?;
        let manifest = decode_binary_cas_manifest(&bytes)?;
        let metadata = validate_live_manifest_identity(blob_id, &manifest)?;
        match manifest {
            BinaryCasManifest::Empty { .. } => {}
            BinaryCasManifest::SingleChunk {
                size_bytes,
                chunk_hash,
            } => {
                mark_live_chunk_expectation(
                    live_chunks,
                    ChunkHash::from_bytes(chunk_hash),
                    size_bytes,
                    &format!("live blob '{}'", blob_id.to_hex()),
                )?;
            }
            BinaryCasManifest::Chunked {
                size_bytes: declared_size,
                chunk_count,
            } => {
                live_manifest_sizes.insert(blob_id, declared_size);
                let chunks = load_declared_manifest_chunks(store, blob_id, declared_size).await?;
                if chunks.len() != chunk_count as usize {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!(
                            "live binary CAS blob '{}' expected {} manifest chunks, found {}",
                            blob_id.to_hex(),
                            chunk_count,
                            chunks.len()
                        ),
                    ));
                }
                let chunk_size_sum = chunks
                    .iter()
                    .try_fold(0_u64, |total, chunk| total.checked_add(chunk.chunk_size));
                let chunk_size_sum = chunk_size_sum.ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        "binary CAS manifest chunk sizes overflowed",
                    )
                })?;
                if chunk_size_sum != declared_size {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!(
                            "live binary CAS blob '{}' declares {} bytes but its chunks declare {}",
                            blob_id.to_hex(),
                            declared_size,
                            chunk_size_sum
                        ),
                    ));
                }
                if BlobId::from_chunks(
                    declared_size,
                    chunks
                        .iter()
                        .map(|chunk| (ChunkHash::from_bytes(chunk.chunk_hash), chunk.chunk_size)),
                ) != blob_id
                {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!(
                            "live binary CAS blob '{}' failed manifest content-address verification",
                            blob_id.to_hex()
                        ),
                    ));
                }
                for chunk in chunks {
                    mark_live_chunk_expectation(
                        live_chunks,
                        ChunkHash::from_bytes(chunk.chunk_hash),
                        chunk.chunk_size,
                        &format!("live blob '{}'", blob_id.to_hex()),
                    )?;
                }
            }
            BinaryCasManifest::Delta { .. } => {
                let BlobLayout::Delta {
                    base_blob_hash,
                    base_size_bytes,
                    base_layout,
                    ..
                } = metadata.layout
                else {
                    unreachable!("delta manifest produces delta metadata")
                };
                let base_manifest_bytes = get_one(
                    store,
                    BINARY_CAS_MANIFEST_SPACE,
                    manifest_key(base_blob_hash),
                )
                .await?
                .ok_or_else(|| {
                    LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!(
                            "live binary CAS delta '{}' has no base manifest '{}'",
                            blob_id.to_hex(),
                            base_blob_hash.to_hex()
                        ),
                    )
                })?;
                let base_manifest = decode_binary_cas_manifest(&base_manifest_bytes)?;
                let base_metadata =
                    validate_live_manifest_identity(base_blob_hash, &base_manifest)?;
                let expected_layout = match base_layout {
                    BlobDeltaBaseLayout::SingleChunk { chunk_hash } => {
                        BlobLayout::SingleChunk { chunk_hash }
                    }
                    BlobDeltaBaseLayout::Chunked { chunk_count } => {
                        BlobLayout::Chunked { chunk_count }
                    }
                };
                if base_metadata.size_bytes != base_size_bytes
                    || base_metadata.layout != expected_layout
                {
                    return Err(LixError::new(
                        LixError::CODE_STORAGE_ERROR,
                        format!(
                            "live binary CAS delta '{}' disagrees with base '{}' size/layout",
                            blob_id.to_hex(),
                            base_blob_hash.to_hex()
                        ),
                    ));
                }
                pending.push(base_blob_hash);
            }
        }
    }
    Ok(())
}

fn validate_live_manifest_identity(
    blob_id: BlobId,
    manifest: &BinaryCasManifest,
) -> Result<BlobMetadata, LixError> {
    let metadata = metadata_from_manifest(blob_id, manifest.clone())?;
    if matches!(manifest, BinaryCasManifest::Delta { .. }) {
        return Ok(metadata);
    }
    match manifest {
        BinaryCasManifest::Empty { size_bytes } if *size_bytes == 0 => {
            if blob_id != BlobId::from_content(&[]) {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "binary CAS empty manifest '{}' has the wrong hash",
                        blob_id.to_hex()
                    ),
                ));
            }
        }
        BinaryCasManifest::SingleChunk {
            size_bytes,
            chunk_hash,
        } => {
            if *size_bytes == 0
                || blob_id != BlobId::from_single_chunk(ChunkHash::from_bytes(*chunk_hash))
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "binary CAS single-chunk manifest '{}' has the wrong hash",
                        blob_id.to_hex()
                    ),
                ));
            }
        }
        BinaryCasManifest::Chunked {
            size_bytes,
            chunk_count,
        } if *size_bytes == 0 || *chunk_count == 0 => {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!(
                    "binary CAS chunked manifest '{}' has invalid size/count",
                    blob_id.to_hex()
                ),
            ));
        }
        BinaryCasManifest::Delta { .. } => unreachable!("delta returned above"),
        _ => {}
    }
    Ok(metadata)
}

fn mark_live_chunk_expectation(
    live_chunks: &mut BTreeMap<ChunkHash, u64>,
    chunk_hash: ChunkHash,
    expected_size: u64,
    source: &str,
) -> Result<(), LixError> {
    if expected_size == 0 || expected_size > MAX_BINARY_CAS_CHUNK_BYTES as u64 {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!(
                "{source} declares invalid size {expected_size} for chunk '{}'",
                chunk_hash.to_hex()
            ),
        ));
    }
    match live_chunks.entry(chunk_hash) {
        std::collections::btree_map::Entry::Vacant(entry) => {
            entry.insert(expected_size);
        }
        std::collections::btree_map::Entry::Occupied(entry) if *entry.get() != expected_size => {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                format!(
                    "live binary CAS chunk '{}' has conflicting declared sizes {} and {}",
                    chunk_hash.to_hex(),
                    entry.get(),
                    expected_size
                ),
            ));
        }
        std::collections::btree_map::Entry::Occupied(_) => {}
    }
    Ok(())
}

fn decode_manifest_chunk_key(key: &StorageKey) -> Result<(BlobId, u64), LixError> {
    if key.0.len() != 40 {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "binary CAS manifest-chunk key is not 40 bytes",
        ));
    }
    let blob_id = BlobId::from_bytes(key.0[..32].try_into().expect("checked blob hash width"));
    let offset = u64::from_be_bytes(key.0[32..].try_into().expect("checked offset width"));
    Ok((blob_id, offset))
}

async fn verify_live_chunk_presence(
    store: &(impl StorageAdapterRead + ?Sized),
    live_chunks: &BTreeMap<ChunkHash, u64>,
) -> Result<BTreeSet<ChunkHash>, LixError> {
    let mut demand_chunks = BTreeSet::new();
    for (hash, expected_size) in live_chunks {
        let key = StorageKey(Bytes::copy_from_slice(hash.as_bytes()));
        let payload_result = PointReadPlan::new(BINARY_CAS_CHUNK_SPACE, std::slice::from_ref(&key))
            .materialize(
                store,
                StorageGetOptions {
                    projection: StorageCoreProjection::FullValue,
                },
            )
            .await?;
        let presence_result =
            PointReadPlan::new(BINARY_CAS_CHUNK_PRESENCE_SPACE, std::slice::from_ref(&key))
                .materialize(
                    store,
                    StorageGetOptions {
                        projection: StorageCoreProjection::KeyOnly,
                    },
                )
                .await?;
        let demand_result =
            PointReadPlan::new(BINARY_CAS_CHUNK_DEMAND_SPACE, std::slice::from_ref(&key))
                .materialize(
                    store,
                    StorageGetOptions {
                        projection: StorageCoreProjection::KeyOnly,
                    },
                )
                .await?;
        let payload = payload_result.value.first().and_then(Option::as_ref);
        let present = presence_result.value.first().is_some_and(Option::is_some);
        let demanded = demand_result.value.first().is_some_and(Option::is_some);
        match (payload, present, demanded) {
            (Some(StorageProjectedValue::FullValue(bytes)), true, _) => {
                decode_and_verify_chunk(
                    bytes,
                    persisted_size_to_usize(*expected_size, "live binary CAS chunk")?,
                    BlobId::from_bytes(hash.into_bytes()),
                    *hash,
                )?;
            }
            (None, false, true) => {
                demand_chunks.insert(*hash);
            }
            (None, false, false) => {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "live binary CAS chunk '{}' is missing without a demand marker",
                        hash.to_hex()
                    ),
                ));
            }
            (None, true, _) => {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "live binary CAS chunk '{}' has a presence row but no payload row",
                        hash.to_hex()
                    ),
                ));
            }
            (Some(StorageProjectedValue::FullValue(_)), false, _) => {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    format!(
                        "live binary CAS chunk '{}' is missing its presence row",
                        hash.to_hex()
                    ),
                ));
            }
            (Some(_), _, _) => {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "binary CAS payload read omitted its full value",
                ));
            }
        }
    }
    Ok(demand_chunks)
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
/// declared byte extent keeps stale suffix rows harmless; the caller still
/// rejects missing declared rows by comparing the resulting count.
async fn load_declared_manifest_chunks(
    store: &(impl StorageAdapterRead + ?Sized),
    blob_hash: BlobId,
    size_bytes: u64,
) -> Result<Vec<KvBlobManifestChunk>, LixError> {
    load_declared_manifest_chunk_range(store, blob_hash, 0, size_bytes).await
}

/// Loads the manifest rows covering one byte interval of a chunked blob.
///
/// `start_offset` must name a chunk start. Callers align it to
/// [`CHUNK_ANCHOR_BYTES`], which the chunker forces to be a boundary.
async fn load_declared_manifest_chunk_range(
    store: &(impl StorageAdapterRead + ?Sized),
    blob_hash: BlobId,
    start_offset: u64,
    end_offset: u64,
) -> Result<Vec<KvBlobManifestChunk>, LixError> {
    if start_offset >= end_offset {
        return Ok(Vec::new());
    }
    let range = StorageKeyRange {
        lower: Bound::Included(StorageKey(Bytes::from(manifest_chunk_key(
            blob_hash,
            start_offset,
        )))),
        upper: Bound::Excluded(StorageKey(Bytes::from(manifest_chunk_key(
            blob_hash, end_offset,
        )))),
    };
    scan_all_values_for_range(store, BINARY_CAS_MANIFEST_CHUNK_SPACE, range)
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
    chunk_offset: u64,
    chunk: &KvBlobManifestChunk,
) {
    writes.put(
        BINARY_CAS_MANIFEST_CHUNK_SPACE,
        key(manifest_chunk_key(blob_hash, chunk_offset)),
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
    writes.delete(BINARY_CAS_CHUNK_DEMAND_SPACE, key(chunk_key(chunk_hash)));
}

pub(in crate::binary_cas) fn stage_chunk_demand(
    writes: &mut StorageWriteSet,
    chunk_hash: ChunkHash,
) {
    writes.put(
        BINARY_CAS_CHUNK_DEMAND_SPACE,
        key(chunk_key(chunk_hash)),
        value(Vec::new()),
    );
}

pub(in crate::binary_cas) fn stage_chunk_available(
    writes: &mut StorageWriteSet,
    chunk_hash: ChunkHash,
) {
    writes.delete(BINARY_CAS_CHUNK_DEMAND_SPACE, key(chunk_key(chunk_hash)));
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

pub(in crate::binary_cas) async fn stage_upload_part_skipping_existing(
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
    // A part is anchor-aligned and at most one anchor long, so chunking it in
    // isolation reproduces exactly the boundaries a whole-buffer write of the
    // same file would choose. That is what keeps parts independent and lets up
    // to four of them complete out of order.
    let ranges = chunk_ranges(bytes);
    let receipts = ranges
        .iter()
        .map(|&(start, end)| crate::binary_cas::BlobChunkReceipt {
            hash: ChunkHash::from_content(&bytes[start..end]),
            size_bytes: (end - start) as u64,
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
    for (&(start, end), receipt) in ranges.iter().zip(&receipts) {
        if missing.remove(&receipt.hash) {
            stage_content_chunk(writes, receipt.hash, &bytes[start..end])?;
        }
    }
    Ok(receipts)
}

pub(in crate::binary_cas) fn stage_upload_manifest(
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
    for chunk in chunks.iter() {
        if chunk.size_bytes == 0 || chunk.size_bytes > MAX_BINARY_CAS_CHUNK_BYTES as u64 {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "resumable file receipts are not canonical chunks",
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
            let mut chunk_offset = 0u64;
            for chunk in chunks.iter() {
                stage_manifest_chunk(
                    writes,
                    hash,
                    chunk_offset,
                    &KvBlobManifestChunk {
                        chunk_hash: chunk.hash.into_bytes(),
                        chunk_size: chunk.size_bytes,
                    },
                );
                chunk_offset += chunk.size_bytes;
            }
        }
        BlobLayout::Delta { .. } => unreachable!("resumable upload cannot produce delta layout"),
    }
    Ok(BlobWriteReceipt {
        hash,
        size_bytes,
        layout,
    })
}

async fn get_one(
    store: &(impl StorageAdapterRead + ?Sized),
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
    let range = StoragePrefix {
        bytes: Bytes::from(prefix),
    }
    .to_range()?;
    scan_all_values_for_range(store, space, range).await
}

async fn scan_all_values_for_range(
    store: &(impl StorageAdapterRead + ?Sized),
    space: StorageSpace,
    range: StorageKeyRange,
) -> Result<Vec<Vec<u8>>, LixError> {
    let mut values = Vec::new();
    let mut cursor = store
        .begin_scan(space, range, StorageBeginScanOptions::default())
        .await?;
    loop {
        let (page, page_has_more) = cursor
            .next_page(crate::storage_adapter::MAX_SCAN_PAGE_ROWS)
            .await?
            .into_parts();
        values.extend(
            page.into_iter()
                .filter_map(|entry| full_value(entry.value))
                .map(|bytes| bytes.to_vec()),
        );
        if !page_has_more {
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
            seen_manifest_hashes.insert(metadata.hash).then_some((
                metadata.hash,
                *chunk_count,
                metadata.size_bytes,
            ))
        })
        .collect::<Vec<_>>();
    let scan_count = chunked_blobs.len();
    // Consume completions out of order so a slow early scan does not prevent
    // the bounded window from refilling. Results cross the gate below only in
    // first-request order, preserving deterministic error selection.
    let mut scans = stream::iter(chunked_blobs.into_iter().enumerate())
        .map(|(order, (blob_hash, chunk_count, size_bytes))| async move {
            let result = async {
                let manifest_chunks =
                    load_declared_manifest_chunks(store, blob_hash, size_bytes).await?;
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

    // Only materialize physical blobs that were requested directly. Delta
    // copy segments can be reconstructed straight from their authenticated
    // chunks into the final output buffer, avoiding a second full-size owner
    // for the base while the delta result is assembled.
    let mut direct_output_counts = HashMap::<BlobId, usize>::new();
    for entry in metadata.iter().flatten() {
        match &entry.layout {
            BlobLayout::Delta { .. } => {}
            _ => {
                *direct_output_counts.entry(entry.hash).or_default() += 1;
            }
        }
    }
    let physical_metadata_by_hash = physical_metadata
        .iter()
        .map(|metadata| (metadata.hash, metadata.clone()))
        .collect::<HashMap<_, _>>();
    // A batch can request many deltas against one physical base. Authenticate
    // and decode that base once, then let every result borrow the same bounded
    // chunk descriptors. Preserve failures in the map so output traversal
    // below still selects errors in first-request order.
    let mut decoded_delta_bases =
        HashMap::<BlobId, Result<Vec<DecodedBlobChunk<'_>>, LixError>>::new();
    for entry in metadata.iter().flatten() {
        let BlobLayout::Delta { base_blob_hash, .. } = &entry.layout else {
            continue;
        };
        if decoded_delta_bases.contains_key(base_blob_hash) {
            continue;
        }
        let decoded = physical_metadata_by_hash
            .get(base_blob_hash)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS delta '{}' is missing base '{}'",
                        entry.hash.to_hex(),
                        base_blob_hash.to_hex()
                    ),
                )
            })
            .and_then(|base_metadata| {
                decode_delta_base_chunks(
                    base_metadata,
                    &chunk_rows_by_hash,
                    chunked_manifests_by_hash.get(base_blob_hash),
                )
            });
        decoded_delta_bases.insert(*base_blob_hash, decoded);
    }
    let mut full_bytes_by_hash = HashMap::<BlobId, Vec<u8>>::new();
    for metadata in physical_metadata {
        let hash = metadata.hash;
        if direct_output_counts.contains_key(&hash) {
            let bytes = assemble_blob_bytes(
                metadata,
                &chunk_rows_by_hash,
                chunked_manifests_by_hash.get(&hash),
            )?;
            full_bytes_by_hash.insert(hash, bytes);
        }
    }

    // Most reads request each full blob exactly once. Transfer that assembled
    // buffer into its output slot instead of cloning every payload after it
    // has already been authenticated and assembled. Repeated output slots
    // still need independent `Vec` ownership.
    let movable_full_hashes = direct_output_counts
        .into_iter()
        .filter_map(|(hash, count)| (count == 1).then_some(hash))
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
                    } => {
                        let decoded_base =
                            decoded_delta_bases.get(&base_blob_hash).ok_or_else(|| {
                                LixError::new(
                                    "LIX_ERROR_UNKNOWN",
                                    format!(
                                        "binary CAS delta '{}' is missing base '{}'",
                                        metadata.hash.to_hex(),
                                        base_blob_hash.to_hex()
                                    ),
                                )
                            })?;
                        let base_chunks = decoded_base.as_ref().map_err(Clone::clone)?;
                        let base_metadata = physical_metadata_by_hash
                            .get(&base_blob_hash)
                            .expect("decoded delta base has physical metadata");
                        apply_flat_delta_from_chunks(
                            metadata.hash,
                            metadata.size_bytes,
                            base_metadata,
                            &segments,
                            base_chunks,
                        )
                    }
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
            // Content-defined boundaries mean the chunk holding an offset
            // cannot be named by division. The chunker forces a boundary at
            // every anchor, so the anchor at or below the requested start is
            // always an exact manifest key: one range scan from there covers
            // the request without reading the whole manifest.
            let anchor_bytes = CHUNK_ANCHOR_BYTES as u64;
            let scan_start = range.start - range.start % anchor_bytes;
            let manifest =
                load_declared_manifest_chunk_range(store, metadata.hash, scan_start, range.end)
                    .await?;
            if manifest.is_empty() || manifest.len() > *chunk_count as usize {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' has no usable manifest rows for the requested range",
                        metadata.hash.to_hex(),
                    ),
                ));
            }
            let mut selected = Vec::with_capacity(manifest.len());
            let mut cursor = scan_start;
            for chunk in manifest {
                let chunk_start = cursor;
                cursor = chunk_start.checked_add(chunk.chunk_size).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "binary CAS chunk offsets overflow u64")
                })?;
                if chunk.chunk_size == 0
                    || chunk.chunk_size > MAX_BINARY_CAS_CHUNK_BYTES as u64
                    || cursor > metadata.size_bytes
                {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "binary CAS blob '{}' chunk at offset {} has size {}",
                            metadata.hash.to_hex(),
                            chunk_start,
                            chunk.chunk_size,
                        ),
                    ));
                }
                if cursor > range.start {
                    selected.push((chunk_start, chunk));
                }
                if cursor >= range.end {
                    break;
                }
            }
            if cursor < range.end {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' manifest ends at {} before the requested {}",
                        metadata.hash.to_hex(),
                        cursor,
                        range.end,
                    ),
                ));
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

fn apply_flat_delta_from_chunks(
    blob_hash: BlobId,
    size_bytes: u64,
    base_metadata: &BlobMetadata,
    segments: &[BlobDeltaSegment],
    base_chunks: &[DecodedBlobChunk<'_>],
) -> Result<Vec<u8>, LixError> {
    let expected_size = persisted_size_to_usize(size_bytes, "binary CAS delta")?;
    let mut out = Vec::with_capacity(expected_size);
    for segment in segments {
        match segment {
            BlobDeltaSegment::Copy { offset, length } => {
                append_blob_range_from_chunks(
                    &mut out,
                    blob_hash,
                    base_metadata,
                    *offset,
                    *length,
                    base_chunks,
                )?;
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

struct DecodedBlobChunk<'a> {
    range: Range<usize>,
    bytes: Cow<'a, [u8]>,
}

fn decode_delta_base_chunks<'a>(
    base_metadata: &BlobMetadata,
    chunk_rows_by_hash: &'a HashMap<ChunkHash, Option<Bytes>>,
    chunked_manifest: Option<&Vec<KvBlobManifestChunk>>,
) -> Result<Vec<DecodedBlobChunk<'a>>, LixError> {
    let base_size = persisted_size_to_usize(base_metadata.size_bytes, "binary CAS delta base")?;
    let chunks = match &base_metadata.layout {
        BlobLayout::SingleChunk { chunk_hash }
            if BlobId::from_single_chunk(*chunk_hash) == base_metadata.hash =>
        {
            vec![DecodedBlobChunk {
                range: 0..base_size,
                bytes: decode_chunk_from_map(
                    chunk_rows_by_hash,
                    base_metadata.hash,
                    *chunk_hash,
                    base_size,
                )?,
            }]
        }
        BlobLayout::Chunked { chunk_count } => {
            let Some(manifest_chunks) = chunked_manifest else {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS blob '{}' missing chunk manifest",
                        base_metadata.hash.to_hex()
                    ),
                ));
            };
            if manifest_chunks.len() != *chunk_count as usize
                || BlobId::from_chunks(
                    base_metadata.size_bytes,
                    manifest_chunks
                        .iter()
                        .map(|chunk| (ChunkHash::from_bytes(chunk.chunk_hash), chunk.chunk_size)),
                ) != base_metadata.hash
            {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS delta base '{}' failed manifest content-address verification",
                        base_metadata.hash.to_hex()
                    ),
                ));
            }
            let mut chunks = Vec::with_capacity(manifest_chunks.len());
            let mut chunk_start = 0usize;
            for manifest_chunk in manifest_chunks {
                let chunk_size =
                    persisted_size_to_usize(manifest_chunk.chunk_size, "binary CAS chunk")?;
                let chunk_end = chunk_start.checked_add(chunk_size).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "binary CAS chunk offsets overflowed")
                })?;
                let chunk_hash = ChunkHash::from_bytes(manifest_chunk.chunk_hash);
                chunks.push(DecodedBlobChunk {
                    range: chunk_start..chunk_end,
                    bytes: decode_chunk_from_map(
                        chunk_rows_by_hash,
                        base_metadata.hash,
                        chunk_hash,
                        chunk_size,
                    )?,
                });
                chunk_start = chunk_end;
            }
            if chunk_start != base_size {
                return Err(LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "binary CAS delta base '{}' has invalid chunk sizes",
                        base_metadata.hash.to_hex()
                    ),
                ));
            }
            chunks
        }
        BlobLayout::Empty | BlobLayout::SingleChunk { .. } | BlobLayout::Delta { .. } => {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "binary CAS delta base '{}' failed manifest content-address verification",
                    base_metadata.hash.to_hex()
                ),
            ));
        }
    };
    Ok(chunks)
}

fn append_blob_range_from_chunks(
    out: &mut Vec<u8>,
    delta_hash: BlobId,
    base_metadata: &BlobMetadata,
    offset: u64,
    length: u64,
    base_chunks: &[DecodedBlobChunk<'_>],
) -> Result<(), LixError> {
    let start = persisted_size_to_usize(offset, "binary CAS delta copy offset")?;
    let length = persisted_size_to_usize(length, "binary CAS delta copy length")?;
    let Some(end) = start.checked_add(length) else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' copy range overflowed",
                delta_hash.to_hex()
            ),
        ));
    };
    let base_size = persisted_size_to_usize(base_metadata.size_bytes, "binary CAS delta base")?;
    if end > base_size {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' has invalid copy ranges",
                delta_hash.to_hex()
            ),
        ));
    }
    if start == end {
        return Ok(());
    }

    let output_start = out.len();
    for chunk in base_chunks {
        let selected_start = start.max(chunk.range.start);
        let selected_end = end.min(chunk.range.end);
        if selected_start < selected_end {
            out.extend_from_slice(
                &chunk.bytes[selected_start - chunk.range.start..selected_end - chunk.range.start],
            );
        }
        if chunk.range.end >= end {
            break;
        }
    }
    if out.len().saturating_sub(output_start) != length {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "binary CAS delta '{}' has invalid copy ranges",
                delta_hash.to_hex()
            ),
        ));
    }
    Ok(())
}

async fn load_chunk_rows(
    store: &(impl StorageAdapterRead + ?Sized),
    hashes: &[ChunkHash],
) -> Result<Vec<Option<Bytes>>, LixError> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    let rows = point_values(
        store,
        BINARY_CAS_CHUNK_SPACE,
        hashes.iter().map(|hash| chunk_key(*hash)).collect(),
    )
    .await?;
    let missing = hashes
        .iter()
        .copied()
        .zip(&rows)
        .filter_map(|(hash, row)| row.is_none().then_some(hash))
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(rows);
    }
    let demand = point_values(
        store,
        BINARY_CAS_CHUNK_DEMAND_SPACE,
        missing.iter().map(|hash| chunk_key(*hash)).collect(),
    )
    .await?;
    if let Some(unmarked) = missing
        .iter()
        .zip(&demand)
        .find_map(|(hash, marker)| marker.is_none().then_some(*hash))
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            format!(
                "binary CAS chunk '{}' is missing without a demand marker",
                unmarked.to_hex()
            ),
        ));
    }
    let chunk_ids = missing
        .into_iter()
        .map(ChunkHash::to_hex)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    Err(LixError::new(
        "LIX_SYNC_CHUNKS_REQUIRED",
        "binary CAS chunks require demand hydration",
    )
    .with_details(serde_json::json!({ "chunkIds": chunk_ids })))
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
    store: &S,
    writes: &mut StorageWriteSet,
    blob_hashes: &mut HashSet<[u8; 32]>,
    chunk_keys: &mut HashSet<Vec<u8>>,
    payload: &crate::binary_cas::BlobPayload,
) -> Result<BlobWriteReceipt, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let bytes = payload.bytes();
    let precomputed_hash = payload.hash();
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
    let plan = prepare_blob_write(payload)?;
    let receipt = plan.receipt.clone();
    if !blob_hashes.insert(plan.blob_hash.into_bytes()) {
        return Ok(receipt);
    }

    let chunks = prepare_chunks(payload, &plan)?;
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
/// ineligible base. A manifest is an ordered content-addressed chunk list and a
/// same-length splice cannot move a boundary, so reusing the base's existing
/// boundaries produces the layout the chunker would have produced anyway.
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
        load_declared_manifest_chunks(&store, splice.base_blob_hash, metadata.size_bytes).await
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
    for (chunk, changed) in chunks {
        let chunk_data = &bytes[chunk.start..chunk.end];
        if changed && chunk_hashes_to_stage.remove(&chunk.hash) {
            stage_content_chunk(writes, chunk.hash, chunk_data)?;
        }
        stage_manifest_chunk(
            writes,
            blob_hash,
            chunk.start as u64,
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

/// Rebuilds the payload's chunk layout from the receipt sizes that
/// [`crate::binary_cas::BlobPayload::from_bytes`] already produced.
///
/// The receipts are the boundary decision, so staging never runs the boundary
/// search a second time over bytes it is about to write. The tiling check below
/// is what makes that safe: receipts that do not cover the payload exactly are
/// rejected rather than trusted.
fn chunk_ranges_from_receipts(
    receipts: &[crate::binary_cas::BlobChunkReceipt],
    len: usize,
) -> Result<Vec<(usize, usize)>, LixError> {
    let mut ranges = Vec::with_capacity(receipts.len());
    let mut cursor = 0usize;
    for receipt in receipts {
        let size = usize::try_from(receipt.size_bytes)
            .ok()
            .filter(|size| *size > 0 && *size <= MAX_BINARY_CAS_CHUNK_BYTES)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "binary CAS payload chunk receipt has an unusable size".to_string(),
                )
            })?;
        let end = cursor
            .checked_add(size)
            .filter(|end| *end <= len)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "binary CAS payload chunk receipts overrun the payload".to_string(),
                )
            })?;
        ranges.push((cursor, end));
        cursor = end;
    }
    if cursor != len {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "binary CAS payload chunk receipts do not tile the payload".to_string(),
        ));
    }
    Ok(ranges)
}

fn prepare_blob_write(payload: &crate::binary_cas::BlobPayload) -> Result<BlobWritePlan, LixError> {
    let bytes = payload.bytes();
    let blob_hash = payload
        .hash()
        .unwrap_or_else(|| BlobId::from_content(bytes));
    let (chunk_ranges, layout) = if bytes.is_empty() {
        if !payload.chunks().is_empty() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "empty binary CAS payload unexpectedly has chunk receipts".to_string(),
            ));
        }
        (Vec::new(), BlobLayout::Empty)
    } else {
        let chunk_ranges = chunk_ranges_from_receipts(payload.chunks(), bytes.len())?;
        let layout = match chunk_ranges.as_slice() {
            [] => unreachable!("non-empty blobs always have at least one chunk"),
            [_] => BlobLayout::SingleChunk {
                chunk_hash: payload.chunks()[0].hash,
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

fn prepare_chunks(
    payload: &crate::binary_cas::BlobPayload,
    plan: &BlobWritePlan,
) -> Result<Vec<PreparedChunk>, LixError> {
    if !matches!(plan.layout, BlobLayout::Chunked { .. }) {
        return Ok(Vec::new());
    }

    let chunks = plan
        .chunk_ranges
        .iter()
        .zip(payload.chunks())
        .map(|(&(start, end), receipt)| PreparedChunk {
            start,
            end,
            hash: receipt.hash,
        })
        .collect::<Vec<_>>();
    if chunks.len() != plan.chunk_ranges.len() {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "binary CAS payload omitted canonical chunk receipts".to_string(),
        ));
    }
    Ok(chunks)
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

            for chunk in chunks.iter().copied() {
                let chunk_data = &bytes[chunk.start..chunk.end];
                let chunk_hash = chunk.hash;
                if should_stage_chunk(chunk_hash)? {
                    stage_content_chunk(writes, chunk_hash, chunk_data)?;
                }

                stage_manifest_chunk(
                    writes,
                    plan.blob_hash,
                    chunk.start as u64,
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

/// Manifest chunk rows are keyed by the chunk's byte offset in the blob rather
/// than by its ordinal, because content-defined boundaries make the ordinal
/// unrelated to the offset. Offsets keep a ranged read to one range scan that
/// starts at a key the anchor rule guarantees exists.
fn manifest_chunk_key(blob_hash: BlobId, chunk_offset: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(40);
    out.extend_from_slice(blob_hash.as_bytes());
    out.extend_from_slice(&chunk_offset.to_be_bytes());
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

    use crate::binary_cas::{BinaryCasContext, BlobChunkReceipt, BlobPayload};
    use crate::storage_adapter::StorageAdapter;
    use crate::storage_adapter::{
        Memory, StorageError, StorageGetManyResult, StorageKeyRange, StorageReadOptions,
        StorageScanCursor, StorageWriteOptions, StorageWriteSet,
    };

    async fn stage_publication_fence_only(
        storage: &StorageAdapter<Memory>,
    ) -> (StorageWriteSet, Vec<StoragePrecondition>) {
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("publication fence read should open");
        let mut writes = storage.new_write_set();
        let mut preconditions = Vec::new();
        crate::binary_cas::stage_cas_publication_fence(&read, &mut writes, &mut preconditions)
            .await
            .expect("publication fence should stage");
        (writes, preconditions)
    }

    /// Publishers are independent of each other: content-addressed payload rows
    /// have no read-modify-write aggregate, so two publications planned from one
    /// snapshot must both commit. A compare-and-set here is what made unrelated
    /// concurrent writers collide with `LIX_TRANSACTION_CONFLICT`.
    #[tokio::test]
    async fn concurrent_publication_fences_planned_from_one_snapshot_both_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let (first_writes, first_preconditions) = stage_publication_fence_only(&storage).await;
        let (second_writes, second_preconditions) = stage_publication_fence_only(&storage).await;
        storage
            .commit_write_set(
                first_writes,
                StorageWriteOptions {
                    preconditions: first_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("first publication fence should commit");
        storage
            .commit_write_set(
                second_writes,
                StorageWriteOptions {
                    preconditions: second_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("a concurrent publication must not invalidate another publication");
    }

    /// The publication token must change on every publication, or a sweep that
    /// observed a value could see that same value restored and commit a plan
    /// that predates the publication.
    #[tokio::test]
    async fn every_publication_rewrites_the_publication_token() {
        let storage = StorageAdapter::new(Memory::new());
        let mut seen = HashSet::new();
        for _ in 0..4 {
            let (writes, preconditions) = stage_publication_fence_only(&storage).await;
            storage
                .commit_write_set(
                    writes,
                    StorageWriteOptions {
                        preconditions,
                        ..StorageWriteOptions::default()
                    },
                )
                .await
                .expect("publication fence should commit");
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("publication token read should open");
            let token = load_revision(&read, REVISION_KEY_BINARY_CAS_PUBLICATION)
                .await
                .expect("publication token should load")
                .expect("publication token should be present");
            assert!(
                seen.insert(token),
                "each publication must write a distinct publication token"
            );
        }
    }

    /// Two sweeps planned from one snapshot must still be mutually exclusive:
    /// each deletes rows the other assumed present.
    #[tokio::test]
    async fn concurrent_reclamation_fences_planned_from_one_snapshot_conflict() {
        let storage = StorageAdapter::new(Memory::new());
        let mut staged = Vec::new();
        for _ in 0..2 {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("reclamation fence read should open");
            let mut writes = storage.new_write_set();
            let mut preconditions = Vec::new();
            crate::binary_cas::stage_cas_reclamation_fence(&read, &mut writes, &mut preconditions)
                .await
                .expect("reclamation fence should stage");
            staged.push((writes, preconditions));
        }
        let (second_writes, second_preconditions) = staged.pop().expect("two sweeps were staged");
        let (first_writes, first_preconditions) = staged.pop().expect("two sweeps were staged");
        storage
            .commit_write_set(
                first_writes,
                StorageWriteOptions {
                    preconditions: first_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect("first sweep should win the reclamation fence");
        let error = storage
            .commit_write_set(
                second_writes,
                StorageWriteOptions {
                    preconditions: second_preconditions,
                    ..StorageWriteOptions::default()
                },
            )
            .await
            .expect_err("a stale sweep must lose the reclamation fence");
        assert!(matches!(
            error,
            crate::storage_adapter::StorageWriteSetError::Storage(
                StorageError::PreconditionFailed(_)
            )
        ));
    }

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

        async fn begin_scan(
            &self,
            space: StorageSpace,
            range: StorageKeyRange,
            opts: StorageBeginScanOptions,
        ) -> Result<StorageScanCursor<'_>, StorageError> {
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
            let result = self.inner.begin_scan(space, range, opts).await;
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
        let mut chunk_offset = 0u64;
        for chunk in chunks.iter() {
            let chunk_hash = ChunkHash::from_content(chunk);
            stage_manifest_chunk(
                writes,
                blob_hash,
                chunk_offset,
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
            chunk_offset += chunk.len() as u64;
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

    fn stage_declared_chunked_manifest(
        writes: &mut StorageWriteSet,
        chunks: &[(ChunkHash, u64)],
    ) -> BlobId {
        let size_bytes = chunks.iter().map(|(_, size)| *size).sum::<u64>();
        let blob_id = BlobId::from_chunks(size_bytes, chunks.iter().copied());
        stage_manifest(
            writes,
            blob_id,
            &BinaryCasManifest::Chunked {
                size_bytes,
                chunk_count: chunks.len() as u32,
            },
        );
        let mut chunk_offset = 0u64;
        for (chunk_hash, chunk_size) in chunks.iter().copied() {
            stage_manifest_chunk(
                writes,
                blob_id,
                chunk_offset,
                &KvBlobManifestChunk {
                    chunk_hash: chunk_hash.into_bytes(),
                    chunk_size,
                },
            );
            chunk_offset += chunk_size;
        }
        blob_id
    }

    #[tokio::test]
    async fn reclamation_keeps_live_payload_while_reclaiming_a_distinct_orphan() {
        let storage = StorageAdapter::new(Memory::new());
        let shared = b"shared chunk";
        let orphan = b"orphan chunk";
        let shared_hash = ChunkHash::from_content(shared);
        let orphan_hash = ChunkHash::from_content(orphan);
        let live_blob = BlobId::from_single_chunk(shared_hash);
        let orphan_blob = BlobId::from_single_chunk(orphan_hash);

        let mut initial = storage.new_write_set();
        stage_manifest(
            &mut initial,
            live_blob,
            &BinaryCasManifest::SingleChunk {
                size_bytes: shared.len() as u64,
                chunk_hash: shared_hash.into_bytes(),
            },
        );
        stage_manifest(
            &mut initial,
            orphan_blob,
            &BinaryCasManifest::SingleChunk {
                size_bytes: orphan.len() as u64,
                chunk_hash: orphan_hash.into_bytes(),
            },
        );
        stage_chunk(
            &mut initial,
            shared_hash,
            BinaryChunkCodec::Raw,
            shared.len() as u64,
            shared,
        );
        stage_chunk(
            &mut initial,
            orphan_hash,
            BinaryChunkCodec::Raw,
            orphan.len() as u64,
            orphan,
        );
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("initial CAS rows should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("CAS read should open");
        let mut writes = storage.new_write_set();
        let result = stage_reclaim_unreachable_binary_cas(
            &store,
            &mut writes,
            &BTreeSet::from([live_blob]),
            &BTreeMap::new(),
        )
        .await
        .expect("live CAS root should be sweepable");
        assert_eq!(result.live_blob_count, 1);
        assert_eq!(result.live_chunk_count, 1);
        assert_eq!(result.reclaimed_manifest_rows, 1);
        assert_eq!(result.reclaimed_chunk_rows, 1);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("CAS sweep should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("CAS read should reopen");
        assert!(
            load_manifest(&store, live_blob)
                .await
                .expect("live manifest read should succeed")
                .is_some()
        );
        assert!(
            load_manifest(&store, orphan_blob)
                .await
                .expect("orphan manifest read should succeed")
                .is_none()
        );
        assert!(
            load_chunk(&store, shared_hash)
                .await
                .expect("shared chunk read should succeed")
                .is_some()
        );
        assert!(
            load_chunk(&store, orphan_hash)
                .await
                .expect("orphan chunk read should succeed")
                .is_none()
        );
    }

    #[tokio::test]
    async fn reclamation_retains_live_demand_and_sweeps_orphan_and_redundant_markers() {
        let storage = StorageAdapter::new(Memory::new());
        let missing = b"lazy missing chunk";
        let hydrated = b"already hydrated chunk";
        let missing_hash = ChunkHash::from_content(missing);
        let hydrated_hash = ChunkHash::from_content(hydrated);
        let orphan_hash = ChunkHash::from_content(b"orphan demand marker");
        let receipts = [
            BlobChunkReceipt {
                hash: missing_hash,
                size_bytes: missing.len() as u64,
            },
            BlobChunkReceipt {
                hash: hydrated_hash,
                size_bytes: hydrated.len() as u64,
            },
        ];

        let mut initial = storage.new_write_set();
        let live_blob = stage_upload_manifest(&mut initial, &receipts)
            .expect("manifest should stage")
            .hash;
        stage_chunk_demand(&mut initial, missing_hash);
        stage_chunk(
            &mut initial,
            hydrated_hash,
            BinaryChunkCodec::Raw,
            hydrated.len() as u64,
            hydrated,
        );
        stage_chunk_demand(&mut initial, orphan_hash);
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("lazy CAS fixture should commit");

        // Simulate stale state left by an interrupted older hydrator. GC must
        // remove this marker because the authenticated payload is available.
        let mut stale = storage.new_write_set();
        stage_chunk_demand(&mut stale, hydrated_hash);
        storage
            .commit_write_set(stale, StorageWriteOptions::default())
            .await
            .expect("redundant marker should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("GC read should open");
        let mut writes = storage.new_write_set();
        let result = stage_reclaim_unreachable_binary_cas(
            &read,
            &mut writes,
            &BTreeSet::from([live_blob]),
            &BTreeMap::new(),
        )
        .await
        .expect("marked live missing chunks should be sweepable");
        assert_eq!(result.reclaimed_demand_marker_rows, 2);
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("GC writes should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        assert!(
            get_one(
                &read,
                BINARY_CAS_CHUNK_DEMAND_SPACE,
                chunk_key(missing_hash)
            )
            .await
            .unwrap()
            .is_some()
        );
        assert!(
            get_one(
                &read,
                BINARY_CAS_CHUNK_DEMAND_SPACE,
                chunk_key(hydrated_hash)
            )
            .await
            .unwrap()
            .is_none()
        );
        assert!(
            get_one(&read, BINARY_CAS_CHUNK_DEMAND_SPACE, chunk_key(orphan_hash))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn reclamation_rejects_unmarked_live_missing_chunks() {
        let storage = StorageAdapter::new(Memory::new());
        let missing = b"unmarked live chunk";
        let missing_hash = ChunkHash::from_content(missing);
        let mut initial = storage.new_write_set();
        let live_blob = stage_upload_manifest(
            &mut initial,
            &[BlobChunkReceipt {
                hash: missing_hash,
                size_bytes: missing.len() as u64,
            }],
        )
        .expect("manifest should stage")
        .hash;
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("unmarked fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("GC read should open");
        let mut writes = storage.new_write_set();
        let error = stage_reclaim_unreachable_binary_cas(
            &read,
            &mut writes,
            &BTreeSet::from([live_blob]),
            &BTreeMap::new(),
        )
        .await
        .expect_err("unmarked missing chunks remain corruption");
        assert_eq!(error.code, LixError::CODE_STORAGE_ERROR);
        assert!(error.message.contains("missing without a demand marker"));
        assert!(writes.is_empty());
    }

    #[tokio::test]
    async fn staging_a_chunk_clears_its_durable_demand_marker() {
        let storage = StorageAdapter::new(Memory::new());
        let payload = b"hydrate this demanded chunk";
        let hash = ChunkHash::from_content(payload);

        let mut initial = storage.new_write_set();
        stage_chunk_demand(&mut initial, hash);
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("demand marker should commit");

        let mut hydration = storage.new_write_set();
        stage_chunk(
            &mut hydration,
            hash,
            BinaryChunkCodec::Raw,
            payload.len() as u64,
            payload,
        );
        storage
            .commit_write_set(hydration, StorageWriteOptions::default())
            .await
            .expect("chunk hydration should commit atomically");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        assert!(
            get_one(&read, BINARY_CAS_CHUNK_DEMAND_SPACE, chunk_key(hash))
                .await
                .unwrap()
                .is_none()
        );
        assert!(load_chunk(&read, hash).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn reclamation_keeps_a_truly_shared_chunk_until_the_final_manifest_releases_it() {
        let storage = StorageAdapter::new(Memory::new());
        let shared = b"shared payload";
        let live_only = b"live-only payload";
        let retired_only = b"retired-only payload";
        let shared_hash = ChunkHash::from_content(shared);
        let live_only_hash = ChunkHash::from_content(live_only);
        let retired_only_hash = ChunkHash::from_content(retired_only);

        let mut initial = storage.new_write_set();
        let live_blob = stage_declared_chunked_manifest(
            &mut initial,
            &[
                (shared_hash, shared.len() as u64),
                (live_only_hash, live_only.len() as u64),
            ],
        );
        let retired_blob = stage_declared_chunked_manifest(
            &mut initial,
            &[
                (shared_hash, shared.len() as u64),
                (retired_only_hash, retired_only.len() as u64),
            ],
        );
        for (hash, payload) in [
            (shared_hash, shared.as_slice()),
            (live_only_hash, live_only.as_slice()),
            (retired_only_hash, retired_only.as_slice()),
        ] {
            stage_chunk(
                &mut initial,
                hash,
                BinaryChunkCodec::Raw,
                payload.len() as u64,
                payload,
            );
        }
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("shared CAS fixture should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("first sweep read should open");
        let mut writes = storage.new_write_set();
        stage_reclaim_unreachable_binary_cas(
            &read,
            &mut writes,
            &BTreeSet::from([live_blob]),
            &BTreeMap::new(),
        )
        .await
        .expect("first shared sweep should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("first shared sweep should commit");

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("shared verification read should open");
        assert!(load_manifest(&read, retired_blob).await.unwrap().is_none());
        assert!(
            load_chunk(&read, retired_only_hash)
                .await
                .unwrap()
                .is_none()
        );
        assert!(load_chunk(&read, shared_hash).await.unwrap().is_some());
        drop(read);

        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("final sweep read should open");
        let mut writes = storage.new_write_set();
        stage_reclaim_unreachable_binary_cas(
            &read,
            &mut writes,
            &BTreeSet::new(),
            &BTreeMap::new(),
        )
        .await
        .expect("final shared sweep should stage");
        drop(read);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("final shared sweep should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("final verification read should open");
        assert!(load_manifest(&read, live_blob).await.unwrap().is_none());
        assert!(load_chunk(&read, shared_hash).await.unwrap().is_none());
        assert!(load_chunk(&read, live_only_hash).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn reclamation_rejects_wrong_single_and_chunked_declared_sizes() {
        let storage = StorageAdapter::new(Memory::new());
        let single = b"single-size";
        let single_hash = ChunkHash::from_content(single);
        let single_blob = BlobId::from_single_chunk(single_hash);
        let chunked = b"chunked-size";
        let chunked_hash = ChunkHash::from_content(chunked);
        let mut initial = storage.new_write_set();
        stage_manifest(
            &mut initial,
            single_blob,
            &BinaryCasManifest::SingleChunk {
                size_bytes: single.len() as u64 + 1,
                chunk_hash: single_hash.into_bytes(),
            },
        );
        let chunked_blob = stage_declared_chunked_manifest(
            &mut initial,
            &[(chunked_hash, chunked.len() as u64 + 1)],
        );
        for (hash, payload) in [
            (single_hash, single.as_slice()),
            (chunked_hash, chunked.as_slice()),
        ] {
            stage_chunk(
                &mut initial,
                hash,
                BinaryChunkCodec::Raw,
                payload.len() as u64,
                payload,
            );
        }
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("declared-size fixture should commit");

        for root in [single_blob, chunked_blob] {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("declared-size read should open");
            let mut writes = storage.new_write_set();
            let error = stage_reclaim_unreachable_binary_cas(
                &read,
                &mut writes,
                &BTreeSet::from([root]),
                &BTreeMap::new(),
            )
            .await
            .expect_err("wrong declared size must fail closed");
            assert!(error.to_string().contains("expected"));
        }
    }

    #[tokio::test]
    async fn reclamation_rejects_conflicting_live_chunk_size_expectations() {
        let storage = StorageAdapter::new(Memory::new());
        let shared = b"conflicting-size";
        let shared_hash = ChunkHash::from_content(shared);
        let single_blob = BlobId::from_single_chunk(shared_hash);
        let mut initial = storage.new_write_set();
        stage_manifest(
            &mut initial,
            single_blob,
            &BinaryCasManifest::SingleChunk {
                size_bytes: shared.len() as u64,
                chunk_hash: shared_hash.into_bytes(),
            },
        );
        let chunked_blob = stage_declared_chunked_manifest(
            &mut initial,
            &[(shared_hash, shared.len() as u64 + 1)],
        );
        stage_chunk(
            &mut initial,
            shared_hash,
            BinaryChunkCodec::Raw,
            shared.len() as u64,
            shared,
        );
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("conflicting-size fixture should commit");
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("conflicting-size read should open");
        let mut writes = storage.new_write_set();
        let error = stage_reclaim_unreachable_binary_cas(
            &read,
            &mut writes,
            &BTreeSet::from([single_blob, chunked_blob]),
            &BTreeMap::new(),
        )
        .await
        .expect_err("conflicting live expectations must fail closed");
        assert!(error.to_string().contains("conflicting declared sizes"));
    }

    #[tokio::test]
    async fn reclamation_reuses_delta_validation_and_authenticates_the_persisted_base_layout() {
        let storage = StorageAdapter::new(Memory::new());
        let base = b"delta-base";
        let base_hash = ChunkHash::from_content(base);
        let base_blob = BlobId::from_single_chunk(base_hash);
        let malformed_delta = BlobId::from_content(b"malformed-delta");
        let mismatched_delta = BlobId::from_content(b"mismatched-delta");
        let mut initial = storage.new_write_set();
        stage_manifest(
            &mut initial,
            base_blob,
            &BinaryCasManifest::SingleChunk {
                size_bytes: base.len() as u64,
                chunk_hash: base_hash.into_bytes(),
            },
        );
        stage_chunk(
            &mut initial,
            base_hash,
            BinaryChunkCodec::Raw,
            base.len() as u64,
            base,
        );
        stage_manifest(
            &mut initial,
            malformed_delta,
            &BinaryCasManifest::Delta {
                size_bytes: base.len() as u64,
                base_blob_hash: base_blob.into_bytes(),
                base_size_bytes: base.len() as u64,
                base_layout: StorageBinaryCasDeltaBaseLayout::SingleChunk {
                    chunk_hash: base_hash.into_bytes(),
                },
                segments: vec![StorageBinaryCasDeltaSegment::Copy {
                    offset: 1,
                    length: base.len() as u64,
                }],
            },
        );
        stage_manifest(
            &mut initial,
            mismatched_delta,
            &BinaryCasManifest::Delta {
                size_bytes: base.len() as u64,
                base_blob_hash: base_blob.into_bytes(),
                base_size_bytes: base.len() as u64 + 1,
                base_layout: StorageBinaryCasDeltaBaseLayout::SingleChunk {
                    chunk_hash: base_hash.into_bytes(),
                },
                segments: vec![StorageBinaryCasDeltaSegment::Copy {
                    offset: 0,
                    length: base.len() as u64,
                }],
            },
        );
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("delta validation fixture should commit");

        for (root, message) in [
            (malformed_delta, "invalid copy ranges"),
            (mismatched_delta, "disagrees with base"),
        ] {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("delta validation read should open");
            let mut writes = storage.new_write_set();
            let error = stage_reclaim_unreachable_binary_cas(
                &read,
                &mut writes,
                &BTreeSet::from([root]),
                &BTreeMap::new(),
            )
            .await
            .expect_err("invalid live delta must fail closed");
            assert!(error.to_string().contains(message), "{error}");
            assert!(writes.is_empty(), "failed validation must stage no deletes");
        }
    }

    #[tokio::test]
    async fn reclamation_rejects_a_tampered_live_payload_before_commit() {
        let storage = StorageAdapter::new(Memory::new());
        let expected = b"expected payload";
        let tampered = b"tampered payload";
        let chunk_hash = ChunkHash::from_content(expected);
        let blob_id = BlobId::from_single_chunk(chunk_hash);

        let mut initial = storage.new_write_set();
        stage_manifest(
            &mut initial,
            blob_id,
            &BinaryCasManifest::SingleChunk {
                size_bytes: expected.len() as u64,
                chunk_hash: chunk_hash.into_bytes(),
            },
        );
        stage_chunk(
            &mut initial,
            chunk_hash,
            BinaryChunkCodec::Raw,
            tampered.len() as u64,
            tampered,
        );
        storage
            .commit_write_set(initial, StorageWriteOptions::default())
            .await
            .expect("tampered fixture should commit");

        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("CAS read should open");
        let mut writes = storage.new_write_set();
        let error = stage_reclaim_unreachable_binary_cas(
            &store,
            &mut writes,
            &BTreeSet::from([blob_id]),
            &BTreeMap::new(),
        )
        .await
        .expect_err("sweep must fail closed on a tampered live payload");
        assert!(error.to_string().contains("content-address verification"));
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
                6,
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
                fixture.1.len() as u64,
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
    fn binary_hash_keys_are_compact_and_manifest_chunks_sort_by_offset() {
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
            let payload = BlobPayload::from_bytes(vec![b'a'; size]);
            let plan = prepare_blob_write(&payload).expect("non-empty blob should plan");
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

        // Content-defined boundaries are not known statically, so locate the
        // first manifest row that starts after the requested range and delete
        // it: a ranged read must never need it.
        let mut trailing_offset = 0u64;
        for chunk in scan_manifest_chunks(&store, blob_hash)
            .await
            .expect("manifest rows should scan")
        {
            if trailing_offset >= requested.end {
                break;
            }
            trailing_offset += chunk.chunk_size;
        }
        assert!(
            trailing_offset < data.len() as u64,
            "the fixture must have a manifest row past the requested range"
        );
        let mut writes = storage.new_write_set();
        writes.delete(
            BINARY_CAS_MANIFEST_CHUNK_SPACE,
            key(manifest_chunk_key(blob_hash, trailing_offset)),
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
    async fn existing_chunk_aware_writer_batches_persisted_chunk_checks_from_payload_receipts() {
        let storage = StorageAdapter::new(Memory::new());
        let data = definitely_multi_chunk_blob_bytes();
        let payload = BlobPayload::from_bytes(data.clone());
        let blob_hash = payload.hash().expect("payload should have a hash");
        let chunk_ranges = chunk_ranges(&data);
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

        // Keep the immutable chunks but remove the top-level manifest so the
        // second publication must exercise the batched presence checks.
        let mut remove_manifest = storage.new_write_set();
        remove_manifest.delete(BINARY_CAS_MANIFEST_SPACE, key(manifest_key(blob_hash)));
        storage
            .commit_write_set(remove_manifest, StorageWriteOptions::default())
            .await
            .expect("manifest removal should commit");

        crate::binary_cas::metrics::reset_binary_cas_write_metrics();
        let store = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let mut writes = storage.new_write_set();
        stage_blob_write_skipping_existing_chunks(
            &store,
            &mut writes,
            &mut HashSet::new(),
            &mut HashSet::new(),
            &payload,
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
            load_bytes_many(
                &store,
                &[
                    full_base_hash,
                    first_hash,
                    second_hash,
                    third_hash,
                    third_hash,
                ],
            )
            .await
            .expect("flat deltas should load")
            .into_vec(),
            vec![
                Some(before),
                Some(first),
                Some(second),
                Some(third.clone()),
                Some(third),
            ],
        );
    }

    #[tokio::test]
    async fn batched_flat_delta_corruption_errors_follow_request_order() {
        let storage = StorageAdapter::new(Memory::new());
        let mut deltas = Vec::new();
        let mut base_chunk_hashes = Vec::new();
        let mut writes = storage.new_write_set();
        for (expected, tampered) in [
            (&b"first-base"[..], &b"FIRST-base"[..]),
            (&b"second-base"[..], &b"SECOND-base"[..]),
        ] {
            let chunk_hash = ChunkHash::from_content(expected);
            let base_hash = BlobId::from_single_chunk(chunk_hash);
            let mut result = expected.to_vec();
            result.push(b'!');
            let delta_hash = BlobId::from_content(&result);
            stage_manifest(
                &mut writes,
                base_hash,
                &BinaryCasManifest::SingleChunk {
                    size_bytes: expected.len() as u64,
                    chunk_hash: chunk_hash.into_bytes(),
                },
            );
            stage_chunk(
                &mut writes,
                chunk_hash,
                BinaryChunkCodec::Raw,
                tampered.len() as u64,
                tampered,
            );
            stage_manifest(
                &mut writes,
                delta_hash,
                &BinaryCasManifest::Delta {
                    size_bytes: result.len() as u64,
                    base_blob_hash: base_hash.into_bytes(),
                    base_size_bytes: expected.len() as u64,
                    base_layout: StorageBinaryCasDeltaBaseLayout::SingleChunk {
                        chunk_hash: chunk_hash.into_bytes(),
                    },
                    segments: vec![
                        StorageBinaryCasDeltaSegment::Copy {
                            offset: 0,
                            length: expected.len() as u64,
                        },
                        StorageBinaryCasDeltaSegment::Insert { bytes: vec![b'!'] },
                    ],
                },
            );
            deltas.push(delta_hash);
            base_chunk_hashes.push(chunk_hash);
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("corrupt delta fixtures should commit");

        for order in [[0usize, 1usize], [1usize, 0usize]] {
            let read = storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("corrupt delta read should open");
            let error = load_bytes_many(&read, &[deltas[order[0]], deltas[order[1]]])
                .await
                .expect_err("corrupt delta base must fail closed");
            assert!(
                error
                    .message
                    .contains(&base_chunk_hashes[order[0]].to_hex()),
                "first requested corrupt delta must select its base error: {error:?}",
            );
        }
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
        let expected_hashes = chunk_ranges(&after)
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
            "fallback must retain the content-defined layout"
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
