//! Epoch-fenced binary content admission for a partial replica.
//!
//! Callers fetch only after an explicit referenced-manifest or marked-chunk
//! demand. The authenticated transport must be bound to `expected`'s authority.
//! These installers never advance roots or certify coverage.
use bytes::Bytes;

use super::partial_state::{
    PARTIAL_REPLICA_STATE_SPACE, PartialReplicaState, load_partial_replica_state,
    partial_replica_state_key,
};
use super::{SyncBlobManifest, SyncBlobRegistration};
use crate::LixError;
use crate::binary_cas::{
    BINARY_CAS_CHUNK_DEMAND_SPACE, BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_SPACE, BlobId,
    CanonicalBlobManifest, ChunkHash, load_metadata_many_bounded,
    stage_deferred_canonical_manifest, stage_transfer_publication_fence,
    stage_verified_inline_canonical_blob, stage_verified_raw_chunk,
};
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageAdapter, StorageAdapterRead, StorageGetManyRequest,
    StorageGetOptions, StorageKey, StoragePrecondition, StorageProjectedValue, StorageWriteOptions,
    StorageWriteSet,
};
use std::collections::{BTreeSet, HashMap};

const MAX_PARTIAL_CHUNK_PAGE_ITEMS: usize = super::transfer::CHUNK_CONCURRENCY;
const MAX_PARTIAL_CHUNK_PAGE_BYTES: usize =
    MAX_PARTIAL_CHUNK_PAGE_ITEMS * crate::binary_cas::raw_chunk_transfer_bounds().max_payload_bytes;
const MAX_PARTIAL_MANIFEST_PAGE_ITEMS: usize = super::transfer::MANIFEST_PAGE_ITEMS;
const MAX_PARTIAL_MANIFEST_READ_BYTES: usize =
    super::transfer::MAX_MANIFEST_SINGLETON_ENCODED_BYTES;
const MAX_PARTIAL_MANIFEST_CHUNK_REFS: usize = 16_384;

struct ChunkPageInspection {
    unique_hashes: Vec<ChunkHash>,
    requested_to_unique: Vec<usize>,
    resident: Vec<bool>,
    demand_markers: Vec<Option<Bytes>>,
}

fn mismatch() -> LixError {
    LixError::new(
        "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
        "blob hydration belongs to a different partial replica admission",
    )
}

fn same_admission(actual: &PartialReplicaState, expected: &PartialReplicaState) -> bool {
    actual.repository_id() == expected.repository_id()
        && actual.remote_id() == expected.remote_id()
        && actual.active_account_id() == expected.active_account_id()
        && actual.epoch_id() == expected.epoch_id()
}

fn is_precondition_failure(error: &crate::storage_adapter::StorageWriteSetError) -> bool {
    matches!(
        error,
        crate::storage_adapter::StorageWriteSetError::Storage(
            crate::storage_adapter::StorageError::PreconditionFailed(_)
        )
    )
}

#[cfg(test)]
async fn prepare_manifest_install(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    requested: BlobId,
    receipt: Bytes,
    manifest: &CanonicalBlobManifest,
    inline: Option<&[u8]>,
) -> Result<(Vec<StoragePrecondition>, Vec<ChunkHash>), LixError> {
    check_manifest_chunk_presence(read, manifest).await?;
    let missing_chunk_hashes = if let Some(bytes) = inline {
        stage_verified_inline_canonical_blob(writes, manifest, bytes)?;
        Vec::new()
    } else {
        stage_deferred_canonical_manifest(read, writes, manifest).await?
    };
    let mut preconditions = vec![
        StoragePrecondition::KeyAbsent {
            space: BINARY_CAS_MANIFEST_SPACE,
            key: StorageKey(Bytes::copy_from_slice(requested.as_bytes())),
        },
        StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: partial_replica_state_key(),
            expected: receipt,
        },
    ];
    // Demand rows are mutable hints, so every payload that appeared missing
    // in this snapshot must remain absent through the atomic marker write.
    for hash in &missing_chunk_hashes {
        preconditions.push(StoragePrecondition::KeyAbsent {
            space: BINARY_CAS_CHUNK_SPACE,
            key: StorageKey(Bytes::copy_from_slice(hash.as_bytes())),
        });
    }
    stage_transfer_publication_fence(read, writes, &mut preconditions).await?;
    Ok((preconditions, missing_chunk_hashes))
}

#[cfg(test)]
pub(super) async fn manifest_is_resident<S>(storage: &StorageAdapter<S>, expected: &PartialReplicaState, requested: BlobId) -> Result<bool, LixError>
where S: Storage + Clone + Send + Sync + 'static {
    Ok(manifests_are_resident(storage, expected, &[requested]).await?[0])
}
#[cfg(test)]
pub(super) async fn chunk_is_resident<S>(storage: &StorageAdapter<S>, expected: &PartialReplicaState, requested: ChunkHash) -> Result<bool, LixError>
where S: Storage + Clone + Send + Sync + 'static {
    Ok(chunks_are_resident(storage, expected, &[requested]).await?[0])
}
#[cfg(test)]
pub(super) async fn scalar_chunk_is_resident<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: ChunkHash,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    let (actual, _) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(mismatch)?;
    if !same_admission(&actual, expected) {
        return Err(mismatch());
    }
    checked_chunk_resident(&read, requested).await
}

/// Resolves the presence of one bounded ordered page of manifests from one
/// receipt-epoch snapshot. Missing metadata remains an ordinary `false`; a
/// malformed resident manifest is still surfaced by the binary-CAS decoder.
pub(super) async fn manifests_are_resident<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: &[BlobId],
) -> Result<Vec<bool>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if requested.len() > MAX_PARTIAL_MANIFEST_PAGE_ITEMS {
        return Err(page_limit_error("manifest count"));
    }
    if requested.is_empty() {
        return Ok(Vec::new());
    }
    let read = storage.begin_read(Default::default()).await?;
    let (actual, _) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(mismatch)?;
    if !same_admission(&actual, expected) {
        return Err(mismatch());
    }
    Ok(load_metadata_many_bounded(
        &read,
        requested,
        crate::storage_adapter::ReadBudget {
            max_result_bytes: MAX_PARTIAL_MANIFEST_READ_BYTES,
            max_single_value_bytes: MAX_PARTIAL_MANIFEST_READ_BYTES,
        },
    )
    .await?
    .into_vec()
    .into_iter()
    .map(|metadata| metadata.is_some())
    .collect())
}

/// Resolves one bounded ordered chunk page from one receipt-epoch snapshot.
/// Full payloads use bounded point reads before decoding, so malformed
/// oversized physical rows cannot bypass the page's retained-byte ceiling.
pub(super) async fn chunks_are_resident<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: &[ChunkHash],
) -> Result<Vec<bool>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if requested.len() > MAX_PARTIAL_CHUNK_PAGE_ITEMS {
        return Err(page_limit_error("chunk count"));
    }
    if requested.is_empty() {
        return Ok(Vec::new());
    }
    let read = storage.begin_read(Default::default()).await?;
    let (actual, _) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(mismatch)?;
    if !same_admission(&actual, expected) {
        return Err(mismatch());
    }
    let inspected = inspect_chunk_page(&read, requested).await?;
    Ok(inspected
        .requested_to_unique
        .iter()
        .map(|index| inspected.resident[*index])
        .collect())
}

fn page_limit_error(label: &str) -> LixError {
    LixError::new(
        LixError::CODE_INVALID_PARAM,
        format!("partial blob {label} exceeds its bounded page limit"),
    )
}

async fn inspect_chunk_page(
    read: &(impl StorageAdapterRead + ?Sized),
    requested: &[ChunkHash],
) -> Result<ChunkPageInspection, LixError> {
    if requested.len() > MAX_PARTIAL_CHUNK_PAGE_ITEMS {
        return Err(page_limit_error("chunk count"));
    }
    let mut unique_hashes = Vec::with_capacity(requested.len());
    let mut unique_by_hash = HashMap::with_capacity(requested.len());
    let mut requested_to_unique = Vec::with_capacity(requested.len());
    for hash in requested {
        let index = match unique_by_hash.get(hash) {
            Some(index) => *index,
            None => {
                let index = unique_hashes.len();
                unique_hashes.push(*hash);
                unique_by_hash.insert(*hash, index);
                index
            }
        };
        requested_to_unique.push(index);
    }
    if unique_hashes.is_empty() {
        return Ok(ChunkPageInspection {
            unique_hashes,
            requested_to_unique,
            resident: Vec::new(),
            demand_markers: Vec::new(),
        });
    }

    let payload_keys = unique_hashes
        .iter()
        .map(|hash| StorageKey(Bytes::copy_from_slice(hash.as_bytes())))
        .collect::<Vec<_>>();
    let chunk_bounds = crate::binary_cas::raw_chunk_transfer_bounds();
    let encoded_limit = chunk_bounds.max_encoded_value_bytes;
    let payload_budget = crate::storage_adapter::ReadBudget {
        max_result_bytes: encoded_limit.saturating_mul(unique_hashes.len()),
        max_single_value_bytes: encoded_limit,
    };
    let payload_rows = read
        .get_many_bounded(
            &[StorageGetManyRequest {
                space: BINARY_CAS_CHUNK_SPACE,
                keys: &payload_keys,
                opts: StorageGetOptions::default(),
            }],
            payload_budget,
        )
        .await?;
    if payload_rows.values.len() != unique_hashes.len() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "partial blob bounded chunk payload read returned the wrong number of slots",
        ));
    }
    let mut resident = Vec::with_capacity(unique_hashes.len());
    for (index, value) in payload_rows.values.into_iter().enumerate() {
        let encoded = match value {
            None => {
                resident.push(false);
                continue;
            }
            Some(StorageProjectedValue::FullValue(encoded)) => encoded,
            Some(StorageProjectedValue::KeyOnly) => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "partial blob chunk read omitted its value",
                ));
            }
        };
        let (uncompressed_len, _payload) =
            crate::binary_cas::validate_raw_chunk_payload(&encoded, unique_hashes[index])?;
        if uncompressed_len > chunk_bounds.max_payload_bytes as u64 {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "binary CAS chunk '{}' failed content-address verification",
                    unique_hashes[index].to_hex()
                ),
            ));
        }
        resident.push(true);
    }

    let presence = crate::binary_cas::chunk_presence_many(read, &unique_hashes).await?;
    if presence.len() != resident.len() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "partial blob chunk presence read returned the wrong number of slots",
        ));
    }
    if resident
        .iter()
        .zip(&presence)
        .any(|(payload, marker)| payload != marker)
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "binary CAS chunk payload and presence marker disagree",
        ));
    }

    let missing_indexes = resident
        .iter()
        .enumerate()
        .filter_map(|(index, present)| (!present).then_some(index))
        .collect::<Vec<_>>();
    let mut demand_markers = vec![None; unique_hashes.len()];
    if !missing_indexes.is_empty() {
        const MAX_DEMAND_MARKER_BYTES: usize = 64 * 1024;
        let marker_keys = missing_indexes
            .iter()
            .map(|index| StorageKey(Bytes::copy_from_slice(unique_hashes[*index].as_bytes())))
            .collect::<Vec<_>>();
        let marker_rows = read
            .get_many_bounded(
                &[StorageGetManyRequest {
                    space: BINARY_CAS_CHUNK_DEMAND_SPACE,
                    keys: &marker_keys,
                    opts: StorageGetOptions::default(),
                }],
                crate::storage_adapter::ReadBudget {
                    max_result_bytes: MAX_DEMAND_MARKER_BYTES * marker_keys.len(),
                    max_single_value_bytes: MAX_DEMAND_MARKER_BYTES,
                },
            )
            .await?;
        if marker_rows.values.len() != missing_indexes.len() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "partial blob demand marker read returned the wrong number of slots",
            ));
        }
        for (index, marker) in missing_indexes.into_iter().zip(marker_rows.values) {
            let Some(StorageProjectedValue::FullValue(marker)) = marker else {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "partial blob chunk is missing without an explicit demand marker",
                ));
            };
            demand_markers[index] = Some(marker);
        }
    }

    Ok(ChunkPageInspection {
        unique_hashes,
        requested_to_unique,
        resident,
        demand_markers,
    })
}

/// Atomically installs one bounded ordered page of demanded raw chunks.
/// Source buffers are capped at 24 MiB. Staging copies each payload into its
/// encoded write-set row, so source plus staged payload bytes can reach about
/// 48 MiB, plus codec framing, until the durable commit completes.
pub(super) async fn install_chunk_page<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    chunks: Vec<(ChunkHash, Vec<u8>)>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let chunk_bounds: crate::binary_cas::RawChunkTransferBounds = crate::binary_cas::raw_chunk_transfer_bounds();
    let max_chunk_bytes = chunk_bounds.max_payload_bytes;
    if chunks.len() > MAX_PARTIAL_CHUNK_PAGE_ITEMS {
        return Err(page_limit_error("chunk count"));
    }
    let total_bytes = chunks.iter().try_fold(0usize, |total, (_, bytes)| {
        if bytes.is_empty() || bytes.len() > max_chunk_bytes {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "partial blob chunk payload exceeds its transfer size limit",
            ));
        }
        total.checked_add(bytes.len()).ok_or_else(|| {
            LixError::new(
                LixError::CODE_INVALID_PARAM,
                "partial blob chunk page byte count overflowed",
            )
        })
    })?;
    if total_bytes > MAX_PARTIAL_CHUNK_PAGE_BYTES {
        return Err(page_limit_error("chunk bytes"));
    }
    if chunks.is_empty() {
        return Ok(());
    }

    let mut unique_hashes = Vec::with_capacity(chunks.len());
    let mut unique_payload_indexes: Vec<usize> = Vec::with_capacity(chunks.len());
    let mut unique_by_hash: HashMap<ChunkHash, usize> = HashMap::with_capacity(chunks.len());
    for (index, (hash, bytes)) in chunks.iter().enumerate() {
        if let Some(previous) = unique_by_hash.get(hash).copied() {
            if chunks[unique_payload_indexes[previous]].1.as_slice() != bytes.as_slice() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "partial blob page repeats a chunk identity with different payloads",
                ));
            }
        } else {
            let unique_index = unique_hashes.len();
            unique_hashes.push(*hash);
            unique_payload_indexes.push(index);
            unique_by_hash.insert(*hash, unique_index);
        }
    }

    loop {
        let read = storage.begin_read(Default::default()).await?;
        let (actual, raw) = load_partial_replica_state(&read)
            .await?
            .ok_or_else(mismatch)?;
        if !same_admission(&actual, expected) {
            return Err(mismatch());
        }
        let inspected = inspect_chunk_page(&read, &unique_hashes).await?;
        let missing = inspected
            .resident
            .iter()
            .enumerate()
            .filter_map(|(index, resident)| (!resident).then_some(index))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return Ok(());
        }

        let mut writes = storage.new_write_set();
        let mut preconditions = vec![StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: partial_replica_state_key(),
            expected: raw,
        }];
        for index in missing {
            let hash = inspected.unique_hashes[index];
            let key = StorageKey(Bytes::copy_from_slice(hash.as_bytes()));
            let marker = inspected.demand_markers[index]
                .as_ref()
                .expect("missing demanded chunks have marker values")
                .clone();
            let payload = &chunks[unique_payload_indexes[index]].1;
            stage_verified_raw_chunk(&mut writes, hash, payload)?;
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: BINARY_CAS_CHUNK_SPACE,
                key: key.clone(),
            });
            preconditions.push(StoragePrecondition::KeyValueEquals {
                space: BINARY_CAS_CHUNK_DEMAND_SPACE,
                key,
                expected: marker,
            });
        }
        stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
        drop(read);

        match storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Err(error) if is_precondition_failure(&error) => continue,
            Err(error) => return Err(error.into()),
            Ok(_) => return Ok(()),
        }
    }
}

#[cfg(test)]
async fn checked_chunk_resident(
    read: &(impl StorageAdapterRead + ?Sized),
    requested: ChunkHash,
) -> Result<bool, LixError> {
    let value = crate::binary_cas::load_verified_chunk(read, requested).await?;
    let present = crate::binary_cas::chunk_presence_many(read, &[requested]).await?[0];
    if value.is_some() != present {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "binary CAS chunk payload and presence marker disagree",
        ));
    }
    if !present {
        use crate::storage_adapter::{PointReadPlan, StorageCoreProjection, StorageGetOptions};
        let key = StorageKey(Bytes::copy_from_slice(requested.as_bytes()));
        let marker = PointReadPlan::new(BINARY_CAS_CHUNK_DEMAND_SPACE, &[key])
            .materialize(
                read,
                StorageGetOptions {
                    projection: StorageCoreProjection::KeyOnly,
                },
            )
            .await?
            .value
            .pop()
            .flatten();
        if marker.is_none() {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "partial blob chunk is missing without an explicit demand marker",
            ));
        }
    }
    Ok(present)
}

pub(super) async fn check_manifest_chunk_presence(
    read: &(impl StorageAdapterRead + ?Sized),
    manifest: &CanonicalBlobManifest,
) -> Result<(), LixError> {
    use crate::storage_adapter::{PointReadPlan, StorageCoreProjection, StorageGetOptions};
    let keys = manifest
        .chunks
        .iter()
        .map(|chunk| StorageKey(Bytes::copy_from_slice(chunk.hash.as_bytes())))
        .collect::<Vec<_>>();
    let payloads = PointReadPlan::new(BINARY_CAS_CHUNK_SPACE, &keys)
        .materialize(
            read,
            StorageGetOptions {
                projection: StorageCoreProjection::KeyOnly,
            },
        )
        .await?
        .value;
    let markers = crate::binary_cas::chunk_presence_many(
        read,
        &manifest
            .chunks
            .iter()
            .map(|chunk| chunk.hash)
            .collect::<Vec<_>>(),
    )
    .await?;
    if payloads.len() != markers.len()
        || payloads
            .iter()
            .zip(markers)
            .any(|(payload, marker)| payload.is_some() != marker)
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "binary CAS manifest references inconsistent chunk presence",
        ));
    }
    Ok(())
}

async fn check_manifest_chunk_presence_many(
    read: &(impl StorageAdapterRead + ?Sized),
    manifests: &[CanonicalBlobManifest],
) -> Result<Vec<ChunkHash>, LixError> {
    let mut hashes = BTreeSet::new();
    for manifest in manifests {
        hashes.extend(manifest.chunks.iter().map(|chunk| chunk.hash));
    }
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    if hashes.len() > MAX_PARTIAL_MANIFEST_CHUNK_REFS {
        return Err(page_limit_error("manifest chunk-reference count"));
    }
    let hashes = hashes.into_iter().collect::<Vec<_>>();
    let keys = hashes
        .iter()
        .map(|hash| StorageKey(Bytes::copy_from_slice(hash.as_bytes())))
        .collect::<Vec<_>>();
    let payloads = PointReadPlan::new(BINARY_CAS_CHUNK_SPACE, &keys)
        .materialize(
            read,
            StorageGetOptions {
                projection: crate::storage_adapter::StorageCoreProjection::KeyOnly,
            },
        )
        .await?
        .value;
    let markers = crate::binary_cas::chunk_presence_many(read, &hashes).await?;
    if payloads.len() != hashes.len()
        || markers.len() != hashes.len()
        || payloads
            .iter()
            .zip(&markers)
            .any(|(payload, marker)| payload.is_some() != *marker)
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "binary CAS manifest references inconsistent chunk presence",
        ));
    }
    Ok(hashes
        .into_iter()
        .zip(markers)
        .filter_map(|(hash, present)| (!present).then_some(hash))
        .collect())
}

/// Installs an absent referenced manifest, with explicit missing chunk markers.
/// An existing manifest is never replaced: layout/corruption is not repaired by
/// downloading a different physical representation of the same logical blob.
#[cfg(test)]
pub(super) async fn install_manifest<S>(storage: &StorageAdapter<S>, expected: &PartialReplicaState, requested: BlobId, wire: &SyncBlobManifest) -> Result<SyncBlobRegistration, LixError>
where S: Storage + Clone + Send + Sync + 'static {
    Ok(install_manifest_pages(storage, expected, &[requested], std::slice::from_ref(wire)).await?.remove(0))
}

pub(super) async fn install_manifest_pages<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: &[BlobId],
    wires: &[SyncBlobManifest],
) -> Result<Vec<SyncBlobRegistration>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if requested.len() != wires.len() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "partial blob manifest request and response cardinalities differ",
        ));
    }
    if wires.len() > MAX_PARTIAL_MANIFEST_PAGE_ITEMS {
        return Err(page_limit_error("manifest count"));
    }

    // Reject conflicting aliases before installing any earlier page. Identical
    // duplicate slots remain valid and keep their original result positions.
    let mut first_by_id = HashMap::with_capacity(requested.len());
    for (index, (blob_id, wire)) in requested.iter().zip(wires).enumerate() {
        if let Some(previous) = first_by_id.get(blob_id).copied() {
            if &wires[previous] != wire {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "partial blob manifest page repeats an identity with different content",
                ));
            }
        } else {
            first_by_id.insert(*blob_id, index);
        }
    }

    let pages = super::transfer::partition_manifest_pages(wires)?;
    let mut registrations = Vec::with_capacity(wires.len());
    for page in pages {
        registrations.extend(
            install_manifest_page(storage, expected, &requested[page.clone()], &wires[page])
                .await?,
        );
    }
    for (index, blob_id) in requested.iter().enumerate() {
        let first = first_by_id[blob_id];
        let registration = registrations[first].clone();
        registrations[index] = registration;
    }
    Ok(registrations)
}

pub(super) async fn install_manifest_page<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: &[BlobId],
    wires: &[SyncBlobManifest],
) -> Result<Vec<SyncBlobRegistration>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if requested.len() != wires.len() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "partial blob manifest request and response cardinalities differ",
        ));
    }
    if wires.len() > MAX_PARTIAL_MANIFEST_PAGE_ITEMS {
        return Err(page_limit_error("manifest count"));
    }
    if wires.is_empty() {
        return Ok(Vec::new());
    }
    super::transfer::validate_manifest_page(wires)?;
    let mut receipt_count = 0usize;
    for wire in wires {
        receipt_count = receipt_count
            .checked_add(wire.chunks.len())
            .ok_or_else(|| page_limit_error("manifest chunk-reference count"))?;
        if receipt_count > MAX_PARTIAL_MANIFEST_CHUNK_REFS {
            return Err(page_limit_error("manifest chunk-reference count"));
        }
    }

    let mut manifests = Vec::with_capacity(wires.len());
    let mut inline_payloads = Vec::with_capacity(wires.len());
    let mut first_by_id = HashMap::with_capacity(wires.len());
    let mut actual_inline_bytes = 0usize;
    for (index, (requested_id, wire)) in requested.iter().zip(wires).enumerate() {
        let manifest = super::blob::decode_manifest(wire)?;
        if manifest.blob_id != *requested_id {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "blob manifest response address differs from request",
            ));
        }
        let inline = super::blob::decode_inline_bytes(wire)?;
        actual_inline_bytes = actual_inline_bytes
            .checked_add(inline.as_ref().map_or(0, Vec::len))
            .ok_or_else(|| page_limit_error("manifest inline bytes"))?;
        if actual_inline_bytes > super::transfer::MANIFEST_PAGE_DECODED_BYTES {
            return Err(page_limit_error("manifest inline bytes"));
        }
        if let Some(previous) = first_by_id.get(requested_id).copied() {
            if &wires[previous] != wire || &manifests[previous] != &manifest {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "partial blob manifest page repeats an identity with different content",
                ));
            }
        } else {
            first_by_id.insert(*requested_id, index);
        }
        manifests.push(manifest);
        inline_payloads.push(inline);
    }

    loop {
        let read = storage.begin_read(Default::default()).await?;
        let (actual, raw) = load_partial_replica_state(&read)
            .await?
            .ok_or_else(mismatch)?;
        if !same_admission(&actual, expected) {
            return Err(mismatch());
        }
        // Decoding existing metadata must still surface corruption, even for
        // duplicate request slots.
        let metadata = load_metadata_many_bounded(
            &read,
            requested,
            crate::storage_adapter::ReadBudget {
                max_result_bytes: MAX_PARTIAL_MANIFEST_READ_BYTES,
                max_single_value_bytes: MAX_PARTIAL_MANIFEST_READ_BYTES,
            },
        )
        .await?
        .into_vec();
        let mut new_indexes = Vec::with_capacity(wires.len());
        let mut planned = BTreeSet::new();
        for (index, (manifest, metadata)) in manifests.iter().zip(metadata).enumerate() {
            if metadata.is_none() && planned.insert(manifest.blob_id) {
                new_indexes.push(index);
            }
        }
        if new_indexes.is_empty() {
            return Ok(vec![
                SyncBlobRegistration {
                    missing_chunk_ids: Vec::new(),
                };
                wires.len()
            ]);
        }

        let new_manifests = new_indexes
            .iter()
            .map(|index| manifests[*index].clone())
            .collect::<Vec<_>>();
        // Preserve the scalar invariant that a manifest cannot turn an
        // inconsistent payload/presence pair into a new demand marker.
        check_manifest_chunk_presence_many(&read, &new_manifests).await?;

        let mut inline_chunks = Vec::new();
        for index in &new_indexes {
            if let Some(bytes) = inline_payloads[*index].as_ref() {
                let manifest = &manifests[*index];
                match manifest.chunks.as_slice() {
                    [] if bytes.is_empty() && manifest.size_bytes == 0 => {}
                    [chunk]
                        if bytes.len() as u64 == manifest.size_bytes
                            && bytes.len() as u64 == chunk.size_bytes
                            && ChunkHash::from_content(bytes) == chunk.hash
                            && BlobId::from_content(bytes) == manifest.blob_id =>
                    {
                        inline_chunks.push(crate::binary_cas::CanonicalBlobChunk {
                            receipt: manifest.chunks[0],
                            bytes: bytes.clone(),
                        });
                    }
                    _ => {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "inline blob payload does not match its manifest",
                        ));
                    }
                }
            }
        }

        let mut writes = storage.new_write_set();
        let mut preconditions = vec![StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: partial_replica_state_key(),
            expected: raw,
        }];
        for index in &new_indexes {
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: BINARY_CAS_MANIFEST_SPACE,
                key: StorageKey(Bytes::copy_from_slice(requested[*index].as_bytes())),
            });
        }
        let missing = crate::binary_cas::stage_deferred_canonical_manifests_with_chunks(
            &read,
            &mut writes,
            &new_manifests,
            &inline_chunks,
        )
        .await?;
        for hash in &missing {
            preconditions.push(StoragePrecondition::KeyAbsent {
                space: BINARY_CAS_CHUNK_SPACE,
                key: StorageKey(Bytes::copy_from_slice(hash.as_bytes())),
            });
        }
        stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
        drop(read);

        match storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Err(error) if is_precondition_failure(&error) => continue,
            Err(error) => return Err(error.into()),
            Ok(_) => {
                let missing = missing.into_iter().collect::<BTreeSet<_>>();
                let missing_by_id = new_indexes
                    .iter()
                    .map(|index| {
                        let missing_for_manifest = manifests[*index]
                            .chunks
                            .iter()
                            .map(|chunk| chunk.hash)
                            .filter(|hash| missing.contains(hash))
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect::<Vec<_>>();
                        (manifests[*index].blob_id, missing_for_manifest)
                    })
                    .collect::<HashMap<_, _>>();
                return Ok(requested
                    .iter()
                    .map(|id| SyncBlobRegistration {
                        missing_chunk_ids: missing_by_id
                            .get(id)
                            .into_iter()
                            .flatten()
                            .map(|hash| hash.to_hex())
                            .collect(),
                    })
                    .collect());
            }
        }
    }
}

/// Installs a hash-checked raw chunk only while its explicit demand marker is
/// still present in the same receipt epoch. Never overwrites resident bytes.
#[cfg(test)]
pub(super) async fn scalar_install_chunk<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: ChunkHash,
    bytes: &[u8],
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use crate::binary_cas::{BINARY_CAS_CHUNK_DEMAND_SPACE, BINARY_CAS_CHUNK_SPACE};
    use crate::storage_adapter::PointReadPlan;
    let read = storage.begin_read(Default::default()).await?;
    let (actual, raw) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(mismatch)?;
    if !same_admission(&actual, expected) {
        return Err(mismatch());
    }
    if checked_chunk_resident(&read, requested).await? {
        return Ok(());
    }
    let key = StorageKey(Bytes::copy_from_slice(requested.as_bytes()));
    let marker = PointReadPlan::new(BINARY_CAS_CHUNK_DEMAND_SPACE, std::slice::from_ref(&key))
        .materialize(&read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    let Some(StorageProjectedValue::FullValue(marker)) = marker else {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "partial blob chunk has no explicit demand marker",
        ));
    };
    let mut writes = storage.new_write_set();
    stage_verified_raw_chunk(&mut writes, requested, bytes)?;
    let mut preconditions = vec![
        StoragePrecondition::KeyAbsent {
            space: BINARY_CAS_CHUNK_SPACE,
            key: key.clone(),
        },
        StoragePrecondition::KeyValueEquals {
            space: BINARY_CAS_CHUNK_DEMAND_SPACE,
            key,
            expected: marker,
        },
        StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: partial_replica_state_key(),
            expected: raw,
        },
    ];
    stage_transfer_publication_fence(&read, &mut writes, &mut preconditions).await?;
    drop(read);
    let installed = storage
        .commit_partial_replica_write_set(
            super::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await;
    if let Err(error) = installed {
        if is_precondition_failure(&error)
            && chunk_is_resident(storage, expected, requested).await?
        {
            return Ok(());
        }
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "partial_blob_tests.rs"]
mod tests;

#[cfg(test)]
pub(super) async fn install_chunk<S>(storage: &StorageAdapter<S>, expected: &PartialReplicaState, requested: ChunkHash, bytes: &[u8]) -> Result<(), LixError>
where S: Storage + Clone + Send + Sync + 'static {
    install_chunk_page(storage, expected, vec![(requested, bytes.to_vec())]).await
}
