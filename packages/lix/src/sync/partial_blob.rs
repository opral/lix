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
    BINARY_CAS_MANIFEST_SPACE, BlobId, ChunkHash, load_metadata_many,
    stage_deferred_canonical_manifest, stage_transfer_publication_fence,
    stage_verified_inline_canonical_blob, stage_verified_raw_chunk,
};
use crate::storage_adapter::{
    Storage, StorageAdapter, StorageKey, StoragePrecondition, StorageWriteOptions,
};

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

pub(super) async fn manifest_is_resident<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: BlobId,
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
    Ok(load_metadata_many(&read, &[requested]).await?.into_vec()[0].is_some())
}

pub(super) async fn chunk_is_resident<S>(
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

async fn checked_chunk_resident(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
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
        let marker = PointReadPlan::new(crate::binary_cas::BINARY_CAS_CHUNK_DEMAND_SPACE, &[key])
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

async fn check_manifest_chunk_presence(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    manifest: &crate::binary_cas::CanonicalBlobManifest,
) -> Result<(), LixError> {
    use crate::storage_adapter::{PointReadPlan, StorageCoreProjection, StorageGetOptions};
    let keys = manifest
        .chunks
        .iter()
        .map(|chunk| StorageKey(Bytes::copy_from_slice(chunk.hash.as_bytes())))
        .collect::<Vec<_>>();
    let payloads = PointReadPlan::new(crate::binary_cas::BINARY_CAS_CHUNK_SPACE, &keys)
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

/// Installs an absent referenced manifest, with explicit missing chunk markers.
/// An existing manifest is never replaced: layout/corruption is not repaired by
/// downloading a different physical representation of the same logical blob.
pub(super) async fn install_manifest<S>(
    storage: &StorageAdapter<S>,
    expected: &PartialReplicaState,
    requested: BlobId,
    wire: &SyncBlobManifest,
) -> Result<SyncBlobRegistration, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let manifest = super::blob::decode_manifest(wire)?;
    if manifest.blob_id != requested {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "blob manifest response address differs from request",
        ));
    }
    let inline = super::blob::decode_inline_bytes(wire)?;
    let read = storage.begin_read(Default::default()).await?;
    let (actual, raw) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(mismatch)?;
    if !same_admission(&actual, expected) {
        return Err(mismatch());
    }
    // Decoding existing metadata must still surface corruption.
    if load_metadata_many(&read, &[requested]).await?.into_vec()[0].is_some() {
        return Ok(SyncBlobRegistration {
            missing_chunk_ids: Vec::new(),
        });
    }
    check_manifest_chunk_presence(&read, &manifest).await?;
    let mut writes = storage.new_write_set();
    let missing_chunk_ids = if let Some(bytes) = inline {
        stage_verified_inline_canonical_blob(&mut writes, &manifest, &bytes)?;
        Vec::new()
    } else {
        stage_deferred_canonical_manifest(&read, &mut writes, &manifest)
            .await?
            .into_iter()
            .map(|hash| hash.to_hex())
            .collect()
    };
    let mut preconditions = vec![
        StoragePrecondition::KeyAbsent {
            space: BINARY_CAS_MANIFEST_SPACE,
            key: StorageKey(Bytes::copy_from_slice(requested.as_bytes())),
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
            && manifest_is_resident(storage, expected, requested).await?
        {
            return Ok(SyncBlobRegistration {
                missing_chunk_ids: Vec::new(),
            });
        }
        return Err(error.into());
    }
    Ok(SyncBlobRegistration { missing_chunk_ids })
}

/// Installs a hash-checked raw chunk only while its explicit demand marker is
/// still present in the same receipt epoch. Never overwrites resident bytes.
pub(super) async fn install_chunk<S>(
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
    let marker = PointReadPlan::new(BINARY_CAS_CHUNK_DEMAND_SPACE, &[key.clone()])
        .materialize(&read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    let Some(crate::storage_adapter::StorageProjectedValue::FullValue(marker)) = marker else {
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
