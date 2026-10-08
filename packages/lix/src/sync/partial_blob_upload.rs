//! Upload only content referenced by a captured local native commit batch.
//! Canonical flattening is temporary; it never mutates the serving cache.
use super::partial_state::{PartialReplicaState, load_partial_replica_state};
use super::{SyncPushRequest, SyncPushResponse, SyncTransport};
use crate::LixError;
use crate::binary_cas::{BlobId, load_canonical_blob_chunks, load_metadata_many};
use crate::storage_adapter::{Storage, StorageAdapter};

pub(super) async fn push_partial_with_blobs<S, T>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &T,
    request: &SyncPushRequest,
) -> Result<SyncPushResponse, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    // Preserve the frozen native publication tuple; inline content is a
    // deterministic transfer representation of its immutable referenced blobs.
    let mut combined = request.clone();
    prepare_partial_upload_blobs_inner(
        storage,
        state,
        transport,
        &mut combined,
        TransferMode::Combined,
    )
    .await?;
    match transport.push(&combined).await {
        Err(error)
            if combined.inline_blobs.len() > request.inline_blobs.len()
                && super::transfer::explicit_body_limit(&error) =>
        {
            // A 413 rejects the request before publication. Retry only the
            // transfer representation, retaining the exact frozen native tuple.
            let mut separate = request.clone();
            prepare_partial_upload_blobs_inner(
                storage,
                state,
                transport,
                &mut separate,
                TransferMode::Chunks,
            )
            .await?;
            transport.push(request).await
        }
        result => result,
    }
}

/// Prepare only blobs referenced by the exact durable native body batch.
/// Retained merge waves use the same authenticated upload before their body RPC.
pub(super) async fn prepare_partial_upload_blobs<S, T>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &T,
    request: &SyncPushRequest,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    let mut transfer = request.clone();
    prepare_partial_upload_blobs_inner(
        storage,
        state,
        transport,
        &mut transfer,
        TransferMode::Prepare,
    )
    .await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransferMode {
    Combined,
    Prepare,
    Chunks,
}

async fn prepare_partial_upload_blobs_inner<S, T>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &T,
    request: &mut SyncPushRequest,
    mode: TransferMode,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    // Keep the combined request below the ordinary partial publication budget;
    // larger content retains the existing independently bounded transfer lane.
    const MAX_COMBINED_REQUEST_BYTES: usize = 1024 * 1024;
    let mut encoded_bytes = serde_json::to_vec(request)
        .map_err(|error| LixError::unknown(format!("encode combined publication: {error}")))?
        .len();
    if transport.active_account_id() != state.active_account_id() {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "blob upload authority account differs",
        ));
    }
    let mut inline_group = super::transfer::TransferBatch::new();
    let mut authority_group = Vec::new();
    let mut canonical_group = super::transfer::TransferBatch::new();
    let ids = super::repository::sync_commit_blob_ids(&request.commits)?
        .into_iter()
        .map(|blob| BlobId::from_hex(&blob))
        .collect::<Result<Vec<_>, _>>()?;
    for ids in ids.chunks(super::transfer::CONTENT_GROUP_ITEMS) {
        let read = guarded_upload_read(storage, state).await?;
        let metadata = load_metadata_many(&read, ids).await?.into_vec();
        drop(read);
        for (id, metadata) in ids.iter().zip(metadata) {
            let id = *id;
            let blob = id.to_hex();
            let Some(metadata) = metadata else {
                // A checkpoint can copy an authority-backed reference whose content
                // was never demanded locally. Confirm its exact immutable identity
                // at the authority instead of hydrating and uploading it again.
                authority_group.push(blob);
                if authority_group.len() == super::MAX_SYNC_BLOB_BATCH_ITEMS {
                    super::transfer::confirm_authority_group(transport, &authority_group).await?;
                    authority_group.clear();
                }
                continue;
            };
            let read = guarded_upload_read(storage, state).await?;
            if metadata.size_bytes > super::blob::MAX_INLINE_SYNC_BLOB_BYTES as u64 {
                let manifest =
                    match crate::binary_cas::load_streaming_canonical_manifest(&read, &metadata)
                        .await
                    {
                        Ok(manifest) => manifest,
                        Err(error) if error.code == "LIX_SYNC_CHUNKS_REQUIRED" => {
                            drop(read);
                            authority_group.push(blob);
                            if authority_group.len() == super::MAX_SYNC_BLOB_BATCH_ITEMS {
                                super::transfer::confirm_authority_group(
                                    transport,
                                    &authority_group,
                                )
                                .await?;
                                authority_group.clear();
                            }
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                drop(read);
                pack_canonical_upload(
                    storage,
                    state,
                    transport,
                    &mut canonical_group,
                    super::transfer::CanonicalUploadPlan {
                        metadata,
                        canonical: manifest,
                    },
                )
                .await?;
                continue;
            }
            let chunks = match load_canonical_blob_chunks(&read, id).await {
                Ok(Some(chunks)) => chunks,
                Err(error) if error.code == "LIX_SYNC_CHUNKS_REQUIRED" => {
                    // A resident deferred manifest is also a valid sparse state.
                    // Other storage errors, including corruption, remain errors.
                    drop(read);
                    authority_group.push(blob);
                    if authority_group.len() == super::MAX_SYNC_BLOB_BATCH_ITEMS {
                        super::transfer::confirm_authority_group(transport, &authority_group)
                            .await?;
                        authority_group.clear();
                    }
                    continue;
                }
                Ok(None) => {
                    return Err(LixError::unknown("captured local blob content is missing"));
                }
                Err(error) => return Err(error),
            };
            let mut manifest = super::blob::encode_manifest(id, &chunks)?;
            drop(read);
            if mode == TransferMode::Combined && manifest.inline_bytes_base64.is_some() {
                let addition = serde_json::to_vec(&manifest)
                    .map_err(|error| LixError::unknown(format!("encode inline content: {error}")))?
                    .len()
                    .saturating_add(usize::from(!request.inline_blobs.is_empty()));
                if encoded_bytes.saturating_add(addition) <= MAX_COMBINED_REQUEST_BYTES {
                    encoded_bytes += addition;
                    request.inline_blobs.push(manifest);
                    continue;
                }
            }
            if mode != TransferMode::Chunks && manifest.inline_bytes_base64.is_some() {
                let encoded = serde_json::to_vec(&manifest)
                    .map_err(|error| LixError::unknown(error.to_string()))?
                    .len();
                let decoded = manifest.size_bytes as usize;
                if let Some(manifest) = inline_group.push(manifest, encoded, decoded)? {
                    super::transfer::register_inline_group(transport, &inline_group).await?;
                    inline_group = super::transfer::TransferBatch::new();
                    if inline_group.push(manifest, encoded, decoded)?.is_some() {
                        return Err(LixError::unknown("single inline group member did not fit"));
                    }
                }
                continue;
            }
            manifest.inline_bytes_base64 = None;
            let canonical = super::blob::decode_manifest(&manifest)?;
            pack_canonical_upload(
                storage,
                state,
                transport,
                &mut canonical_group,
                super::transfer::CanonicalUploadPlan {
                    metadata,
                    canonical,
                },
            )
            .await?;
        }
    }
    upload_canonical_group(storage, state, transport, &canonical_group.items).await?;
    super::transfer::register_inline_group(transport, &inline_group).await?;
    super::transfer::confirm_authority_group(transport, &authority_group).await?;
    Ok(())
}

async fn pack_canonical_upload<S, T>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &T,
    group: &mut super::transfer::TransferBatch<super::transfer::CanonicalUploadPlan>,
    plan: super::transfer::CanonicalUploadPlan,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    super::transfer::pack_canonical_plan(transport, group, plan, || async {
        guarded_upload_read(storage, state).await
    })
    .await
}

async fn upload_canonical_group<S, T>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &T,
    plans: &[super::transfer::CanonicalUploadPlan],
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    super::transfer::upload_canonical_page(transport, plans, || async {
        guarded_upload_read(storage, state).await
    })
    .await
}

async fn guarded_upload_read<S>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
) -> Result<impl crate::storage_adapter::StorageAdapterRead, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    if load_partial_replica_state(&read)
        .await?
        .as_ref()
        .map(|(actual, _)| actual)
        != Some(state)
    {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "blob upload epoch changed",
        ));
    }
    Ok(read)
}
