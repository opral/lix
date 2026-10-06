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
    for blob in super::repository::sync_commit_blob_ids(&request.commits)? {
        let id = BlobId::from_hex(&blob)?;
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
        let metadata = load_metadata_many(&read, &[id])
            .await?
            .into_vec()
            .into_iter()
            .next()
            .flatten();
        let Some(metadata) = metadata else {
            // A checkpoint can copy an authority-backed reference whose content
            // was never demanded locally. Confirm its exact immutable identity
            // at the authority instead of hydrating and uploading it again.
            drop(read);
            authority_group.push(blob);
            if authority_group.len() == super::MAX_SYNC_BLOB_BATCH_ITEMS {
                super::transfer::confirm_authority_group(transport, &authority_group).await?;
                authority_group.clear();
            }
            continue;
        };
        if metadata.size_bytes > super::blob::MAX_INLINE_SYNC_BLOB_BYTES as u64 {
            let manifest = match crate::binary_cas::load_streaming_canonical_manifest(
                &read, &metadata,
            )
            .await
            {
                Ok(manifest) => manifest,
                Err(error) if error.code == "LIX_SYNC_CHUNKS_REQUIRED" => {
                    drop(read);
                    authority_group.push(blob);
                    if authority_group.len() == super::MAX_SYNC_BLOB_BATCH_ITEMS {
                        super::transfer::confirm_authority_group(transport, &authority_group)
                            .await?;
                        authority_group.clear();
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            drop(read);
            upload_streaming_blob(storage, state, transport, &metadata, &manifest).await?;
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
                    super::transfer::confirm_authority_group(transport, &authority_group).await?;
                    authority_group.clear();
                }
                continue;
            }
            Ok(None) => return Err(LixError::unknown("captured local blob content is missing")),
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
        if mode == TransferMode::Prepare && manifest.inline_bytes_base64.is_some() {
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
        if mode == TransferMode::Chunks {
            manifest.inline_bytes_base64 = None;
        }
        let registration =
            super::transfer::register_manifest_page(transport, std::slice::from_ref(&manifest))
                .await?
                .remove(0);
        let mut missing = std::collections::BTreeSet::new();
        for id in &registration.missing_chunk_ids {
            if !manifest.chunks.iter().any(|chunk| &chunk.chunk_id == id) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "authority requested an unrelated upload chunk",
                ));
            }
            missing.insert(id.as_str());
        }
        let uploaded = !missing.is_empty();
        let uploads = chunks
            .iter()
            .filter_map(|chunk| {
                let id = chunk.receipt.hash.to_hex();
                missing
                    .remove(id.as_str())
                    .then_some((id, chunk.bytes.as_slice()))
            })
            .collect::<Vec<_>>();
        for page in uploads.chunks(super::transfer::CHUNK_CONCURRENCY) {
            super::transfer::upload_chunk_page(transport, page).await?;
        }
        if uploaded
            && !super::transfer::register_manifest_page(transport, std::slice::from_ref(&manifest))
                .await?
                .remove(0)
                .missing_chunk_ids
                .is_empty()
        {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "authority blob remains incomplete after upload",
            ));
        }
    }
    super::transfer::register_inline_group(transport, &inline_group).await?;
    super::transfer::confirm_authority_group(transport, &authority_group).await?;
    Ok(())
}

/// Transfer large flat or delta-backed blobs with at most one forced anchor's
/// payload in memory. No storage read remains open across an HTTP request.
async fn upload_streaming_blob<S, T>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &T,
    metadata: &crate::binary_cas::BlobMetadata,
    canonical: &crate::binary_cas::CanonicalBlobManifest,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    super::transfer::upload_canonical_blob(transport, metadata, canonical, || async {
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
    })
    .await
}
