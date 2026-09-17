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
                && is_explicit_body_limit(&error) =>
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

fn is_explicit_body_limit(error: &LixError) -> bool {
    error.details.as_ref().and_then(|details| details.get("httpStatus"))
        .and_then(serde_json::Value::as_u64) == Some(413)
        // HttpSyncTransport maps an unstructured intermediary HTTP 413 here.
        || (error.code == "LIX_ERROR_REQUEST_BODY_TOO_LARGE" && error.details.is_none())
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
            confirm_authority_blob(transport, &blob).await?;
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
                    confirm_authority_blob(transport, &blob).await?;
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
                confirm_authority_blob(transport, &blob).await?;
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
        if mode == TransferMode::Chunks {
            manifest.inline_bytes_base64 = None;
        }
        let registration = transport.register_blob(&manifest).await?;
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
        for chunk in &chunks {
            let id = chunk.receipt.hash.to_hex();
            if missing.contains(id.as_str()) {
                transport.put_chunk(&id, &chunk.bytes).await?;
                missing.remove(id.as_str());
            }
        }
        if uploaded
            && !transport
                .register_blob(&manifest)
                .await?
                .missing_chunk_ids
                .is_empty()
        {
            return Err(LixError::new(
                LixError::CODE_STORAGE_ERROR,
                "authority blob remains incomplete after upload",
            ));
        }
    }
    Ok(())
}

async fn confirm_authority_blob<T: SyncTransport>(
    transport: &T,
    blob: &str,
) -> Result<(), LixError> {
    let ids = [blob.to_owned()];
    let manifests = transport.get_blobs(&ids).await?;
    if manifests.len() != 1 || manifests[0].blob_id != blob {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "authority must confirm exactly the referenced checkpoint blob",
        ));
    }
    super::blob::validate_sync_blob_manifest(&manifests[0])
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
    let manifest = super::SyncBlobManifest {
        blob_id: canonical.blob_id.to_hex(),
        size_bytes: canonical.size_bytes,
        chunks: canonical
            .chunks
            .iter()
            .map(|chunk| super::SyncBlobChunk {
                chunk_id: chunk.hash.to_hex(),
                size_bytes: chunk.size_bytes,
            })
            .collect(),
        inline_bytes_base64: None,
    };
    super::blob::validate_sync_blob_manifest(&manifest)?;
    let registration = transport.register_blob(&manifest).await?;
    let declared = manifest
        .chunks
        .iter()
        .map(|chunk| chunk.chunk_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut missing = std::collections::BTreeSet::new();
    for id in registration.missing_chunk_ids {
        if !declared.contains(id.as_str()) {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "authority requested an unrelated upload chunk",
            ));
        }
        // A flat manifest can reference the same chunk at many offsets. The
        // authority may report each occurrence; transfer the immutable bytes once.
        missing.insert(id);
    }
    let uploaded = !missing.is_empty();
    let mut offset = 0u64;
    let mut first = 0;
    while first < manifest.chunks.len() && !missing.is_empty() {
        let mut end = first;
        let mut size = 0u64;
        while end < manifest.chunks.len() && size < crate::binary_cas::CHUNK_ANCHOR_BYTES as u64 {
            size += manifest.chunks[end].size_bytes;
            end += 1;
        }
        let expected = &manifest.chunks[first..end];
        if expected
            .iter()
            .any(|chunk| missing.contains(&chunk.chunk_id))
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
            let chunks =
                crate::binary_cas::load_canonical_blob_anchor(&read, metadata, offset).await?;
            drop(read);
            if chunks.len() != expected.len()
                || chunks.iter().zip(expected).any(|(chunk, receipt)| {
                    chunk.receipt.hash.to_hex() != receipt.chunk_id
                        || chunk.receipt.size_bytes != receipt.size_bytes
                })
            {
                return Err(LixError::new(
                    LixError::CODE_STORAGE_ERROR,
                    "canonical upload anchor changed after manifest preparation",
                ));
            }
            for chunk in chunks {
                let id = chunk.receipt.hash.to_hex();
                if missing.contains(&id) {
                    transport.put_chunk(&id, &chunk.bytes).await?;
                    missing.remove(&id);
                }
            }
        }
        offset += size;
        first = end;
    }
    if uploaded
        && !transport
            .register_blob(&manifest)
            .await?
            .missing_chunk_ids
            .is_empty()
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "authority blob remains incomplete after upload",
        ));
    }
    Ok(())
}
