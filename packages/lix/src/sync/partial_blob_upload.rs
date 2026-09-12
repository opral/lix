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
    prepare_partial_upload_blobs_inner(storage, state, transport, &mut combined, true).await?;
    transport.push(&combined).await
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
    prepare_partial_upload_blobs_inner(storage, state, transport, &mut transfer, false).await
}

async fn prepare_partial_upload_blobs_inner<S, T>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &T,
    request: &mut SyncPushRequest,
    combine: bool,
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
    // Existing canonical flattening can allocate the complete requested file.
    // Bound one file independently of native commit/request output budgets.
    const MAX_PREPARED_BLOB: u64 = 64 * 1024 * 1024;
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
            .flatten()
            .ok_or_else(|| LixError::unknown("captured local blob metadata is missing"))?;
        if metadata.size_bytes > MAX_PREPARED_BLOB {
            return Err(LixError::new(
                "LIX_PARTIAL_UPLOAD_PREPARATION_REQUIRED",
                "large file upload requires streaming canonical preparation",
            ));
        }
        let chunks = load_canonical_blob_chunks(&read, id)
            .await?
            .ok_or_else(|| LixError::unknown("captured local blob content is missing"))?;
        let manifest = super::blob::encode_manifest(id, &chunks)?;
        drop(read);
        if combine && manifest.inline_bytes_base64.is_some() {
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
        let registration = transport.register_blob(&manifest).await?;
        let mut missing = std::collections::BTreeSet::new();
        for id in &registration.missing_chunk_ids {
            if !missing.insert(id.as_str())
                || !manifest.chunks.iter().any(|chunk| &chunk.chunk_id == id)
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "authority requested a duplicate or unrelated upload chunk",
                ));
            }
        }
        for chunk in &chunks {
            let id = chunk.receipt.hash.to_hex();
            if missing.contains(id.as_str()) {
                transport.put_chunk(&id, &chunk.bytes).await?;
            }
        }
        if !missing.is_empty()
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
