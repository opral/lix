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
    transport.push(request).await
}
