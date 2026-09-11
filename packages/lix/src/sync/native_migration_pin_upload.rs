//! Explicit migration native transport. The migration owner persists a prepared
//! wave before calling this helper, then records acceptance after its response.
//! Native source storage remains read-only under the epoch migration claim.
use super::protocol::SyncRefUpdate;
use super::{PartialMergeRequest, SyncPushRequest, SyncPushResponse, SyncTransport};
use crate::storage_adapter::{Storage, StorageAdapter, StorageAdapterRead};
use crate::{LixError, changelog::CommitId};
use std::collections::BTreeSet;
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_CONVERSION_UNRESOLVED", message)
}
fn id(s: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(s, "migration pinned upload")
}

pub(super) async fn native_migration_pin_wave(
    read: &(impl StorageAdapterRead + ?Sized),
    request: &PartialMergeRequest,
    source_branch: &str,
    accepted: &str,
    prepared_target: Option<&str>,
    pin_exists: bool,
) -> Result<SyncPushRequest, LixError> {
    request.validate()?;
    id(source_branch)?;
    if source_branch == request.branch_id || source_branch == crate::GLOBAL_BRANCH_ID {
        return Err(invalid("migration source pin is not isolated"));
    }
    let target = match prepared_target {
        Some(v) => id(v)?,
        None => {
            super::partial_upload::wave_target(
                read,
                id(&request.captured_local_head_commit_id)?,
                id(accepted)?,
                32,
            )
            .await?
        }
    };
    let mut cursor = target;
    let mut reverse = Vec::new();
    let mut seen = BTreeSet::new();
    while cursor != id(accepted)? {
        if reverse.len() == 32 || !seen.insert(cursor) {
            return Err(invalid("migration prepared wave exceeds its linear bound"));
        }
        let commit = super::commit::load_sync_commit(read, cursor)
            .await?
            .ok_or_else(|| invalid("migration source commit absent"))?;
        if commit.is_checkpoint
            || commit.parent_commit_ids.len() != 1
            || commit.global_scope
            || commit.state_alias.is_some()
            || commit.selected_source_commit_id.is_some()
        {
            return Err(invalid(
                "migration currently requires ordinary unchanged-global/checkpoint commits",
            ));
        }
        cursor = id(&commit.parent_commit_ids[0])?;
        reverse.push(commit);
    }
    reverse.reverse();
    let result = SyncPushRequest {
        commits: reverse,
        ref_updates: vec![SyncRefUpdate {
            branch_id: source_branch.into(),
            expected_head_commit_id: pin_exists.then(|| accepted.into()),
            expected_checkpoint_commit_id: pin_exists.then(|| request.checkpoint_commit_id.clone()),
            head_commit_id: Some(target.to_string()),
            checkpoint_commit_id: Some(request.checkpoint_commit_id.clone()),
        }],
        inline_blobs: vec![],
    };
    if serde_json::to_vec(&result)
        .map_err(|_| invalid("migration wave encoding failed"))?
        .len()
        > 64 * 1024 * 1024
    {
        return Err(invalid("migration wave exceeds byte bound"));
    }
    Ok(result)
}

use crate::binary_cas::{BlobId, load_canonical_blob_chunks, load_metadata_many};
// Caller owns frozen full-source migration claim, not a partial receipt.
pub(super) async fn push_native_migration_with_blobs<S, T>(
    storage: &StorageAdapter<S>,
    expected_account: &str,
    transport: &T,
    request: &SyncPushRequest,
) -> Result<SyncPushResponse, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    prepare_native_migration_blobs(storage, expected_account, transport, request).await?;
    transport.push(request).await
}

pub(super) async fn prepare_native_migration_blobs<S, T>(
    storage: &StorageAdapter<S>,
    expected_account: &str,
    transport: &T,
    request: &SyncPushRequest,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    T: SyncTransport,
{
    // Existing canonical flattening can allocate the complete requested file.
    // Bound one file independently of native commit/request output budgets.
    const MAX_PREPARED_BLOB: u64 = 64 * 1024 * 1024;
    if transport.active_account_id() != expected_account {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "blob upload authority account differs",
        ));
    }
    for blob in super::repository::sync_commit_blob_ids(&request.commits)? {
        let id = BlobId::from_hex(&blob)?;
        let read = storage.begin_read(Default::default()).await?;
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
        let mut missing = BTreeSet::new();
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
