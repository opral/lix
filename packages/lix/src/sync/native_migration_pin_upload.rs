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
    let ref_author = reverse
        .last()
        .map(|commit| commit.account_id.clone())
        .ok_or_else(|| invalid("migration pin has no authored commit"))?;
    let result = SyncPushRequest {
        commits: reverse,
        ref_updates: vec![SyncRefUpdate {
            branch_id: source_branch.into(),
            ref_change_id: Some(super::partial_push_state::deterministic_ref_change_id(
                source_branch,
                &target.to_string(),
                &request.checkpoint_commit_id,
                &ref_author,
            )?),
            expected_ref_change_id: None,
            author_id: Some(ref_author),
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
    if transport.active_account_id() != expected_account {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "blob upload authority account differs",
        ));
    }
    let mut inline_group = super::transfer::TransferBatch::new();
    let mut canonical_group = super::transfer::TransferBatch::new();
    let begin_read = || async {
        crate::migration::MigrationPlanningRead::new(storage)
            .await
            .map_err(LixError::from)
    };
    let ids = super::repository::sync_commit_blob_ids(&request.commits)?
        .into_iter()
        .map(|blob| BlobId::from_hex(&blob))
        .collect::<Result<Vec<_>, _>>()?;
    for ids in ids.chunks(super::transfer::CONTENT_GROUP_ITEMS) {
        let read = begin_read().await?;
        let metadata = load_metadata_many(&read, ids).await?.into_vec();
        drop(read);
        for (id, metadata) in ids.iter().zip(metadata) {
            let id = *id;
            let metadata = metadata
                .ok_or_else(|| LixError::unknown("captured local blob metadata is missing"))?;
            let read = begin_read().await?;
            if metadata.size_bytes > super::blob::MAX_INLINE_SYNC_BLOB_BYTES as u64 {
                let canonical =
                    crate::binary_cas::load_streaming_canonical_manifest(&read, &metadata).await?;
                drop(read);
                super::transfer::pack_canonical_plan(
                    transport,
                    &mut canonical_group,
                    super::transfer::CanonicalUploadPlan {
                        metadata,
                        canonical,
                    },
                    &begin_read,
                )
                .await?;
                continue;
            }
            let chunks = load_canonical_blob_chunks(&read, id)
                .await?
                .ok_or_else(|| LixError::unknown("captured local blob content is missing"))?;
            let manifest = super::blob::encode_manifest(id, &chunks)?;
            drop(read);
            let encoded = serde_json::to_vec(&manifest)
                .map_err(|error| LixError::unknown(error.to_string()))?
                .len();
            let decoded = manifest.size_bytes as usize;
            if let Some(manifest) = inline_group.push(manifest, encoded, decoded)? {
                super::transfer::register_inline_group(transport, &inline_group).await?;
                inline_group = super::transfer::TransferBatch::new();
                if inline_group.push(manifest, encoded, decoded)?.is_some() {
                    return Err(LixError::unknown(
                        "single migration content member did not fit",
                    ));
                }
            }
        }
    }
    super::transfer::upload_canonical_page(transport, &canonical_group.items, &begin_read).await?;
    super::transfer::register_inline_group(transport, &inline_group).await?;
    Ok(())
}
