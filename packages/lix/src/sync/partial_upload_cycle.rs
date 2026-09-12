//! Durable capture/send/acknowledge cycle. No local read or collaboration lock
//! survives across network I/O; an own ACK only moves upload bookkeeping.
use super::partial_push_state::{stage_acknowledge_partial_upload, stage_prepare_partial_upload};
use super::partial_state::PartialReplicaState;
use super::partial_upload::prepare_partial_ordinary_upload;
use super::{SyncPushRequest, SyncPushResponse};
use crate::LixError;
use crate::storage_adapter::{Storage, StorageAdapter, StorageWriteOptions};

/// The caller supplies an authenticated connection bound to `state`. A failed
/// or cancelled send leaves the exact attempt durable for the next cycle.
/// The returned cursor is deliberately discarded: successful upload does not
/// prove that unrelated authority updates have been applied locally.
pub(super) async fn upload_partial_once<S, Sender, Sent>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
    send: Sender,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    Sender: FnOnce(SyncPushRequest) -> Sent,
    Sent: Future<Output = Result<SyncPushResponse, LixError>>,
{
    let read = storage.begin_read(Default::default()).await?;
    let (push, _, _) =
        super::partial_push_state::load_partial_push_state(&read, state, branch_id).await?;
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load(branch_id)
        .await?
        .ok_or_else(|| LixError::unknown("partial upload branch disappeared"))?;
    let target_checkpoint = push
        .prepared
        .as_ref()
        .map(|upload| upload.target.checkpoint.clone())
        .or_else(|| {
            control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
        })
        .ok_or_else(|| LixError::unknown("partial upload branch checkpoint disappeared"))?;
    let prepared = if target_checkpoint != push.confirmed.checkpoint {
        let checkpoint = super::partial_checkpoint_upload::prepare_partial_checkpoint_upload(
            &read,
            state,
            branch_id,
            attempt_id.clone(),
            max_commits,
            max_wire_bytes,
        )
        .await;
        match checkpoint {
            Err(error)
                if error.code == "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED" && push.prepared.is_none() =>
            {
                super::partial_checkpoint_upload::prepare_partial_checkpoint_page(
                    &read,
                    state,
                    branch_id,
                    attempt_id,
                    max_commits,
                    max_wire_bytes,
                )
                .await?
            }
            result => result?,
        }
    } else {
        prepare_partial_ordinary_upload(
            &read,
            state,
            branch_id,
            attempt_id,
            max_commits,
            max_wire_bytes,
        )
        .await?
    };
    let Some(prepared) = prepared else {
        return Ok(false);
    };
    let mut writes = storage.new_write_set();
    let mut guards =
        stage_prepare_partial_upload(&read, &mut writes, state, branch_id, &prepared.upload)
            .await?;
    guards.extend(prepared.control_guard);
    drop(read);
    // A resumed attempt was proven durable by the coherent read above. An
    // empty rewrite would only contend with foreground writes and invalidate
    // local caches. No read handle survives across the network call.
    if !writes.is_empty() {
        storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;
    }
    send(prepared.request).await?;
    let read = storage.begin_read(Default::default()).await?;
    let mut writes = storage.new_write_set();
    let guards = stage_acknowledge_partial_upload(
        &read,
        &mut writes,
        state,
        branch_id,
        &prepared.upload,
        true,
    )
    .await?;
    drop(read);
    storage
        .commit_partial_replica_write_set(
            super::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions: guards,
                await_durable: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(true)
}
