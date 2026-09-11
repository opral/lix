//! Bounded per-branch upload bookkeeping for a partial replica.
//! Own acknowledgments move only confirmed remote coordinates. Native commits,
//! local controls, root markers, and working-set ownership remain untouched.

use super::partial_state::{
    PARTIAL_REPLICA_STATE_SPACE, PartialReplicaState, load_partial_replica_state,
    partial_replica_state_key,
};
use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) const PARTIAL_BRANCH_PUSH_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_001a),
    "sync.partial_branch_push.v1",
    ValueSemantics::Mutable,
);
const MAX_RECORD_BYTES: usize = 2048;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PartialPushCoordinate {
    pub(super) head: String,
    pub(super) checkpoint: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PreparedPartialUpload {
    pub(super) attempt_id: String,
    pub(super) expected: PartialPushCoordinate,
    pub(super) target: PartialPushCoordinate,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PartialBranchPushState {
    version: u32,
    epoch_id: String,
    branch_id: String,
    pub(super) confirmed: PartialPushCoordinate,
    pub(super) prepared: Option<PreparedPartialUpload>,
    pub(super) bodies_acknowledged: bool,
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_PUSH_STATE_INVALID", message)
}
fn uuid(value: &str) -> Result<[u8; 16], LixError> {
    crate::storage_codec::id_string::uuid_bytes_from_canonical(value)
        .ok_or_else(|| invalid("partial upload ID must be a canonical UUID"))
}
fn key(branch: &str) -> Result<StorageKey, LixError> {
    Ok(StorageKey(Bytes::copy_from_slice(&uuid(branch)?)))
}
impl PartialPushCoordinate {
    fn validate(&self) -> Result<(), LixError> {
        uuid(&self.head)?;
        uuid(&self.checkpoint)?;
        Ok(())
    }
}
impl PreparedPartialUpload {
    fn validate(&self) -> Result<(), LixError> {
        uuid(&self.attempt_id)?;
        self.expected.validate()?;
        self.target.validate()
    }
}
impl PartialBranchPushState {
    fn validate(&self) -> Result<(), LixError> {
        if self.version != 1 {
            return Err(invalid("unsupported partial upload state version"));
        }
        uuid(&self.epoch_id)?;
        uuid(&self.branch_id)?;
        self.confirmed.validate()?;
        if let Some(prepared) = &self.prepared {
            prepared.validate()?;
            if prepared.expected != self.confirmed {
                return Err(invalid(
                    "prepared upload does not continue confirmed coordinate",
                ));
            }
        } else if self.bodies_acknowledged {
            return Err(invalid("body acknowledgment has no prepared upload"));
        }
        Ok(())
    }
}
fn stage_record(
    writes: &mut StorageWriteSet,
    record: &PartialBranchPushState,
    previous: Option<Bytes>,
) -> Result<StoragePrecondition, LixError> {
    record.validate()?;
    let bytes =
        serde_json::to_vec(record).map_err(|_| invalid("partial upload encoding failed"))?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid("partial upload record exceeds bound"));
    }
    let key = key(&record.branch_id)?;
    let guard = match previous {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: PARTIAL_BRANCH_PUSH_SPACE,
            key: key.clone(),
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: PARTIAL_BRANCH_PUSH_SPACE,
            key: key.clone(),
        },
    };
    writes.put(
        PARTIAL_BRANCH_PUSH_SPACE,
        key,
        StorageValue {
            bytes: bytes.into(),
        },
    );
    Ok(guard)
}

/// Initialize only the selected/global coordinates in the atomic bootstrap.
/// The bootstrap caller also publishes/fences the enclosing epoch receipt.
#[must_use = "commit all returned record guards with bootstrap writes"]
pub(super) fn stage_initial_partial_push_states(
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let mut guards = Vec::with_capacity(2);
    let descriptor = state.descriptor();
    for (index, branch) in [&descriptor.selected_branch, &descriptor.global_branch]
        .into_iter()
        .enumerate()
    {
        if index == 1 && branch.branch_id == descriptor.selected_branch.branch_id {
            continue;
        }
        guards.push(stage_record(
            writes,
            &PartialBranchPushState {
                version: 1,
                epoch_id: state.epoch_id().into(),
                branch_id: branch.branch_id.clone(),
                confirmed: PartialPushCoordinate {
                    head: branch.head.commit_id.clone(),
                    checkpoint: branch.checkpoint.commit_id.clone(),
                },
                prepared: None,
                bodies_acknowledged: false,
            },
            None,
        )?);
    }
    Ok(guards)
}

/// Two point reads bind the per-branch state to the exact admitted epoch.
pub(super) async fn load_partial_push_state(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
) -> Result<(PartialBranchPushState, Bytes, StoragePrecondition), LixError> {
    if branch_id != state.descriptor().selected_branch.branch_id
        && branch_id != state.descriptor().global_branch.branch_id
    {
        return Err(invalid("branch is outside admitted partial selection"));
    }
    let (actual, receipt) = load_partial_replica_state(read)
        .await?
        .ok_or_else(|| invalid("partial upload has no epoch receipt"))?;
    if &actual != state {
        return Err(invalid("partial upload epoch admission changed"));
    }
    let values = PointReadPlan::new(PARTIAL_BRANCH_PUSH_SPACE, &[key(branch_id)?])
        .materialize(read, Default::default())
        .await?;
    let Some(StorageProjectedValue::FullValue(bytes)) = values.value.into_iter().next().flatten()
    else {
        return Err(invalid("partial branch upload state is missing"));
    };
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid("partial upload record exceeds bound"));
    }
    let record: PartialBranchPushState =
        serde_json::from_slice(&bytes).map_err(|_| invalid("malformed partial upload state"))?;
    record.validate()?;
    if record.epoch_id != state.epoch_id() || record.branch_id != branch_id {
        return Err(invalid("partial upload record identity mismatch"));
    }
    Ok((
        record,
        bytes,
        StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: partial_replica_state_key(),
            expected: receipt,
        },
    ))
}

/// Persist the exact attempt before network I/O. The caller must separately
/// prove that target names a prepared local publication; this is not a ref API.
#[must_use = "commit prepared upload and all guards durably before sending"]
pub(super) async fn stage_prepare_partial_upload(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    branch_id: &str,
    upload: &PreparedPartialUpload,
) -> Result<Vec<StoragePrecondition>, LixError> {
    upload.validate()?;
    let mut guards = super::partial_merge_state::ordinary_upload_merge_guards(read, state).await?;
    let (mut record, previous, epoch_guard) =
        load_partial_push_state(read, state, branch_id).await?;
    if upload.expected != record.confirmed {
        return Err(invalid("prepared upload expected coordinate is stale"));
    }
    if record
        .prepared
        .as_ref()
        .is_some_and(|current| current != upload)
    {
        return Err(invalid("another upload attempt is still prepared"));
    }
    guards.push(epoch_guard);
    if record.prepared.as_ref() == Some(upload) {
        // The exact immutable request is already durable. Retry validates its
        // journal and admission but must not rewrite it or rotate revisions.
        guards.push(StoragePrecondition::KeyValueEquals {
            space: PARTIAL_BRANCH_PUSH_SPACE,
            key: key(branch_id)?,
            expected: previous,
        });
        return Ok(guards);
    }
    record.prepared = Some(upload.clone());
    record.bodies_acknowledged = false;
    guards.push(stage_record(writes, &record, Some(previous))?);
    Ok(guards)
}

/// Body acceptance never advances a ref. Ref acceptance must echo the entire
/// durably prepared tuple, not merely a head ID or an attempt ID.
#[must_use = "commit acknowledgment with every epoch and branch record guard"]
pub(super) async fn stage_acknowledge_partial_upload(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    branch_id: &str,
    accepted: &PreparedPartialUpload,
    ref_accepted: bool,
) -> Result<Vec<StoragePrecondition>, LixError> {
    accepted.validate()?;
    let mut guards = super::partial_merge_state::ordinary_upload_merge_guards(read, state).await?;
    let (mut record, previous, epoch_guard) =
        load_partial_push_state(read, state, branch_id).await?;
    if record.prepared.as_ref() != Some(accepted) {
        return Err(invalid("acknowledgment does not match prepared upload"));
    }
    if ref_accepted {
        record.confirmed = accepted.target.clone();
        record.prepared = None;
        record.bodies_acknowledged = false;
    } else {
        record.bodies_acknowledged = true;
    }
    guards.push(epoch_guard);
    guards.push(stage_record(writes, &record, Some(previous))?);
    Ok(guards)
}

/// Publication of prepared remote state may advance a clean confirmed ref.
/// Unlike an own ACK, this accompanies a new serving basis. The caller must
/// also CAS the observed local control and prepare all retained read scopes.
#[must_use = "publish confirmation with the prepared serving basis and every guard"]
pub(super) async fn stage_remote_partial_confirmation(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    branch_id: &str,
    expected: &PartialPushCoordinate,
    target: PartialPushCoordinate,
) -> Result<Vec<StoragePrecondition>, LixError> {
    target.validate()?;
    let (mut record, previous, epoch_guard) =
        load_partial_push_state(read, state, branch_id).await?;
    if record.prepared.is_some() || &record.confirmed != expected {
        return Err(invalid("remote publication raced local upload state"));
    }
    record.confirmed = target;
    Ok(vec![
        epoch_guard,
        stage_record(writes, &record, Some(previous))?,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
    use crate::open_lix;

    async fn commit<S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static>(
        adapter: &StorageAdapter<S>,
        writes: StorageWriteSet,
        guards: Vec<StoragePrecondition>,
    ) -> Result<(), LixError> {
        adapter
            .commit_partial_replica_write_set(
                super::super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions: guards,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn exact_own_ack_preserves_newer_local_control_and_rejects_stale_ack() {
        let lix = open_lix().await.unwrap();
        let descriptor = lix.partial_replica_descriptor(None).await.unwrap();
        let branch_id = descriptor.selected_branch.branch_id.clone();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('uploaded-local', 'accepted')",
            &[],
        )
        .await
        .unwrap();
        let uploaded = lix
            .partial_replica_descriptor(None)
            .await
            .unwrap()
            .selected_branch
            .head
            .commit_id;
        // A newer native local commit already exists when the older upload is
        // acknowledged. This is a bookkeeping fixture, not a partial SQL proof.
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('newer-local', 'retained')",
            &[],
        )
        .await
        .unwrap();
        let adapter = lix.storage_adapter();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let before = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch_id)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            before.head_commit_id.to_string(),
            descriptor.selected_branch.head.commit_id
        );
        drop(read);
        let state = PartialReplicaState::new(
            format!("https://example.test/lix/{}", lix.lix_id()),
            lix.active_account_id().into(),
            "00000000-0000-7000-8000-000000000599".into(),
            descriptor,
        )
        .unwrap();
        let mut writes = adapter.new_write_set();
        let mut guards = stage_initial_partial_push_states(&mut writes, &state).unwrap();
        guards.push(
            super::super::partial_state::stage_partial_replica_state(&mut writes, &state, None)
                .unwrap(),
        );
        commit(&adapter, writes, guards).await.unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let initial = load_partial_push_state(&read, &state, &branch_id)
            .await
            .unwrap()
            .0;
        let accepted = PreparedPartialUpload {
            attempt_id: "00000000-0000-7000-8000-000000000699".into(),
            expected: initial.confirmed.clone(),
            target: PartialPushCoordinate {
                head: uploaded,
                checkpoint: initial.confirmed.checkpoint.clone(),
            },
        };
        let mut writes = adapter.new_write_set();
        let guards =
            stage_prepare_partial_upload(&read, &mut writes, &state, &branch_id, &accepted)
                .await
                .unwrap();
        drop(read);
        commit(&adapter, writes, guards).await.unwrap();
        // Simulate a lost network reply with a newer local head already
        // present. Every retry must send the original durable target and must
        // perform no storage commit (even a same-value commit rotates revision).
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let revision = crate::storage_adapter::load_repository_mutation_revision(&read)
            .await
            .unwrap();
        let mut retry_writes = adapter.new_write_set();
        stage_prepare_partial_upload(&read, &mut retry_writes, &state, &branch_id, &accepted)
            .await
            .unwrap();
        assert!(retry_writes.is_empty());
        drop(read);
        let sent = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        for _ in 0..3 {
            let sent = sent.clone();
            let error = super::super::partial_upload_cycle::upload_partial_once(
                &adapter,
                &state,
                &branch_id,
                uuid::Uuid::now_v7().to_string(),
                64,
                1024 * 1024,
                move |request| async move {
                    sent.lock()
                        .unwrap()
                        .push(serde_json::to_value(request).unwrap());
                    Err(LixError::new("TEST_LOST_REPLY", "simulated lost reply"))
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, "TEST_LOST_REPLY");
        }
        {
            let sent = sent.lock().unwrap();
            assert_eq!(sent.len(), 3);
            assert!(sent.windows(2).all(|pair| pair[0] == pair[1]));
        }
        let read = adapter.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            crate::storage_adapter::load_repository_mutation_revision(&read)
                .await
                .unwrap(),
            revision,
            "durable retries must perform zero commits"
        );
        assert_eq!(
            load_partial_push_state(&read, &state, &branch_id)
                .await
                .unwrap()
                .0
                .prepared,
            Some(accepted.clone())
        );
        assert_eq!(
            crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch_id)
                .await
                .unwrap()
                .unwrap(),
            before
        );
        let mut wrong = accepted.clone();
        wrong.target.checkpoint = wrong.target.head.clone();
        let mut rejected = adapter.new_write_set();
        assert!(
            stage_acknowledge_partial_upload(
                &read,
                &mut rejected,
                &state,
                &branch_id,
                &wrong,
                true
            )
            .await
            .is_err()
        );
        assert!(
            rejected
                .staged_value(PARTIAL_BRANCH_PUSH_SPACE, &key(&branch_id).unwrap().0)
                .is_none()
        );
        let mut body = adapter.new_write_set();
        let body_guards = stage_acknowledge_partial_upload(
            &read, &mut body, &state, &branch_id, &accepted, false,
        )
        .await
        .unwrap();
        drop(read);
        commit(&adapter, body, body_guards).await.unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let body_state = load_partial_push_state(&read, &state, &branch_id)
            .await
            .unwrap()
            .0;
        assert_eq!(body_state.confirmed, initial.confirmed);
        assert!(body_state.bodies_acknowledged);
        let mut ack = adapter.new_write_set();
        let ack_guards =
            stage_acknowledge_partial_upload(&read, &mut ack, &state, &branch_id, &accepted, true)
                .await
                .unwrap();
        let mut racing = adapter.new_write_set();
        let racing_guards = stage_acknowledge_partial_upload(
            &read,
            &mut racing,
            &state,
            &branch_id,
            &accepted,
            true,
        )
        .await
        .unwrap();
        drop(read);
        commit(&adapter, ack, ack_guards).await.unwrap();
        assert!(commit(&adapter, racing, racing_guards).await.is_err());
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let final_state = load_partial_push_state(&read, &state, &branch_id)
            .await
            .unwrap()
            .0;
        assert_eq!(final_state.confirmed, accepted.target);
        assert!(final_state.prepared.is_none());
        assert_eq!(
            crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load(&branch_id)
                .await
                .unwrap()
                .unwrap(),
            before
        );
        let mut stale = adapter.new_write_set();
        assert!(
            stage_acknowledge_partial_upload(
                &read, &mut stale, &state, &branch_id, &accepted, true
            )
            .await
            .is_err()
        );
        let other_epoch = PartialReplicaState::new(
            state.remote_id().into(),
            state.active_account_id().into(),
            "00000000-0000-7000-8000-000000000899".into(),
            state.descriptor().clone(),
        )
        .unwrap();
        assert!(
            load_partial_push_state(&read, &other_epoch, &branch_id)
                .await
                .is_err()
        );
    }
}

/// Only a native verified captured merge may settle a pending ordinary tuple.
/// The proof carries the merge record and exact local control guards.
#[must_use = "publish settlement with the prepared candidate and every returned guard"]
pub(super) async fn stage_settle_partial_merge_confirmation(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    verified: super::partial_merge_settlement::VerifiedPartialMergeSettlement,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let merge = verified.record();
    let branch = &merge.request.branch_id;
    let (mut push, raw, epoch_guard) = load_partial_push_state(read, state, branch).await?;
    if push.confirmed != merge.original_confirmed || push.prepared != merge.original_upload {
        return Err(invalid(
            "merge settlement raced original upload bookkeeping",
        ));
    }
    push.confirmed = verified.target().clone();
    push.prepared = None;
    push.bodies_acknowledged = false;
    let push_guard = stage_record(writes, &push, Some(raw))?;
    writes.delete(
        super::partial_merge_state::PARTIAL_BRANCH_MERGE_SPACE,
        key(branch)?,
    );
    let mut guards = verified.into_guards();
    guards.push(epoch_guard);
    guards.push(push_guard);
    Ok(guards)
}
