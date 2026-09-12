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
const MAX_RECORD_BYTES: usize = 16384;

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
    pub(super) created_refs: Vec<CreatedPartialRef>,
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
        if self.created_refs.len() > 32
            || self
                .created_refs
                .windows(2)
                .any(|v| v[0].branch_id >= v[1].branch_id)
        {
            return Err(invalid("created refs are not a bounded sorted set"));
        }
        for child in &self.created_refs {
            uuid(&child.branch_id)?;
            uuid(&child.head_commit_id)?;
            uuid(&child.checkpoint_commit_id)?;
            if child.branch_id == crate::GLOBAL_BRANCH_ID {
                return Err(invalid("created ref cannot target GLOBAL"));
            }
        }
        uuid(&self.attempt_id)?;
        self.expected.validate()?;
        self.target.validate()
    }
}
impl PartialBranchPushState {
    fn validate(&self) -> Result<(), LixError> {
        if self.version != 2 {
            return Err(invalid("unsupported partial upload state version"));
        }
        uuid(&self.epoch_id)?;
        uuid(&self.branch_id)?;
        self.confirmed.validate()?;
        if let Some(prepared) = &self.prepared {
            prepared.validate()?;
            if self.branch_id != crate::GLOBAL_BRANCH_ID && !prepared.created_refs.is_empty() {
                return Err(invalid("non-GLOBAL push record contains created refs"));
            }
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
                version: 2,
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
    let global_outbox_guards = ordinary_global_outbox_guards(read, state, branch_id).await?;
    let mut guards = super::partial_merge_state::ordinary_upload_merge_guards(read, state).await?;
    guards.extend(global_outbox_guards);
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
    if branch_id != crate::GLOBAL_BRANCH_ID && !upload.created_refs.is_empty() {
        return Err(invalid("created refs require GLOBAL upload"));
    }
    guards.extend(guard_created_ref_capture(read, upload).await?);
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
    let global_outbox_guards = ordinary_global_outbox_guards(read, state, branch_id).await?;
    let mut guards = super::partial_merge_state::ordinary_upload_merge_guards(read, state).await?;
    guards.extend(global_outbox_guards);
    let (mut record, previous, epoch_guard) =
        load_partial_push_state(read, state, branch_id).await?;
    if record.prepared.as_ref() != Some(accepted) {
        return Err(invalid("acknowledgment does not match prepared upload"));
    }
    if ref_accepted {
        guards.extend(stage_created_ref_ack(read, writes, state, accepted).await?);
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

/// Recover a lost ordinary ACK from authenticated native inclusion. This does
/// not resend a body or create a later LWW operation, and never advances local
/// controls over a genuinely newer local suffix.
pub(super) async fn stage_acknowledge_included_partial_upload(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    authority: &super::PartialReplicaDescriptor,
) -> Result<Option<Vec<StoragePrecondition>>, LixError> {
    let branch = &state.descriptor().selected_branch.branch_id;
    if authority.selected_branch.branch_id != *branch {
        return Err(invalid("included upload authority branch differs"));
    }
    let (merge, _, _) =
        super::partial_merge_state::load_partial_merge_state(read, state, branch).await?;
    if merge.is_some() {
        return Ok(None);
    }
    let (push, _, _) = load_partial_push_state(read, state, branch).await?;
    let Some(accepted) = push.prepared.as_ref() else {
        return Ok(None);
    };
    // Created refs need their own accepted ref proof; selected uploads do not
    // carry them. Head ancestry alone cannot certify a different branch ref.
    if !accepted.created_refs.is_empty() {
        return Ok(None);
    }
    let mut cache = std::collections::BTreeMap::new();
    for (local, remote) in [
        (
            &accepted.target.head,
            &authority.selected_branch.head.commit_id,
        ),
        (
            &accepted.target.checkpoint,
            &authority.selected_branch.checkpoint.commit_id,
        ),
    ] {
        let local = crate::changelog::CommitId::parse_lix(local, "included upload coordinate")?;
        let remote =
            crate::changelog::CommitId::parse_lix(remote, "authority inclusion coordinate")?;
        if local == remote {
            continue;
        }
        let ancestor = super::partial_merge_analysis::record(read, local, true).await?;
        if !super::partial_merge_analysis::bounded_ancestor(
            read, &ancestor, remote, &mut cache, 1024,
        )
        .await?
        {
            return Ok(None);
        }
    }
    stage_acknowledge_partial_upload(read, writes, state, branch, accepted, true)
        .await
        .map(Some)
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
    use crate::open_lix;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};

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
            created_refs: Vec::new(),
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

// Append within partial_push_state owner. Caller carries exact source receipt
// CAS and proves the requested target using its authenticated descriptor.
pub(super) async fn stage_admitted_branch_coordinate(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    previous: &PartialReplicaState,
    branch: &super::partial_replica::PartialReplicaBranch,
    observation: &crate::branch::BranchHeadControlObservation,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let branch_key = key(&branch.branch_id)?;
    let raw = PointReadPlan::new(PARTIAL_BRANCH_PUSH_SPACE, std::slice::from_ref(&branch_key))
        .materialize(read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    let raw = match raw {
        None => None,
        Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes),
        Some(_) => return Err(invalid("branch admission push coordinate omitted value")),
    };
    if let Some(bytes) = &raw {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("branch admission push record exceeds bound"));
        }
        let record: PartialBranchPushState = serde_json::from_slice(bytes)
            .map_err(|_| invalid("branch admission push record is malformed"))?;
        record.validate()?;
        if record.epoch_id != previous.epoch_id() || record.branch_id != branch.branch_id {
            return Err(invalid(
                "branch admission push record belongs to another owner",
            ));
        }
        let control = observation
            .control
            .as_ref()
            .ok_or_else(|| invalid("archived branch lost its local control"))?;
        if record.prepared.is_some()
            || control.head_commit_id != record.confirmed.head
            || control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
                .as_ref()
                != Some(&record.confirmed.checkpoint)
        {
            return Err(LixError::new(
                "LIX_PARTIAL_BRANCH_SWITCH_PENDING",
                "target branch has unconfirmed local work",
            ));
        }
    } else if observation.control.is_some() {
        // A locally created branch without a confirmed authority lane is not
        // an empty cache; it may own unpublished descriptor/ref/native data.
        return Err(LixError::new(
            "LIX_PARTIAL_BRANCH_SWITCH_PENDING",
            "target branch has no confirmed authority coordinate",
        ));
    }
    let mut guards =
        super::partial_merge_state::require_no_branch_merge(read, &branch.branch_id).await?;
    guards.push(stage_record(
        writes,
        &PartialBranchPushState {
            version: 2,
            epoch_id: previous.epoch_id().into(),
            branch_id: branch.branch_id.clone(),
            confirmed: PartialPushCoordinate {
                head: branch.head.commit_id.clone(),
                checkpoint: branch.checkpoint.commit_id.clone(),
            },
            prepared: None,
            bodies_acknowledged: false,
        },
        raw,
    )?);
    Ok(guards)
}

pub(super) async fn clean_branch_source_guards(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let mut guards = super::partial_merge_state::ordinary_upload_merge_guards(read, state).await?;
    let mut branches = std::collections::BTreeSet::new();
    for branch in [
        &state.descriptor().selected_branch,
        &state.descriptor().global_branch,
    ] {
        if !branches.insert(branch.branch_id.clone()) {
            continue;
        }
        let (push, raw, receipt_guard) =
            load_partial_push_state(read, state, &branch.branch_id).await?;
        let observation =
            crate::branch::observe_branch_control_coordinate(read, &branch.branch_id).await?;
        let control = observation
            .control
            .as_ref()
            .ok_or_else(|| invalid("admitted branch control disappeared"))?;
        if push.prepared.is_some()
            || control.head_commit_id != push.confirmed.head
            || control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
                .as_ref()
                != Some(&push.confirmed.checkpoint)
        {
            return Err(LixError::new(
                "LIX_PARTIAL_BRANCH_SWITCH_PENDING",
                "branch switch requires confirmed selected and global work; retry after sync",
            ));
        }
        guards.push(receipt_guard);
        guards.push(StoragePrecondition::KeyValueEquals {
            space: PARTIAL_BRANCH_PUSH_SPACE,
            key: key(&branch.branch_id)?,
            expected: raw,
        });
        guards.push(crate::branch::branch_head_control_precondition(
            &branch.branch_id,
            observation.raw_token,
        )?);
    }
    Ok(guards)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreatedPartialRef {
    pub(super) branch_id: String,
    pub(super) head_commit_id: String,
    pub(super) checkpoint_commit_id: String,
}
impl PreparedPartialUpload {
    pub(super) fn append_created_ref_updates(&self, request: &mut super::SyncPushRequest) {
        request
            .ref_updates
            .extend(
                self.created_refs
                    .iter()
                    .map(|child| super::protocol::SyncRefUpdate {
                        branch_id: child.branch_id.clone(),
                        expected_head_commit_id: None,
                        expected_checkpoint_commit_id: None,
                        head_commit_id: Some(child.head_commit_id.clone()),
                        checkpoint_commit_id: Some(child.checkpoint_commit_id.clone()),
                    }),
            );
    }
}
async fn guard_created_ref_capture(
    read: &(impl StorageAdapterRead + ?Sized),
    upload: &PreparedPartialUpload,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let mut guards = Vec::new();
    for child in &upload.created_refs {
        let observed =
            crate::branch::observe_branch_control_coordinate(read, &child.branch_id).await?;
        let control = observed
            .control
            .ok_or_else(|| invalid("created branch disappeared during capture"))?;
        if control.head_commit_id != child.head_commit_id
            || control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
                .as_deref()
                != Some(&child.checkpoint_commit_id)
        {
            return Err(invalid("created branch changed during capture"));
        }
        guards.push(crate::branch::branch_head_control_precondition(
            &child.branch_id,
            observed.raw_token,
        )?);
    }
    Ok(guards)
}
pub(super) async fn stage_created_ref_ack(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    accepted: &PreparedPartialUpload,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let mut guards = Vec::new();
    for child in &accepted.created_refs {
        let values = PointReadPlan::new(PARTIAL_BRANCH_PUSH_SPACE, &[key(&child.branch_id)?])
            .materialize(read, Default::default())
            .await?
            .value;
        let coordinate = PartialPushCoordinate {
            head: child.head_commit_id.clone(),
            checkpoint: child.checkpoint_commit_id.clone(),
        };
        match values.into_iter().next().flatten() {
            None => guards.push(stage_record(
                writes,
                &PartialBranchPushState {
                    version: 2,
                    epoch_id: state.epoch_id().into(),
                    branch_id: child.branch_id.clone(),
                    confirmed: coordinate,
                    prepared: None,
                    bodies_acknowledged: false,
                },
                None,
            )?),
            Some(StorageProjectedValue::FullValue(bytes)) => {
                if bytes.len() > MAX_RECORD_BYTES {
                    return Err(invalid("created child push record exceeds bound"));
                }
                let record: PartialBranchPushState = serde_json::from_slice(&bytes)
                    .map_err(|_| invalid("created child push state malformed"))?;
                record.validate()?;
                if record.epoch_id != state.epoch_id()
                    || record.branch_id != child.branch_id
                    || record.confirmed != coordinate
                {
                    return Err(invalid(
                        "created child ACK disagrees with confirmed coordinate",
                    ));
                }
                // Never overwrite a later prepared child upload or local control.
                guards.push(StoragePrecondition::KeyValueEquals {
                    space: PARTIAL_BRANCH_PUSH_SPACE,
                    key: key(&child.branch_id)?,
                    expected: bytes,
                });
            }
            _ => return Err(invalid("created child push point read incomplete")),
        }
    }
    Ok(guards)
}

#[cfg(test)]
mod created_ref_codec_tests {
    use super::*;
    #[test]
    fn durable_selected_push_cannot_smuggle_created_refs() {
        let id = |suffix: u8| format!("00000000-0000-4000-8000-{suffix:012x}");
        let wire = serde_json::json!({
            "version": 2, "epochId": id(1), "branchId": id(2),
            "confirmed": {"head": id(3), "checkpoint": id(4)},
            "prepared": {"attemptId": id(5), "expected": {"head": id(3), "checkpoint": id(4)},
                "target": {"head": id(6), "checkpoint": id(4)},
                "createdRefs": [{"branchId": id(7), "headCommitId": id(3), "checkpointCommitId": id(3)}]},
            "bodiesAcknowledged": false
        });
        let mut record: PartialBranchPushState = serde_json::from_value(wire).unwrap();
        assert!(record.validate().is_err());
        record.branch_id = crate::GLOBAL_BRANCH_ID.into();
        record.validate().unwrap();
        let mut request = crate::sync::SyncPushRequest {
            commits: Vec::new(),
            ref_updates: Vec::new(),
            inline_blobs: Vec::new(),
        };
        record
            .prepared
            .as_ref()
            .unwrap()
            .append_created_ref_updates(&mut request);
        assert_eq!(request.ref_updates.len(), 1);
        assert_eq!(request.ref_updates[0].branch_id, id(7));
        assert!(request.ref_updates[0].expected_head_commit_id.is_none());
        assert!(
            request.ref_updates[0]
                .expected_checkpoint_commit_id
                .is_none()
        );
    }
}

/// One-way owner-only upgrade. Old prepared attempts retain exactly their old
/// wire refs (none); never infer children after an ambiguous previous send.
pub(crate) async fn prepare_owned_partial_push_upgrade(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
) -> Result<Vec<StoragePrecondition>, LixError> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct OldUpload {
        attempt_id: String,
        expected: PartialPushCoordinate,
        target: PartialPushCoordinate,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct OldRecord {
        version: u32,
        epoch_id: String,
        branch_id: String,
        confirmed: PartialPushCoordinate,
        prepared: Option<OldUpload>,
        bodies_acknowledged: bool,
    }
    #[derive(Deserialize)]
    struct Version {
        version: u32,
    }
    let mut branches = state
        .archived_branch_ids()
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    branches.insert(state.descriptor().selected_branch.branch_id.clone());
    branches.insert(state.descriptor().global_branch.branch_id.clone());
    let mut guards = Vec::new();
    for branch in branches {
        let values = PointReadPlan::new(PARTIAL_BRANCH_PUSH_SPACE, &[key(&branch)?])
            .materialize(read, Default::default())
            .await?
            .value;
        let Some(StorageProjectedValue::FullValue(bytes)) = values.into_iter().next().flatten()
        else {
            return Err(invalid(
                "owned push upgrade lacks a named admitted coordinate",
            ));
        };
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("owned push upgrade exceeds bound"));
        }
        let version: Version = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("push upgrade version malformed"))?;
        if version.version == 2 {
            let record: PartialBranchPushState = serde_json::from_slice(&bytes)
                .map_err(|_| invalid("push upgrade record malformed"))?;
            record.validate()?;
            if record.epoch_id != state.epoch_id() || record.branch_id != branch {
                return Err(invalid("push upgrade owner mismatch"));
            }
            continue;
        }
        let old: OldRecord = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("old push upgrade record malformed"))?;
        if old.version != 1 || old.epoch_id != state.epoch_id() || old.branch_id != branch {
            return Err(invalid("old push upgrade owner/version mismatch"));
        }
        let record = PartialBranchPushState {
            version: 2,
            epoch_id: old.epoch_id,
            branch_id: old.branch_id,
            confirmed: old.confirmed,
            prepared: old.prepared.map(|old| PreparedPartialUpload {
                attempt_id: old.attempt_id,
                expected: old.expected,
                target: old.target,
                created_refs: Vec::new(),
            }),
            bodies_acknowledged: old.bodies_acknowledged,
        };
        guards.push(stage_record(writes, &record, Some(bytes))?);
    }
    Ok(guards)
}

/// Ordinary uploads carry the exact GLOBAL outbox observation through capture
/// and ACK. A received HTTP response alone never opens the selected lane.
#[must_use = "commit every outbox guard with upload bookkeeping"]
pub(super) async fn ordinary_global_outbox_guards(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
) -> Result<Vec<StoragePrecondition>, LixError> {
    if branch_id != crate::GLOBAL_BRANCH_ID
        && branch_id != state.descriptor().selected_branch.branch_id
    {
        return Err(invalid("ordinary upload lane is outside admitted branches"));
    }
    let (outbox, _, guards) =
        super::partial_global_merge_state::load_partial_global_merge_state(read, state).await?;
    if let Some(outbox) = outbox {
        let may_upload = if branch_id == crate::GLOBAL_BRANCH_ID {
            outbox.upload_settled
        } else {
            // load validated the immutable receipt against its exact request.
            outbox.receipt.is_some()
        };
        if !may_upload {
            return Err(LixError::new(
                "LIX_PARTIAL_REPLICA_MERGE_PENDING",
                "ordinary upload awaits durable GLOBAL merge settlement",
            ));
        }
    }
    Ok(guards)
}

/// Called only with the native inclusion proof. This stages bookkeeping beside
/// candidate controls/root generations; it never publishes those pieces alone.
#[must_use = "publish all returned guards with the full candidate write set"]
pub(super) async fn stage_settle_partial_global_merge_confirmation(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    verified: super::partial_global_merge_settlement::VerifiedPartialGlobalMergeSettlement,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (current, outbox_raw, mut guards) =
        super::partial_global_merge_state::load_partial_global_merge_state(read, state).await?;
    let mut outbox = current.ok_or_else(|| invalid("GLOBAL settlement outbox disappeared"))?;
    if &outbox != verified.record() || outbox.upload_settled || outbox.receipt.is_none() {
        return Err(invalid(
            "GLOBAL settlement differs from proved unadopted receipt",
        ));
    }
    let (mut push, push_raw, epoch_guard) =
        load_partial_push_state(read, state, crate::GLOBAL_BRANCH_ID).await?;
    if push.confirmed != outbox.original_upload.expected
        || push.prepared.as_ref() != Some(&outbox.original_upload)
    {
        return Err(invalid(
            "GLOBAL settlement raced the frozen ordinary upload",
        ));
    }
    // Child ACKs confirm only the frozen refs, preserving any newer child head.
    guards.extend(stage_created_ref_ack(read, writes, state, &outbox.original_upload).await?);
    push.confirmed = verified.target().clone();
    push.prepared = None;
    push.bodies_acknowledged = false;
    guards.push(stage_record(writes, &push, Some(push_raw))?);
    outbox.upload_settled = true;
    guards.push(super::partial_global_merge_state::stage_global_record(
        writes, &outbox, outbox_raw,
    )?);
    // Keep the outbox and authority pin alive until exact cleanup completes.
    guards.extend(verified.into_guards());
    guards.push(epoch_guard);
    Ok(guards)
}
