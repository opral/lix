//! Durable merge attempts are separate from ordinary upload confirmation.
//! Receiving authority M=[R,L] does not move a newer local L2 to M's basis.
use super::partial_merge_protocol::{PartialMergeReceipt, PartialMergeRequest};
use super::partial_push_state::{
    PARTIAL_BRANCH_PUSH_SPACE, PartialPushCoordinate, PreparedPartialUpload,
    load_partial_push_state,
};
use super::partial_state::{
    PARTIAL_REPLICA_STATE_SPACE, PartialReplicaState, load_partial_replica_state,
    partial_replica_state_key,
};
use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageWriteSet, ValueSemantics,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) const PARTIAL_BRANCH_MERGE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_001c),
    "sync.partial_branch_merge.v1",
    ValueSemantics::Mutable,
);
mod restart;
pub(super) use restart::{
    PartialMergeRestart, stage_capture_restarted_partial_merge,
    stage_prepare_partial_merge_restart, stage_record_partial_merge_restart_outcome,
};
const MAX_RECORD_BYTES: usize = 4096;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PartialBranchMergeState {
    version: u32,
    epoch_id: String,
    pub request: PartialMergeRequest,
    pub original_confirmed: PartialPushCoordinate,
    pub previous_receipt: Option<PartialMergeReceipt>,
    pub accepted_body_tip: String,
    pub prepared_body_wave: Option<PreparedMergeBodyWave>,
    pub original_upload: Option<PreparedPartialUpload>,
    pub authority_receipt: Option<PartialMergeReceipt>,
    pub restart: Option<PartialMergeRestart>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PreparedMergeBodyWave {
    pub previous: String,
    pub target: String,
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_MERGE_STATE_INVALID", message)
}
fn conflict(message: &str) -> LixError {
    LixError::new(LixError::CODE_TRANSACTION_CONFLICT, message)
}
fn key(branch: &str) -> Result<StorageKey, LixError> {
    let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(branch)
        .ok_or_else(|| invalid("merge branch is not a canonical UUID"))?;
    Ok(StorageKey(Bytes::copy_from_slice(&bytes)))
}
impl PartialBranchMergeState {
    fn validate(&self, state: &PartialReplicaState, branch: &str) -> Result<(), LixError> {
        self.request.validate()?;
        if self.version != 5
            || self.epoch_id != state.epoch_id()
            || self.request.branch_id != branch
            || branch != state.descriptor().selected_branch.branch_id
        {
            return Err(invalid(
                "merge attempt does not belong to the admitted selected branch",
            ));
        }
        for coordinate in [
            &self.original_confirmed.head,
            &self.original_confirmed.checkpoint,
            &self.accepted_body_tip,
        ] {
            if crate::storage_codec::id_string::uuid_bytes_from_canonical(coordinate).is_none() {
                return Err(invalid("merge frontier contains a noncanonical coordinate"));
            }
        }
        if self.previous_receipt.is_none()
            && self.original_confirmed.checkpoint != self.request.checkpoint_commit_id
        {
            return Err(invalid("merge frontier changed its original checkpoint"));
        }
        if let Some(prior) = &self.previous_receipt {
            prior.validate_for(&prior.request)?;
            if self.request.base_commit_id != prior.request.captured_local_head_commit_id
                || self.request.branch_id != prior.request.branch_id
                || self.request.checkpoint_commit_id
                    != prior.request.captured_local_checkpoint_commit_id
                || self.request.global_head_commit_id != prior.request.global_head_commit_id
                || self.request.global_checkpoint_commit_id
                    != prior.request.global_checkpoint_commit_id
                || self.request.attempt_id == prior.request.attempt_id
            {
                return Err(invalid(
                    "rolled merge frontier lost its previous native outcome",
                ));
            }
        } else if self.request.base_commit_id != self.original_confirmed.head {
            return Err(invalid(
                "initial merge base differs from original confirmation",
            ));
        }
        if let Some(wave) = &self.prepared_body_wave {
            if wave.previous != self.accepted_body_tip
                || wave.previous == wave.target
                || crate::storage_codec::id_string::uuid_bytes_from_canonical(&wave.target)
                    .is_none()
            {
                return Err(invalid(
                    "prepared merge body wave differs from its durable frontier",
                ));
            }
        }
        if let Some(upload) = &self.original_upload {
            for value in [
                &upload.attempt_id,
                &upload.expected.head,
                &upload.expected.checkpoint,
                &upload.target.head,
                &upload.target.checkpoint,
            ] {
                if crate::storage_codec::id_string::uuid_bytes_from_canonical(value).is_none() {
                    return Err(invalid("captured upload has invalid coordinates"));
                }
            }
            if upload.expected != self.original_confirmed {
                return Err(invalid("merge attempt changed the captured upload base"));
            }
        }
        if let Some(restart) = &self.restart {
            restart.request.validate()?;
            if restart.request.old != self.request || self.authority_receipt.is_some() {
                return Err(invalid(
                    "restart intent differs from unresolved current attempt",
                ));
            }
            if let Some(receipt) = &restart.receipt {
                receipt.validate_for(
                    state.repository_id(),
                    state.active_account_id(),
                    &restart.request,
                )?;
            }
        }
        if let Some(receipt) = &self.authority_receipt {
            receipt.validate_for(&self.request)?;
        }
        Ok(())
    }
}
#[must_use = "commit the complete merge record with every returned guard"]
fn stage_record(
    writes: &mut StorageWriteSet,
    record: &PartialBranchMergeState,
    previous: Option<Bytes>,
) -> Result<StoragePrecondition, LixError> {
    let bytes =
        serde_json::to_vec(record).map_err(|_| invalid("merge record serialization failed"))?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid("merge record exceeds byte limit"));
    }
    let key = key(&record.request.branch_id)?;
    let guard = match previous {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: PARTIAL_BRANCH_MERGE_SPACE,
            key: key.clone(),
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: PARTIAL_BRANCH_MERGE_SPACE,
            key: key.clone(),
        },
    };
    writes.put(PARTIAL_BRANCH_MERGE_SPACE, key, bytes);
    Ok(guard)
}
/// Constant-sized lookup; absence is meaningful only at this exact admission.
pub(super) async fn load_partial_merge_state(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch: &str,
) -> Result<
    (
        Option<PartialBranchMergeState>,
        Option<Bytes>,
        Vec<StoragePrecondition>,
    ),
    LixError,
> {
    if branch != state.descriptor().selected_branch.branch_id {
        return Err(invalid("merge branch is outside selected scope"));
    }
    let (actual, receipt) = load_partial_replica_state(read)
        .await?
        .ok_or_else(|| invalid("merge state has no partial receipt"))?;
    if &actual != state {
        return Err(conflict("merge state admission changed"));
    }
    let key = key(branch)?;
    let values = PointReadPlan::new(PARTIAL_BRANCH_MERGE_SPACE, std::slice::from_ref(&key))
        .materialize(read, Default::default())
        .await?;
    let raw = match values.value.into_iter().next().flatten() {
        None => None,
        Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes),
        _ => return Err(invalid("merge point read omitted its value")),
    };
    let record = raw
        .as_ref()
        .map(|bytes| {
            if bytes.len() > MAX_RECORD_BYTES {
                return Err(invalid("merge record exceeds byte limit"));
            }
            let record: PartialBranchMergeState =
                serde_json::from_slice(bytes).map_err(|_| invalid("malformed merge record"))?;
            record.validate(state, branch)?;
            Ok(record)
        })
        .transpose()?;
    let guard = match &raw {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: PARTIAL_BRANCH_MERGE_SPACE,
            key,
            expected: expected.clone(),
        },
        None => StoragePrecondition::KeyAbsent {
            space: PARTIAL_BRANCH_MERGE_SPACE,
            key,
        },
    };
    Ok((
        record,
        raw,
        vec![
            StoragePrecondition::KeyValueEquals {
                space: PARTIAL_REPLICA_STATE_SPACE,
                key: partial_replica_state_key(),
                expected: receipt,
            },
            guard,
        ],
    ))
}
/// Capture before sending any merge request. Native analysis separately proves
/// B→L ancestry and uploadable bodies; these guards freeze its local coordinates.
#[must_use = "persist capture durably with all guards before network I/O"]
pub(super) async fn stage_capture_partial_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &PartialMergeRequest,
) -> Result<Vec<StoragePrecondition>, LixError> {
    request.validate()?;
    let (existing, raw, mut guards) =
        load_partial_merge_state(read, state, &request.branch_id).await?;
    if let Some(existing) = existing {
        if existing.restart.is_some() {
            return Err(conflict("restart must settle before old-attempt HTTP"));
        }
        if &existing.request != request {
            return Err(conflict("another merge attempt remains unresolved"));
        }
        return Ok(guards);
    }
    let (push, push_raw, _) = load_partial_push_state(read, state, &request.branch_id).await?;
    let (global, global_raw, _) =
        load_partial_push_state(read, state, crate::GLOBAL_BRANCH_ID).await?;
    if push.confirmed.head != request.base_commit_id
        || push.confirmed.checkpoint != request.checkpoint_commit_id
        || global.confirmed.head != request.global_head_commit_id
        || global.confirmed.checkpoint != request.global_checkpoint_commit_id
        || global.prepared.is_some()
    {
        return Err(conflict("merge capture changed confirmed coordinates"));
    }
    let ids = [
        request.branch_id.clone(),
        crate::GLOBAL_BRANCH_ID.to_owned(),
    ];
    let controls = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&ids)
        .await?;
    for (index, observed) in controls.into_iter().enumerate() {
        let control = observed
            .control
            .ok_or_else(|| conflict("merge capture branch disappeared"))?;
        let (head, checkpoint) = if index == 0 {
            (
                &request.captured_local_head_commit_id,
                &request.captured_local_checkpoint_commit_id,
            )
        } else {
            (
                &request.global_head_commit_id,
                &request.global_checkpoint_commit_id,
            )
        };
        if control.head_commit_id != head.as_str()
            || control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
                .as_ref()
                != Some(checkpoint)
        {
            return Err(conflict("merge capture raced local publication"));
        }
        guards.push(crate::branch::branch_head_control_precondition(
            &ids[index],
            observed.raw_token,
        )?);
    }
    guards.push(StoragePrecondition::KeyValueEquals {
        space: PARTIAL_BRANCH_PUSH_SPACE,
        key: key(&request.branch_id)?,
        expected: push_raw,
    });
    guards.push(StoragePrecondition::KeyValueEquals {
        space: PARTIAL_BRANCH_PUSH_SPACE,
        key: key(crate::GLOBAL_BRANCH_ID)?,
        expected: global_raw,
    });
    let record = PartialBranchMergeState {
        version: 5,
        epoch_id: state.epoch_id().into(),
        request: request.clone(),
        original_confirmed: push.confirmed,
        previous_receipt: None,
        accepted_body_tip: request.base_commit_id.clone(),
        prepared_body_wave: None,
        original_upload: push.prepared,
        authority_receipt: None,
        restart: None,
    };
    record.validate(state, &request.branch_id)?;
    guards.push(stage_record(writes, &record, raw)?);
    Ok(guards)
}
/// Record an authenticated exact server outcome even if newer local L2 exists.
/// Neither confirmed coordinates nor local controls change here. Adoption must
/// separately verify native M and atomically settle only the captured L.
#[must_use = "persist receipt with every returned guard; it is not serving adoption"]
pub(super) async fn stage_record_partial_merge_receipt(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    receipt: &PartialMergeReceipt,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (record, raw, mut guards) =
        load_partial_merge_state(read, state, &receipt.request.branch_id).await?;
    let mut record = record.ok_or_else(|| conflict("merge receipt has no captured attempt"))?;
    if record
        .restart
        .as_ref()
        .is_some_and(|restart| restart.receipt.is_some())
    {
        return Err(invalid(
            "terminal restarted attempt cannot accept a stale merge receipt",
        ));
    }
    receipt.validate_for(&record.request)?;
    if let Some(existing) = &record.authority_receipt {
        if existing != receipt {
            return Err(invalid(
                "one merge attempt returned different authority outcomes",
            ));
        }
        return Ok(guards);
    }
    record.authority_receipt = Some(receipt.clone());
    record.restart = None;
    record.accepted_body_tip = record.request.captured_local_head_commit_id.clone();
    record.prepared_body_wave = None;
    record.validate(state, &receipt.request.branch_id)?;
    guards.push(stage_record(writes, &record, raw)?);
    Ok(guards)
}

/// Both ordinary lanes must remain fenced while the selected branch has a
/// durable merge attempt. The absence guard also rejects a capture racing ACK.
pub(super) async fn ordinary_upload_merge_guards(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (record, _, guards) =
        load_partial_merge_state(read, state, &state.descriptor().selected_branch.branch_id)
            .await?;
    if record.is_some() {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_MERGE_PENDING",
            "ordinary upload is deferred until the captured merge is settled",
        ));
    }
    Ok(guards)
}

async fn require_local_dependency_closure(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    ancestor: &str,
    descendant: &str,
    global: &str,
) -> Result<(), LixError> {
    use crate::changelog::CommitId;
    let ancestor = CommitId::parse_lix(ancestor, "merge body ancestor")?;
    let descendant = CommitId::parse_lix(descendant, "merge body descendant")?;
    if ancestor == descendant {
        return Ok(());
    }
    let global = CommitId::parse_lix(global, "merge body global base")?;
    let known = [
        ancestor,
        global,
        CommitId::parse_lix(
            &state.descriptor().selected_branch.head.commit_id,
            "merge selected base",
        )?,
        CommitId::parse_lix(
            &state.descriptor().selected_branch.checkpoint.commit_id,
            "merge checkpoint",
        )?,
        CommitId::parse_lix(
            &state.descriptor().global_branch.checkpoint.commit_id,
            "merge global checkpoint",
        )?,
    ]
    .into_iter()
    .collect();
    super::partial_checkpoint_upload::load_local_dependency_closure(
        read,
        &state.descriptor().selected_branch.branch_id,
        state.active_account_id(),
        &[descendant],
        known,
        global,
        1024,
        64 * 1024 * 1024,
    )
    .await?;
    Ok(())
}

/// Capture the exact oldest wave before sending it. Repeated capture is safe;
/// a different wave cannot replace an unresolved network outcome.
#[must_use = "persist body wave before network I/O with all returned guards"]
pub(super) async fn stage_prepare_partial_merge_body_wave(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &PartialMergeRequest,
    previous: &str,
    target: &str,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (record, raw, mut guards) =
        load_partial_merge_state(read, state, &request.branch_id).await?;
    let mut record = record.ok_or_else(|| conflict("body wave has no captured merge attempt"))?;
    if record.restart.is_some() {
        return Err(conflict("body work is fenced by a durable restart intent"));
    }
    if &record.request != request
        || record.authority_receipt.is_some()
        || record.accepted_body_tip != previous
        || previous == target
    {
        return Err(conflict("body wave no longer continues the captured merge"));
    }
    let wave = PreparedMergeBodyWave {
        previous: previous.into(),
        target: target.into(),
    };
    if let Some(existing) = &record.prepared_body_wave {
        if existing != &wave {
            return Err(conflict("another body wave remains unresolved"));
        }
        return Ok(guards);
    }
    // Reconstruct the exact deterministic captured closure page. A cursor may
    // be a checkpoint source rather than a first-parent ancestor.
    super::partial_merge_runtime::captured_wave(read, request, previous, Some(&wave)).await?;
    record.prepared_body_wave = Some(wave);
    record.validate(state, &request.branch_id)?;
    guards.push(stage_record(writes, &record, raw)?);
    Ok(guards)
}

#[must_use = "persist exact body acknowledgment before advancing the next wave"]
pub(super) async fn stage_acknowledge_partial_merge_body_wave(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &PartialMergeRequest,
    previous: &str,
    target: &str,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (record, raw, mut guards) =
        load_partial_merge_state(read, state, &request.branch_id).await?;
    let mut record = record.ok_or_else(|| conflict("body acknowledgment has no merge attempt"))?;
    if record.restart.is_some() {
        return Err(conflict("body work is fenced by a durable restart intent"));
    }
    if &record.request != request || record.authority_receipt.is_some() {
        return Err(conflict(
            "body acknowledgment belongs to a replaced merge attempt",
        ));
    }
    if record.prepared_body_wave.is_none() && record.accepted_body_tip == target {
        // Exact attempt already advanced to this target. This replay does not
        // claim a new body frontier and cannot erase a newer prepared wave.
        return Ok(guards);
    }
    let expected = PreparedMergeBodyWave {
        previous: previous.into(),
        target: target.into(),
    };
    if record.prepared_body_wave.as_ref() != Some(&expected) {
        return Err(conflict(
            "body acknowledgment does not echo the durably prepared wave",
        ));
    }
    record.accepted_body_tip = target.into();
    record.prepared_body_wave = None;
    record.validate(state, &request.branch_id)?;
    guards.push(stage_record(writes, &record, raw)?);
    Ok(guards)
}

/// Advance only the reconciliation base after a verified M=[R,L]. The ordinary
/// upload confirmation remains at its original B until a serving candidate is
/// durably admitted. One prior receipt bounds crash recovery metadata.
#[must_use = "persist rollover with every native frontier and record guard"]
pub(super) async fn stage_rollover_partial_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &PartialMergeRequest,
) -> Result<Vec<StoragePrecondition>, LixError> {
    request.validate()?;
    let (record, raw, mut guards) =
        load_partial_merge_state(read, state, &request.branch_id).await?;
    let mut record = record.ok_or_else(|| conflict("rollover has no captured merge"))?;
    if &record.request == request && record.previous_receipt.is_some() {
        // The exact rollover is already durable. Preserve any subsequently
        // prepared/acknowledged wave or authority receipt on this attempt.
        return Ok(guards);
    }
    let prior = record
        .authority_receipt
        .clone()
        .ok_or_else(|| conflict("rollover must first recover the exact authority outcome"))?;
    if request.attempt_id == record.request.attempt_id
        || request.base_commit_id != record.request.captured_local_head_commit_id
        || request.checkpoint_commit_id != record.request.captured_local_checkpoint_commit_id
        || request.global_head_commit_id != record.request.global_head_commit_id
        || request.global_checkpoint_commit_id != record.request.global_checkpoint_commit_id
    {
        return Err(conflict(
            "rollover changed the previous captured native frontier",
        ));
    }
    let id = |text: &str| crate::changelog::CommitId::parse_lix(text, "merge rollover coordinate");
    let merge =
        super::partial_merge_analysis::record(read, id(&prior.merge_commit_id)?, false).await?;
    if merge.is_checkpoint
        || merge.parent_commit_ids
            != vec![
                id(&prior.request.expected_authority_head_commit_id)?,
                id(&prior.request.captured_local_head_commit_id)?,
            ]
        || merge.base_commit_id != Some(id(&request.global_head_commit_id)?)
        || merge.account_id != state.active_account_id()
    {
        return Err(invalid(
            "rollover receipt disagrees with its native merge record",
        ));
    }
    if !super::partial_merge_analysis::bounded_ancestor(
        read,
        &merge,
        id(&request.expected_authority_head_commit_id)?,
        &mut Default::default(),
        1024,
    )
    .await?
    {
        return Err(conflict(
            "new authority frontier does not contain the prior merge",
        ));
    }
    require_local_dependency_closure(
        read,
        state,
        &request.base_commit_id,
        &request.captured_local_head_commit_id,
        &request.global_head_commit_id,
    )
    .await?;
    for (branch, head, checkpoint) in [
        (
            &request.branch_id,
            &request.captured_local_head_commit_id,
            &request.captured_local_checkpoint_commit_id,
        ),
        (
            &state.descriptor().global_branch.branch_id,
            &request.global_head_commit_id,
            &request.global_checkpoint_commit_id,
        ),
    ] {
        let observed = crate::branch::BranchHeadControlContext::new()
            .reader(read)
            .load_observed(std::slice::from_ref(branch))
            .await?
            .pop()
            .ok_or_else(|| conflict("rollover branch observation absent"))?;
        let control = observed
            .control
            .ok_or_else(|| conflict("rollover branch absent"))?;
        if control.head_commit_id != id(head)?
            || control.working_diff_checkpoint_commit_id != Some(id(checkpoint)?)
        {
            return Err(conflict("rollover raced local branch publication"));
        }
        guards.push(crate::branch::branch_head_control_precondition(
            branch,
            observed.raw_token,
        )?);
    }
    let (push, push_raw, _) = load_partial_push_state(read, state, &request.branch_id).await?;
    let (global, global_raw, _) =
        load_partial_push_state(read, state, crate::GLOBAL_BRANCH_ID).await?;
    if push.confirmed != record.original_confirmed
        || push.prepared != record.original_upload
        || global.prepared.is_some()
        || global.confirmed.head != request.global_head_commit_id
        || global.confirmed.checkpoint != request.global_checkpoint_commit_id
    {
        return Err(conflict("rollover changed original upload confirmation"));
    }
    for (branch, expected) in [
        (&request.branch_id, push_raw),
        (&state.descriptor().global_branch.branch_id, global_raw),
    ] {
        guards.push(StoragePrecondition::KeyValueEquals {
            space: PARTIAL_BRANCH_PUSH_SPACE,
            key: key(branch)?,
            expected,
        });
    }
    record.request = request.clone();
    record.previous_receipt = Some(prior);
    record.authority_receipt = None;
    record.accepted_body_tip = request.base_commit_id.clone();
    record.prepared_body_wave = None;
    record.validate(state, &request.branch_id)?;
    guards.push(stage_record(writes, &record, raw)?);
    Ok(guards)
}

// Append within partial_merge_state; admission cannot erase an archived attempt.
pub(super) async fn require_no_branch_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    branch: &str,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let key = key(branch)?;
    let value = PointReadPlan::new(PARTIAL_BRANCH_MERGE_SPACE, std::slice::from_ref(&key))
        .materialize(read, Default::default())
        .await?
        .value
        .pop()
        .flatten();
    if value.is_some() {
        return Err(LixError::new(
            "LIX_PARTIAL_BRANCH_SWITCH_PENDING",
            "branch has a retained merge attempt",
        ));
    }
    Ok(vec![StoragePrecondition::KeyAbsent {
        space: PARTIAL_BRANCH_MERGE_SPACE,
        key,
    }])
}

/// Bounded owned migration: v3 adds the empty frozen created-ref list; v4
/// adds explicit in-memory B/R/L checkpoints while preserving canonical wire
/// bytes. Normal journal reads accept only v5. No controls or data are reset.
pub(crate) async fn prepare_owned_partial_merge_upload_upgrade(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let mut branches = state
        .archived_branch_ids()
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    branches.insert(state.descriptor().selected_branch.branch_id.clone());
    let mut guards = Vec::new();
    for branch in branches {
        if branch == crate::GLOBAL_BRANCH_ID {
            continue;
        }
        let values = PointReadPlan::new(PARTIAL_BRANCH_MERGE_SPACE, &[key(&branch)?])
            .materialize(read, Default::default())
            .await?
            .value;
        let bytes = match values.into_iter().next().flatten() {
            None => continue,
            Some(StorageProjectedValue::FullValue(bytes)) => bytes,
            Some(_) => return Err(invalid("merge migration point read omitted value")),
        };
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(invalid("merge migration exceeds metadata bound"));
        }
        let mut value: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|_| invalid("old merge record malformed"))?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| invalid("old merge record must be object"))?;
        match object.get("version").and_then(serde_json::Value::as_u64) {
            Some(5) => {
                let record: PartialBranchMergeState =
                    serde_json::from_value(value).map_err(|_| invalid("merge v5 malformed"))?;
                record.validate(state, &branch)?;
                continue;
            }
            Some(3) | Some(4) => {}
            _ => return Err(invalid("unsupported merge migration version")),
        }
        let old_version = object
            .get("version")
            .and_then(serde_json::Value::as_u64)
            .unwrap();
        if let Some(upload) = object
            .get_mut("originalUpload")
            .filter(|v| old_version == 3 && !v.is_null())
        {
            let upload = upload
                .as_object_mut()
                .ok_or_else(|| invalid("old original upload must be object"))?;
            if upload.len() != 3
                || !["attemptId", "expected", "target"]
                    .iter()
                    .all(|key| upload.contains_key(*key))
            {
                return Err(invalid("old original upload fields do not match v3 schema"));
            }
            upload.insert("createdRefs".into(), serde_json::json!([]));
        }
        validate_legacy_checkpoint_encoding(&serde_json::Value::Object(object.clone()))?;
        object.insert("version".into(), serde_json::json!(5));
        let record: PartialBranchMergeState = serde_json::from_value(value)
            .map_err(|_| invalid("upgraded merge fields malformed"))?;
        record.validate(state, &branch)?;
        let encoded =
            serde_json::to_vec(&record).map_err(|_| invalid("merge migration encoding failed"))?;
        if encoded.len() > MAX_RECORD_BYTES {
            return Err(invalid("upgraded merge exceeds bound"));
        }
        writes.put(
            PARTIAL_BRANCH_MERGE_SPACE,
            key(&branch)?,
            crate::storage_adapter::StorageValue {
                bytes: encoded.into(),
            },
        );
        guards.push(StoragePrecondition::KeyValueEquals {
            space: PARTIAL_BRANCH_MERGE_SPACE,
            key: key(&branch)?,
            expected: bytes,
        });
    }
    Ok(guards)
}

// v3/v4 froze one checkpoint for B/R/L. Nested previous receipts and restart
// outcomes must retain that invariant as well as their exact canonical digest.
fn validate_legacy_checkpoint_encoding(value: &serde_json::Value) -> Result<(), LixError> {
    match value {
        serde_json::Value::Object(object) => {
            if object.contains_key("expectedAuthorityCheckpointCommitId")
                || object.contains_key("capturedLocalCheckpointCommitId")
            {
                return Err(invalid("old merge record contains new checkpoint fields"));
            }
            for child in object.values() {
                validate_legacy_checkpoint_encoding(child)?;
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                validate_legacy_checkpoint_encoding(child)?;
            }
        }
        _ => {}
    }
    Ok(())
}
