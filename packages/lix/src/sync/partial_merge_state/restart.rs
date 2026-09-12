//! Client-owned exact restart intent. Child of partial_merge_state.
use super::*;
use crate::sync::{
    PartialAttemptRestartOutcome, PartialAttemptRestartReceipt, PartialAttemptRestartRequest,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(in crate::sync) struct PartialMergeRestart {
    pub request: PartialAttemptRestartRequest,
    pub receipt: Option<PartialAttemptRestartReceipt>,
}

/// Persist the next UUID before HTTP. A lost reply must retry this exact intent;
/// a second random UUID cannot replace the authority's immutable restart fence.
#[must_use = "commit exact restart intent durably before HTTP"]
pub(in crate::sync) async fn stage_prepare_partial_merge_restart(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    intent: &PartialAttemptRestartRequest,
) -> Result<Vec<StoragePrecondition>, LixError> {
    intent.validate()?;
    let (record, raw, mut guards) =
        load_partial_merge_state(read, state, &intent.old.branch_id).await?;
    let mut record = record.ok_or_else(|| conflict("restart has no captured attempt"))?;
    if record.request != intent.old || record.authority_receipt.is_some() {
        return Err(conflict(
            "restart is not for the unresolved captured attempt",
        ));
    }
    if let Some(existing) = &record.restart {
        if &existing.request != intent {
            return Err(conflict("another restart UUID is already durable"));
        }
        return Ok(guards);
    }
    record.restart = Some(PartialMergeRestart {
        request: intent.clone(),
        receipt: None,
    });
    record.validate(state, &intent.old.branch_id)?;
    guards.push(stage_record(writes, &record, raw)?);
    Ok(guards)
}

/// Persist the authenticated terminal outcome. Restarted never clears the old
/// attempt until the new native coordinates have been proven and captured.
#[must_use = "publish exact restart outcome durably before preparing new network work"]
pub(in crate::sync) async fn stage_record_partial_merge_restart_outcome(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    intent: &PartialAttemptRestartRequest,
    outcome: &PartialAttemptRestartOutcome,
) -> Result<Vec<StoragePrecondition>, LixError> {
    outcome.validate_for(state.repository_id(), state.active_account_id(), intent)?;
    let (record, raw, mut guards) =
        load_partial_merge_state(read, state, &intent.old.branch_id).await?;
    let mut record = record.ok_or_else(|| conflict("restart outcome has no captured attempt"))?;
    if record.request != intent.old {
        return Err(conflict("restart outcome belongs to a replaced attempt"));
    }
    // An identical Committed replay after durable publication is safe even
    // though stage_record_partial_merge_receipt already cleared the intent.
    if let PartialAttemptRestartOutcome::Committed { receipt, .. } = outcome {
        if record.authority_receipt.as_ref() == Some(receipt) && record.restart.is_none() {
            return Ok(guards);
        }
    }
    let restart = record
        .restart
        .as_mut()
        .ok_or_else(|| conflict("restart outcome has no durable intent"))?;
    if &restart.request != intent {
        return Err(conflict("restart outcome changed its durable next UUID"));
    }
    match outcome {
        PartialAttemptRestartOutcome::Committed { receipt, .. } => {
            if restart.receipt.is_some() {
                return Err(invalid("terminal restarted attempt cannot later commit"));
            }
            stage_record_partial_merge_receipt(read, writes, state, receipt).await
        }
        PartialAttemptRestartOutcome::Restarted { receipt } => {
            if let Some(existing) = &restart.receipt {
                if existing != receipt {
                    return Err(invalid(
                        "restart intent returned different terminal outcomes",
                    ));
                }
                return Ok(guards);
            }
            restart.receipt = Some(receipt.clone());
            record.validate(state, &intent.old.branch_id)?;
            guards.push(stage_record(writes, &record, raw)?);
            Ok(guards)
        }
    }
}

/// This is the first point where the next UUID may become eligible for HTTP.
/// Native analysis is read-only and bounded; it must finish before publication.
#[must_use = "commit new native capture and every guard before new-attempt HTTP"]
pub(in crate::sync) async fn stage_capture_restarted_partial_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: &PartialMergeRequest,
) -> Result<Vec<StoragePrecondition>, LixError> {
    request.validate()?;
    let (record, raw, mut guards) =
        load_partial_merge_state(read, state, &request.branch_id).await?;
    let mut record = record.ok_or_else(|| conflict("restart capture has no previous attempt"))?;
    if &record.request == request && record.restart.is_none() {
        return Ok(guards);
    }
    let restart = record
        .restart
        .as_ref()
        .ok_or_else(|| conflict("restart capture has no durable terminal receipt"))?;
    let receipt = restart
        .receipt
        .as_ref()
        .ok_or_else(|| conflict("recover exact restart outcome before capture"))?;
    receipt.validate_for(
        state.repository_id(),
        state.active_account_id(),
        &restart.request,
    )?;
    if request.attempt_id != restart.request.next_attempt_id
        || request.base_commit_id != record.request.base_commit_id
        || request.branch_id != record.request.branch_id
        || request.checkpoint_commit_id != record.request.checkpoint_commit_id
        || request.global_head_commit_id != record.request.global_head_commit_id
        || request.global_checkpoint_commit_id != record.request.global_checkpoint_commit_id
        || record.authority_receipt.is_some()
    {
        return Err(conflict(
            "restart capture changed its durable base or catalog",
        ));
    }
    let id = |text: &str| crate::changelog::CommitId::parse_lix(text, "restart capture coordinate");
    let _analysis = crate::sync::partial_merge_analysis::analyze_native_divergence(
        read,
        id(&request.base_commit_id)?,
        id(&request.expected_authority_head_commit_id)?,
        id(&request.captured_local_head_commit_id)?,
        state.active_account_id(),
        &request.branch_id,
        &[
            id(&request.checkpoint_commit_id)?,
            id(&request.expected_authority_checkpoint_commit_id)?,
        ],
        id(&request.global_head_commit_id)?,
        crate::sync::partial_merge_analysis::PartialMergeBudget {
            max_local_commits: 1024,
            max_local_members: 65536,
            max_local_payload_bytes: 64 * 1024 * 1024,
            max_remote_graph_records: 1024,
        },
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
            .ok_or_else(|| conflict("restart capture branch observation missing"))?;
        let control = observed
            .control
            .ok_or_else(|| conflict("restart capture branch disappeared"))?;
        if control.head_commit_id != id(head)?
            || control.working_diff_checkpoint_commit_id != Some(id(checkpoint)?)
        {
            return Err(conflict("restart capture raced newer local publication"));
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
        return Err(conflict(
            "restart capture changed original ordinary confirmation",
        ));
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
    record.accepted_body_tip = request.base_commit_id.clone();
    record.prepared_body_wave = None;
    record.authority_receipt = None;
    record.restart = None;
    record.validate(state, &request.branch_id)?;
    guards.push(stage_record(writes, &record, raw)?);
    Ok(guards)
}
