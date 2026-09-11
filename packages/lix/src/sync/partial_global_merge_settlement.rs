//! Native GLOBAL proof that a captured descriptor-only suffix is included in an authority basis.
//! This proof settles only captured L; concurrent L2 makes its guards fail.
use super::partial_global_merge_state::{PartialGlobalMergeState, load_partial_global_merge_state};
use super::partial_push_state::PartialPushCoordinate;
use super::partial_state::PartialReplicaState;
use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};

pub(super) struct VerifiedPartialGlobalMergeSettlement {
    record: PartialGlobalMergeState,
    target: PartialPushCoordinate,
    guards: Vec<StoragePrecondition>,
}
fn conflict(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_GLOBAL_MERGE_PENDING", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "partial merge settlement")
}
impl VerifiedPartialGlobalMergeSettlement {
    pub(super) fn record(&self) -> &PartialGlobalMergeState {
        &self.record
    }
    pub(super) fn target(&self) -> &PartialPushCoordinate {
        &self.target
    }
    pub(super) fn into_guards(self) -> Vec<StoragePrecondition> {
        self.guards
    }
}
pub(super) async fn verify_partial_global_merge_settlement(
    read: &(impl StorageAdapterRead + ?Sized),
    previous: &PartialReplicaState,
    next: &PartialReplicaState,
) -> Result<VerifiedPartialGlobalMergeSettlement, LixError> {
    let branch = &previous.descriptor().global_branch.branch_id;
    let (record, _, mut guards) = load_partial_global_merge_state(read, previous).await?;
    let record = record.ok_or_else(|| conflict("merge adoption has no captured attempt"))?;
    let receipt = record
        .receipt
        .as_ref()
        .ok_or_else(|| conflict("merge outcome has not been recorded durably"))?;
    let request = &record.request;
    let descriptor = next.descriptor();
    if descriptor.selected_branch.branch_id != previous.descriptor().selected_branch.branch_id
        || descriptor.global_branch.branch_id != crate::GLOBAL_BRANCH_ID
        || descriptor.global_branch.checkpoint.commit_id != request.checkpoint_commit_id
    {
        return Err(conflict(
            "global merge adoption changed selected identity or global checkpoint",
        ));
    }
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(std::slice::from_ref(branch))
        .await?
        .pop()
        .ok_or_else(|| conflict("merge local control is absent"))?;
    let control = observed
        .control
        .ok_or_else(|| conflict("merge local control is absent"))?;
    if control.head_commit_id != id(&request.captured_local_head_commit_id)?
        || control.working_diff_checkpoint_commit_id != Some(id(&request.checkpoint_commit_id)?)
    {
        return Err(LixError::new(
            "LIX_PARTIAL_GLOBAL_NEWER_LOCAL_RECONCILIATION_REQUIRED",
            "newer local GLOBAL edits require reconciliation; the exact outcome and pending edits remain retained",
        ));
    }
    let merge =
        super::partial_merge_analysis::record(read, id(&receipt.merge_commit_id)?, false).await?;
    let header = crate::tracked_state::load_published_commit_state_topology(
        read,
        id(&receipt.merge_commit_id)?,
    )
    .await?
    .ok_or_else(|| {
        crate::tracked_state::NativeMetadataRef::CommitStateHeader(receipt.merge_commit_id.clone())
            .annotate_missing(conflict("GLOBAL merge header must be hydrated"))
    })?;
    if !header.global_scope()
        || merge.is_checkpoint
        || merge.parent_commit_ids
            != vec![
                id(&request.expected_authority_head_commit_id)?,
                id(&request.captured_local_head_commit_id)?,
            ]
        || merge.base_commit_id.is_some()
        || merge.account_id != previous.active_account_id()
    {
        return Err(LixError::new(
            "LIX_PARTIAL_GLOBAL_MERGE_STATE_INVALID",
            "authority merge native record does not contain the captured parents and catalog",
        ));
    }
    if !super::partial_merge_analysis::bounded_ancestor(
        read,
        &merge,
        id(&descriptor.global_branch.head.commit_id)?,
        &mut Default::default(),
        1024,
    )
    .await?
    {
        return Err(conflict(
            "candidate authority head does not contain the recorded merge",
        ));
    }
    guards.push(crate::branch::branch_head_control_precondition(
        branch,
        observed.raw_token,
    )?);
    Ok(VerifiedPartialGlobalMergeSettlement {
        record,
        target: PartialPushCoordinate {
            head: descriptor.global_branch.head.commit_id.clone(),
            checkpoint: descriptor.global_branch.checkpoint.commit_id.clone(),
        },
        guards,
    })
}

/// Settle only captured L without changing a newer local GLOBAL control.
pub(super) async fn verify_partial_global_prefix_settlement(
    read: &(impl StorageAdapterRead + ?Sized),
    previous: &PartialReplicaState,
) -> Result<VerifiedPartialGlobalMergeSettlement, LixError> {
    let branch = &previous.descriptor().global_branch.branch_id;
    let (record, _, mut guards) = load_partial_global_merge_state(read, previous).await?;
    let record = record.ok_or_else(|| conflict("merge adoption has no captured attempt"))?;
    let receipt = record
        .receipt
        .as_ref()
        .ok_or_else(|| conflict("merge outcome has not been recorded durably"))?;
    let request = &record.request;
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(std::slice::from_ref(branch))
        .await?
        .pop()
        .ok_or_else(|| conflict("merge local control is absent"))?;
    let control = observed
        .control
        .ok_or_else(|| conflict("merge local control is absent"))?;
    if control.working_diff_checkpoint_commit_id != Some(id(&request.checkpoint_commit_id)?) {
        return Err(conflict(
            "GLOBAL checkpoint changed while the captured prefix was being merged",
        ));
    }
    let mut cursor = control.head_commit_id;
    let captured = id(&request.captured_local_head_commit_id)?;
    let mut seen = std::collections::BTreeSet::new();
    while cursor != captured {
        if seen.len() >= 1024 || !seen.insert(cursor) {
            return Err(conflict("GLOBAL successor exceeds ordinary prefix budget"));
        }
        let node = super::partial_merge_analysis::record(read, cursor, true).await?;
        if node.is_checkpoint
            || node.parent_commit_ids.len() != 1
            || node.base_commit_id.is_some()
            || node.account_id != previous.active_account_id()
        {
            return Err(conflict(
                "GLOBAL successor is outside the ordinary preserved prefix",
            ));
        }
        cursor = node.parent_commit_ids[0];
    }
    let merge =
        super::partial_merge_analysis::record(read, id(&receipt.merge_commit_id)?, false).await?;
    let header = crate::tracked_state::load_published_commit_state_topology(
        read,
        id(&receipt.merge_commit_id)?,
    )
    .await?
    .ok_or_else(|| {
        crate::tracked_state::NativeMetadataRef::CommitStateHeader(receipt.merge_commit_id.clone())
            .annotate_missing(conflict("GLOBAL merge header must be hydrated"))
    })?;
    if !header.global_scope()
        || merge.is_checkpoint
        || merge.parent_commit_ids
            != vec![
                id(&request.expected_authority_head_commit_id)?,
                id(&request.captured_local_head_commit_id)?,
            ]
        || merge.base_commit_id.is_some()
        || merge.account_id != previous.active_account_id()
    {
        return Err(LixError::new(
            "LIX_PARTIAL_GLOBAL_MERGE_STATE_INVALID",
            "authority merge native record does not contain the captured parents and catalog",
        ));
    }
    guards.push(crate::branch::branch_head_control_precondition(
        branch,
        observed.raw_token,
    )?);
    let target = PartialPushCoordinate {
        head: request.captured_local_head_commit_id.clone(),
        checkpoint: request.checkpoint_commit_id.clone(),
    };
    Ok(VerifiedPartialGlobalMergeSettlement {
        record,
        target,
        guards,
    })
}
