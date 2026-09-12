//! Native proof that a captured local suffix is included in an authority basis.
//! This proof settles only captured L; concurrent L2 makes its guards fail.
use super::partial_merge_state::{PartialBranchMergeState, load_partial_merge_state};
use super::partial_push_state::PartialPushCoordinate;
use super::partial_state::PartialReplicaState;
use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};

pub(super) struct VerifiedPartialMergeSettlement {
    record: PartialBranchMergeState,
    target: PartialPushCoordinate,
    guards: Vec<StoragePrecondition>,
}
fn conflict(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_REPLICA_MERGE_PENDING", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "partial merge settlement")
}
impl VerifiedPartialMergeSettlement {
    pub(super) fn record(&self) -> &PartialBranchMergeState {
        &self.record
    }
    pub(super) fn target(&self) -> &PartialPushCoordinate {
        &self.target
    }
    pub(super) fn into_guards(self) -> Vec<StoragePrecondition> {
        self.guards
    }
}
pub(super) async fn verify_partial_merge_settlement(
    read: &(impl StorageAdapterRead + ?Sized),
    previous: &PartialReplicaState,
    next: &PartialReplicaState,
) -> Result<VerifiedPartialMergeSettlement, LixError> {
    let branch = &previous.descriptor().selected_branch.branch_id;
    if branch == crate::GLOBAL_BRANCH_ID {
        return Err(conflict(
            "global branch merge settlement is outside the prepared scope",
        ));
    }
    let (record, _, mut guards) = load_partial_merge_state(read, previous, branch).await?;
    let record = record.ok_or_else(|| conflict("merge adoption has no captured attempt"))?;
    let receipt = record
        .authority_receipt
        .as_ref()
        .ok_or_else(|| conflict("merge outcome has not been recorded durably"))?;
    let request = &record.request;
    let descriptor = next.descriptor();
    if descriptor.selected_branch.branch_id != *branch
        || descriptor.global_branch.head.commit_id != request.global_head_commit_id
        || descriptor.global_branch.checkpoint.commit_id != request.global_checkpoint_commit_id
    {
        return Err(conflict(
            "merge adoption changed checkpoint or global dependencies",
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
        || control.working_diff_checkpoint_commit_id
            != Some(id(&request.captured_local_checkpoint_commit_id)?)
    {
        return Err(conflict(
            "newer local edits require another native reconciliation",
        ));
    }
    let merge =
        super::partial_merge_analysis::record(read, id(&receipt.merge_commit_id)?, false).await?;
    if merge.is_checkpoint
        || merge.parent_commit_ids
            != vec![
                id(&request.expected_authority_head_commit_id)?,
                id(&request.captured_local_head_commit_id)?,
            ]
        || merge.base_commit_id != Some(id(&request.global_head_commit_id)?)
        || merge.account_id != previous.active_account_id()
    {
        return Err(LixError::new(
            "LIX_PARTIAL_MERGE_STATE_INVALID",
            "authority merge native record does not contain the captured parents and catalog",
        ));
    }
    if !super::partial_merge_analysis::bounded_ancestor(
        read,
        &merge,
        id(&descriptor.selected_branch.head.commit_id)?,
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
    Ok(VerifiedPartialMergeSettlement {
        record,
        target: PartialPushCoordinate {
            head: descriptor.selected_branch.head.commit_id.clone(),
            checkpoint: descriptor.selected_branch.checkpoint.commit_id.clone(),
        },
        guards,
    })
}
