//! Authority-side bounded native reconciliation plan, never client-selected rows.
use super::partial_authority_merge_receipt::{
    PreparedAuthorityMergeReceipt, load_authority_merge_receipt,
};
use super::partial_merge_analysis::{
    NativeKvMergeAnalysis, PartialMergeBudget, analyze_native_kv_divergence,
};
use super::partial_merge_protocol::{PartialMergeReceipt, PartialMergeRequest};
use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};

pub(crate) enum AuthorityMergePreparation {
    AlreadyCommitted(PartialMergeReceipt),
    Ready(AuthorityKvMergePlan),
}
pub(crate) struct AuthorityKvMergePlan {
    repository_id: String,
    account_id: String,
    request: PartialMergeRequest,
    native: NativeKvMergeAnalysis,
    control_guards: Vec<StoragePrecondition>,
}
fn conflict(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_MERGE_AUTHORITY_CHANGED", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "authority merge coordinate")
}

/// `repository` and `account` must come from the authenticated authority session,
/// not request JSON. Caller owns a live attempt retention lease through commit.
/// This helper does not accept or publish an unretained upload body.
pub(crate) async fn prepare_authority_kv_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: PartialMergeRequest,
    budget: PartialMergeBudget,
) -> Result<AuthorityMergePreparation, LixError> {
    request.validate()?;
    // Lookup precedes expected-R comparison: lost responses remain recoverable
    // after unrelated later authority commits.
    if let Some(receipt) = load_authority_merge_receipt(read, repository, account, &request).await?
    {
        return Ok(AuthorityMergePreparation::AlreadyCommitted(receipt));
    }
    let identity = crate::gc::NativeUploadAttemptIdentity {
        repository_id: repository.into(),
        account_id: account.into(),
        branch_id: request.branch_id.clone(),
        attempt_id: request.attempt_id.clone(),
    };
    let digest = *blake3::hash(
        &serde_json::to_vec(&request).map_err(|_| conflict("merge request encoding failed"))?,
    )
    .as_bytes();
    crate::gc::require_native_upload_attempt(
        read,
        &identity,
        digest,
        id(&request.captured_local_head_commit_id)?,
        crate::telemetry::unix_time_ms(),
    )
    .await?;
    let ids = [
        request.branch_id.clone(),
        crate::GLOBAL_BRANCH_ID.to_owned(),
    ];
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&ids)
        .await?;
    for (index, (head, checkpoint)) in [
        (
            &request.expected_authority_head_commit_id,
            &request.checkpoint_commit_id,
        ),
        (
            &request.global_head_commit_id,
            &request.global_checkpoint_commit_id,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let control = observed[index]
            .control
            .as_ref()
            .ok_or_else(|| conflict("merge branch control is absent"))?;
        if control.head_commit_id != id(head)?
            || control.working_diff_checkpoint_commit_id != Some(id(checkpoint)?)
        {
            return Err(conflict(
                "authority selected/global coordinates changed before merge analysis",
            ));
        }
    }
    let native = analyze_native_kv_divergence(
        read,
        id(&request.base_commit_id)?,
        id(&request.expected_authority_head_commit_id)?,
        id(&request.captured_local_head_commit_id)?,
        account,
        id(&request.global_head_commit_id)?,
        budget,
    )
    .await?;
    let control_guards = ids
        .iter()
        .zip(observed)
        .map(|(branch, value)| {
            crate::branch::branch_head_control_precondition(branch, value.raw_token)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AuthorityMergePreparation::Ready(AuthorityKvMergePlan {
        repository_id: repository.into(),
        account_id: account.into(),
        request,
        native,
        control_guards,
    }))
}
impl AuthorityKvMergePlan {
    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
    }
    pub(crate) fn branch_id(&self) -> &str {
        &self.request.branch_id
    }
    pub(crate) fn source_parent(&self) -> Result<CommitId, LixError> {
        id(&self.request.captured_local_head_commit_id)
    }
    pub(crate) fn groups(&self) -> &[crate::tracked_state::TrackedStateMergePlan] {
        &self.native.groups
    }
    pub(crate) fn has_conflicts(&self) -> bool {
        self.native
            .groups
            .iter()
            .any(|group| !group.conflicts.is_empty())
    }
    pub(crate) fn into_receipt(
        self,
        merge: CommitId,
    ) -> Result<PreparedAuthorityMergeReceipt, LixError> {
        if self.has_conflicts() {
            return Err(LixError::new(
                "LIX_PARTIAL_MERGE_CONFLICT",
                "native merge contains unresolved conflicts",
            ));
        }
        PreparedAuthorityMergeReceipt::new(
            &self.repository_id,
            &self.account_id,
            self.request,
            merge,
            self.control_guards,
        )
    }
}
