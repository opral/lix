//! Authority-side bounded native reconciliation plan, never client-selected rows.
use super::partial_authority_merge_receipt::{
    PreparedAuthorityMergeReceipt, load_authority_merge_receipt,
};
use super::partial_merge_analysis::{
    NativeMergeAnalysis, PartialMergeBudget, analyze_native_divergence,
};
use super::partial_merge_protocol::{PartialMergeReceipt, PartialMergeRequest};
use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};

pub(crate) enum AuthorityMergePreparation {
    AlreadyCommitted(PartialMergeReceipt),
    Ready(AuthorityMergePlan),
}
pub(crate) struct AuthorityMergePlan {
    repository_id: String,
    account_id: String,
    request: PartialMergeRequest,
    native: NativeMergeAnalysis,
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
pub(crate) async fn prepare_authority_merge(
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
            &request.expected_authority_checkpoint_commit_id,
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
    // An incoming checkpoint is a retained native coordinate, never a client
    // assertion about current state. Its working head must include it.
    if request.captured_local_checkpoint_commit_id != request.checkpoint_commit_id {
        let checkpoint = super::partial_merge_analysis::record(
            read,
            id(&request.captured_local_checkpoint_commit_id)?,
            true,
        )
        .await?;
        if !checkpoint.is_checkpoint
            || !super::partial_merge_analysis::bounded_ancestor(
                read,
                &checkpoint,
                id(&request.captured_local_head_commit_id)?,
                &mut Default::default(),
                budget.max_remote_graph_records,
            )
            .await?
        {
            return Err(conflict(
                "incoming checkpoint is not retained in the captured working head",
            ));
        }
    }
    let native = analyze_native_divergence(
        read,
        id(&request.base_commit_id)?,
        id(&request.expected_authority_head_commit_id)?,
        id(&request.captured_local_head_commit_id)?,
        account,
        &request.branch_id,
        &[
            id(&request.checkpoint_commit_id)?,
            id(&request.expected_authority_checkpoint_commit_id)?,
        ],
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
    Ok(AuthorityMergePreparation::Ready(AuthorityMergePlan {
        repository_id: repository.into(),
        account_id: account.into(),
        request,
        native,
        control_guards,
    }))
}
impl AuthorityMergePlan {
    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
    }
    pub(crate) fn branch_id(&self) -> &str {
        &self.request.branch_id
    }
    pub(crate) fn accepted_checkpoint_commit_id(&self) -> Result<CommitId, LixError> {
        id(self.request.accepted_checkpoint_commit_id())
    }
    pub(crate) fn application(&self) -> Result<&crate::session::MergeAnalysis, LixError> {
        self.native
            .application
            .as_ref()
            .ok_or_else(|| conflict("incoming changes are already included"))
    }
    pub(crate) fn into_receipt(
        self,
        merge: CommitId,
    ) -> Result<PreparedAuthorityMergeReceipt, LixError> {
        PreparedAuthorityMergeReceipt::new(
            &self.repository_id,
            &self.account_id,
            self.request,
            merge,
            self.control_guards,
        )
    }
}
