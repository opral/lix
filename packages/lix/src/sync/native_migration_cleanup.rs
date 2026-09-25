//! Exact retirement of an explicit migration's temporary native source root.
//! The repository importer owns the actual ref deletion, cursor event, and
//! durable commit; this owner adds proof/CAS for the unchanged surviving root.
use super::protocol::SyncRefUpdate;
use super::{NativeMigrationMergeRequest, SyncPushRequest};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};
use crate::{LixError, changelog::CommitId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeMigrationCleanupRequest {
    pub migration: NativeMigrationMergeRequest,
}
impl NativeMigrationCleanupRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.migration.validate()
    }
    pub(crate) fn deletion(&self) -> Result<SyncPushRequest, LixError> {
        self.validate()?;
        Ok(SyncPushRequest {
            commits: vec![],
            inline_blobs: vec![],
            ref_updates: vec![SyncRefUpdate {
                branch_id: self.migration.source_branch_id.clone(),
                expected_head_commit_id: Some(
                    self.migration.request.captured_local_head_commit_id.clone(),
                ),
                expected_checkpoint_commit_id: Some(
                    self.migration.request.checkpoint_commit_id.clone(),
                ),
                head_commit_id: None,
                checkpoint_commit_id: None,
            }],
        })
    }
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_MIGRATION_CLEANUP_UNRESOLVED", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "migration cleanup")
}
/// Must run on the SAME final publication read used by normal ref deletion.
/// Returning guards is mandatory even though the selected ref is unchanged.
pub(crate) async fn native_migration_cleanup_guards(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &NativeMigrationCleanupRequest,
    payload: &SyncPushRequest,
) -> Result<Vec<StoragePrecondition>, LixError> {
    request.validate()?;
    let expected = request.deletion()?;
    if !payload.commits.is_empty()
        || !payload.inline_blobs.is_empty()
        || payload.ref_updates != expected.ref_updates
    {
        return Err(invalid(
            "cleanup may only retire its exact temporary source ref",
        ));
    }
    let receipt = super::partial_authority_merge_receipt::load_authority_merge_receipt(
        read,
        repository,
        account,
        &request.migration.request,
    )
    .await?
    .ok_or_else(|| invalid("cleanup lacks an exact durable native merge outcome"))?;
    let branches = [
        request.migration.request.branch_id.clone(),
        request.migration.source_branch_id.clone(),
    ];
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&branches)
        .await?;
    let target = observed[0]
        .control
        .as_ref()
        .ok_or_else(|| invalid("surviving authority branch is absent"))?;
    let merge =
        super::partial_merge_analysis::record(read, id(&receipt.merge_commit_id)?, false).await?;
    if !super::partial_merge_analysis::bounded_ancestor(
        read,
        &merge,
        target.head_commit_id,
        &mut BTreeMap::new(),
        65536,
    )
    .await?
    {
        return Err(invalid(
            "surviving authority branch no longer contains the native migration outcome",
        ));
    }
    if let Some(pin) = observed[1].control.as_ref() {
        if pin.head_commit_id != id(&request.migration.request.captured_local_head_commit_id)?
            || pin.working_diff_checkpoint_commit_id
                != Some(id(&request.migration.request.checkpoint_commit_id)?)
        {
            return Err(invalid(
                "temporary migration ref changed; preserve it for explicit recovery",
            ));
        }
    }
    branches
        .iter()
        .zip(observed)
        .map(|(branch, entry)| {
            crate::branch::branch_head_control_precondition(branch, entry.raw_token)
        })
        .collect()
}
