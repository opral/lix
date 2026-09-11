//! Constructor-private evidence for one atomic descriptor merge and new-ref set.
use super::migration_global_descriptor_proof::{
    prove_compatible_global_basis, prove_descriptor_only_global_merge,
};
use super::native_global_migration_receipt::{
    PreparedNativeGlobalMigrationReceipt, load_native_global_migration_receipt,
};
use super::partial_merge_analysis::{bounded_ancestor, record};
use super::{NativeGlobalMigrationReceipt, NativeGlobalMigrationRequest};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};
use crate::{GLOBAL_BRANCH_ID, LixError, changelog::CommitId};
use std::collections::{BTreeMap, BTreeSet};
pub(crate) enum NativeGlobalMigrationAdmission {
    Committed(NativeGlobalMigrationReceipt),
    Ready(AdmittedNativeGlobalMigration),
}
pub(crate) struct AdmittedNativeGlobalMigration {
    repository: String,
    account: String,
    request: NativeGlobalMigrationRequest,
    guards: Vec<StoragePrecondition>,
}
fn blocked(s: &str) -> LixError {
    LixError::new("LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED", s)
}
fn id(s: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(s, "global migration coordinate")
}
pub(crate) async fn admit_native_global_migration(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: NativeGlobalMigrationRequest,
) -> Result<NativeGlobalMigrationAdmission, LixError> {
    request.validate()?;
    // Exact immutable outcome wins after publication, even after later ref edits.
    if let Some(receipt) =
        load_native_global_migration_receipt(read, repository, account, &request).await?
    {
        return Ok(NativeGlobalMigrationAdmission::Committed(receipt));
    }
    let abort_guard =
        super::require_unaborted_global_migration(read, repository, account, &request).await?;
    let mut branches = vec![GLOBAL_BRANCH_ID.to_owned()];
    branches.extend(request.new_branches.iter().map(|b| b.branch_id.clone()));
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&branches)
        .await?;
    let global = observed[0]
        .control
        .as_ref()
        .ok_or_else(|| blocked("global authority ref absent"))?;
    if global.head_commit_id != id(&request.expected_authority_head_commit_id)?
        || global.working_diff_checkpoint_commit_id != Some(id(&request.checkpoint_commit_id)?)
    {
        return Err(blocked(
            "authority global coordinate changed before migration publication",
        ));
    }
    if observed
        .iter()
        .skip(1)
        .any(|o| o.control.is_some() || o.raw_token.is_some())
    {
        return Err(blocked("new branch identity already has an authority ref"));
    }
    prove_descriptor_only_global_merge(
        read,
        id(&request.base_commit_id)?,
        id(&request.expected_authority_head_commit_id)?,
        id(&request.captured_local_head_commit_id)?,
        account,
        &request.branch_ids(),
    )
    .await?;
    let mut prepared_bases = BTreeSet::new();
    for branch in &request.new_branches {
        let c = id(&branch.checkpoint_commit_id)?;
        let l = id(&branch.head_commit_id)?;
        let head = crate::tracked_state::load_published_commit_state_topology(read, l)
            .await?
            .ok_or_else(|| blocked("new branch complete native head is absent"))?;
        if head.global_scope() {
            return Err(blocked("new selected branch cannot point at a global root"));
        }
        let checkpoint = record(read, c, true).await?;
        if !bounded_ancestor(read, &checkpoint, l, &mut BTreeMap::new(), 65536).await? {
            return Err(blocked(
                "new branch checkpoint is not an ancestor of its retained head",
            ));
        }
        let mut cursor = l;
        let mut seen = BTreeSet::new();
        loop {
            if seen.len() >= 65536 || !seen.insert(cursor) {
                return Err(blocked(
                    "new branch ancestry exceeds explicit migration bound",
                ));
            }
            let node = record(read, cursor, true).await?;
            let basis = node
                .base_commit_id
                .ok_or_else(|| blocked("new selected native head lacks a global basis"))?;
            if prepared_bases.insert(basis) {
                prove_compatible_global_basis(
                    read,
                    basis,
                    id(&request.captured_local_head_commit_id)?,
                )
                .await?;
            }
            if cursor == c {
                break;
            }
            if node.is_checkpoint || node.account_id != account || node.parent_commit_ids.len() != 1
            {
                return Err(blocked(
                    "new selected suffix is not an ordinary account-stable interval",
                ));
            }
            cursor = node.parent_commit_ids[0];
        }
    }
    let mut guards = branches
        .iter()
        .zip(observed)
        .map(|(branch, o)| crate::branch::branch_head_control_precondition(branch, o.raw_token))
        .collect::<Result<Vec<_>, _>>()?;
    guards.push(abort_guard);
    guards.push(
        crate::gc::require_complete_global_migration_pin(read, repository, account, &request)
            .await?,
    );
    Ok(NativeGlobalMigrationAdmission::Ready(
        AdmittedNativeGlobalMigration {
            repository: repository.into(),
            account: account.into(),
            request,
            guards,
        },
    ))
}
impl AdmittedNativeGlobalMigration {
    pub(crate) fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn request(&self) -> &NativeGlobalMigrationRequest {
        &self.request
    }
    pub(crate) fn into_receipt(
        self,
        merge: CommitId,
    ) -> Result<PreparedNativeGlobalMigrationReceipt, LixError> {
        PreparedNativeGlobalMigrationReceipt::new(
            &self.repository,
            &self.account,
            self.request,
            merge,
            self.guards,
        )
    }
}
