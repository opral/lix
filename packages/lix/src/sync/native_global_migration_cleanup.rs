//! Exact surviving-root proof for explicit migration pin cleanup.
use super::{NativeGlobalMigrationReceipt, NativeGlobalMigrationRequest};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};
use crate::{GLOBAL_BRANCH_ID, LixError, changelog::CommitId};
use std::collections::BTreeMap;
pub(crate) struct AuthorizedGlobalMigrationCleanup {
    repository: String,
    account: String,
    request: NativeGlobalMigrationRequest,
    guards: Vec<StoragePrecondition>,
}
fn unresolved() -> LixError {
    LixError::new(
        "LIX_MIGRATION_GLOBAL_CLEANUP_UNRESOLVED",
        "migration roots must remain pinned until every published native head is retained by its current authority ref",
    )
}
pub(crate) async fn authorize_global_migration_cleanup(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &NativeGlobalMigrationRequest,
) -> Result<AuthorizedGlobalMigrationCleanup, LixError> {
    request.validate()?;
    let receipt: NativeGlobalMigrationReceipt =
        super::native_global_migration_receipt::load_native_global_migration_receipt(
            read, repository, account, request,
        )
        .await?
        .ok_or_else(unresolved)?;
    // An immutable M receipt already fences every body import. If cleanup was
    // acknowledged only remotely, later user ref deletion must not prevent retry.
    if let Some(absent) =
        crate::gc::global_migration_pin_absence(read, repository, account, request).await?
    {
        return Ok(AuthorizedGlobalMigrationCleanup {
            repository: repository.into(),
            account: account.into(),
            request: request.clone(),
            guards: vec![absent],
        });
    }
    let mut branches = vec![GLOBAL_BRANCH_ID.to_owned()];
    branches.extend(request.new_branches.iter().map(|b| b.branch_id.clone()));
    let mut heads = vec![receipt.merge_commit_id];
    heads.extend(
        request
            .new_branches
            .iter()
            .map(|b| b.head_commit_id.clone()),
    );
    let observations = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&branches)
        .await?;
    for (head, observation) in heads.iter().zip(&observations) {
        let source = super::partial_merge_analysis::record(
            read,
            CommitId::parse_lix(head, "migration surviving root")?,
            true,
        )
        .await?;
        let current = observation.control.as_ref().ok_or_else(unresolved)?;
        if !super::partial_merge_analysis::bounded_ancestor(
            read,
            &source,
            current.head_commit_id,
            &mut BTreeMap::new(),
            65536,
        )
        .await?
        {
            return Err(unresolved());
        }
    }
    let guards = branches
        .iter()
        .zip(observations)
        .map(|(branch, o)| crate::branch::branch_head_control_precondition(branch, o.raw_token))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AuthorizedGlobalMigrationCleanup {
        repository: repository.into(),
        account: account.into(),
        request: request.clone(),
        guards,
    })
}
impl AuthorizedGlobalMigrationCleanup {
    pub(crate) fn repository(&self) -> &str {
        &self.repository
    }
    pub(crate) fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn request(&self) -> &NativeGlobalMigrationRequest {
        &self.request
    }
    pub(crate) fn guards(&self) -> &[StoragePrecondition] {
        &self.guards
    }
}
