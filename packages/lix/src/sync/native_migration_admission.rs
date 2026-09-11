//! Explicit migration only. Complete source bodies must already be pinned by a
//! native temporary branch ref accepted through the normal push validator.
//! This admission never widens the browser's KV reconciliation protocol.
use super::partial_authority_merge_receipt::{
    PreparedAuthorityMergeReceipt, load_authority_merge_receipt,
};
use super::partial_merge_analysis::{bounded_ancestor, record};
use super::partial_merge_protocol::{PartialMergeReceipt, PartialMergeRequest};
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};
use crate::{LixError, changelog::CommitId};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) enum NativeMigrationAdmission {
    Committed(PartialMergeReceipt),
    Ready(AdmittedNativeMigrationMerge),
}
/// Constructor-private proof of exact authority/account/branch/source pin.
/// This is consumed by one otherwise empty native transaction.
pub(crate) struct AdmittedNativeMigrationMerge {
    repository: String,
    account: String,
    source_branch: String,
    request: PartialMergeRequest,
    guards: Vec<StoragePrecondition>,
}
fn blocked(message: &str) -> LixError {
    LixError::new("LIX_MIGRATION_MERGE_SCOPE_UNSUPPORTED", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "native migration coordinate")
}
/// Authenticated repository/account come from authority session admission.
/// `source_branch` is a durable migration-journal UUID, not a native object key.
pub(crate) async fn admit_native_migration_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    source_branch: &str,
    request: PartialMergeRequest,
) -> Result<NativeMigrationAdmission, LixError> {
    request.validate()?;
    id(source_branch)?;
    if let Some(receipt) = load_authority_merge_receipt(read, repository, account, &request).await?
    {
        return Ok(NativeMigrationAdmission::Committed(receipt));
    }
    if request.branch_id == crate::GLOBAL_BRANCH_ID
        || source_branch == request.branch_id
        || source_branch == crate::GLOBAL_BRANCH_ID
    {
        return Err(blocked("migration requires a distinct selected source pin"));
    }
    let b = id(&request.base_commit_id)?;
    let r = id(&request.expected_authority_head_commit_id)?;
    let l = id(&request.captured_local_head_commit_id)?;
    let g = id(&request.global_head_commit_id)?;
    if b == l || r == l {
        return Err(blocked(
            "native migration requires local changes and distinct heads; existing inclusion uses a separate native ancestry proof",
        ));
    }
    let ids = [
        request.branch_id.clone(),
        crate::GLOBAL_BRANCH_ID.into(),
        source_branch.into(),
    ];
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&ids)
        .await?;
    let expected = [
        (r, id(&request.checkpoint_commit_id)?),
        (g, id(&request.global_checkpoint_commit_id)?),
        (l, id(&request.checkpoint_commit_id)?),
    ];
    for (index, (entry, (head, checkpoint))) in observed.iter().zip(expected).enumerate() {
        let control = entry
            .control
            .as_ref()
            .ok_or_else(|| blocked("migration source or target pin is absent"))?;
        if control.head_commit_id != head
            || (index != 2 && control.working_diff_checkpoint_commit_id != Some(checkpoint))
        {
            return Err(LixError::new(
                "LIX_PARTIAL_MERGE_AUTHORITY_CHANGED",
                "captured migration coordinates changed",
            ));
        }
    }
    // Ordinary unchanged-checkpoint interval. Historical global bases are only
    // compatible when native ancestry proves fresh branch descriptors are the
    // entire semantic difference; original commit IDs/bases stay unchanged. Migration may
    // read every local commit; limits reject rather than infer missing ancestry.
    let mut cursor = l;
    let mut seen = BTreeSet::new();
    let mut checked_global_bases = BTreeSet::new();
    while cursor != b {
        if seen.len() >= 65536 || !seen.insert(cursor) {
            return Err(blocked("migration local ancestry exceeds limit or cycles"));
        }
        let node = record(read, cursor, true).await?;
        if node.is_checkpoint
            || node.parent_commit_ids.len() != 1
            || node.account_id != account
            || node.base_commit_id.is_none()
        {
            return Err(blocked(
                "migration local suffix changes checkpoint/catalog or author",
            ));
        }
        let basis = node
            .base_commit_id
            .expect("validated selected global basis");
        if basis != g && checked_global_bases.insert(basis) {
            super::migration_global_descriptor_proof::prove_compatible_global_basis(read, basis, g)
                .await?;
        }
        cursor = node.parent_commit_ids[0];
    }
    let base = record(read, b, true).await?;
    if !bounded_ancestor(read, &base, r, &mut BTreeMap::new(), 65536).await? {
        return Err(blocked(
            "authority head does not descend from captured base",
        ));
    }
    let mut guards = ids
        .iter()
        .zip(observed)
        .map(|(branch, entry)| {
            crate::branch::branch_head_control_precondition(branch, entry.raw_token)
        })
        .collect::<Result<Vec<_>, _>>()?;
    guards.push(super::require_unrestarted_attempt(read, repository, account, &request).await?);
    Ok(NativeMigrationAdmission::Ready(
        AdmittedNativeMigrationMerge {
            repository: repository.into(),
            account: account.into(),
            source_branch: source_branch.into(),
            request,
            guards,
        },
    ))
}
impl AdmittedNativeMigrationMerge {
    pub(crate) fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn branch(&self) -> &str {
        &self.request.branch_id
    }
    pub(crate) fn source_branch(&self) -> &str {
        &self.source_branch
    }
    pub(crate) fn base(&self) -> Result<CommitId, LixError> {
        id(&self.request.base_commit_id)
    }
    pub(crate) fn remote(&self) -> Result<CommitId, LixError> {
        id(&self.request.expected_authority_head_commit_id)
    }
    pub(crate) fn local(&self) -> Result<CommitId, LixError> {
        id(&self.request.captured_local_head_commit_id)
    }
    pub(crate) fn into_receipt(
        self,
        merge: CommitId,
    ) -> Result<PreparedAuthorityMergeReceipt, LixError> {
        // Source ref remains a GC root throughout this atomic CAS. Its cleanup
        // is a separate exact-ref operation after durable outcome recovery.
        PreparedAuthorityMergeReceipt::new(
            &self.repository,
            &self.account,
            self.request,
            merge,
            self.guards,
        )
    }
}
