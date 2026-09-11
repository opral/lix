//! Explicit migration proof: the only changed global facts are new branch
//! descriptors. No catalog, plugin, account, checkpoint or default-branch fact
//! may be discarded or silently admitted by this narrow lane.
use super::partial_merge_analysis::{bounded_ancestor, record};
use crate::storage_adapter::StorageAdapterRead;
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDiff, TrackedStateDiffKind, TrackedStateDiffRequest,
};
use crate::{LixError, changelog::CommitId};
use std::collections::{BTreeMap, BTreeSet};
fn unsupported(message: &str) -> LixError {
    LixError::new("LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED", message)
}

pub(crate) struct DescriptorOnlyGlobalProof {
    _base: CommitId,
    _remote: CommitId,
    _local: CommitId,
    _local_descriptors: TrackedStateDiff,
    _local_branch_ids: BTreeSet<String>,
}
fn added_branch_ids(diff: &TrackedStateDiff) -> Result<BTreeSet<String>, LixError> {
    let mut ids = BTreeSet::new();
    for entry in &diff.entries {
        if entry.identity.schema_key() != crate::branch::BRANCH_DESCRIPTOR_SCHEMA_KEY
            || entry.identity.file_id().is_some()
            || entry.kind != TrackedStateDiffKind::Added
            || entry.before.is_some()
            || entry.after.as_ref().is_none_or(|row| row.deleted)
        {
            return Err(unsupported(
                "global migration contains more than fresh branch descriptors",
            ));
        }
        let id = entry.identity.row_pk().as_single_string_owned()?;
        crate::storage_codec::id_string::uuid_bytes_from_canonical(&id)
            .ok_or_else(|| unsupported("branch descriptor key is not canonical UUID"))?;
        if id == crate::GLOBAL_BRANCH_ID || !ids.insert(id) {
            return Err(unsupported("global branch descriptor set is invalid"));
        }
    }
    Ok(ids)
}
pub(crate) async fn prove_descriptor_only_global_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    base: CommitId,
    remote: CommitId,
    local: CommitId,
    account: &str,
    expected_new_branches: &BTreeSet<String>,
) -> Result<DescriptorOnlyGlobalProof, LixError> {
    let base_node = record(read, base, true).await?;
    for key in [base, remote, local] {
        let header = crate::tracked_state::load_published_commit_state_topology(read, key)
            .await?
            .ok_or_else(|| unsupported("global merge endpoint header absent"))?;
        let graph = record(read, key, true).await?;
        if !header.global_scope() || graph.base_commit_id.is_some() {
            return Err(unsupported(
                "global merge endpoint is not a native global commit",
            ));
        }
    }
    let mut cursor = local;
    let mut seen = BTreeSet::new();
    while cursor != base {
        if seen.len() >= 65536 || !seen.insert(cursor) {
            return Err(unsupported(
                "local global ancestry exceeds its migration budget",
            ));
        }
        let node = record(read, cursor, true).await?;
        let header = crate::tracked_state::load_published_commit_state_topology(read, cursor)
            .await?
            .ok_or_else(|| unsupported("global source header absent"))?;
        if node.is_checkpoint
            || node.base_commit_id.is_some()
            || node.parent_commit_ids.len() != 1
            || node.account_id != account
            || !header.global_scope()
        {
            return Err(unsupported(
                "global source is not an ordinary account-stable global suffix",
            ));
        }
        cursor = node.parent_commit_ids[0];
    }
    if !bounded_ancestor(read, &base_node, remote, &mut BTreeMap::new(), 65536).await? {
        return Err(unsupported(
            "remote global head does not descend from the captured base",
        ));
    }
    let context = TrackedStateContext::new();
    let mut reader = context.reader(read);
    let local_diff = reader
        .diff_commit_members(
            &base.to_string(),
            &local.to_string(),
            &TrackedStateDiffRequest::default(),
        )
        .await?;
    let remote_diff = reader
        .diff_commit_members(
            &base.to_string(),
            &remote.to_string(),
            &TrackedStateDiffRequest::default(),
        )
        .await?;
    let local_ids = added_branch_ids(&local_diff)?;
    let remote_ids = added_branch_ids(&remote_diff)?;
    if &local_ids != expected_new_branches || !local_ids.is_disjoint(&remote_ids) {
        return Err(unsupported(
            "global descriptor additions do not exactly match isolated new source branches",
        ));
    }
    Ok(DescriptorOnlyGlobalProof {
        _base: base,
        _remote: remote,
        _local: local,
        _local_descriptors: local_diff,
        _local_branch_ids: local_ids,
    })
}

/// A selected commit's original global basis is preserved, never rewritten to
/// current G. This proof only permits an ancestral basis whose account, schema
/// and plugin facts are unchanged; added branch descriptors are the sole delta.
pub(crate) async fn prove_compatible_global_basis(
    read: &(impl StorageAdapterRead + ?Sized),
    basis: CommitId,
    current: CommitId,
) -> Result<(), LixError> {
    if basis == current {
        return Ok(());
    }
    let basis_node = record(read, basis, true).await?;
    if !bounded_ancestor(read, &basis_node, current, &mut BTreeMap::new(), 65536).await? {
        return Err(unsupported(
            "historical global basis is not an ancestor of current global head",
        ));
    }
    for key in [basis, current] {
        let header = crate::tracked_state::load_published_commit_state_topology(read, key)
            .await?
            .ok_or_else(|| unsupported("global basis header absent"))?;
        if !header.global_scope() {
            return Err(unsupported("selected basis is not a native global root"));
        }
    }
    let context = TrackedStateContext::new();
    let mut reader = context.reader(read);
    let diff = reader
        .diff_commit_members(
            &basis.to_string(),
            &current.to_string(),
            &TrackedStateDiffRequest::default(),
        )
        .await?;
    added_branch_ids(&diff)?;
    Ok(())
}

/// Validate only immutable local B→L coordinates before the explicit migration
/// performs network I/O. R=B here is a local validation baseline, never a ref.
pub(crate) async fn prove_local_descriptor_global_source(
    read: &(impl StorageAdapterRead + ?Sized),
    request: &super::NativeGlobalMigrationRequest,
    account: &str,
) -> Result<(), LixError> {
    request.validate()?;
    let b = CommitId::parse_lix(&request.base_commit_id, "global source base")?;
    let l = CommitId::parse_lix(
        &request.captured_local_head_commit_id,
        "global source head",
    )?;
    prove_descriptor_only_global_merge(read, b, b, l, account, &request.branch_ids()).await?;
    Ok(())
}
