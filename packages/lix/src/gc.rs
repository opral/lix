//! Checkpoint control and recovery compatibility-free state.
//!
//! Physical reachability collection is owned exclusively by
//! `forktree::advance_gc`.  This module only carries recovery metadata that is
//! published alongside semantic checkpoint
//! commits; it never plans object deletion or owns GC progress.

use crate::LixError;
use crate::changelog::CommitId;
use crate::forktree::snapshot_selector_key;
use crate::forktree::{
    CanonicalBranchId, PreparedPublication, SelectorExpectation, SnapshotRole, SnapshotSelectorId,
    SnapshotSelectorV1, SnapshotTargetV1, open_coherent_view_on_read,
};
use crate::storage_adapter::StorageAdapterRead;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointRecoveryRef {
    pub(crate) branch_id: String,
    pub(crate) recovered_head_commit_id: CommitId,
    pub(crate) interval_has_commits: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointPublication {
    pub(crate) recovery_ref: CheckpointRecoveryRef,
}

/// Stages one stable per-branch recovery selector in the same authenticated
/// publication as the checkpoint commit. The prior selector is CASed away, so
/// recovery retention has one canonical owner and never creates a duplicate
/// untracked progress/root row.
pub(crate) async fn stage_checkpoint_publication<R>(
    publication: &mut PreparedPublication,
    view: &crate::forktree::CoherentView<R>,
    checkpoint: &CheckpointPublication,
) -> Result<(), LixError>
where
    R: StorageAdapterRead,
{
    let branch_id = CanonicalBranchId::from_bytes(
        *uuid::Uuid::parse_str(&checkpoint.recovery_ref.branch_id)
            .map_err(|error| LixError::new(LixError::CODE_STORAGE_ERROR, error.to_string()))?
            .as_bytes(),
    );
    if branch_id != view.branch_id() {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "checkpoint recovery selector branch does not match its retained view",
        ));
    }
    let retained_head_object_id = view.branch_snapshot().semantic_head_commit_object_id;
    let retained_head_bytes = view
        .load_object_bytes(retained_head_object_id)
        .await
        .map_err(LixError::from)?;
    let retained_head =
        crate::forktree::CommitObjectV1::decode(retained_head_object_id, &retained_head_bytes)
            .map_err(LixError::from)?;
    view.validate_retained_commit(
        view.repository_root().commit_catalog_root,
        view.repository_root().change_catalog_root,
        retained_head_object_id,
        &retained_head,
    )
    .await
    .map_err(LixError::from)?;
    let retained_head = CommitId::new(uuid::Uuid::from_bytes(*retained_head.commit_id.as_bytes()));
    if retained_head != checkpoint.recovery_ref.recovered_head_commit_id {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "checkpoint recovery head does not match its retained authenticated view",
        ));
    }
    if !checkpoint.recovery_ref.interval_has_commits {
        // An empty checkpoint must not rotate the sole recovery root. Keeping
        // the existing selector preserves the last non-empty recoverable
        // interval without reintroducing a cadence/progress row.
        return Ok(());
    }
    let selector_id = SnapshotSelectorId::from_bytes(*branch_id.as_bytes());
    let key = snapshot_selector_key(SnapshotRole::Recovery, selector_id);
    let expected = view
        .load_selector_value(&key)
        .await?
        .map(SelectorExpectation::Equals)
        .unwrap_or(SelectorExpectation::Absent);
    publication
        .publish_current_snapshot_pin(view, SnapshotRole::Recovery, selector_id, expected)
        .map(|_| ())
        .map_err(LixError::from)
}

fn global_branch_id() -> CanonicalBranchId {
    CanonicalBranchId::from_bytes(
        *uuid::Uuid::parse_str(crate::GLOBAL_BRANCH_ID)
            .expect("global branch ID is canonical")
            .as_bytes(),
    )
}

pub(crate) async fn load_checkpoint_publication_state<R>(
    read: &R,
    branch_id: &str,
) -> Result<Option<CheckpointRecoveryRef>, LixError>
where
    R: StorageAdapterRead + ?Sized,
{
    let canonical_branch = CanonicalBranchId::from_bytes(
        *uuid::Uuid::parse_str(branch_id)
            .map_err(|error| LixError::new(LixError::CODE_STORAGE_ERROR, error.to_string()))?
            .as_bytes(),
    );
    let view = open_coherent_view_on_read(read, global_branch_id()).await?;
    let selector_id = SnapshotSelectorId::from_bytes(*canonical_branch.as_bytes());
    let key = snapshot_selector_key(SnapshotRole::Recovery, selector_id);
    let Some(raw_selector) = view.load_selector_value(&key).await? else {
        return Ok(None);
    };
    let selector = SnapshotSelectorV1::decode(&raw_selector).map_err(LixError::from)?;
    if selector.role != SnapshotRole::Recovery || selector.selector_id != selector_id {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "recovery selector identity is inconsistent",
        ));
    }
    let target_bytes = view
        .load_object_bytes(selector.target_object_id)
        .await
        .map_err(LixError::from)?;
    let target = SnapshotTargetV1::decode(selector.target_object_id, &target_bytes)
        .map_err(LixError::from)?;
    if target.role != SnapshotRole::Recovery
        || target.selector_id != selector_id
        || target.branch_id != canonical_branch
    {
        return Err(LixError::new(
            LixError::CODE_STORAGE_ERROR,
            "recovery target identity is inconsistent",
        ));
    }
    let commit_bytes = view
        .load_object_bytes(target.semantic_commit_object_id)
        .await
        .map_err(LixError::from)?;
    let commit =
        crate::forktree::CommitObjectV1::decode(target.semantic_commit_object_id, &commit_bytes)
            .map_err(LixError::from)?;
    let recovered_head_commit_id =
        CommitId::new(uuid::Uuid::from_bytes(*commit.commit_id.as_bytes()));
    Ok(Some(CheckpointRecoveryRef {
        branch_id: branch_id.to_owned(),
        recovered_head_commit_id,
        interval_has_commits: true,
    }))
}
