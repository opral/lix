//! Changelog-owned retirement of canonical records.
//!
//! Garbage collection proves unreachability from refs; it does not know which
//! physical rows a commit projection or a standalone change occupies. These
//! entry points keep that knowledge inside the module that writes those rows,
//! so `changelog` remains the sole writer of its own storage spaces.

use bytes::Bytes;

use super::context::ChangelogContext;
use super::store::{CHANGE_SPACE, COMMIT_SPACE, ChangelogReader, change_key, commit_key};
use super::types::{ChangeId, CommitId, CommitLoadRequest};
use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StorageWriteSet,
};

/// Removes the semantic commit projection once its root interval is no longer
/// reachable. Physical tracked-state authority may remain alive as a selected
/// source or serving dependency, so this decision is intentionally separate
/// from manifest/CAS retirement.
pub(crate) async fn stage_delete_commit_projection<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    commit_id: CommitId,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let commit_ids = [commit_id];
    let record = ChangelogContext::new()
        .reader(store)
        .load_commits(CommitLoadRequest {
            commit_ids: &commit_ids,
        })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, record)| record);
    let Some(record) = record else {
        // A prior GC pass may already have removed the semantic projection
        // while its physical authority remained pinned.
        return Ok(());
    };
    if record.commit_id != commit_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit projection key for '{commit_id}' contains '{}'",
                record.commit_id
            ),
        ));
    }
    writes.delete(COMMIT_SPACE, StorageKey(Bytes::from(commit_key(commit_id))));
    writes.delete(
        CHANGE_SPACE,
        StorageKey(Bytes::from(change_key(record.change_id()))),
    );
    Ok(())
}

/// Removes one standalone change record whose last owning branch control has
/// been retired.
///
/// The record must still exist: a retired control naming an absent standalone
/// fact means the ledger and the control plane already disagree, and deleting
/// nothing would hide that.
pub(crate) async fn stage_delete_standalone_change<S>(
    store: &S,
    writes: &mut StorageWriteSet,
    change_id: ChangeId,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let key = StorageKey(Bytes::from(change_key(change_id)));
    let existing = PointReadPlan::new(CHANGE_SPACE, std::slice::from_ref(&key))
        .materialize(store, StorageGetOptions::default())
        .await?
        .value
        .into_iter()
        .next()
        .flatten();
    if existing.is_none() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("retired branch control references missing standalone change '{change_id}'"),
        ));
    }
    writes.delete(CHANGE_SPACE, key);
    Ok(())
}

/// Removes canonical commit projections in bulk.
///
/// Only the whole-repository oracle collector reaches this: ordinary GC
/// retires one proven-unreachable projection at a time through
/// [`stage_delete_commit_projection`].
#[cfg(test)]
pub(crate) fn stage_delete_commits(
    writes: &mut StorageWriteSet,
    commit_ids: impl IntoIterator<Item = CommitId>,
) {
    writes.delete_batch(COMMIT_SPACE, commit_ids.into_iter().map(commit_key));
}

/// Removes change records in bulk.
#[cfg(test)]
pub(crate) fn stage_delete_changes(
    writes: &mut StorageWriteSet,
    change_ids: impl IntoIterator<Item = ChangeId>,
) {
    writes.delete_batch(CHANGE_SPACE, change_ids.into_iter().map(change_key));
}
