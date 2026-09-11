//! Finite immutable jump-spine preparation for repeated local appends.
//!
//! This is demand-time preparation, never bootstrap work. Only the two admitted
//! baseline heads and their Myers jump edges are followed; ordinary parents,
//! merge parents and repository inventories are not enumerated.

use std::collections::BTreeMap;
use std::future::Future;

use super::partial_state::PartialReplicaState;
use crate::LixError;
use crate::changelog::{ChangelogReader as _, CommitId, CommitLoadRequest, CommitRecord};
use crate::storage_adapter::{Storage, StorageAdapter};
use crate::tracked_state::NativeMetadataRef;

// Resource policy, not a native-format ancestry limit. A malformed or unusually
// large frontier fails explicitly rather than silently preparing only a prefix.
const MAX_FRONTIER_RECORDS: usize = 256;

/// Return the first absent graph address, or `None` only after both complete
/// spines validate in one pinned local read. No network or publication occurs.
pub(crate) async fn next_missing_baseline_write_frontier<S>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
) -> Result<Option<NativeMetadataRef>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let invalid = |message: &str| LixError::new("LIX_PARTIAL_WRITE_FRONTIER_INVALID", message);
    let read = storage.begin_read(Default::default()).await?;
    if super::partial_state::load_partial_replica_state(&read)
        .await?
        .as_ref()
        .map(|(actual, _)| actual)
        != Some(state)
    {
        return Err(invalid("baseline write preparation admission is stale"));
    }
    next_missing_candidate_write_frontier(&read, state).await
}

/// Validate immutable candidate graph inputs under the caller's pinned read.
/// The unpublished target need not equal the current receipt; caller retains
/// exact source receipt/control guards through eventual publication.
pub(crate) async fn next_missing_candidate_write_frontier(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
) -> Result<Option<NativeMetadataRef>, LixError> {
    let invalid = |message: &str| LixError::new("LIX_PARTIAL_WRITE_FRONTIER_INVALID", message);
    let mut prepared = BTreeMap::<CommitId, CommitRecord>::new();
    for branch in [
        &state.descriptor().selected_branch,
        &state.descriptor().global_branch,
    ] {
        let mut cursor = CommitId::parse(&branch.head.commit_id)
            .map_err(|_| invalid("invalid admitted baseline head"))?;
        let mut previous: Option<CommitRecord> = None;
        loop {
            let record = if let Some(record) = prepared.get(&cursor) {
                record.clone()
            } else {
                if prepared.len() >= MAX_FRONTIER_RECORDS {
                    return Err(invalid("baseline jump frontier exceeds preparation budget"));
                }
                let ids = [cursor];
                let record = crate::changelog::ChangelogContext::new()
                    .reader(read)
                    .load_commits(CommitLoadRequest { commit_ids: &ids })
                    .await?
                    .into_iter()
                    .next()
                    .and_then(|(_, record)| record);
                match record {
                    Some(record) => record,
                    None => {
                        return Ok(Some(NativeMetadataRef::CommitGraphRecord(
                            cursor.to_string(),
                        )));
                    }
                }
            };
            if record.commit_id != cursor {
                return Err(invalid("graph record does not match requested address"));
            }
            if let Some(previous) = &previous {
                if previous.generation.checked_sub(record.generation)
                    != Some(previous.first_parent_jump_span)
                    || previous.first_parent_jump_span == 0
                {
                    return Err(invalid(
                        "baseline jump edge has inconsistent generation or span",
                    ));
                }
            }
            if prepared.contains_key(&cursor) {
                break;
            }
            prepared.insert(cursor, record.clone());
            if record.first_parent_jump_commit_id == cursor {
                if record.first_parent_jump_span != 0 {
                    return Err(invalid("self jump must have zero span"));
                }
                break;
            }
            if record.first_parent_jump_span == 0 {
                return Err(invalid("non-self jump must make generation progress"));
            }
            cursor = record.first_parent_jump_commit_id;
            previous = Some(record);
        }
    }
    Ok(None)
}

/// Hydrate the finite frontier through the caller's epoch-fenced native installer.
/// Recheck from a fresh snapshot after each installation; the frontier is small
/// and this avoids combining records read under different receipt epochs.
pub(crate) async fn prepare_baseline_write_frontier<S, F, Fut>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    mut hydrate: F,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: FnMut(NativeMetadataRef) -> Fut,
    Fut: Future<Output = Result<(), LixError>>,
{
    let mut requested = std::collections::BTreeSet::new();
    while let Some(address) = next_missing_baseline_write_frontier(storage, state).await? {
        if requested.len() >= MAX_FRONTIER_RECORDS || !requested.insert(address.clone()) {
            return Err(LixError::new(
                "LIX_PARTIAL_WRITE_FRONTIER_INVALID",
                "baseline graph hydration exceeded budget or made no progress",
            ));
        }
        hydrate(address).await?;
    }
    Ok(())
}
