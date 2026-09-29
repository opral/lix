//! The durable, bounded ownership witness for each admitted serving branch.
//!
//! A receipt describes the remote basis. A branch control and HOT root may
//! advance locally. The storage adapter publishes this record in the same
//! atomic write as either coordinate, after checking the complete postimage.

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::LixError;
use crate::branch::{BRANCH_HEAD_CONTROL_SPACE, BranchHeadControlContext};
use crate::changelog::CommitId;
use crate::commit_graph::CommitGraphContext;
use crate::hot_state::{ROOT_CURRENT_BASE_SPACE, hot_generation_scope_prefix};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageGetOptions, StorageKey, StoragePrecondition,
    StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet,
    ValueSemantics,
};

use super::partial_candidate_prepare::CandidateRead;
use super::partial_state::{PARTIAL_REPLICA_STATE_SPACE, load_partial_replica_state};

pub(crate) const PARTIAL_SERVING_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0023),
    "sync.partial_serving.v1",
    ValueSemantics::Mutable,
);

pub(crate) fn has_coordinate_mutations(writes: &StorageWriteSet) -> bool {
    [
        PARTIAL_REPLICA_STATE_SPACE,
        BRANCH_HEAD_CONTROL_SPACE,
        ROOT_CURRENT_BASE_SPACE,
        PARTIAL_SERVING_SPACE,
        crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
    ]
    .into_iter()
    .any(|space| writes.has_mutations_in_space(space))
        || writes.has_deletions_in_space(
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
        )
}

/// Append-only ancestry is required for every admitted partial write, even
/// when no serving coordinate changes. The collector alone may remove nodes
/// after sealing its retained-root proof.
pub(crate) fn commit_graph_guards(
    writes: &StorageWriteSet,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let commits = crate::changelog::COMMIT_SPACE;
    if writes.has_deletions_in_space(commits) && !writes.changelog_gc_is_sealed() {
        return Err(mismatch(
            "partial serving commit graph deletion requires certified garbage collection",
        ));
    }
    Ok(writes
        .declared_keys(commits)
        .into_iter()
        .filter(|key| writes.contains_put(commits, key))
        .map(|key| StoragePrecondition::KeyAbsent {
            space: commits,
            key: StorageKey(Bytes::from(key)),
        })
        .collect())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialServing {
    version: u32,
    epoch_id: String,
    branch_id: String,
    admitted_base: String,
    head: String,
    generation: String,
    root: String,
    local_root_owned: bool,
}

fn mismatch(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH", message)
}

fn key(branch_id: &str) -> Result<StorageKey, LixError> {
    let bytes = crate::storage_codec::id_string::uuid_bytes_from_canonical(branch_id)
        .ok_or_else(|| mismatch("partial serving branch ID is invalid"))?;
    Ok(StorageKey(Bytes::copy_from_slice(&bytes)))
}

pub(crate) async fn load(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
) -> Result<Option<(PartialServing, Bytes)>, LixError> {
    let value = PointReadPlan::new(PARTIAL_SERVING_SPACE, &[key(branch_id)?])
        .materialize(read, StorageGetOptions::default())
        .await?
        .value
        .pop()
        .flatten();
    match value {
        None => Ok(None),
        Some(StorageProjectedValue::FullValue(bytes)) => {
            if bytes.len() > 512 {
                return Err(mismatch("partial serving record exceeds its bound"));
            }
            let record: PartialServing = serde_json::from_slice(&bytes)
                .map_err(|_| mismatch("partial serving record is malformed"))?;
            if record.version != 1 || record.branch_id != branch_id {
                return Err(mismatch("partial serving record has another owner"));
            }
            Ok(Some((record, bytes)))
        }
        Some(_) => Err(mismatch("partial serving read omitted its value")),
    }
}

async fn root_marker(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    generation: CommitId,
) -> Result<(CommitId, Bytes), LixError> {
    let marker_key = StorageKey(Bytes::from(hot_generation_scope_prefix(branch_id, generation)));
    let value = PointReadPlan::new(ROOT_CURRENT_BASE_SPACE, &[marker_key])
        .materialize(read, StorageGetOptions::default())
        .await?
        .value
        .pop()
        .flatten();
    let Some(StorageProjectedValue::FullValue(bytes)) = value else {
        return Err(mismatch("partial serving root marker is absent"));
    };
    let root = uuid::Uuid::from_slice(&bytes)
        .map(CommitId::from)
        .map_err(|_| mismatch("partial serving root marker is malformed"))?;
    Ok((root, bytes))
}

async fn head_reaches_base(
    read: &(impl StorageAdapterRead + ?Sized),
    mut head: CommitId,
    base: CommitId,
) -> Result<bool, LixError> {
    let mut graph = CommitGraphContext::new().reader(read);
    let mut previous_generation = None;
    while head != base {
        let Some(node) = graph.load_node(&head).await? else {
            return Ok(false);
        };
        if previous_generation.is_some_and(|generation| node.generation >= generation) {
            return Ok(false);
        }
        let Some(parent) = node.parent_commit_ids.first() else {
            return Ok(false);
        };
        previous_generation = Some(node.generation);
        head = *parent;
    }
    Ok(true)
}

/// Verify a native local root against the resident first-parent interval.
/// This full walk is used only for legacy migration or a root transition.
pub(crate) async fn local_root_is_owned(
    read: &(impl StorageAdapterRead + ?Sized),
    head: CommitId,
    root: CommitId,
    base: CommitId,
) -> Result<bool, LixError> {
    let headers = crate::tracked_state::load_commit_state_authority_ids(read, &[root]).await?;
    if headers.into_iter().all(|header| header.is_none()) {
        return Ok(false);
    }
    let mut graph = CommitGraphContext::new().reader(read);
    let mut cursor = head;
    let mut previous_generation = None;
    let mut saw_root = false;
    while cursor != base {
        let Some(node) = graph.load_node(&cursor).await? else {
            return Ok(false);
        };
        if previous_generation.is_some_and(|generation| node.generation >= generation) {
            return Ok(false);
        }
        saw_root |= cursor == root;
        let Some(parent) = node.parent_commit_ids.first() else {
            return Ok(false);
        };
        previous_generation = Some(node.generation);
        cursor = *parent;
    }
    Ok(saw_root)
}

/// A checkpoint is a semantic boundary: its first parent is the previous
/// checkpoint, while its physical state aliases the head it recovered. The
/// durable recovery ref is published in the same transaction as the new head.
async fn checkpoint_extends_prior(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    head: CommitId,
    prior_head: CommitId,
) -> Result<bool, LixError> {
    let Some(recovery) = crate::gc::load_recovery_ref(read, branch_id).await? else {
        return Ok(false);
    };
    if recovery.recovered_head_commit_id != prior_head {
        return Ok(false);
    }
    let mut graph = CommitGraphContext::new().reader(read);
    let Some(checkpoint) = graph.load_node(&recovery.checkpoint_commit_id).await? else {
        return Ok(false);
    };
    if !checkpoint.is_checkpoint {
        return Ok(false);
    }
    if recovery.checkpoint_commit_id == head {
        return Ok(true);
    }
    let node = graph.load_node(&head).await?;
    Ok(node.is_some_and(|node| {
        node.parent_commit_ids.first() == Some(&recovery.checkpoint_commit_id)
    }))
}

/// Validate the atomic postimage and stage any changed serving witnesses.
/// This is called by the adapter for every admitted partial write. Ordinary
/// writes without coordinate mutations take the constant-time empty path.
pub(crate) async fn prepare_write(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: StorageWriteSet,
    migrate_missing: bool,
) -> Result<(StorageWriteSet, Vec<StoragePrecondition>), LixError> {
    if !migrate_missing && !has_coordinate_mutations(&writes) {
        return Ok((writes, Vec::new()));
    }
    if [
        PARTIAL_REPLICA_STATE_SPACE,
        BRANCH_HEAD_CONTROL_SPACE,
        ROOT_CURRENT_BASE_SPACE,
        PARTIAL_SERVING_SPACE,
        crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
    ]
    .into_iter()
    .any(|space| writes.has_range_delete_in_space(space))
        || writes.staged_delete(
            PARTIAL_REPLICA_STATE_SPACE,
            &super::partial_replica_state_key().0,
        )
    {
        return Err(mismatch("partial serving coordinates cannot be range-deleted"));
    }
    let writes = Arc::new(writes);
    let overlay = CandidateRead {
        base: read,
        staged: Arc::clone(&writes),
    };
    let Some((state, state_bytes)) = load_partial_replica_state(&overlay).await? else {
        // Fresh non-partial storage has no admission to protect.
        drop(overlay);
        return Ok((Arc::try_unwrap(writes).expect("overlay dropped"), Vec::new()));
    };
    let previous_state = if writes.contains_put(
        PARTIAL_REPLICA_STATE_SPACE,
        &super::partial_replica_state_key().0,
    ) {
        load_partial_replica_state(read).await?
    } else {
        Some((state.clone(), state_bytes))
    };
    if let Some((previous, _)) = &previous_state {
        // An existing witness is never silently repaired by a later write.
        // Missing witnesses are the sole legacy migration case.
        assert_admitted(read, previous).await?;
    }
    let mut retired_recovery_refs = Vec::new();
    if writes.has_mutations_in_space(crate::gc::CHECKPOINT_RECOVERY_REF_SPACE) {
        let allowed = [
            crate::gc::recovery_ref_key(&state.descriptor().selected_branch.branch_id)?,
            crate::gc::recovery_ref_key(&state.descriptor().global_branch.branch_id)?,
        ];
        for key in writes.declared_keys(crate::gc::CHECKPOINT_RECOVERY_REF_SPACE) {
            if !allowed.contains(&key) {
                let branch_id = crate::gc::recovery_ref_branch_id(&key)?;
                let control_key = crate::branch::branch_head_control_key(&branch_id)?;
                if !writes.staged_delete(crate::gc::CHECKPOINT_RECOVERY_REF_SPACE, &key)
                    || !writes.staged_delete(BRANCH_HEAD_CONTROL_SPACE, &control_key)
                {
                    return Err(mismatch(
                        "partial serving can retire an unrelated recovery ref only with its branch control",
                    ));
                }
                retired_recovery_refs.push((branch_id, key));
            }
        }
    }
    if writes.has_deletions_in_space(
        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
    ) {
        let mut visited = std::collections::BTreeSet::new();
        for branch in [&state.descriptor().selected_branch, &state.descriptor().global_branch] {
            if !visited.insert(&branch.branch_id) {
                continue;
            }
            if let Some((serving, _)) = load(read, &branch.branch_id).await? {
                if serving.local_root_owned {
                    let root = CommitId::parse_lix(&serving.root, "partial serving root")?;
                    let root_key = crate::tracked_state::commit_state_authority_key(root);
                    if writes.staged_delete(
                        crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                        &root_key.0,
                    ) {
                        return Err(mismatch("partial serving native root authority was deleted"));
                    }
                }
            }
        }
    }
    let mut guards = Vec::new();
    if let Some((_, bytes)) = previous_state {
        guards.push(StoragePrecondition::KeyValueEquals {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: super::partial_replica_state_key(),
            expected: bytes,
        });
    } else {
        guards.push(StoragePrecondition::KeyAbsent {
            space: PARTIAL_REPLICA_STATE_SPACE,
            key: super::partial_replica_state_key(),
        });
    }
    for (branch_id, recovery_key) in retired_recovery_refs {
        let control = crate::branch::observe_branch_control_coordinate(read, &branch_id).await?;
        guards.push(crate::branch::branch_head_control_precondition(
            &branch_id,
            control.raw_token,
        )?);
        let recovery_storage_key = StorageKey(Bytes::from(recovery_key));
        let prior_recovery = PointReadPlan::new(
            crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
            std::slice::from_ref(&recovery_storage_key),
        )
        .materialize(read, StorageGetOptions::default())
        .await?
        .value
        .pop()
        .flatten();
        guards.push(match prior_recovery {
            Some(StorageProjectedValue::FullValue(bytes)) => StoragePrecondition::KeyValueEquals {
                space: crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
                key: recovery_storage_key,
                expected: bytes,
            },
            None => StoragePrecondition::KeyAbsent {
                space: crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
                key: recovery_storage_key,
            },
            Some(_) => return Err(mismatch("partial serving recovery ref read omitted its value")),
        });
    }
    let mut staged = Vec::new();
    let mut visited = std::collections::BTreeSet::new();
    let all_branches = migrate_missing
        || writes.has_mutations_in_space(PARTIAL_REPLICA_STATE_SPACE)
        || writes.has_mutations_in_space(ROOT_CURRENT_BASE_SPACE);
    for branch in [&state.descriptor().selected_branch, &state.descriptor().global_branch] {
        if !visited.insert(&branch.branch_id) {
            continue;
        }
        let branch_key = crate::branch::branch_head_control_key(&branch.branch_id)?;
        let serving_key = key(&branch.branch_id)?;
        let recovery_key = crate::gc::recovery_ref_key(&branch.branch_id)?;
        let recovery_changed = writes.contains_put(
            crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
            &recovery_key,
        ) || writes.staged_delete(crate::gc::CHECKPOINT_RECOVERY_REF_SPACE, &recovery_key);
        if !all_branches
            && !writes.contains_put(BRANCH_HEAD_CONTROL_SPACE, &branch_key)
            && !writes.staged_delete(BRANCH_HEAD_CONTROL_SPACE, &branch_key)
            && !writes.contains_put(PARTIAL_SERVING_SPACE, &serving_key.0)
            && !writes.staged_delete(PARTIAL_SERVING_SPACE, &serving_key.0)
            && !recovery_changed
        {
            continue;
        }
        if writes.staged_delete(BRANCH_HEAD_CONTROL_SPACE, &branch_key)
            || writes.staged_delete(PARTIAL_SERVING_SPACE, &serving_key.0)
        {
            return Err(mismatch("an admitted branch serving coordinate was deleted"));
        }
        let observed =
            crate::branch::observe_branch_control_coordinate(read, &branch.branch_id).await?;
        let control = crate::branch::staged_branch_head_control(&writes, &branch.branch_id)?
            .or(observed.control)
            .ok_or_else(|| mismatch("partial serving branch control is absent"))?;
        let (root, post_marker_bytes) =
            root_marker(&overlay, &branch.branch_id, control.tracked_generation).await?;
        let active_marker_key = hot_generation_scope_prefix(&branch.branch_id, control.tracked_generation);
        if writes.staged_delete(ROOT_CURRENT_BASE_SPACE, &active_marker_key) {
            return Err(mismatch("an admitted branch root marker was deleted"));
        }
        let base = CommitId::parse_lix(&branch.head.commit_id, "partial admitted base")?;
        let admitted = root == base
            && control.tracked_generation == state.serving_generation(&branch.branch_id)?;
        let old = load(read, &branch.branch_id).await?;
        let previous = old.as_ref().map(|(record, _)| record);
        if recovery_changed {
            let prior_head = previous
                .ok_or_else(|| mismatch("partial serving recovery ref has no prior witness"))
                .and_then(|prior| CommitId::parse_lix(&prior.head, "partial prior serving head"))?;
            if writes.staged_delete(crate::gc::CHECKPOINT_RECOVERY_REF_SPACE, &recovery_key)
                || !writes.contains_put(BRANCH_HEAD_CONTROL_SPACE, &branch_key)
                || prior_head == control.head_commit_id
                || !checkpoint_extends_prior(
                    &overlay,
                    &branch.branch_id,
                    control.head_commit_id,
                    prior_head,
                )
                .await?
            {
                return Err(mismatch(
                    "partial serving recovery ref requires a certified checkpoint transition",
                ));
            }
            let recovery_storage_key = StorageKey(Bytes::from(recovery_key));
            let prior_recovery = PointReadPlan::new(
                crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
                std::slice::from_ref(&recovery_storage_key),
            )
            .materialize(read, StorageGetOptions::default())
            .await?
            .value
            .pop()
            .flatten();
            guards.push(match prior_recovery {
                Some(StorageProjectedValue::FullValue(bytes)) => StoragePrecondition::KeyValueEquals {
                    space: crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
                    key: recovery_storage_key,
                    expected: bytes,
                },
                None => StoragePrecondition::KeyAbsent {
                    space: crate::gc::CHECKPOINT_RECOVERY_REF_SPACE,
                    key: recovery_storage_key,
                },
                Some(_) => return Err(mismatch("partial serving recovery ref read omitted its value")),
            });
        }
        let stable_local_root = previous.is_some_and(|prior| {
            prior.local_root_owned
                && prior.epoch_id == state.epoch_id()
                && prior.admitted_base == base.to_string()
                && prior.root == root.to_string()
                && prior.generation == control.tracked_generation.to_string()
        });
        let prior_head_extends = if previous.is_some_and(|prior| {
            prior.epoch_id == state.epoch_id()
                && prior.admitted_base == base.to_string()
                && prior.head == control.head_commit_id.to_string()
        }) {
            true
        } else if let Some(prior) = previous.filter(|prior| {
            prior.epoch_id == state.epoch_id() && prior.admitted_base == base.to_string()
        }) {
            let prior_head = CommitId::parse_lix(&prior.head, "partial prior serving head")?;
            let node = CommitGraphContext::new()
                .reader(&overlay)
                .load_node(&control.head_commit_id)
                .await?;
            node.is_some_and(|node| {
                node.parent_commit_ids
                    .first()
                    .is_some_and(|parent| prior.head == parent.to_string())
            })
                || checkpoint_extends_prior(
                    &overlay,
                    &branch.branch_id,
                    control.head_commit_id,
                    prior_head,
                )
                .await?
        } else {
            false
        };
        let local_root_owned = if admitted {
            // Legacy repositories have no durable transition witness. Their
            // checkpoint chain can skip the admitted base, so the one-time
            // migration preserves the already admitted coordinates instead
            // of demanding a first-parent walk they cannot provide.
            if !prior_head_extends {
                if migrate_missing {
                    if control.head_commit_id != base
                        && CommitGraphContext::new()
                            .reader(&overlay)
                            .load_node(&control.head_commit_id)
                            .await?
                            .is_none()
                    {
                        return Err(mismatch("legacy partial serving head is absent"));
                    }
                } else if !head_reaches_base(&overlay, control.head_commit_id, base).await? {
                    return Err(mismatch("partial serving head is outside its admitted interval"));
                }
            }
            false
        } else {
            let incremental = stable_local_root && prior_head_extends;
            incremental
                || local_root_is_owned(
                    &overlay,
                    control.head_commit_id,
                    root,
                    base,
                )
                .await?
        };
        if !admitted && !local_root_owned {
            return Err(mismatch("partial serving root is outside its admitted local interval"));
        }
        let next = PartialServing {
            version: 1,
            epoch_id: state.epoch_id().to_owned(),
            branch_id: branch.branch_id.clone(),
            admitted_base: base.to_string(),
            head: control.head_commit_id.to_string(),
            generation: control.tracked_generation.to_string(),
            root: root.to_string(),
            local_root_owned,
        };
        // Fence the exact source records on which the postimage validation
        // depended. All coordinates and their witness then commit together.
        guards.push(crate::branch::branch_head_control_precondition(
            &branch.branch_id,
            observed.raw_token,
        )?);
        let marker_key = StorageKey(Bytes::from(hot_generation_scope_prefix(
            &branch.branch_id,
            control.tracked_generation,
        )));
        let old_marker = if writes.contains_put(ROOT_CURRENT_BASE_SPACE, &marker_key.0) {
            PointReadPlan::new(ROOT_CURRENT_BASE_SPACE, &[marker_key.clone()])
                .materialize(read, StorageGetOptions::default())
                .await?
                .value
                .pop()
                .flatten()
        } else {
            Some(StorageProjectedValue::FullValue(post_marker_bytes))
        };
        guards.push(match old_marker {
            Some(StorageProjectedValue::FullValue(bytes)) => StoragePrecondition::KeyValueEquals {
                space: ROOT_CURRENT_BASE_SPACE,
                key: marker_key,
                expected: bytes,
            },
            None => StoragePrecondition::KeyAbsent {
                space: ROOT_CURRENT_BASE_SPACE,
                key: marker_key,
            },
            Some(_) => return Err(mismatch("partial serving root read omitted its value")),
        });
        if local_root_owned && !stable_local_root {
            // A legacy or newly selected native root must retain the exact
            // authority bytes used for its ownership proof until this witness
            // commits. Once witnessed, ordinary root deletion is fenced by
            // the adapter and GC retains the root.
            let authority_key = crate::tracked_state::commit_state_authority_key(root);
            let prior_authority = PointReadPlan::new(
                crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                std::slice::from_ref(&authority_key),
            )
            .materialize(read, StorageGetOptions::default())
            .await?
            .value
            .pop()
            .flatten();
            guards.push(match prior_authority {
                Some(StorageProjectedValue::FullValue(bytes)) => {
                    StoragePrecondition::KeyValueEquals {
                        space: crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                        key: authority_key,
                        expected: bytes,
                    }
                }
                None => StoragePrecondition::KeyAbsent {
                    space: crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
                    key: authority_key,
                },
                Some(_) => return Err(mismatch("partial serving authority read omitted its value")),
            });
        }
        guards.push(match &old {
            Some((_, bytes)) => StoragePrecondition::KeyValueEquals {
                space: PARTIAL_SERVING_SPACE,
                key: serving_key.clone(),
                expected: bytes.clone(),
            },
            None => StoragePrecondition::KeyAbsent {
                space: PARTIAL_SERVING_SPACE,
                key: serving_key.clone(),
            },
        });
        if previous != Some(&next) {
            let encoded = serde_json::to_vec(&next)
                .map_err(|_| mismatch("partial serving encoding failed"))?;
            if let Some(existing) = writes.staged_value(PARTIAL_SERVING_SPACE, &serving_key.0) {
                if existing.as_ref() != encoded {
                    return Err(mismatch("partial serving write disagrees with its coordinates"));
                }
            } else {
                staged.push((serving_key, Bytes::from(encoded)));
            }
        }
    }
    drop(overlay);
    let mut writes = Arc::try_unwrap(writes).expect("overlay dropped");
    for (key, bytes) in staged {
        writes.put(PARTIAL_SERVING_SPACE, key, StorageValue { bytes });
    }
    Ok((writes, guards))
}

/// Derive the exact witness bytes a migrated partial repository must retain.
/// The same postimage validator used by live partial writes supplies missing
/// witnesses, while existing witnesses must already agree with the source.
pub(crate) async fn preservation_entries(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<(String, Vec<u8>, Vec<u8>)>, LixError> {
    let Some((state, upgrade_writes, _)) =
        super::prepare_owned_partial_metadata_upgrade(read).await?
    else {
        return Err(mismatch("partial migration lost its admission"));
    };
    let upgraded = CandidateRead {
        base: read,
        staged: Arc::new(upgrade_writes),
    };
    let (writes, _) = prepare_write(&upgraded, StorageWriteSet::new(), true).await?;
    let mut entries = Vec::new();
    let mut visited = std::collections::BTreeSet::new();
    for branch in [&state.descriptor().selected_branch, &state.descriptor().global_branch] {
        if !visited.insert(&branch.branch_id) {
            continue;
        }
        let branch_key = key(&branch.branch_id)?;
        let bytes = if let Some(staged) = writes.staged_value(PARTIAL_SERVING_SPACE, &branch_key.0) {
            staged
        } else {
            load(&upgraded, &branch.branch_id)
                .await?
                .ok_or_else(|| mismatch("partial migration source witness is absent"))?
                .1
        };
        entries.push((branch.branch_id.clone(), branch_key.0.to_vec(), bytes.to_vec()));
    }
    Ok(entries)
}

pub(crate) async fn assert_admitted(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &super::PartialReplicaState,
) -> Result<bool, LixError> {
    let mut missing = false;
    let mut visited = std::collections::BTreeSet::new();
    for branch in [&state.descriptor().selected_branch, &state.descriptor().global_branch] {
        if !visited.insert(&branch.branch_id) {
            continue;
        }
        let Some((serving, _)) = load(read, &branch.branch_id).await? else {
            missing = true;
            continue;
        };
        let control = BranchHeadControlContext::new()
            .reader(read)
            .load(&branch.branch_id)
            .await?
            .ok_or_else(|| mismatch("partial serving branch control is absent"))?;
        let (root, _) = root_marker(read, &branch.branch_id, control.tracked_generation).await?;
        let base = CommitId::parse_lix(&branch.head.commit_id, "partial admitted base")?;
        if serving.epoch_id != state.epoch_id()
            || serving.admitted_base != base.to_string()
            || serving.head != control.head_commit_id.to_string()
            || serving.generation != control.tracked_generation.to_string()
            || serving.root != root.to_string()
            || serving.local_root_owned
                != (root != base || control.tracked_generation != state.serving_generation(&branch.branch_id)?)
        {
            return Err(mismatch("partial serving witness disagrees with its coordinates")
                .with_details(serde_json::json!({
                    "branchId": branch.branch_id,
                    "headCommitId": control.head_commit_id.to_string(),
                    "servingGeneration": control.tracked_generation.to_string(),
                    "rootCommitId": root.to_string(),
                    "expectedRootCommitId": base.to_string(),
                    "witness": serving,
                })));
        }
        if serving.local_root_owned
            && crate::tracked_state::load_commit_state_authority_ids(read, &[root])
                .await?
                .into_iter()
                .all(|authority| authority.is_none())
        {
            return Err(mismatch("partial serving native root lost its authority"));
        }
    }
    Ok(!missing)
}

/// GC must retain a locally selected native root as a hard physical owner.
/// Reaching it only through the history chain is insufficient when older
/// repositories have already lost optional historical manifests.
pub(crate) async fn retained_local_roots(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<Vec<CommitId>, LixError> {
    let Some((state, _)) = load_partial_replica_state(read).await? else {
        return Ok(Vec::new());
    };
    if !assert_admitted(read, &state).await? {
        return Err(mismatch("partial serving witness is absent during garbage collection"));
    }
    let mut roots = Vec::new();
    let mut visited = std::collections::BTreeSet::new();
    for branch in [&state.descriptor().selected_branch, &state.descriptor().global_branch] {
        if !visited.insert(&branch.branch_id) {
            continue;
        }
        let (serving, _) = load(read, &branch.branch_id)
            .await?
            .ok_or_else(|| mismatch("partial serving witness is absent"))?;
        if serving.local_root_owned {
            roots.push(CommitId::parse_lix(&serving.root, "partial serving root")?);
        }
    }
    Ok(roots)
}

/// One-time upgrade for pre-manifest partial repositories. Existing controls,
/// roots, and pending commits are preserved. Ownership must be provable from
/// their resident native interval; otherwise the old repository stays intact.
pub(crate) async fn migrate_missing<S>(
    storage: &crate::storage_adapter::StorageAdapter<S>,
) -> Result<(), LixError>
where
    S: crate::storage_adapter::Storage + Clone + Send + Sync + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    let Some((state, _)) = load_partial_replica_state(&read).await? else {
        return Ok(());
    };
    let mut missing = false;
    let mut visited = std::collections::BTreeSet::new();
    for branch in [&state.descriptor().selected_branch, &state.descriptor().global_branch] {
        if visited.insert(&branch.branch_id) && load(&read, &branch.branch_id).await?.is_none() {
            missing = true;
        }
    }
    if !missing {
        return Ok(());
    }
    let (writes, guards) = prepare_write(&read, StorageWriteSet::new(), true).await?;
    drop(read);
    if !writes.is_empty() {
        let result = storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                crate::storage_adapter::StorageWriteOptions {
                    await_durable: true,
                    preconditions: guards,
                    ..Default::default()
                },
            )
            .await;
        match result {
            Ok(_) => {}
            Err(crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_)
                | crate::storage_adapter::StorageError::WriteConflict,
            )) => {
                // Another opener may have completed the same one-time
                // migration. The caller's subsequent admission check still
                // verifies its exact expected receipt.
                let read = storage.begin_read(Default::default()).await?;
                let (current, _) = load_partial_replica_state(&read)
                    .await?
                    .ok_or_else(|| mismatch("partial receipt disappeared during migration"))?;
                if !assert_admitted(&read, &current).await? {
                    return Err(mismatch("partial serving migration conflicted"));
                }
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
