//! Dependency-closed native checkpoint upload preparation. No state scans,
//! remote history walks, or publication. Closure overflow requires paged
//! preparation; no incomplete dependency set may advance a branch ref.
use super::commit::{SyncCommit, load_sync_checkpoint_source, load_sync_commit};
use super::partial_push_state::{
    PartialPushCoordinate, PreparedPartialUpload, load_partial_push_state,
};
use super::partial_state::PartialReplicaState;
use super::partial_upload::PreparedPartialPush;
use super::protocol::{SyncPushRequest, SyncRefUpdate};
use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::StorageAdapterRead;
use std::collections::{BTreeMap, BTreeSet};
fn blocked(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_UPLOAD_PREPARATION_REQUIRED", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "partial checkpoint upload")
}

struct WireBudget {
    remaining: usize,
    written: usize,
}
impl std::io::Write for WireBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other(
                "checkpoint upload byte budget exceeded",
            ));
        }
        self.remaining -= bytes.len();
        self.written += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(super) async fn prepare_partial_checkpoint_upload(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
) -> Result<Option<PreparedPartialPush>, LixError> {
    prepare_checkpoint_target(
        read,
        state,
        branch_id,
        attempt_id,
        max_commits,
        max_wire_bytes,
        None,
    )
    .await
}

/// Find the direct first-parent child without walking the whole local suffix.
async fn first_parent_child(
    read: &(impl StorageAdapterRead + ?Sized),
    head: CommitId,
    boundary: CommitId,
) -> Result<CommitId, LixError> {
    let base = super::partial_upload::local_record(read, boundary).await?;
    let goal = base
        .generation
        .checked_add(1)
        .ok_or_else(|| blocked("checkpoint generation overflow"))?;
    let mut current = super::partial_upload::local_record(read, head).await?;
    for _ in 0..256 {
        if current.generation == goal && current.parent_commit_ids.as_slice() == [boundary] {
            return Ok(current.commit_id);
        }
        if current.generation <= goal || current.parent_commit_ids.len() != 1 {
            return Err(blocked(
                "checkpoint page does not extend the confirmed first-parent scope",
            ));
        }
        let jump_generation = current
            .generation
            .checked_sub(current.first_parent_jump_span)
            .ok_or_else(|| blocked("invalid checkpoint page jump"))?;
        let (next, generation) = if current.first_parent_jump_span > 0 && jump_generation >= goal {
            (current.first_parent_jump_commit_id, jump_generation)
        } else {
            (current.parent_commit_ids[0], current.generation - 1)
        };
        current = super::partial_upload::local_record(read, next).await?;
        if current.generation != generation {
            return Err(blocked("checkpoint page jump generation mismatch"));
        }
    }
    Err(blocked("checkpoint page traversal exceeds its bound"))
}

/// Stage the earliest pending checkpoint, or a bounded ordinary prefix of its
/// captured source. Every intermediate checkpoint has its original working
/// child, preserving unselected rows; no body-only frontier is inferred.
pub(super) async fn prepare_partial_checkpoint_page(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
) -> Result<Option<PreparedPartialPush>, LixError> {
    let (push, _, _) = load_partial_push_state(read, state, branch_id).await?;
    if push.prepared.is_some() {
        return Err(blocked("checkpoint page cannot replace a durable attempt"));
    }
    let control = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load(branch_id)
        .await?
        .ok_or_else(|| blocked("checkpoint branch disappeared"))?;
    let latest = control
        .working_diff_checkpoint_commit_id
        .ok_or_else(|| blocked("checkpoint missing"))?;
    let checkpoint = first_parent_child(read, latest, id(&push.confirmed.checkpoint)?).await?;
    let working_tip = if checkpoint == latest {
        control.head_commit_id
    } else {
        let next = first_parent_child(read, latest, checkpoint).await?;
        let (source_branch, source) = load_sync_checkpoint_source(read, next)
            .await?
            .ok_or_else(|| blocked("next checkpoint source is missing"))?;
        if source_branch != branch_id {
            return Err(blocked("next checkpoint source belongs to another branch"));
        }
        source
    };
    let head = if working_tip == checkpoint {
        checkpoint
    } else {
        first_parent_child(read, working_tip, checkpoint).await?
    };
    let target = PartialPushCoordinate {
        head: head.to_string(),
        checkpoint: checkpoint.to_string(),
    };
    match prepare_checkpoint_target(
        read,
        state,
        branch_id,
        attempt_id.clone(),
        max_commits,
        max_wire_bytes,
        Some(target),
    )
    .await
    {
        Err(error) if error.code == "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED" => {
            match super::partial_upload::prepare_partial_checkpoint_prefix(
                read,
                state,
                branch_id,
                attempt_id,
                max_commits,
                max_wire_bytes,
                checkpoint,
            )
            .await?
            {
                Some(page) => Ok(Some(page)),
                None => Err(blocked(
                    "minimal checkpoint closure exceeds the upload request budget; multipart body preparation is required",
                )),
            }
        }
        result => result,
    }
}

async fn prepare_checkpoint_target(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
    branch_id: &str,
    attempt_id: String,
    max_commits: usize,
    max_wire_bytes: usize,
    page_target: Option<PartialPushCoordinate>,
) -> Result<Option<PreparedPartialPush>, LixError> {
    if max_commits == 0
        || max_commits > super::MAX_SYNC_REQUEST_ITEMS
        || max_wire_bytes == 0
        || crate::storage_codec::id_string::uuid_bytes_from_canonical(&attempt_id).is_none()
    {
        return Err(blocked("invalid checkpoint upload budget or attempt"));
    }
    let (branch, _, _) = load_partial_push_state(read, state, branch_id).await?;
    let branches = [branch_id.to_owned()];
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&branches)
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| blocked("checkpoint branch observation is missing"))?;
    let control = observed
        .control
        .ok_or_else(|| blocked("checkpoint branch is deleted"))?;
    let target = branch
        .prepared
        .as_ref()
        .map(|upload| upload.target.clone())
        .or(page_target)
        .unwrap_or(PartialPushCoordinate {
            head: control.head_commit_id.to_string(),
            checkpoint: control
                .working_diff_checkpoint_commit_id
                .ok_or_else(|| blocked("checkpoint branch has no checkpoint"))?
                .to_string(),
        });
    if target == branch.confirmed {
        return Ok(None);
    }
    let (global, _, _) = load_partial_push_state(read, state, crate::GLOBAL_BRANCH_ID).await?;
    let mut known = BTreeSet::from([
        id(&branch.confirmed.head)?,
        id(&branch.confirmed.checkpoint)?,
        id(&global.confirmed.head)?,
        id(&global.confirmed.checkpoint)?,
        id(&state.descriptor().global_branch.head.commit_id)?,
        id(&state.descriptor().global_branch.checkpoint.commit_id)?,
    ]);
    if branch_id != crate::GLOBAL_BRANCH_ID {
        known.extend(
            super::partial_global_merge_state::confirmed_global_merge_bases(read, state).await?,
        );
    }

    let commits = load_local_dependency_closure(
        read,
        branch_id,
        state.active_account_id(),
        &[id(&target.checkpoint)?, id(&target.head)?],
        known,
        id(&global.confirmed.head)?,
        max_commits,
        max_wire_bytes,
    )
    .await?;
    let resumed = branch.prepared.is_some();
    let mut upload = branch.prepared.unwrap_or(PreparedPartialUpload {
        attempt_id,
        created_refs: Vec::new(),
        expected: branch.confirmed,
        target,
    });
    if !resumed {
        upload.created_refs = super::partial_created_refs::capture_created_refs(
            read,
            state,
            branch_id,
            &upload.expected,
            &upload.target,
            &commits,
        )
        .await?;
    }
    let mut request = SyncPushRequest {
        commits,
        ref_updates: vec![SyncRefUpdate {
            branch_id: branch_id.into(),
            expected_head_commit_id: Some(upload.expected.head.clone()),
            expected_checkpoint_commit_id: Some(upload.expected.checkpoint.clone()),
            head_commit_id: Some(upload.target.head.clone()),
            checkpoint_commit_id: Some(upload.target.checkpoint.clone()),
        }],
        inline_blobs: Vec::new(),
    };
    upload.append_created_ref_updates(&mut request);
    let mut budget = WireBudget {
        remaining: max_wire_bytes,
        written: 0,
    };
    serde_json::to_writer(&mut budget, &request).map_err(|_| {
        LixError::new(
            "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED",
            "checkpoint request exceeds wire budget",
        )
    })?;
    let control_guard = if resumed {
        None
    } else {
        Some(crate::branch::branch_head_control_precondition(
            branch_id,
            observed.raw_token,
        )?)
    };
    Ok(Some(PreparedPartialPush {
        upload,
        request,
        control_guard,
        encoded_bytes: budget.written,
    }))
}

/// Shared native dependency closure for checkpoint publication and retained
/// reconciliation bodies. Only explicit local roots and their dependencies are
/// visited; confirmed boundary roots terminate traversal.
pub(super) async fn load_local_dependency_closure(
    read: &(impl StorageAdapterRead + ?Sized),
    branch_id: &str,
    account: &str,
    roots: &[CommitId],
    mut known: BTreeSet<CommitId>,
    confirmed_global: CommitId,
    max_commits: usize,
    max_wire_bytes: usize,
) -> Result<Vec<SyncCommit>, LixError> {
    let mut global_ancestry = BTreeMap::new();
    let mut stack = roots
        .iter()
        .copied()
        .map(|root| (root, false))
        .collect::<Vec<_>>();
    let mut visiting = BTreeSet::new();
    let mut done = known.clone();
    let mut loaded = BTreeMap::<CommitId, SyncCommit>::new();
    let mut commits = Vec::new();
    let mut body_budget = WireBudget {
        remaining: max_wire_bytes,
        written: 0,
    };
    while let Some((current, expanded)) = stack.pop() {
        if done.contains(&current) {
            continue;
        }
        if expanded {
            visiting.remove(&current);
            done.insert(current);
            commits.push(
                loaded
                    .remove(&current)
                    .expect("expanded native commit was loaded"),
            );
            continue;
        }
        if !visiting.insert(current) {
            return Err(blocked("checkpoint dependency cycle"));
        }
        if commits.len() + loaded.len() >= max_commits {
            return Err(LixError::new(
                "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED",
                "checkpoint closure requires an earlier bounded upload wave",
            ));
        }
        let commit = load_sync_commit(read, current)
            .await?
            .ok_or_else(|| blocked("locally authored checkpoint dependency is missing"))?;
        if commit.account_id != account {
            return Err(blocked(
                "checkpoint dependency belongs to an unprepared account scope",
            ));
        }
        if let Some(base) = commit.base_commit_id.as_deref().map(id).transpose()? {
            if !known.contains(&base)
                && branch_id != crate::GLOBAL_BRANCH_ID
                && super::partial_upload::is_confirmed_global_base(
                    read,
                    base,
                    confirmed_global,
                    &mut global_ancestry,
                )
                .await?
            {
                known.insert(base);
                done.insert(base);
            }
            if !known.contains(&base) {
                return Err(blocked(
                    "checkpoint requires confirmed global base preparation",
                ));
            }
        }
        // Blob manifests and chunks are prepared by the same bounded sender
        // used for ordinary uploads, before this checkpoint's refs are pushed.
        if commit.parent_commit_ids.len() > max_commits {
            return Err(blocked(
                "checkpoint parent fanout exceeds preparation budget",
            ));
        }
        let mut dependencies = commit
            .parent_commit_ids
            .iter()
            .map(|value| id(value))
            .collect::<Result<BTreeSet<_>, _>>()?;
        if let Some(alias) = &commit.state_alias {
            dependencies.insert(id(&alias.source_commit_id)?);
        }
        if let Some(source) = &commit.selected_source_commit_id {
            dependencies.insert(id(source)?);
        }
        if let Some((source_branch, source)) = load_sync_checkpoint_source(read, current).await? {
            if source_branch != branch_id {
                return Err(blocked("checkpoint provenance belongs to another branch"));
            }
            dependencies.insert(source);
        }
        serde_json::to_writer(&mut body_budget, &commit).map_err(|_| {
            LixError::new(
                "LIX_PARTIAL_UPLOAD_PAGE_REQUIRED",
                "checkpoint bodies exceed wire budget",
            )
        })?;
        loaded.insert(current, commit);
        stack.push((current, true));
        for dependency in dependencies.into_iter().rev() {
            if !done.contains(&dependency) {
                stack.push((dependency, false));
            }
        }
    }
    Ok(commits)
}
