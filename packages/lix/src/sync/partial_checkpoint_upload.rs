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

    let mut stack = vec![(id(&target.checkpoint)?, false), (id(&target.head)?, false)];
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
            return Err(blocked(
                "checkpoint closure exceeds bounded preparation; paged body preparation required",
            ));
        }
        let commit = load_sync_commit(read, current)
            .await?
            .ok_or_else(|| blocked("locally authored checkpoint dependency is missing"))?;
        if commit.account_id != state.active_account_id() {
            return Err(blocked(
                "checkpoint dependency belongs to an unprepared account scope",
            ));
        }
        if commit
            .base_commit_id
            .as_deref()
            .map(id)
            .transpose()?
            .is_some_and(|base| !known.contains(&base))
        {
            return Err(blocked(
                "checkpoint requires confirmed global base preparation",
            ));
        }
        if commit
            .members
            .iter()
            .any(|member| member.schema_key == "lix_binary_blob_ref" && !member.deleted)
        {
            return Err(blocked(
                "checkpoint requires binary manifest and chunk upload preparation",
            ));
        }
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
        serde_json::to_writer(&mut body_budget, &commit)
            .map_err(|_| blocked("checkpoint bodies exceed wire budget"))?;
        loaded.insert(current, commit);
        stack.push((current, true));
        for dependency in dependencies.into_iter().rev() {
            if !done.contains(&dependency) {
                stack.push((dependency, false));
            }
        }
    }
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
    serde_json::to_writer(&mut budget, &request)
        .map_err(|_| blocked("checkpoint request exceeds wire budget"))?;
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
