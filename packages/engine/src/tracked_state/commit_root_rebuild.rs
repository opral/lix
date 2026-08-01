use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest, CommitRecord,
};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::TrackedStateDeltaRef;
use crate::tracked_state::context::{
    TrackedStateContext, TrackedStateRootRebuilder, TrackedStateWriteReport, TrackedStateWriter,
};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::{
    TrackedStateCommitRoot, TrackedStateRootId, TrackedStateTreeScanRequest,
};

/// Owned delta used only by explicit commit-root rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitRootRebuildDelta {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) snapshot: crate::json_store::JsonSlot,
    pub(crate) metadata: crate::json_store::JsonSlot,
    pub(crate) created_at: LixTimestamp,
    pub(crate) updated_at: LixTimestamp,
}

pub(crate) async fn rebuild_commit_root_at<S>(
    rebuilder: &mut TrackedStateRootRebuilder<'_, S>,
    commit_id: &str,
) -> Result<TrackedStateWriteReport, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let plans =
        load_rebuild_plans_to_nearest_available_root(rebuilder.store, commit_id, true).await?;
    let mut report = None;
    let context = TrackedStateContext::new();
    let mut writer = context.writer(rebuilder.store, rebuilder.writes);
    for plan in plans.iter().rev() {
        report = Some(stage_rebuild_plan_with_writer(&mut writer, plan).await?);
    }
    let report = report.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_root rebuild for commit '{commit_id}' did not stage a root"
            ),
        )
    })?;
    writer
        .validate_staged_commit_root_against_changelog(commit_id)
        .await?;
    Ok(report)
}

/// Stages every missing first-parent root from the nearest durable checkpoint
/// through `commit_id` into one caller-owned root writer. Normal commits no
/// longer materialize roots, so merge/checkpoint fences use this cold helper
/// to recover their first-parent base without exposing an intermediate write.
pub(crate) async fn stage_missing_commit_root_chain<S>(
    writer: &mut TrackedStateWriter<'_, S>,
    commit_id: &str,
) -> Result<(), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let plans =
        load_rebuild_plans_to_nearest_available_root(writer.store(), commit_id, false).await?;
    for plan in plans.iter().rev() {
        stage_rebuild_plan_with_writer(writer, plan).await?;
    }
    Ok(())
}

pub(crate) async fn load_rebuild_plans_to_nearest_available_root<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
) -> Result<Vec<CommitRootRebuildPlan>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let mut plans = Vec::new();
    let mut current_commit_id = commit_id.to_string();
    let mut force_current = force_head;
    let mut seen_commit_ids = HashSet::new();
    loop {
        if !seen_commit_ids.insert(current_commit_id.clone()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state commit_root for commit '{commit_id}': first-parent cycle includes commit '{current_commit_id}'"
                ),
            ));
        }
        if !force_current
            && load_available_root(store, &current_commit_id, &mut HashSet::new())
                .await?
                .is_some()
        {
            break;
        }
        let plan = load_commit_root_rebuild_plan(store, &current_commit_id).await?;
        let parent_commit_id = plan.parent_commit_id;
        plans.push(plan);
        let Some(parent_commit_id) = parent_commit_id else {
            break;
        };
        current_commit_id = parent_commit_id.to_string();
        force_current = false;
    }
    Ok(plans)
}

fn load_available_root<'a, S>(
    store: &'a S,
    commit_id: &'a str,
    seen: &'a mut HashSet<String>,
) -> Pin<Box<dyn Future<Output = Result<Option<TrackedStateRootId>, LixError>> + 'a>>
where
    S: StorageAdapterRead + ?Sized + 'a,
{
    Box::pin(async move {
        if !seen.insert(commit_id.to_string()) {
            return Ok(None);
        }
        let Some(metadata) = storage::load_commit_root(store, commit_id).await? else {
            seen.remove(commit_id);
            return Ok(None);
        };
        if !commit_root_tree_is_readable(store, &metadata).await? {
            seen.remove(commit_id);
            return Ok(None);
        }
        if !commit_root_matches_canonical_rebuild(store, commit_id, &metadata, seen).await? {
            seen.remove(commit_id);
            return Ok(None);
        }
        seen.remove(commit_id);
        Ok(Some(metadata.root_id))
    })
}

async fn commit_root_tree_is_readable<S>(
    store: &S,
    metadata: &TrackedStateCommitRoot,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    match TrackedStateTree::new()
        .scan(
            store,
            &metadata.root_id,
            &TrackedStateTreeScanRequest::default(),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

async fn commit_root_matches_canonical_rebuild<S>(
    store: &S,
    commit_id: &str,
    metadata: &TrackedStateCommitRoot,
    seen: &mut HashSet<String>,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let plan = load_commit_root_rebuild_plan(store, commit_id).await?;
    if let Some(parent_commit_id) = plan.parent_commit_id.as_ref() {
        let parent_commit_id_text = parent_commit_id.to_string();
        let Some(parent_root_id) = load_available_root(store, &parent_commit_id_text, seen).await?
        else {
            return Ok(false);
        };
        match metadata.parent_roots.first() {
            Some(parent)
                if parent.commit_id == *parent_commit_id && parent.root_id == parent_root_id => {}
            _ => return Ok(false),
        }
    } else if !metadata.parent_roots.is_empty() {
        return Ok(false);
    }
    let mut scratch_writes = StorageWriteSet::new();
    let context = TrackedStateContext::new();
    let mut writer = context.writer(store, &mut scratch_writes);
    let report = stage_rebuild_plan_with_writer(&mut writer, &plan).await?;
    Ok(report.root_id == metadata.root_id)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitRootRebuildPlan {
    pub(crate) commit_id: CommitId,
    pub(crate) parent_commit_id: Option<CommitId>,
    pub(crate) deltas: Vec<CommitRootRebuildDelta>,
}

async fn load_commit_root_rebuild_plan<S>(
    store: &S,
    commit_id: &str,
) -> Result<CommitRootRebuildPlan, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let mut reader = ChangelogContext::new().reader(store);
    let commit_ids = [CommitId::parse_lix(
        commit_id,
        "commit-root rebuild commit_id",
    )?];
    let batch = reader
        .load_commits(CommitLoadRequest {
            commit_ids: &commit_ids,
        })
        .await?;
    let entry = batch.entries.into_iter().next().flatten().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("cannot rebuild tracked_state commit_root for unknown commit '{commit_id}'"),
        )
    })?;
    let commit = entry;
    // The packed delta is the sole membership and payload authority. Preserve
    // the identity value's original created_at independently from this
    // mutation's updated_at while rebuilding a root.
    let deltas = storage::load_commit_delta_members_with_payloads(store, commit.commit_id)
        .await?
        .into_iter()
        .map(|member| CommitRootRebuildDelta {
            schema_key: member.key.schema_key,
            file_id: member.key.file_id,
            entity_pk: member.key.entity_pk,
            change_id: member.value.change_id,
            commit_id: member.value.commit_id,
            snapshot: member.change.snapshot,
            metadata: member.change.metadata,
            created_at: member.value.created_at,
            updated_at: member.value.updated_at,
        })
        .collect();

    Ok(CommitRootRebuildPlan {
        commit_id: commit.commit_id,
        parent_commit_id: first_parent_commit_id(&commit),
        deltas,
    })
}

pub(crate) async fn stage_rebuild_plan_with_writer<S>(
    writer: &mut TrackedStateWriter<'_, S>,
    plan: &CommitRootRebuildPlan,
) -> Result<TrackedStateWriteReport, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let deltas = plan
        .deltas
        .iter()
        .map(|delta| TrackedStateDeltaRef {
            schema_key: &delta.schema_key,
            file_id: delta.file_id.as_deref(),
            entity_pk: &delta.entity_pk,
            change_id: delta.change_id,
            commit_id: delta.commit_id,
            deleted: delta.snapshot.is_none(),
            created_at: delta.created_at,
            updated_at: delta.updated_at,
        })
        .collect::<Vec<_>>();
    let commit_id = plan.commit_id.to_string();
    let parent_commit_id = plan.parent_commit_id.map(|commit_id| commit_id.to_string());
    writer
        .stage_commit_root(&commit_id, parent_commit_id.as_deref(), deltas)
        .await
}

fn first_parent_commit_id(commit: &CommitRecord) -> Option<CommitId> {
    commit.parent_commit_ids.first().copied()
}
