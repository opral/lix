use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangeLoadRequest, ChangelogContext, ChangelogReader, CommitId, CommitLoadEntry,
    CommitLoadRequest, CommitProjection, CommitRecord,
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
    report.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_root rebuild for commit '{commit_id}' did not stage a root"
            ),
        )
    })
}

async fn load_rebuild_plans_to_nearest_available_root<S>(
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
struct CommitRootRebuildPlan {
    commit_id: CommitId,
    parent_commit_id: Option<CommitId>,
    deltas: Vec<CommitRootRebuildDelta>,
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
            projection: CommitProjection::Full,
        })
        .await?;
    let entry = batch.entries.into_iter().next().flatten().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("cannot rebuild tracked_state commit_root for unknown commit '{commit_id}'"),
        )
    })?;
    let (commit, change_refs) = match entry {
        CommitLoadEntry::Full {
            record,
            change_ref_chunks,
        } => (
            record,
            change_ref_chunks
                .into_iter()
                .flat_map(|chunk| chunk.entries)
                .collect::<Vec<_>>(),
        ),
        CommitLoadEntry::Record(_) => {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "changelog returned a partial commit load for commit-root rebuild",
            ));
        }
    };
    let changes = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &change_refs,
        })
        .await?;
    let mut deltas = change_refs
        .iter()
        .zip(changes.entries)
        .map(|(change_id, change)| {
            let change = change.ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "commit '{commit_id}' references missing changelog.change '{change_id}'"
                    ),
                )
            })?;
            rebuild_delta_from_change_ref(commit_id, *change_id, change)
        })
        .collect::<Result<Vec<_>, _>>()?;
    deltas.push(rebuild_delta_from_commit_record(&commit)?);

    Ok(CommitRootRebuildPlan {
        commit_id: commit.commit_id,
        parent_commit_id: first_parent_commit_id(&commit),
        deltas,
    })
}

async fn stage_rebuild_plan_with_writer<S>(
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

fn rebuild_delta_from_commit_record(
    commit: &CommitRecord,
) -> Result<CommitRootRebuildDelta, LixError> {
    let snapshot_content = commit_row_snapshot_content(&commit.commit_id.to_string())?;
    Ok(CommitRootRebuildDelta {
        schema_key: "lix_commit".to_string(),
        file_id: None,
        entity_pk: EntityPk::single(commit.commit_id),
        change_id: commit.change_id,
        commit_id: commit.commit_id,
        snapshot: crate::json_store::JsonSlot::from_json(&snapshot_content),
        metadata: crate::json_store::JsonSlot::None,
        created_at: commit.created_at,
        updated_at: commit.created_at,
    })
}

fn commit_row_snapshot_content(commit_id: &str) -> Result<String, LixError> {
    crate::changelog::commit_row_snapshot_json(commit_id)
}

fn rebuild_delta_from_change_ref(
    commit_id: &str,
    ref_change_id: ChangeId,
    change: crate::changelog::ChangeRecord,
) -> Result<CommitRootRebuildDelta, LixError> {
    if change.change_id != ref_change_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "commit '{commit_id}' change ref '{ref_change_id}' loaded mismatched changelog.change '{}'",
                change.change_id
            ),
        ));
    }
    Ok(CommitRootRebuildDelta {
        schema_key: change.schema_key,
        file_id: change.file_id,
        entity_pk: change.entity_pk,
        change_id: change.change_id,
        commit_id: CommitId::parse_lix(commit_id, "commit-root rebuild delta commit_id")?,
        snapshot: change.snapshot,
        metadata: change.metadata,
        created_at: change.created_at,
        updated_at: change.created_at,
    })
}
