use std::collections::{BTreeMap, HashSet};

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest, CommitRecord,
};
use crate::common::LixTimestamp;
use crate::entity_pk::EntityPk;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::context::{
    TrackedStateContext, TrackedStateRootRebuilder, TrackedStateTransientRebuildState,
    TrackedStateWriteReport, TrackedStateWriter,
};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::TrackedStateRootId;
use crate::tracked_state::{
    TrackedStateDeltaRef, TrackedStateKeyRef, TrackedStateRootMutationRef, encode_key_ref,
};

/// Owned delta used only by explicit commit-root rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitRootRebuildDelta {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) entity_pk: EntityPk,
    pub(crate) change_id: ChangeId,
    pub(crate) commit_id: CommitId,
    pub(crate) deleted: bool,
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
    let typed_commit_id = CommitId::parse_lix(commit_id, "commit-root rebuild authority")?;
    let manifest = storage::load_commit_state_manifest(rebuilder.store, typed_commit_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state root for commit '{commit_id}' without its commit-state manifest"
                ),
            )
        })?;
    if manifest.snapshot_root.is_none() {
        // Rootless commits are intentionally bounded-replay layouts. Build and
        // audit the canonical state transiently, but do not persist chunks that
        // immutable authority cannot address.
        let mut scratch_writes = StorageWriteSet::new();
        let mut scratch_rebuilder = TrackedStateRootRebuilder {
            store: rebuilder.store,
            writes: &mut scratch_writes,
        };
        return rebuild_commit_root_at_inner(&mut scratch_rebuilder, commit_id).await;
    }
    rebuild_commit_root_at_inner(rebuilder, commit_id).await
}

async fn rebuild_commit_root_at_inner<S>(
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
    let mut state = TrackedStateTransientRebuildState::default();
    for plan in plans.iter().rev() {
        let manifest = storage::load_commit_state_manifest(rebuilder.store, plan.commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot rebuild tracked_state root for commit '{}' without its commit-state manifest",
                        plan.commit_id
                    ),
                )
            })?;
        if manifest.snapshot_root.is_some() {
            let mut writer =
                context.writer_with_rebuild_state(rebuilder.store, rebuilder.writes, state);
            let rooted_report = stage_rebuild_plan_with_writer(&mut writer, plan).await?;
            writer
                .promote_reachable_transient_chunks(&rooted_report.root_id)
                .await?;
            report = Some(rooted_report);
            state = writer.into_transient_rebuild_state();
        } else {
            // Rootless intermediates may feed a rooted descendant through the
            // in-memory content-addressed overlay, but their chunks have no
            // immutable root pointer and must never enter the durable write set.
            let previously_known = state.chunk_hashes();
            let mut scratch_writes = StorageWriteSet::new();
            let mut writer =
                context.writer_with_rebuild_state(rebuilder.store, &mut scratch_writes, state);
            report = Some(stage_rebuild_plan_with_writer(&mut writer, plan).await?);
            state = writer.into_transient_rebuild_state();
            state.mark_new_chunks_transient(&previously_known);
        }
    }
    let report = report.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state commit_root rebuild for commit '{commit_id}' did not stage a root"
            ),
        )
    })?;
    let writer = context.writer_with_rebuild_state(rebuilder.store, rebuilder.writes, state);
    writer
        .validate_staged_commit_root_against_changelog(commit_id)
        .await?;
    let staged_roots = writer.staged_commit_roots().cloned().collect::<Vec<_>>();
    drop(writer);
    for snapshot_root in staged_roots {
        let manifest = storage::load_published_commit_state_manifest(rebuilder.store, snapshot_root.commit_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "cannot publish rebuilt tracked_state root for commit '{}' without its commit-state manifest",
                        snapshot_root.commit_id
                    ),
                )
            })?;
        if let Some(expected) = manifest.snapshot_root.as_ref()
            && !expected.has_same_authoritative_layout(&snapshot_root)
        {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "rebuilt tracked_state root for commit '{}' disagrees with immutable commit authority: expected {expected:?}, rebuilt {snapshot_root:?}",
                    snapshot_root.commit_id,
                ),
            ));
        }
        // Root metadata is immutable authority. Rebuilds restore its
        // content-addressed chunks; rootless commits remain replay-only.
    }
    Ok(report)
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
            && load_available_root(store, &current_commit_id)
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

async fn load_available_root<S>(
    store: &S,
    commit_id: &str,
) -> Result<Option<TrackedStateRootId>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let Some(metadata) = storage::load_snapshot_commit_root(store, commit_id).await? else {
        return Ok(None);
    };
    // Immutable manifest/root metadata is the serving authority. Availability
    // checks validate its bounded content-addressed closure; full canonical
    // replay remains an explicit rebuild/integrity operation.
    TrackedStateTree::new()
        .validate_root_metadata(store, &metadata)
        .await?;
    Ok(Some(metadata.root_id))
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
    let entry = batch
        .into_iter()
        .next()
        .and_then(|(_, value)| value)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state commit_root for unknown commit '{commit_id}'"
                ),
            )
        })?;
    let commit = entry;
    // Commit roots contain only identity/index facts. Avoid hydrating JSON
    // sidecars while rebuilding them; the packed delta index already carries
    // deletion, owner ids, and original timestamps.
    let deltas = storage::scan_commit_delta_members(store, commit.commit_id)
        .await?
        .into_iter()
        .map(|(key, value)| CommitRootRebuildDelta {
            schema_key: key.schema_key,
            file_id: key.file_id,
            entity_pk: key.entity_pk,
            change_id: value.change_id,
            commit_id: value.commit_id,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
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
            deleted: delta.deleted,
            created_at: delta.created_at,
            updated_at: delta.updated_at,
        })
        .collect::<Vec<_>>();
    let commit_id = plan.commit_id.to_string();
    let parent_commit_id = plan.parent_commit_id.map(|commit_id| commit_id.to_string());
    let strictly_sorted = plan.deltas.windows(2).all(|pair| {
        pair[0]
            .schema_key
            .cmp(&pair[1].schema_key)
            .then_with(|| pair[0].file_id.cmp(&pair[1].file_id))
            .then_with(|| pair[0].entity_pk.cmp(&pair[1].entity_pk))
            .is_lt()
    });
    if strictly_sorted && plan.deltas.len() >= 2 {
        let first = &plan.deltas[0];
        let first_key = encode_key_ref(TrackedStateKeyRef {
            schema_key: &first.schema_key,
            file_id: first.file_id.as_deref(),
            entity_pk: &first.entity_pk,
        });
        let file_delete_cascades = plan
            .deltas
            .iter()
            .filter(|delta| delta.schema_key == "lix_file_descriptor" && delta.deleted)
            .map(|delta| {
                Ok((
                    delta.entity_pk.as_single_string_owned().map_err(|error| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            format!("file descriptor tombstone has invalid identity: {error}"),
                        )
                    })?,
                    TrackedStateDeltaRef {
                        schema_key: &delta.schema_key,
                        file_id: delta.file_id.as_deref(),
                        entity_pk: &delta.entity_pk,
                        change_id: delta.change_id,
                        commit_id: delta.commit_id,
                        deleted: true,
                        created_at: delta.created_at,
                        updated_at: delta.updated_at,
                    },
                ))
            })
            .collect::<Result<BTreeMap<_, _>, LixError>>()?;
        if let Some(report) = writer
            .try_stage_bulk_parent_root_from_ordered_mutations(
                &commit_id,
                parent_commit_id.as_deref(),
                deltas.len(),
                &first_key,
                &file_delete_cascades,
                RebuildRootMutations::new(&deltas),
            )
            .await?
        {
            return Ok(report);
        }
    }
    writer
        .stage_commit_root(&commit_id, parent_commit_id.as_deref(), deltas)
        .await
}

struct RebuildRootMutations<'iter, 'delta> {
    inner: std::slice::Iter<'iter, TrackedStateDeltaRef<'delta>>,
}

impl<'iter, 'delta> RebuildRootMutations<'iter, 'delta> {
    fn new(deltas: &'iter [TrackedStateDeltaRef<'delta>]) -> Self {
        Self {
            inner: deltas.iter(),
        }
    }
}

impl<'delta> Iterator for RebuildRootMutations<'_, 'delta> {
    type Item = Result<TrackedStateRootMutationRef<'delta>, LixError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().copied().map(|delta| {
            Ok(TrackedStateRootMutationRef {
                delta,
                require_absence: false,
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for RebuildRootMutations<'_, '_> {}

fn first_parent_commit_id(commit: &CommitRecord) -> Option<CommitId> {
    commit.parent_commit_ids.first().copied()
}
