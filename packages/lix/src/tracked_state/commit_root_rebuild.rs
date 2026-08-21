use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::LixError;
use crate::changelog::{
    ChangeId, ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest, CommitRecord,
};
use crate::common::LixTimestamp;
use crate::row_pk::RowPk;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};
use crate::tracked_state::context::{
    TrackedStateContext, TrackedStateRootRebuilder, TrackedStateTransientRebuildState,
    TrackedStateWriteReport, TrackedStateWriter,
};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::{
    TrackedStateCommitRoot, TrackedStateRootId, TrackedStateTreeScanRequest,
};
use crate::tracked_state::{TrackedStateDeltaRef, TrackedStateKey, TrackedStateKeyRef};

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";

/// Owned delta used only by explicit commit-root rebuild.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitRootRebuildDelta {
    pub(crate) schema_key: String,
    pub(crate) file_id: Option<String>,
    pub(crate) row_pk: RowPk,
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
    // Explicit repair is the caller whose job is to distrust the chunk plane,
    // so it — and only it — pays for the total closure proof.
    let plans = load_rebuild_plans_to_nearest_available_root_with_proof(
        rebuilder.store,
        commit_id,
        true,
        RootAvailabilityProof::Complete,
    )
    .await?;
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
            && **expected != snapshot_root
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

/// How much of a candidate resume point's chunk closure a caller demands to be
/// proved readable before it may resume from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RootAvailabilityProof {
    /// Prove the root is *addressable*: the root chunk and one root-to-leaf
    /// path load and decode. `O(tree depth)`.
    ///
    /// Closure completeness is not re-derived. Chunks are staged in the same
    /// atomic write set that publishes the root and the manifest, and GC
    /// reaches them from refs, so the write path and GC are already its single
    /// authority. This is what the commit path uses.
    Addressable,
    /// Prove the whole addressed closure loads and decodes. `O(tree rows)`.
    ///
    /// Used only by explicit repair (`rebuild_commit_root_at`), whose contract
    /// is to distrust the chunk plane: rejecting a damaged resume point is what
    /// makes it walk back and re-stage every chunk, so repair stays total.
    Complete,
}

pub(crate) async fn load_rebuild_plans_to_nearest_available_root<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
) -> Result<Vec<CommitRootRebuildPlan>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    load_rebuild_plans_to_nearest_available_root_with_proof(
        store,
        commit_id,
        force_head,
        RootAvailabilityProof::Addressable,
    )
    .await
}

pub(crate) async fn load_rebuild_plans_to_nearest_available_root_bounded<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
    max_members: usize,
    known_commit_ids: &BTreeSet<CommitId>,
) -> Result<Vec<CommitRootRebuildPlan>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    load_rebuild_plans_to_nearest_available_root_inner(
        store,
        commit_id,
        force_head,
        RootAvailabilityProof::Addressable,
        Some(max_members),
        known_commit_ids,
    )
    .await
}

pub(crate) async fn load_rebuild_plans_to_nearest_available_root_with_proof<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
    proof: RootAvailabilityProof,
) -> Result<Vec<CommitRootRebuildPlan>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    load_rebuild_plans_to_nearest_available_root_inner(
        store,
        commit_id,
        force_head,
        proof,
        None,
        &BTreeSet::new(),
    )
    .await
}

async fn load_rebuild_plans_to_nearest_available_root_inner<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
    proof: RootAvailabilityProof,
    max_members: Option<usize>,
    known_commit_ids: &BTreeSet<CommitId>,
) -> Result<Vec<CommitRootRebuildPlan>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let mut plans = Vec::new();
    let mut loaded_members = 0usize;
    let mut current_commit_id = commit_id.to_string();
    let mut force_current = force_head;
    let mut physical_alias_source = false;
    let mut seen_commit_ids = HashSet::new();
    loop {
        let typed_current_commit_id =
            CommitId::parse_lix(&current_commit_id, "tracked-state rebuild commit id")?;
        if known_commit_ids.contains(&typed_current_commit_id) {
            break;
        }
        if !seen_commit_ids.insert(current_commit_id.clone()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state commit_root for commit '{commit_id}': first-parent cycle includes commit '{current_commit_id}'"
                ),
            ));
        }
        if !force_current {
            #[cfg(feature = "storage-benches")]
            let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
                crate::storage_bench::PlanLoadPhase::AvailProbe,
            );
            let available =
                load_available_root(store, &current_commit_id, proof, physical_alias_source)
                    .await?;
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_root_replay_available_root_probe(available.is_some());
            if available.is_some() {
                break;
            }
        }
        let plan = load_commit_root_rebuild_plan(
            store,
            &current_commit_id,
            physical_alias_source,
            max_members.map(|limit| limit.saturating_sub(loaded_members)),
        )
        .await?;
        loaded_members = loaded_members.saturating_add(plan.deltas.len());
        let parent_commit_id = plan
            .complete_state_source_commit_id
            .or(plan.parent_commit_id);
        physical_alias_source = plan.complete_state_source_commit_id.is_some();
        plans.push(plan);
        let Some(parent_commit_id) = parent_commit_id else {
            break;
        };
        current_commit_id = parent_commit_id.to_string();
        force_current = false;
    }
    Ok(plans)
}

/// Resolves the durable root a replay may resume from.
///
/// The commit-state manifest is the single immutable authority for a commit's
/// tracked-state root: it is sealed atomically with the commit, keyed by that
/// commit id, and never rewritten. `rebuild_commit_root_at_inner` already
/// treats a rebuild that disagrees with it as a hard error rather than a
/// repair, so the manifest — not a replay of the changelog — is what makes a
/// root canonical. Availability therefore has exactly two obligations:
///
/// 1. the root pointer is live (`load_snapshot_commit_root` also proves an
///    ordinary commit still exists in the changelog; a complete-state alias
///    may instead authorize its physically retained source manifest after the
///    source's semantic projection has been collected), and
/// 2. the content-addressed chunk closure it names is physically addressable,
///    so a damaged root is never resumed from and explicit repair stays total.
///
/// It deliberately does **not** recurse through the ancestry: ordinary commits
/// are rootless bounded-replay layouts by design, so an ancestry-wide proof can
/// only ever succeed by reaching genesis, which makes every interval closure
/// replay the entire history.
///
/// How much of obligation (2) is proved is the caller's choice — see
/// [`RootAvailabilityProof`]. The commit path proves addressability only:
/// re-deriving closure completeness there cost `O(total state)` per boundary
/// and, at one boundary per `COMMIT_STATE_MAX_REPLAY_DEPTH` commits, put an
/// `O(N^2)` term back into commit for a fact atomic publication and GC already
/// own. Explicit repair still proves the whole closure.
async fn load_available_root<S>(
    store: &S,
    commit_id: &str,
    proof: RootAvailabilityProof,
    physical_alias_source: bool,
) -> Result<Option<TrackedStateRootId>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let metadata = if physical_alias_source {
        let commit_id = CommitId::parse_lix(commit_id, "physical checkpoint source commit id")?;
        storage::load_commit_state_manifest(store, commit_id)
            .await?
            .and_then(|manifest| manifest.snapshot_root.map(|root| *root))
    } else {
        storage::load_snapshot_commit_root(store, commit_id).await?
    };
    let Some(metadata) = metadata else {
        return Ok(None);
    };
    let readable = {
        #[cfg(feature = "storage-benches")]
        let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
            crate::storage_bench::PlanLoadPhase::AvailTreeScan,
        );
        commit_root_tree_is_readable(store, &metadata, proof).await?
    };
    if !readable {
        return Ok(None);
    }
    Ok(Some(metadata.root_id))
}

async fn commit_root_tree_is_readable<S>(
    store: &S,
    metadata: &TrackedStateCommitRoot,
    proof: RootAvailabilityProof,
) -> Result<bool, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    // One row is enough for addressability: the scan walks root -> leftmost
    // overlapping child -> leaf and stops, so a missing or corrupt root chunk
    // still fails while the probe stays `O(tree depth)`.
    let request = TrackedStateTreeScanRequest {
        limit: match proof {
            RootAvailabilityProof::Addressable => Some(1),
            RootAvailabilityProof::Complete => None,
        },
        ..TrackedStateTreeScanRequest::default()
    };
    match TrackedStateTree::new()
        .scan(store, &metadata.root_id, &request)
        .await
    {
        Ok(_) => Ok(true),
        Err(_) => Ok(false),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommitRootRebuildPlan {
    pub(crate) commit_id: CommitId,
    pub(crate) parent_commit_id: Option<CommitId>,
    pub(crate) complete_state_source_commit_id: Option<CommitId>,
    pub(crate) deltas: Vec<CommitRootRebuildDelta>,
}

async fn load_commit_root_rebuild_plan<S>(
    store: &S,
    commit_id: &str,
    allow_missing_commit: bool,
    max_members: Option<usize>,
) -> Result<CommitRootRebuildPlan, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let typed_commit_id = CommitId::parse_lix(commit_id, "commit-root rebuild commit_id")?;
    let commit = {
        #[cfg(feature = "storage-benches")]
        let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
            crate::storage_bench::PlanLoadPhase::CommitRecord,
        );
        let mut reader = ChangelogContext::new().reader(store);
        let commit_ids = [typed_commit_id];
        let batch = reader
            .load_commits(CommitLoadRequest {
                commit_ids: &commit_ids,
            })
            .await?;
        batch.into_iter().next().and_then(|(_, value)| value)
    };
    if commit.is_none() && !allow_missing_commit {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("cannot rebuild tracked_state commit_root for unknown commit '{commit_id}'"),
        ));
    }
    let manifest = storage::load_commit_state_manifest(store, typed_commit_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state root for commit '{}' without its commit-state manifest",
                    typed_commit_id
                ),
            )
        })?;
    let complete_state_source_commit_id = manifest
        .snapshot_root
        .as_ref()
        .filter(|root| root.complete_state_fence)
        .and_then(|root| root.parent_roots.first())
        .map(|source| source.commit_id);
    if complete_state_source_commit_id.is_none()
        && max_members.is_some_and(|limit| manifest.mutations.member_count as usize > limit)
    {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "commit-root migration replay exceeds configured change bound before hydration: commit '{}' has {} members",
                typed_commit_id, manifest.mutations.member_count
            ),
        ));
    }
    // Complete-state aliases rebuild by sharing their source root. Ordinary
    // roots contain only identity/index facts, so avoid hydrating JSON
    // sidecars while rebuilding them.
    let members = if complete_state_source_commit_id.is_some() {
        Vec::new()
    } else {
        storage::scan_commit_delta_members(store, typed_commit_id).await?
    };
    #[cfg(feature = "root-replay-trace")]
    let member_bytes = members
        .iter()
        .map(|(key, _)| {
            (key.schema_key.len()
                + key.file_id.as_ref().map(String::len).unwrap_or(0)
                + key.row_pk.estimated_heap_bytes()) as u64
        })
        .sum::<u64>();
    #[cfg(feature = "root-replay-trace")]
    crate::storage_bench::record_plan_load_plan(
        members.len() as u64,
        members.len() as u64,
        member_bytes,
    );
    let deltas = members
        .into_iter()
        .map(|(key, value)| CommitRootRebuildDelta {
            schema_key: key.schema_key,
            file_id: key.file_id,
            row_pk: key.row_pk,
            change_id: value.change_id,
            commit_id: value.commit_id,
            deleted: value.deleted,
            created_at: value.created_at,
            updated_at: value.updated_at,
        })
        .collect();

    Ok(CommitRootRebuildPlan {
        commit_id: typed_commit_id,
        parent_commit_id: commit
            .as_ref()
            .and_then(first_parent_commit_id)
            .or_else(|| {
                manifest
                    .snapshot_root
                    .as_ref()
                    .filter(|root| !root.complete_state_fence)
                    .and_then(|root| root.parent_roots.first())
                    .map(|parent| parent.commit_id)
            }),
        complete_state_source_commit_id,
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
    if let Some(source_commit_id) = plan.complete_state_source_commit_id {
        return writer
            .stage_complete_state_alias(plan.commit_id, source_commit_id)
            .await;
    }
    let deltas = plan
        .deltas
        .iter()
        .map(|delta| TrackedStateDeltaRef {
            schema_key: &delta.schema_key,
            file_id: delta.file_id.as_deref(),
            row_pk: &delta.row_pk,
            change_id: delta.change_id,
            commit_id: delta.commit_id,
            deleted: delta.deleted,
            created_at: delta.created_at,
            updated_at: delta.updated_at,
        })
        .collect::<Vec<_>>();
    let commit_id = plan.commit_id.to_string();
    let parent_commit_id = plan.parent_commit_id.map(|commit_id| commit_id.to_string());
    let replacement_markers = replacement_marker_keys(plan.deltas.iter())?;
    // Explicit repair must replay the same identity-aware path used by the
    // canonical root writer. The ordered bulk merge is a publication
    // optimization whose parent-stream assumptions are not sufficient for a
    // repair frontier containing shuffled filesystem lifecycle changes.
    writer
        .stage_commit_root_with_absence_guards(
            &commit_id,
            parent_commit_id.as_deref(),
            deltas,
            &BTreeSet::new(),
            &replacement_markers,
        )
        .await
}

fn replacement_marker_keys<'a>(
    deltas: impl IntoIterator<Item = &'a CommitRootRebuildDelta>,
) -> Result<BTreeSet<TrackedStateKey>, LixError> {
    deltas
        .into_iter()
        .filter(|delta| {
            !delta.deleted
                && delta.schema_key
                    == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        })
        .map(|delta| {
            // Parsing here authenticates the scope identity before the root
            // writer uses this marker to retire an older collection image.
            crate::collection_generation::collection_scope_from_row_pk(&delta.row_pk)?;
            Ok(TrackedStateKey {
                schema_key: delta.schema_key.clone(),
                file_id: delta.file_id.clone(),
                row_pk: delta.row_pk.clone(),
            })
        })
        .collect()
}

/// Collapses one contiguous rootless first-parent replay interval into its
/// canonical terminal root.
///
/// Rootless commit roots are transient implementation details: immutable
/// commit-delta authority remains the source of every mutation, while only the
/// terminal root is needed as the parent of the publication being assembled.
/// Applying the latest authenticated delta for each key once changes replay
/// from O(H * D * log N) frontier rewrites to
/// O(H * D * log U + U * log N), where U is the number of unique keys in the
/// interval.
///
/// File-descriptor deletion has ordered cascade semantics, so those uncommon
/// intervals stay on the canonical sequential algorithm. Certified collection
/// generations collapse by clearing older members of their scope before the
/// replacement commit is folded into the terminal map.
pub(crate) async fn try_stage_collapsed_rebuild_plans_with_writer<S>(
    writer: &mut TrackedStateWriter<'_, S>,
    plans: &[CommitRootRebuildPlan],
) -> Result<Option<TrackedStateWriteReport>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    if plans.len() < 2 {
        return Ok(None);
    }
    for pair in plans.windows(2) {
        if pair[0].parent_commit_id != Some(pair[1].commit_id) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked-state collapsed rebuild plans are not one first-parent interval",
            ));
        }
    }
    if plans
        .iter()
        .flat_map(|plan| &plan.deltas)
        .any(|delta| delta.deleted && delta.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
    {
        return Ok(None);
    }

    #[derive(Clone, Copy)]
    struct ReplacementTombstone {
        change_id: ChangeId,
        commit_id: CommitId,
        updated_at: LixTimestamp,
    }
    #[derive(Clone, Copy)]
    struct TerminalDelta<'a> {
        source: &'a CommitRootRebuildDelta,
        created_at: LixTimestamp,
        replacement_tombstone: Option<ReplacementTombstone>,
    }
    impl TerminalDelta<'_> {
        fn as_ref(&self) -> TrackedStateDeltaRef<'_> {
            let replacement = self.replacement_tombstone;
            TrackedStateDeltaRef {
                schema_key: &self.source.schema_key,
                file_id: self.source.file_id.as_deref(),
                row_pk: &self.source.row_pk,
                change_id: replacement.map_or(self.source.change_id, |value| value.change_id),
                commit_id: replacement.map_or(self.source.commit_id, |value| value.commit_id),
                deleted: replacement.is_some() || self.source.deleted,
                created_at: self.created_at,
                updated_at: replacement.map_or(self.source.updated_at, |value| value.updated_at),
            }
        }
    }

    // Every key and terminal delta borrows immutable rebuild-plan authority.
    // This avoids cloning schema/file strings and primary keys twice for every
    // row in a long rootless interval.
    let mut terminal_by_key = BTreeMap::<TrackedStateKeyRef<'_>, TerminalDelta<'_>>::new();
    for plan in plans.iter().rev() {
        let replacement_scopes = plan
            .deltas
            .iter()
            .filter(|delta| {
                !delta.deleted
                    && delta.schema_key
                        == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
            })
            .map(|delta| {
                let scope =
                    crate::collection_generation::collection_scope_from_row_pk(&delta.row_pk)?;
                Ok((scope, delta))
            })
            .collect::<Result<Vec<_>, LixError>>()?;
        for ((schema_key, file_id), marker) in replacement_scopes {
            for (key, terminal) in &mut terminal_by_key {
                if key.schema_key == schema_key && key.file_id == file_id.as_deref() {
                    terminal.replacement_tombstone = Some(ReplacementTombstone {
                        change_id: marker.change_id,
                        commit_id: marker.commit_id,
                        updated_at: marker.updated_at,
                    });
                }
            }
        }
        for delta in &plan.deltas {
            let key = TrackedStateKeyRef {
                schema_key: &delta.schema_key,
                file_id: delta.file_id.as_deref(),
                row_pk: &delta.row_pk,
            };
            match terminal_by_key.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(TerminalDelta {
                        source: delta,
                        created_at: delta.created_at,
                        replacement_tombstone: None,
                    });
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let created_at = entry.get().created_at;
                    // Sequential replay preserves the first visible lifecycle
                    // timestamp across every later update, tombstone, and
                    // reinsert. The durable base, when present, may replace it
                    // once more inside the canonical root writer.
                    entry.insert(TerminalDelta {
                        source: delta,
                        created_at,
                        replacement_tombstone: None,
                    });
                }
            }
        }
    }
    let deltas = terminal_by_key
        .values()
        .map(TerminalDelta::as_ref)
        .collect::<Vec<_>>();
    let terminal_commit_id = plans[0].commit_id.to_string();
    let base_commit_id = plans
        .last()
        .and_then(|plan| plan.parent_commit_id)
        .map(|commit_id| commit_id.to_string());
    let replacement_markers = terminal_by_key
        .iter()
        .filter(|(_, terminal)| {
            terminal.replacement_tombstone.is_none()
                && !terminal.source.deleted
                && terminal.source.schema_key
                    == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        })
        .map(|(key, _)| TrackedStateKey {
            schema_key: key.schema_key.to_owned(),
            file_id: key.file_id.map(str::to_owned),
            row_pk: key.row_pk.clone(),
        })
        .collect::<BTreeSet<_>>();
    writer
        .stage_commit_root_with_absence_guards(
            &terminal_commit_id,
            base_commit_id.as_deref(),
            deltas,
            &BTreeSet::new(),
            &replacement_markers,
        )
        .await
        .map(Some)
}

fn first_parent_commit_id(commit: &CommitRecord) -> Option<CommitId> {
    commit.parent_commit_ids.first().copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::LixTimestamp;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageReadOptions};

    fn delta(
        key: &str,
        commit: &str,
        created_millis: i64,
        updated_millis: i64,
        deleted: bool,
    ) -> CommitRootRebuildDelta {
        CommitRootRebuildDelta {
            schema_key: "test_row".to_owned(),
            file_id: None,
            row_pk: RowPk::single(key),
            change_id: ChangeId::for_test_label(&format!("{commit}-{key}")),
            commit_id: CommitId::for_test_label(commit),
            deleted,
            created_at: LixTimestamp::from_unix_millis_utc_lossy(created_millis),
            updated_at: LixTimestamp::from_unix_millis_utc_lossy(updated_millis),
        }
    }

    fn plan(
        commit: &str,
        parent: Option<&str>,
        deltas: Vec<CommitRootRebuildDelta>,
    ) -> CommitRootRebuildPlan {
        CommitRootRebuildPlan {
            commit_id: CommitId::for_test_label(commit),
            parent_commit_id: parent.map(CommitId::for_test_label),
            complete_state_source_commit_id: None,
            deltas,
        }
    }

    async fn sequential_and_collapsed_roots(
        plans: &[CommitRootRebuildPlan],
    ) -> (TrackedStateRootId, TrackedStateRootId) {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("test read should open");
        let context = TrackedStateContext::new();

        let mut sequential_writes = StorageWriteSet::new();
        let mut sequential = context.writer(&read, &mut sequential_writes);
        let mut sequential_report = None;
        for plan in plans.iter().rev() {
            sequential_report = Some(
                stage_rebuild_plan_with_writer(&mut sequential, plan)
                    .await
                    .expect("sequential replay should stage"),
            );
        }

        let mut collapsed_writes = StorageWriteSet::new();
        let mut collapsed = context.writer(&read, &mut collapsed_writes);
        let collapsed_report = try_stage_collapsed_rebuild_plans_with_writer(&mut collapsed, plans)
            .await
            .expect("collapsed replay should validate")
            .expect("multi-plan ordinary replay should collapse");
        (
            sequential_report
                .expect("sequential replay has a root")
                .root_id,
            collapsed_report.root_id,
        )
    }

    #[tokio::test]
    async fn collapsed_replay_matches_sequential_lifecycle_roots() {
        for terminal in [
            delta("row", "second-update", 20, 20, false),
            delta("row", "second-delete", 20, 20, true),
        ] {
            let plans = vec![
                plan("second", Some("first"), vec![terminal]),
                plan("first", None, vec![delta("row", "first", 10, 10, false)]),
            ];
            let (sequential, collapsed) = sequential_and_collapsed_roots(&plans).await;
            assert_eq!(collapsed, sequential);
        }

        let plans = vec![
            plan(
                "third",
                Some("second"),
                vec![delta("row", "third", 30, 30, false)],
            ),
            plan(
                "second",
                Some("first"),
                vec![delta("row", "second", 20, 20, true)],
            ),
            plan("first", None, vec![delta("row", "first", 10, 10, false)]),
        ];
        let (sequential, collapsed) = sequential_and_collapsed_roots(&plans).await;
        assert_eq!(collapsed, sequential);
    }

    #[tokio::test]
    async fn shared_rootless_suffixes_stage_independent_terminal_and_child_roots() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("test read should open");
        let context = TrackedStateContext::new();
        let mut writes = StorageWriteSet::new();
        let mut writer = context.writer(&read, &mut writes);
        let suffix = vec![
            plan(
                "suffix-new",
                Some("suffix-old"),
                vec![delta("shared-new", "suffix-new", 20, 20, false)],
            ),
            plan(
                "suffix-old",
                None,
                vec![delta("shared-old", "suffix-old", 10, 10, false)],
            ),
        ];
        let mut left = vec![plan(
            "left",
            Some("suffix-new"),
            vec![delta("left", "left", 30, 30, false)],
        )];
        left.extend(suffix.clone());
        let mut right = vec![plan(
            "right",
            Some("suffix-new"),
            vec![delta("right", "right", 30, 30, false)],
        )];
        right.extend(suffix);

        let left_report = try_stage_collapsed_rebuild_plans_with_writer(&mut writer, &left)
            .await
            .expect("left collapse should validate")
            .expect("left collapse should stage");
        let right_report = try_stage_collapsed_rebuild_plans_with_writer(&mut writer, &right)
            .await
            .expect("right collapse should validate")
            .expect("right collapse should stage");
        assert_ne!(left_report.root_id, right_report.root_id);

        for (parent, child) in [("left", "left-child"), ("right", "right-child")] {
            let child_delta = delta(child, child, 40, 40, false);
            writer
                .stage_commit_root(
                    &CommitId::for_test_label(child).to_string(),
                    Some(&CommitId::for_test_label(parent).to_string()),
                    [TrackedStateDeltaRef {
                        schema_key: &child_delta.schema_key,
                        file_id: child_delta.file_id.as_deref(),
                        row_pk: &child_delta.row_pk,
                        change_id: child_delta.change_id,
                        commit_id: child_delta.commit_id,
                        deleted: child_delta.deleted,
                        created_at: child_delta.created_at,
                        updated_at: child_delta.updated_at,
                    }],
                )
                .await
                .expect("child should use its independently staged parent root");
        }
    }

    #[tokio::test]
    async fn order_sensitive_lifecycle_intervals_keep_sequential_replay() {
        let storage = StorageAdapter::new(Memory::new());
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("test read should open");
        let context = TrackedStateContext::new();
        for sensitive_delta in [CommitRootRebuildDelta {
            schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
            deleted: true,
            ..delta("file", "file-delete", 20, 20, true)
        }] {
            let plans = vec![
                plan("second", Some("first"), vec![sensitive_delta]),
                plan("first", None, vec![delta("row", "first", 10, 10, false)]),
            ];
            let mut writes = StorageWriteSet::new();
            let mut writer = context.writer(&read, &mut writes);
            assert!(
                try_stage_collapsed_rebuild_plans_with_writer(&mut writer, &plans)
                    .await
                    .expect("sensitive replay classification should validate")
                    .is_none(),
                "order-sensitive lifecycle replay must use the sequential writer"
            );
        }
    }

    #[tokio::test]
    async fn collapsed_replay_matches_sequential_collection_replacement() {
        let marker = |commit: &str, millis: i64| CommitRootRebuildDelta {
            schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY.to_owned(),
            row_pk: RowPk::single(crate::collection_generation::collection_scope_key(
                crate::collection_generation::CollectionScopeRef {
                    schema_key: "test_row",
                    file_id: None,
                },
            )),
            ..delta("marker", commit, millis, millis, false)
        };
        let plans = vec![
            plan(
                "second",
                Some("first"),
                vec![
                    delta("new", "second", 20, 20, false),
                    marker("second-marker", 20),
                ],
            ),
            plan(
                "first",
                None,
                vec![
                    delta("old-a", "first-a", 10, 10, false),
                    delta("old-b", "first-b", 10, 10, false),
                    marker("first-marker", 10),
                ],
            ),
        ];
        let (sequential, collapsed) = sequential_and_collapsed_roots(&plans).await;
        assert_eq!(collapsed, sequential);
    }
}
