use std::collections::{BTreeMap, HashSet};

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
use crate::tracked_state::{TrackedStateDeltaRef, TrackedStateKey};

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

pub(crate) async fn load_rebuild_plans_to_nearest_available_root_with_proof<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
    proof: RootAvailabilityProof,
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
        if !force_current {
            #[cfg(feature = "storage-benches")]
            let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
                crate::storage_bench::PlanLoadPhase::AvailProbe,
            );
            let available = load_available_root(store, &current_commit_id, proof).await?;
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_root_replay_available_root_probe(available.is_some());
            if available.is_some() {
                break;
            }
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

/// Resolves the durable root a replay may resume from.
///
/// The commit-state manifest is the single immutable authority for a commit's
/// tracked-state root: it is sealed atomically with the commit, keyed by that
/// commit id, and never rewritten. `rebuild_commit_root_at_inner` already
/// treats a rebuild that disagrees with it as a hard error rather than a
/// repair, so the manifest — not a replay of the changelog — is what makes a
/// root canonical. Availability therefore has exactly two obligations:
///
/// 1. the root pointer is live (`load_snapshot_commit_root` also proves the
///    commit itself exists in the changelog, so an orphaned manifest cannot
///    authorize a resume), and
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
) -> Result<Option<TrackedStateRootId>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let Some(metadata) = storage::load_snapshot_commit_root(store, commit_id).await? else {
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
    parent_commit_id: Option<CommitId>,
    replacement_generation: Option<storage::CommitDeltaReplacementGeneration>,
    deltas: Vec<CommitRootRebuildDelta>,
}

async fn load_commit_root_rebuild_plan<S>(
    store: &S,
    commit_id: &str,
) -> Result<CommitRootRebuildPlan, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let commit = {
        #[cfg(feature = "storage-benches")]
        let _phase = crate::storage_bench::PlanLoadPhaseScope::enter(
            crate::storage_bench::PlanLoadPhase::CommitRecord,
        );
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
        batch
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
            })?
    };
    // Rebuild needs the authenticated authored/selected discriminator but not
    // payload bodies. This bounded member load decodes immutable identities
    // without hydrating terminal JSON/public projections.
    let manifest = storage::load_commit_state_manifest(store, commit.commit_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state root for commit '{}' without its commit-state manifest",
                    commit.commit_id
                ),
            )
        })?;
    let replacement_generation =
        storage::commit_delta_replay_metadata_from_inventory(&manifest.mutations)
            .replacement_generation;
    let members = storage::load_commit_delta_members_for_schemas(
        store,
        commit.commit_id,
        &[],
        &[],
        usize::MAX,
        false,
    )
    .await?
    .expect("unbounded authenticated member load is complete");
    #[cfg(feature = "root-replay-trace")]
    let member_bytes = members
        .iter()
        .map(|member| {
            (member.key.schema_key.len()
                + member.key.file_id.as_ref().map(String::len).unwrap_or(0)
                + member.key.row_pk.estimated_heap_bytes()) as u64
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
        .map(|member| CommitRootRebuildDelta {
            schema_key: member.key.schema_key,
            file_id: member.key.file_id,
            row_pk: member.key.row_pk,
            change_id: member.value.change_id,
            commit_id: member.value.commit_id,
            deleted: member.value.deleted,
            created_at: member.value.created_at,
            updated_at: member.value.updated_at,
        })
        .collect();

    let parent_commit_id = manifest
        .mutations
        .selected_source_commit_id()
        .or_else(|| {
            manifest
                .snapshot_root
                .as_ref()
                .and_then(|root| root.parent_roots.first().map(|parent| parent.commit_id))
        })
        .or_else(|| first_parent_commit_id(&commit))
        // The replacement fallback is scope-local replay authority, not the
        // whole-root ancestry. It is usable only when the commit has no graph
        // parent/root metadata from which unaffected scopes can be retained.
        .or_else(|| {
            replacement_generation
                .as_ref()
                .and_then(|generation| generation.fallback_commit_id)
        });

    Ok(CommitRootRebuildPlan {
        commit_id: commit.commit_id,
        parent_commit_id,
        replacement_generation,
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
    // Explicit repair must replay the same identity-aware path used by the
    // canonical root writer. The ordered bulk merge is a publication
    // optimization whose parent-stream assumptions are not sufficient for a
    // repair frontier containing shuffled filesystem lifecycle changes.
    let lifecycle_override_keys = plan
        .deltas
        .iter()
        .filter(|delta| {
            plan.replacement_generation
                .as_ref()
                .is_some_and(|generation| {
                    generation.scope.schema_key == delta.schema_key
                        && generation.scope.file_id == delta.file_id
                })
        })
        .map(|delta| TrackedStateKey {
            schema_key: delta.schema_key.clone(),
            file_id: delta.file_id.clone(),
            row_pk: delta.row_pk.clone(),
        })
        .collect::<std::collections::BTreeSet<_>>();
    writer
        .stage_commit_root_from_authenticated_rebuild(
            &commit_id,
            parent_commit_id.as_deref(),
            deltas,
            &std::collections::BTreeSet::new(),
            plan.replacement_generation
                .as_ref()
                .map(|generation| &generation.scope),
            &lifecycle_override_keys,
        )
        .await
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
/// intervals stay on the canonical sequential algorithm.
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
        .any(|plan| plan.replacement_generation.is_some())
        || plans.iter().flat_map(|plan| &plan.deltas).any(|delta| {
            (delta.deleted && delta.schema_key == FILE_DESCRIPTOR_SCHEMA_KEY)
                || delta.schema_key
                    == crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
        })
    {
        return Ok(None);
    }

    let mut terminal_by_key = BTreeMap::<TrackedStateKey, CommitRootRebuildDelta>::new();
    for plan in plans.iter().rev() {
        for delta in &plan.deltas {
            let key = TrackedStateKey {
                schema_key: delta.schema_key.clone(),
                file_id: delta.file_id.clone(),
                row_pk: delta.row_pk.clone(),
            };
            match terminal_by_key.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(delta.clone());
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let created_at = entry.get().created_at;
                    let mut terminal = delta.clone();
                    // Sequential replay preserves the first visible lifecycle
                    // timestamp across every later update, tombstone, and
                    // reinsert. The durable base, when present, may replace it
                    // once more inside the canonical root writer.
                    terminal.created_at = created_at;
                    entry.insert(terminal);
                }
            }
        }
    }
    let deltas = terminal_by_key
        .values()
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
    let terminal_commit_id = plans[0].commit_id.to_string();
    let base_commit_id = plans
        .last()
        .and_then(|plan| plan.parent_commit_id)
        .map(|commit_id| commit_id.to_string());
    writer
        .stage_commit_root(&terminal_commit_id, base_commit_id.as_deref(), deltas)
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
            replacement_generation: None,
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
        for sensitive_delta in [
            CommitRootRebuildDelta {
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
                deleted: true,
                ..delta("file", "file-delete", 20, 20, true)
            },
            CommitRootRebuildDelta {
                schema_key: crate::collection_generation::COLLECTION_GENERATION_SCHEMA_KEY
                    .to_owned(),
                ..delta("replacement", "replacement", 20, 20, false)
            },
        ] {
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
}
