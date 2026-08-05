use std::collections::{BTreeMap, BTreeSet};

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

#[derive(Debug, PartialEq, Eq)]
enum RootAvailability {
    Missing,
    Corrupt(LixError),
    Available(TrackedStateRootId),
}

const MISSING_TREE_CHUNK_CODE: &str = "LIX_TRACKED_STATE_MISSING_CHUNK";

#[cfg(any(test, feature = "storage-benches"))]
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RootAvailabilityCounters {
    pub(crate) probes: u64,
    pub(crate) root_chunk_reads: u64,
    pub(crate) available: u64,
    pub(crate) missing: u64,
    pub(crate) corrupt: u64,
    pub(crate) missing_descendant_retries: u64,
    pub(crate) full_tree_scans: u64,
    pub(crate) canonical_rebuilds: u64,
}

#[cfg(test)]
#[allow(dead_code)]
mod counters {
    use std::cell::Cell;

    use super::RootAvailabilityCounters;

    thread_local! {
        static STATE: Cell<RootAvailabilityCounters> = const {
            Cell::new(RootAvailabilityCounters {
                probes: 0,
                root_chunk_reads: 0,
                available: 0,
                missing: 0,
                corrupt: 0,
                missing_descendant_retries: 0,
                full_tree_scans: 0,
                canonical_rebuilds: 0,
            })
        };
    }

    pub(super) fn reset() {
        STATE.with(|state| state.set(RootAvailabilityCounters::default()));
    }

    pub(super) fn snapshot() -> RootAvailabilityCounters {
        STATE.with(Cell::get)
    }

    fn update(update: impl FnOnce(&mut RootAvailabilityCounters)) {
        STATE.with(|state| {
            let mut counters = state.get();
            update(&mut counters);
            state.set(counters);
        });
    }

    pub(super) fn probe() {
        update(|counters| counters.probes += 1);
    }

    pub(super) fn root_chunk_read() {
        update(|counters| counters.root_chunk_reads += 1);
    }

    pub(super) fn available() {
        update(|counters| counters.available += 1);
    }

    pub(super) fn missing() {
        update(|counters| counters.missing += 1);
    }

    pub(super) fn corrupt() {
        update(|counters| counters.corrupt += 1);
    }

    pub(super) fn missing_descendant_retry() {
        update(|counters| counters.missing_descendant_retries += 1);
    }
}

#[cfg(all(not(test), feature = "storage-benches"))]
#[allow(dead_code)]
mod counters {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::RootAvailabilityCounters;

    static PROBES: AtomicU64 = AtomicU64::new(0);
    static ROOT_CHUNK_READS: AtomicU64 = AtomicU64::new(0);
    static AVAILABLE: AtomicU64 = AtomicU64::new(0);
    static MISSING: AtomicU64 = AtomicU64::new(0);
    static CORRUPT: AtomicU64 = AtomicU64::new(0);
    static MISSING_DESCENDANT_RETRIES: AtomicU64 = AtomicU64::new(0);

    pub(super) fn reset() {
        for counter in [
            &PROBES,
            &ROOT_CHUNK_READS,
            &AVAILABLE,
            &MISSING,
            &CORRUPT,
            &MISSING_DESCENDANT_RETRIES,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }

    pub(super) fn snapshot() -> RootAvailabilityCounters {
        RootAvailabilityCounters {
            probes: PROBES.load(Ordering::Relaxed),
            root_chunk_reads: ROOT_CHUNK_READS.load(Ordering::Relaxed),
            available: AVAILABLE.load(Ordering::Relaxed),
            missing: MISSING.load(Ordering::Relaxed),
            corrupt: CORRUPT.load(Ordering::Relaxed),
            missing_descendant_retries: MISSING_DESCENDANT_RETRIES.load(Ordering::Relaxed),
            // These paths were deleted by the hard cut. Keeping explicit
            // zero-valued fields makes the test/benchmark proof auditable.
            full_tree_scans: 0,
            canonical_rebuilds: 0,
        }
    }

    pub(super) fn probe() {
        PROBES.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn root_chunk_read() {
        ROOT_CHUNK_READS.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn available() {
        AVAILABLE.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn missing() {
        MISSING.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn corrupt() {
        CORRUPT.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn missing_descendant_retry() {
        MISSING_DESCENDANT_RETRIES.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(any(test, feature = "storage-benches"))]
#[allow(dead_code)]
pub(crate) fn reset_root_availability_counters() {
    counters::reset();
}

#[cfg(any(test, feature = "storage-benches"))]
#[allow(dead_code)]
pub(crate) fn root_availability_counters() -> RootAvailabilityCounters {
    counters::snapshot()
}

#[cfg(any(test, feature = "storage-benches"))]
pub(crate) fn record_missing_descendant_retry() {
    counters::missing_descendant_retry();
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
    let mut skipped_root = None;
    let mut retried_missing_descendant = false;
    let (report, state) = loop {
        let plans = load_rebuild_plans_skipping_root(
            rebuilder.store,
            commit_id,
            true,
            skipped_root.as_deref(),
        )
        .await?;
        match stage_rebuild_plans_once(rebuilder, &plans).await {
            Ok(result) => break result,
            Err(error) if !retried_missing_descendant && is_missing_tree_chunk_error(&error) => {
                let Some(missing_base_root) = plans
                    .iter()
                    .find(|plan| plan.parent_commit_id.is_some())
                    .and_then(|plan| plan.parent_commit_id)
                else {
                    return Err(error);
                };
                retried_missing_descendant = true;
                #[cfg(any(test, feature = "storage-benches"))]
                counters::missing_descendant_retry();
                skipped_root = Some(missing_base_root.to_string());
            }
            Err(error) => return Err(error),
        }
    };
    let context = TrackedStateContext::new();
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

async fn stage_rebuild_plans_once<S>(
    rebuilder: &mut TrackedStateRootRebuilder<'_, S>,
    plans: &[CommitRootRebuildPlan],
) -> Result<(TrackedStateWriteReport, TrackedStateTransientRebuildState), LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let context = TrackedStateContext::new();
    let mut report = None;
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
            "tracked_state commit_root rebuild did not stage a root",
        )
    })?;
    Ok((report, state))
}

#[cfg_attr(not(any(test, feature = "storage-benches")), allow(dead_code))]
pub(crate) fn is_missing_tree_chunk_error(error: &LixError) -> bool {
    error.code == MISSING_TREE_CHUNK_CODE
}

#[allow(dead_code)]
pub(crate) async fn load_rebuild_plans_to_nearest_available_root<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
) -> Result<Vec<CommitRootRebuildPlan>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    load_rebuild_plans_skipping_root(store, commit_id, force_head, None).await
}

pub(crate) async fn load_rebuild_plans_skipping_root<S>(
    store: &S,
    commit_id: &str,
    force_head: bool,
    skipped_root: Option<&str>,
) -> Result<Vec<CommitRootRebuildPlan>, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    let mut plans = Vec::new();
    let mut current_commit_id = commit_id.to_string();
    let mut force_current = force_head;
    let mut seen_commit_ids = BTreeSet::new();
    loop {
        if !seen_commit_ids.insert(current_commit_id.clone()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state commit_root for commit '{commit_id}': first-parent cycle includes commit '{current_commit_id}'"
                ),
            ));
        }
        if !force_current && skipped_root != Some(current_commit_id.as_str()) {
            match load_available_root(store, &current_commit_id).await? {
                RootAvailability::Available(_) => break,
                RootAvailability::Missing => {}
                RootAvailability::Corrupt(error) => return Err(error),
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

async fn load_available_root<S>(store: &S, commit_id: &str) -> Result<RootAvailability, LixError>
where
    S: StorageAdapterRead + ?Sized,
{
    #[cfg(any(test, feature = "storage-benches"))]
    counters::probe();
    let metadata = match storage::load_snapshot_commit_root(store, commit_id).await {
        Ok(Some(metadata)) => metadata,
        Ok(None) => {
            #[cfg(any(test, feature = "storage-benches"))]
            counters::missing();
            return Ok(RootAvailability::Missing);
        }
        Err(error) => {
            #[cfg(any(test, feature = "storage-benches"))]
            counters::corrupt();
            return Ok(RootAvailability::Corrupt(error));
        }
    };
    #[cfg(any(test, feature = "storage-benches"))]
    counters::root_chunk_read();
    let tree = TrackedStateTree::new();
    match tree
        .validate_root_chunk_availability(store, &metadata.root_id, metadata.row_count_estimate)
        .await
    {
        Ok(true) => {
            #[cfg(any(test, feature = "storage-benches"))]
            counters::available();
            Ok(RootAvailability::Available(metadata.root_id))
        }
        Ok(false) => {
            #[cfg(any(test, feature = "storage-benches"))]
            counters::missing();
            Ok(RootAvailability::Missing)
        }
        Err(error) => {
            #[cfg(any(test, feature = "storage-benches"))]
            counters::corrupt();
            Ok(RootAvailability::Corrupt(error))
        }
    }
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
