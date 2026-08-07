use std::collections::BTreeMap;
use std::ops::Bound;

use bytes::Bytes;

use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, KeyRange, Precondition, ProjectedValue,
    PutBatch, PutEntry, ReadOptions, ScanOptions, Storage, StorageError, StorageWrite, StoredValue,
    WriteOptions,
};
use crate::storage_adapter::{StorageAdapterRead, StorageAdapterReadScope};

use super::blob::authenticate_open_upload_progress;
use super::codec::{corruption, keyed_hash};
use super::gc_index::{
    MaintenanceEdit, live_contains, live_insert, mark_insert, mark_range_iter, queue_pop,
    queue_push,
};
use super::model::{
    BlobChunkV1, BlobManifestV1, BranchSelectorV1, BranchSnapshotV1, ChangeCatalogEntry,
    ChangeCatalogOwner, ChangeId, ChangeObjectV1, CommitCatalogEntry, CommitId, CommitObjectV1,
    GcEdgeCursorV1, GcLiveBranchEntryV1, GcMarkEntryV2, GcPhaseV2, GcProgressSelectorV2,
    GcProgressV2, GcQueueEntryV1, GlobalSelectorV1, RepositoryRootV1, SnapshotSelectorV1,
    SnapshotTargetV1, UploadPartV1, UploadProgressV1, UploadSelectorV1, branch_selector_key,
    gc_progress_selector_key, global_selector_key, snapshot_selector_key, upload_selector_key,
};
use super::object::{OBJECT_SPACE, ObjectDomain, ObjectId, authenticate_object_domain};
use super::serving::{validate_retained_commit, validate_retained_ref_change};
use super::state::{UNTRACKED_ROW_SPACE, decode_untracked_key, decode_untracked_value};
use super::tree::ordered_tree_edges;
use super::view::{SELECTOR_SPACE, load_object_bytes};

const SELECTOR_PAGE_ROWS: usize = 256;
const UNTRACKED_PAGE_ROWS: usize = 1;
const TRAVERSAL_BATCH_CLAIMS: usize = 128;
const EDGE_PAGE_ENTRIES: usize = super::model::AUTHENTICATED_EDGE_PAGE_ENTRIES;
const SWEEP_PAGE_ROWS: usize = 256;
const DELETE_BATCH_IDS: usize = 256;
const LIVE_BRANCH_DIGEST_DOMAIN: &str = "lix forktree gc live branch v1";
const GC_CYCLE_DOMAIN: &str = "forktree.gc-cycle.v2";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GcBudget {
    max_claims: usize,
    max_deletes: usize,
}

impl Default for GcBudget {
    fn default() -> Self {
        Self {
            max_claims: TRAVERSAL_BATCH_CLAIMS,
            max_deletes: DELETE_BATCH_IDS,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GcStepStatus {
    Started,
    Advanced {
        phase: GcPhaseV2,
        marked: u64,
        validated: u64,
        reclaimed: u64,
    },
    Complete {
        reclaimed: u64,
    },
    AbortedCorruptProgress,
}

struct GcSnapshot<R> {
    read: R,
    raw_global: Bytes,
    global: GlobalSelectorV1,
    raw_progress_selector: Option<Bytes>,
    progress_selector: Option<GcProgressSelectorV2>,
    progress: Option<GcProgressV2>,
}

/// Authenticated semantic object deletions produced only by the sweep owner.
///
/// The private field prevents sibling owners from forging a batch, while the
/// bounded constructor keeps one commit independent of total orphan count.
/// Sealed shape: `SweepBatch { private: SweepBatchState }`.
#[derive(Default)]
struct SweepBatch {
    private: SweepBatchState,
}

#[derive(Default)]
struct SweepBatchState {
    object_ids: Vec<ObjectId>,
}

impl SweepBatch {
    fn push(&mut self, id: ObjectId, limit: usize) -> Result<(), StorageError> {
        if self.private.object_ids.len() >= limit
            || self.private.object_ids.len() >= DELETE_BATCH_IDS
        {
            return Err(corruption(
                "ForkTree sweep batch exceeds its bounded contract",
            ));
        }
        self.private.object_ids.push(id);
        Ok(())
    }

    fn keys(&self) -> impl Iterator<Item = Key> + '_ {
        self.private.object_ids.iter().copied().map(object_key)
    }

    fn len(&self) -> usize {
        self.private.object_ids.len()
    }
}

/// Advances at most one bounded ForkTree GC checkpoint.
///
/// Every invocation opens one fresh coherent read and commits at most one
/// epoch-fenced page. No adapter snapshot token, root set, reachable set, or
/// orphan set survives the call in memory. The persisted maintenance graph is
/// rebuildable and is never consulted by serving reads.
pub(crate) async fn advance_gc<S>(
    storage: &S,
    budget: GcBudget,
) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
{
    let snapshot = load_gc_snapshot(storage).await?;
    let Some(mut progress) = snapshot.progress.clone() else {
        return start_cycle(storage, snapshot).await;
    };
    match progress.phase {
        GcPhaseV2::RootSelectors => advance_selector_roots(storage, snapshot, &mut progress).await,
        GcPhaseV2::RootUntracked => advance_untracked_roots(storage, snapshot, &mut progress).await,
        GcPhaseV2::Traverse => advance_traversal(storage, snapshot, &mut progress, budget).await,
        GcPhaseV2::Sweep => advance_sweep(storage, snapshot, &mut progress, budget).await,
        GcPhaseV2::Cleanup => advance_cleanup(storage, snapshot, &mut progress, budget).await,
    }
}

/// Removes only a malformed GC selector/progress edge under the exact current
/// global epoch. It never accepts semantic object IDs and cannot stage a
/// semantic deletion. Orphaned maintenance objects are reclaimed by a later
/// complete cycle.
pub(crate) async fn abort_corrupt_gc<S>(storage: &S) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
{
    let read = StorageAdapterReadScope::new(storage.begin_read(ReadOptions::default()).await?);
    let keys = [Key(global_selector_key()), Key(gc_progress_selector_key())];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    let raw_global = required_full(
        loaded.values.first().cloned().flatten(),
        "global selector is absent",
    )?;
    let raw_progress = match loaded.values.get(1).cloned().flatten() {
        None => return Ok(GcStepStatus::AbortedCorruptProgress),
        Some(ProjectedValue::FullValue(bytes)) => bytes,
        Some(ProjectedValue::KeyOnly) => {
            return Err(corruption("GC selector read returned key-only data"));
        }
    };
    let global = GlobalSelectorV1::decode(&raw_global)?;
    let next_global = global.rotated()?.encode()?;
    let mut write = storage
        .begin_write(WriteOptions {
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: SELECTOR_SPACE,
                    key: Key(global_selector_key()),
                    expected: raw_global,
                },
                Precondition::KeyValueEquals {
                    space: SELECTOR_SPACE,
                    key: Key(gc_progress_selector_key()),
                    expected: raw_progress,
                },
            ],
            ..WriteOptions::default()
        })
        .await?;
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(global_selector_key()),
                    value: StoredValue { bytes: next_global },
                }],
            },
        )
        .await?;
    write
        .delete_many(SELECTOR_SPACE, &[Key(gc_progress_selector_key())])
        .await?;
    write.commit().await?;
    Ok(GcStepStatus::AbortedCorruptProgress)
}

async fn load_gc_snapshot<S>(
    storage: &S,
) -> Result<GcSnapshot<StorageAdapterReadScope<S::Read<'_>>>, StorageError>
where
    S: Storage,
{
    let read = StorageAdapterReadScope::new(storage.begin_read(ReadOptions::default()).await?);
    let keys = [Key(global_selector_key()), Key(gc_progress_selector_key())];
    let loaded = read
        .get_many(&[GetManyRequest {
            space: SELECTOR_SPACE,
            keys: &keys,
            opts: GetOptions {
                projection: CoreProjection::FullValue,
            },
        }])
        .await?;
    if loaded.values.len() != 2 {
        return Err(corruption("GC selector pair read has invalid cardinality"));
    }
    let raw_global = required_full(loaded.values[0].clone(), "global selector is absent")?;
    let global = GlobalSelectorV1::decode(&raw_global)?;
    let raw_progress_selector = match loaded.values[1].clone() {
        None => None,
        Some(ProjectedValue::FullValue(bytes)) => Some(bytes),
        Some(ProjectedValue::KeyOnly) => {
            return Err(corruption("GC selector pair returned key-only data"));
        }
    };
    let (progress_selector, progress) = match &raw_progress_selector {
        None => (None, None),
        Some(raw) => {
            let selector = GcProgressSelectorV2::decode(raw)?;
            let bytes = load_object_bytes(&read, selector.progress_object_id).await?;
            let progress = GcProgressV2::decode(selector.progress_object_id, &bytes)?;
            if progress.cycle_id != selector.cycle_id
                || progress.expected_global_digest != global_digest(&raw_global)
                || progress.expected_global_epoch != global.epoch
            {
                return Err(corruption(
                    "GC progress selector, cycle, or global fence is inconsistent",
                ));
            }
            authenticate_progress_roots(&read, &progress).await?;
            (Some(selector), Some(progress))
        }
    };
    Ok(GcSnapshot {
        read,
        raw_global,
        global,
        raw_progress_selector,
        progress_selector,
        progress,
    })
}

async fn authenticate_progress_roots<R>(
    read: &R,
    progress: &GcProgressV2,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    for (root, expected_leaf, expected_kind) in [
        (
            progress.live_branch_index_root,
            ObjectDomain::GcLiveBranchPackV1,
            super::model::GcRadixKindV1::LiveBranch,
        ),
        (
            progress.mark_index_root,
            ObjectDomain::GcMarkPackV2,
            super::model::GcRadixKindV1::Mark,
        ),
        (
            progress.queue_index_root,
            ObjectDomain::GcQueuePackV1,
            super::model::GcRadixKindV1::Queue,
        ),
    ] {
        let Some(root) = root else { continue };
        let bytes = load_object_bytes(read, root).await?;
        let domain = authenticate_object_domain(root, &bytes)?;
        if domain != ObjectDomain::GcRadixNodeV1 && domain != expected_leaf {
            return Err(corruption(
                "GC progress root has another maintenance domain",
            ));
        }
        match domain {
            ObjectDomain::GcRadixNodeV1 => {
                let node = super::model::GcRadixNodeV1::decode(root, &bytes)?;
                if node.cycle_id != progress.cycle_id || node.kind != expected_kind {
                    return Err(corruption(
                        "GC progress radix belongs to another cycle or owner index",
                    ));
                }
            }
            ObjectDomain::GcMarkPackV2 => {
                if super::model::GcMarkPackV2::decode(root, &bytes)?.cycle_id != progress.cycle_id {
                    return Err(corruption("GC mark root belongs to another cycle"));
                }
            }
            ObjectDomain::GcQueuePackV1 => {
                if super::model::GcQueuePackV1::decode(root, &bytes)?.cycle_id != progress.cycle_id
                {
                    return Err(corruption("GC queue root belongs to another cycle"));
                }
            }
            ObjectDomain::GcLiveBranchPackV1 => {
                if super::model::GcLiveBranchPackV1::decode(root, &bytes)?.cycle_id
                    != progress.cycle_id
                {
                    return Err(corruption("GC live-branch root belongs to another cycle"));
                }
            }
            _ => unreachable!("domain checked above"),
        }
    }
    Ok(())
}

async fn start_cycle<S, R>(
    storage: &S,
    snapshot: GcSnapshot<R>,
) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
    R: StorageAdapterRead,
{
    if snapshot.raw_progress_selector.is_some() {
        return Err(corruption(
            "GC start observed an undecoded progress selector",
        ));
    }
    let next_generation = 1_u64;
    let cycle_id = derive_cycle_id(&snapshot.raw_global, next_generation);
    let progress = GcProgressV2 {
        cycle_id,
        phase: GcPhaseV2::RootSelectors,
        expected_global_digest: [1; 32],
        expected_global_epoch: snapshot.global.epoch,
        selector_resume_after: None,
        untracked_resume_after: None,
        object_resume_after: None,
        maintenance_resume_after: None,
        saw_global_selector: false,
        live_branch_index_root: None,
        mark_index_root: None,
        queue_index_root: None,
        queue_pop_sequence: 0,
        queue_push_sequence: 0,
        marked_count: 0,
        validated_count: 0,
        reclaimed_count: 0,
    };
    commit_progress(
        storage,
        snapshot,
        progress,
        MaintenanceEdit::default(),
        SweepBatch::default(),
        false,
    )
    .await?;
    Ok(GcStepStatus::Started)
}

async fn advance_selector_roots<S, R>(
    storage: &S,
    snapshot: GcSnapshot<R>,
    progress: &mut GcProgressV2,
) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
    R: StorageAdapterRead,
{
    let page = snapshot
        .read
        .scan(
            SELECTOR_SPACE,
            unbounded_range(),
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: SELECTOR_PAGE_ROWS,
                resume_after: progress
                    .selector_resume_after
                    .as_ref()
                    .map(|key| Key(Bytes::copy_from_slice(key))),
            },
        )
        .await?;
    let mut edit = MaintenanceEdit::default();
    let mut previous = progress.selector_resume_after.as_deref();
    for entry in &page.entries {
        if previous.is_some_and(|previous| previous >= entry.key.0.as_ref()) {
            return Err(corruption("GC selector page is not strictly ordered"));
        }
        previous = Some(entry.key.0.as_ref());
        let bytes = full_value(&entry.value, "GC selector scan returned key-only data")?;
        let key = entry.key.0.as_ref();
        if key == global_selector_key().as_ref() {
            let selector = GlobalSelectorV1::decode(bytes)?;
            if progress.saw_global_selector
                || bytes.as_ref() != snapshot.raw_global.as_ref()
                || selector != snapshot.global
            {
                return Err(corruption(
                    "GC global selector scan disagrees with its fence",
                ));
            }
            progress.saw_global_selector = true;
            enqueue_root(
                &snapshot.read,
                &mut edit,
                progress,
                selector.repository_root,
                ObjectDomain::RepositoryRoot,
            )
            .await?;
        } else if key.starts_with(b"branch/") {
            let selector = BranchSelectorV1::decode(bytes)?;
            if key != branch_selector_key(selector.branch_id).as_ref() {
                return Err(corruption("GC branch selector key/identity mismatch"));
            }
            let digest = live_branch_digest(selector.branch_id);
            let (root, _) = live_insert(
                &snapshot.read,
                &mut edit,
                progress.live_branch_index_root,
                progress.cycle_id,
                GcLiveBranchEntryV1 {
                    key_digest: digest,
                    branch_id: selector.branch_id,
                },
            )
            .await?;
            progress.live_branch_index_root = root;
            enqueue_root(
                &snapshot.read,
                &mut edit,
                progress,
                selector.branch_snapshot_object_id,
                ObjectDomain::BranchSnapshot,
            )
            .await?;
        } else if key.starts_with(b"upload/") {
            let selector = UploadSelectorV1::decode(bytes)?;
            if key != upload_selector_key(&selector.upload_id)?.as_ref() {
                return Err(corruption("GC upload selector key/identity mismatch"));
            }
            let progress_bytes =
                load_object_bytes(&snapshot.read, selector.progress_object_id).await?;
            let upload = UploadProgressV1::decode(selector.progress_object_id, &progress_bytes)?;
            if upload.upload_id != selector.upload_id
                || upload.binding_digest != selector.binding_digest
            {
                return Err(corruption("GC upload selector/progress binding mismatch"));
            }
            enqueue_root(
                &snapshot.read,
                &mut edit,
                progress,
                selector.progress_object_id,
                ObjectDomain::UploadProgress,
            )
            .await?;
        } else if key == gc_progress_selector_key().as_ref() {
            let selector = GcProgressSelectorV2::decode(bytes)?;
            if Some(selector) != snapshot.progress_selector
                || bytes.as_ref()
                    != snapshot
                        .raw_progress_selector
                        .as_ref()
                        .expect("active progress has raw selector")
                        .as_ref()
            {
                return Err(corruption(
                    "GC progress selector scan disagrees with its fence",
                ));
            }
        } else {
            let selector = SnapshotSelectorV1::decode(bytes)?;
            if key != snapshot_selector_key(selector.role, selector.selector_id).as_ref() {
                return Err(corruption("GC retained selector key/identity mismatch"));
            }
            enqueue_root(
                &snapshot.read,
                &mut edit,
                progress,
                selector.target_object_id,
                ObjectDomain::SnapshotTarget,
            )
            .await?;
        }
    }
    progress.selector_resume_after = page.entries.last().map(|entry| entry.key.0.to_vec());
    if !page.has_more {
        if !progress.saw_global_selector {
            return Err(corruption(
                "GC selector scan did not observe the global selector",
            ));
        }
        progress.phase = GcPhaseV2::RootUntracked;
        progress.selector_resume_after = None;
    } else if page.entries.is_empty() {
        return Err(corruption(
            "GC selector scan claims more after an empty page",
        ));
    }
    commit_progress(
        storage,
        snapshot,
        progress.clone(),
        edit,
        SweepBatch::default(),
        false,
    )
    .await?;
    Ok(status(progress))
}

async fn advance_untracked_roots<S, R>(
    storage: &S,
    snapshot: GcSnapshot<R>,
    progress: &mut GcProgressV2,
) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
    R: StorageAdapterRead,
{
    let page = snapshot
        .read
        .scan(
            UNTRACKED_ROW_SPACE,
            unbounded_range(),
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: UNTRACKED_PAGE_ROWS,
                resume_after: progress
                    .untracked_resume_after
                    .as_ref()
                    .map(|key| Key(Bytes::copy_from_slice(key))),
            },
        )
        .await?;
    let mut edit = MaintenanceEdit::default();
    for entry in &page.entries {
        let (branch_id, _) =
            decode_untracked_key(&entry.key.0).map_err(|error| corruption(error.to_string()))?;
        let value = decode_untracked_value(full_value(
            &entry.value,
            "GC untracked scan returned key-only data",
        )?)
        .map_err(|error| corruption(error.to_string()))?;
        if live_contains(
            &snapshot.read,
            &edit,
            progress.live_branch_index_root,
            progress.cycle_id,
            &live_branch_digest(branch_id),
            branch_id,
        )
        .await?
        {
            for root in value.blob_manifest_object_ids {
                enqueue_root(
                    &snapshot.read,
                    &mut edit,
                    progress,
                    root,
                    ObjectDomain::BlobManifest,
                )
                .await?;
            }
        }
    }
    progress.untracked_resume_after = page.entries.last().map(|entry| entry.key.0.to_vec());
    if !page.has_more {
        progress.phase = GcPhaseV2::Traverse;
        progress.untracked_resume_after = None;
    } else if page.entries.is_empty() {
        return Err(corruption(
            "GC untracked scan claims more after an empty page",
        ));
    }
    commit_progress(
        storage,
        snapshot,
        progress.clone(),
        edit,
        SweepBatch::default(),
        false,
    )
    .await?;
    Ok(status(progress))
}

async fn advance_traversal<S, R>(
    storage: &S,
    snapshot: GcSnapshot<R>,
    progress: &mut GcProgressV2,
    budget: GcBudget,
) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
    R: StorageAdapterRead,
{
    let mut edit = MaintenanceEdit::default();
    let work = budget.max_claims.min(TRAVERSAL_BATCH_CLAIMS).max(1);
    let mut paged_edges = 0_usize;
    for _ in 0..work {
        if progress.queue_pop_sequence == progress.queue_push_sequence {
            break;
        }
        let sequence = progress.queue_pop_sequence;
        let (queue_root, claim) = queue_pop(
            &snapshot.read,
            &mut edit,
            progress.queue_index_root,
            progress.cycle_id,
            sequence,
        )
        .await?;
        progress.queue_index_root = queue_root;
        progress.queue_pop_sequence = progress
            .queue_pop_sequence
            .checked_add(1)
            .ok_or_else(|| corruption("GC queue pop sequence overflowed"))?;
        let page = edge_page(&snapshot.read, snapshot.global.repository_root, &claim).await?;
        paged_edges = paged_edges.saturating_add(page.edges.len());
        for edge in page.edges {
            enqueue_root(&snapshot.read, &mut edit, progress, edge.id, edge.domain).await?;
        }
        if let Some(next_ordinal) = page.next_ordinal {
            push_queue_claim(
                &snapshot.read,
                &mut edit,
                progress,
                claim.object_id,
                claim.expected_domain,
                Some(GcEdgeCursorV1 {
                    source_object_id: claim.object_id,
                    source_domain: claim.expected_domain,
                    next_edge_ordinal: next_ordinal,
                    owner_cursor: Vec::new(),
                }),
            )
            .await?;
        } else {
            progress.validated_count = progress
                .validated_count
                .checked_add(1)
                .ok_or_else(|| corruption("GC validated count overflowed"))?;
        }
        // Keep one checkpoint's changed mark/queue paths bounded by roughly
        // one authenticated edge page. Claims with no edges may still use the
        // 128-claim work budget; a high-fanout claim checkpoints before the
        // next page can accumulate another page of immutable index rewrites.
        if paged_edges >= EDGE_PAGE_ENTRIES {
            break;
        }
    }
    if progress.queue_pop_sequence == progress.queue_push_sequence {
        if progress.queue_index_root.is_some() || progress.validated_count != progress.marked_count
        {
            return Err(corruption(
                "GC traversal terminated with inconsistent queue/mark counts",
            ));
        }
        progress.phase = GcPhaseV2::Sweep;
    }
    commit_progress(
        storage,
        snapshot,
        progress.clone(),
        edit,
        SweepBatch::default(),
        false,
    )
    .await?;
    Ok(status(progress))
}

async fn advance_sweep<S, R>(
    storage: &S,
    snapshot: GcSnapshot<R>,
    progress: &mut GcProgressV2,
    budget: GcBudget,
) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
    R: StorageAdapterRead,
{
    let page = snapshot
        .read
        .scan(
            OBJECT_SPACE,
            unbounded_range(),
            ScanOptions {
                projection: CoreProjection::KeyOnly,
                limit_rows: SWEEP_PAGE_ROWS,
                resume_after: progress.object_resume_after.map(object_key),
            },
        )
        .await?;
    let edit = MaintenanceEdit::default();
    let mut sweep = SweepBatch::default();
    let mut resume_after = progress.object_resume_after;
    let mut processed_all = true;
    if let (Some(first), Some(last)) = (page.entries.first(), page.entries.last()) {
        let lower = object_id_from_key(&first.key)?;
        let upper = object_id_from_key(&last.key)?;
        let marked = mark_range_iter(
            &snapshot.read,
            &edit,
            progress.mark_index_root,
            progress.cycle_id,
            lower,
            upper,
        )
        .await?;
        let delete_limit = budget.max_deletes.min(DELETE_BATCH_IDS).max(1);
        for entry in &page.entries {
            let id = object_id_from_key(&entry.key)?;
            if marked.contains_key(&id) {
                resume_after = Some(id);
                continue;
            }
            let bytes = load_object_bytes(&snapshot.read, id).await?;
            let domain = authenticate_object_domain(id, &bytes)?;
            if maintenance_cycle(domain, id, &bytes)? == Some(progress.cycle_id) {
                resume_after = Some(id);
                continue;
            }
            if sweep.len() >= delete_limit {
                processed_all = false;
                break;
            }
            sweep.push(id, delete_limit)?;
            resume_after = Some(id);
            progress.reclaimed_count = progress
                .reclaimed_count
                .checked_add(1)
                .ok_or_else(|| corruption("GC reclaimed count overflowed"))?;
        }
    }
    progress.object_resume_after = resume_after;
    if processed_all && !page.has_more {
        progress.phase = GcPhaseV2::Cleanup;
        progress.object_resume_after = None;
        progress.live_branch_index_root = None;
        progress.mark_index_root = None;
        progress.queue_index_root = None;
        progress.maintenance_resume_after = None;
    } else if page.entries.is_empty() {
        return Err(corruption("GC object scan claims more after an empty page"));
    }
    commit_progress(storage, snapshot, progress.clone(), edit, sweep, false).await?;
    Ok(status(progress))
}

async fn advance_cleanup<S, R>(
    storage: &S,
    snapshot: GcSnapshot<R>,
    progress: &mut GcProgressV2,
    budget: GcBudget,
) -> Result<GcStepStatus, StorageError>
where
    S: Storage,
    R: StorageAdapterRead,
{
    let page = snapshot
        .read
        .scan(
            OBJECT_SPACE,
            unbounded_range(),
            ScanOptions {
                projection: CoreProjection::KeyOnly,
                limit_rows: SWEEP_PAGE_ROWS,
                resume_after: progress.maintenance_resume_after.map(object_key),
            },
        )
        .await?;
    let current_progress_id = snapshot
        .progress_selector
        .expect("cleanup has a progress selector")
        .progress_object_id;
    let mut edit = MaintenanceEdit::default();
    let delete_limit = budget.max_deletes.min(DELETE_BATCH_IDS).max(1);
    let mut resume_after = progress.maintenance_resume_after;
    let mut processed_all = true;
    for entry in &page.entries {
        let id = object_id_from_key(&entry.key)?;
        if id == current_progress_id {
            resume_after = Some(id);
            continue;
        }
        let bytes = load_object_bytes(&snapshot.read, id).await?;
        let domain = authenticate_object_domain(id, &bytes)?;
        if maintenance_cycle(domain, id, &bytes)? == Some(progress.cycle_id) {
            if edit.deletes().count() >= delete_limit {
                processed_all = false;
                break;
            }
            edit.supersede(id, None);
        }
        resume_after = Some(id);
    }
    progress.maintenance_resume_after = resume_after;
    if processed_all && !page.has_more {
        let reclaimed = progress.reclaimed_count;
        commit_progress(
            storage,
            snapshot,
            progress.clone(),
            edit,
            SweepBatch::default(),
            true,
        )
        .await?;
        return Ok(GcStepStatus::Complete { reclaimed });
    }
    if page.entries.is_empty() {
        return Err(corruption(
            "GC cleanup scan claims more after an empty page",
        ));
    }
    commit_progress(
        storage,
        snapshot,
        progress.clone(),
        edit,
        SweepBatch::default(),
        false,
    )
    .await?;
    Ok(status(progress))
}

async fn enqueue_root<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    progress: &mut GcProgressV2,
    id: ObjectId,
    domain: ObjectDomain,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let (root, inserted) = mark_insert(
        read,
        edit,
        progress.mark_index_root,
        progress.cycle_id,
        GcMarkEntryV2 {
            object_id: id,
            expected_domain: domain.code(),
        },
    )
    .await?;
    progress.mark_index_root = root;
    if inserted {
        progress.marked_count = progress
            .marked_count
            .checked_add(1)
            .ok_or_else(|| corruption("GC marked count overflowed"))?;
        push_queue_claim(read, edit, progress, id, domain.code(), None).await?;
    }
    Ok(())
}

async fn push_queue_claim<R>(
    read: &R,
    edit: &mut MaintenanceEdit,
    progress: &mut GcProgressV2,
    object_id: ObjectId,
    expected_domain: u16,
    edge_cursor: Option<GcEdgeCursorV1>,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let sequence = progress.queue_push_sequence;
    progress.queue_index_root = queue_push(
        read,
        edit,
        progress.queue_index_root,
        progress.cycle_id,
        GcQueueEntryV1 {
            sequence,
            object_id,
            expected_domain,
            edge_cursor,
        },
    )
    .await?;
    progress.queue_push_sequence = progress
        .queue_push_sequence
        .checked_add(1)
        .ok_or_else(|| corruption("GC queue push sequence overflowed"))?;
    Ok(())
}

#[derive(Clone, Copy)]
struct TypedEdge {
    id: ObjectId,
    domain: ObjectDomain,
}

struct EdgePage {
    edges: Vec<TypedEdge>,
    next_ordinal: Option<u64>,
}

async fn edge_page<R>(
    read: &R,
    repository_root_id: ObjectId,
    claim: &GcQueueEntryV1,
) -> Result<EdgePage, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let expected = ObjectDomain::decode(claim.expected_domain)?;
    let start = match &claim.edge_cursor {
        None => 0,
        Some(cursor)
            if cursor.source_object_id == claim.object_id
                && cursor.source_domain == claim.expected_domain
                && cursor.owner_cursor.is_empty() =>
        {
            usize::try_from(cursor.next_edge_ordinal)
                .map_err(|_| corruption("GC edge ordinal exceeds usize"))?
        }
        Some(_) => return Err(corruption("GC edge continuation does not match its claim")),
    };
    let bytes = load_object_bytes(read, claim.object_id).await?;
    let actual = authenticate_object_domain(claim.object_id, &bytes)?;
    if actual != expected {
        return Err(corruption(
            "GC claim expected another authenticated object domain",
        ));
    }
    let edges = typed_edges(read, repository_root_id, claim.object_id, &bytes, actual).await?;
    if start > edges.len() {
        return Err(corruption(
            "GC edge continuation is past the object edge set",
        ));
    }
    let end = start.saturating_add(EDGE_PAGE_ENTRIES).min(edges.len());
    Ok(EdgePage {
        edges: edges[start..end].to_vec(),
        next_ordinal: (end < edges.len()).then_some(end as u64),
    })
}

async fn typed_edges<R>(
    read: &R,
    repository_root_id: ObjectId,
    id: ObjectId,
    bytes: &[u8],
    domain: ObjectDomain,
) -> Result<Vec<TypedEdge>, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut edges = Vec::new();
    match domain {
        ObjectDomain::RepositoryRoot => {
            let value = RepositoryRootV1::decode(id, bytes)?;
            edges.extend([
                typed(value.global_state_root, ObjectDomain::OrderedTreeNode),
                typed(value.commit_catalog_root, ObjectDomain::OrderedTreeNode),
                typed(value.change_catalog_root, ObjectDomain::OrderedTreeNode),
                typed(value.retention_policy_root, ObjectDomain::OrderedTreeNode),
            ]);
        }
        ObjectDomain::BranchSnapshot => {
            let value = BranchSnapshotV1::decode(id, bytes)?;
            validate_selected_ref_change(read, repository_root_id, &value).await?;
            edges.extend([
                typed(value.local_state_root, ObjectDomain::OrderedTreeNode),
                typed(value.semantic_head_commit_object_id, ObjectDomain::Commit),
                typed(
                    value.historical_global_state_root,
                    ObjectDomain::OrderedTreeNode,
                ),
            ]);
            if let Some(id) = value.latest_ref_change_object_id {
                edges.push(typed(id, ObjectDomain::BranchRefChange));
            }
        }
        ObjectDomain::Commit => {
            let value = CommitObjectV1::decode(id, bytes)?;
            let change_catalog_root = repository_change_catalog(read, repository_root_id).await?;
            validate_retained_commit(read, change_catalog_root, id, &value).await?;
            edges.extend(
                value
                    .parent_commit_object_ids
                    .into_iter()
                    .map(|id| typed(id, ObjectDomain::Commit)),
            );
            edges.extend(
                value
                    .member_change_object_ids
                    .into_iter()
                    .map(|id| typed(id, ObjectDomain::SemanticChange)),
            );
            edges.extend([
                typed(value.global_state_root, ObjectDomain::OrderedTreeNode),
                typed(value.local_state_root, ObjectDomain::OrderedTreeNode),
            ]);
        }
        ObjectDomain::SemanticChange => {
            if !matches!(
                ChangeObjectV1::decode(id, bytes)?,
                ChangeObjectV1::Semantic { .. }
            ) {
                return Err(corruption("semantic Change decoded as another domain"));
            }
        }
        ObjectDomain::BranchRefChange => {
            let change = ChangeObjectV1::decode(id, bytes)?;
            let change_catalog_root = repository_change_catalog(read, repository_root_id).await?;
            validate_retained_ref_change(read, change_catalog_root, id, &change).await?;
            let ChangeObjectV1::BranchRef {
                before_semantic_head_commit_object_id,
                after_semantic_head_commit_object_id,
                previous_ref_change_object_id,
                ..
            } = change
            else {
                return Err(corruption("RefChange decoded as a semantic Change"));
            };
            edges.extend(
                before_semantic_head_commit_object_id
                    .into_iter()
                    .chain(after_semantic_head_commit_object_id)
                    .map(|id| typed(id, ObjectDomain::Commit)),
            );
            if let Some(id) = previous_ref_change_object_id {
                edges.push(typed(id, ObjectDomain::BranchRefChange));
            }
        }
        ObjectDomain::OrderedTreeNode => {
            let tree = ordered_tree_edges(id, bytes)?;
            validate_catalog_claims(read, &tree.commit_entries, &tree.change_entries).await?;
            edges.extend(
                tree.object_ids
                    .into_iter()
                    .map(|(id, domain)| typed(id, domain)),
            );
        }
        ObjectDomain::UploadPart => {
            let value = UploadPartV1::decode(id, bytes)?;
            validate_chunk_sequence(
                read,
                &value.ordered_chunks,
                value.part_digest,
                "upload part",
            )
            .await?;
            edges.extend(
                value
                    .ordered_chunks
                    .into_iter()
                    .map(|chunk| typed(chunk.chunk_object_id, ObjectDomain::BlobChunk)),
            );
        }
        ObjectDomain::UploadProgress => {
            let value = UploadProgressV1::decode(id, bytes)?;
            authenticate_open_upload_progress(read, &value).await?;
            edges.push(typed(
                value.receipt_tree_root,
                ObjectDomain::OrderedTreeNode,
            ));
        }
        ObjectDomain::BlobChunk => {
            let _ = BlobChunkV1::decode(id, bytes)?;
        }
        ObjectDomain::BlobManifest => {
            let value = BlobManifestV1::decode(id, bytes)?;
            validate_chunk_sequence(
                read,
                &value.ordered_chunks,
                value.content_digest,
                "blob manifest",
            )
            .await?;
            edges.extend(
                value
                    .ordered_chunks
                    .into_iter()
                    .map(|chunk| typed(chunk.chunk_object_id, ObjectDomain::BlobChunk)),
            );
        }
        ObjectDomain::SnapshotTarget => {
            let value = SnapshotTargetV1::decode(id, bytes)?;
            edges.extend([
                typed(
                    value.branch_snapshot_object_id,
                    ObjectDomain::BranchSnapshot,
                ),
                typed(value.semantic_commit_object_id, ObjectDomain::Commit),
            ]);
        }
        ObjectDomain::GcMarkPackV2
        | ObjectDomain::GcProgressV2
        | ObjectDomain::GcRadixNodeV1
        | ObjectDomain::GcQueuePackV1
        | ObjectDomain::GcLiveBranchPackV1 => {
            return Err(corruption(
                "semantic reachability reaches GC maintenance state",
            ));
        }
    }
    if edges.iter().any(|edge| edge.id == ObjectId::ZERO) {
        return Err(corruption("authenticated object contains a zero edge"));
    }
    edges.sort_unstable_by_key(|edge| (edge.id, edge.domain.code()));
    if edges
        .windows(2)
        .any(|pair| pair[0].id == pair[1].id && pair[0].domain != pair[1].domain)
    {
        return Err(corruption(
            "object repeats one edge with conflicting domains",
        ));
    }
    edges.dedup_by_key(|edge| (edge.id, edge.domain.code()));
    Ok(edges)
}

async fn repository_change_catalog<R>(
    read: &R,
    repository_root_id: ObjectId,
) -> Result<ObjectId, StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let bytes = load_object_bytes(read, repository_root_id).await?;
    Ok(RepositoryRootV1::decode(repository_root_id, &bytes)?.change_catalog_root)
}

async fn validate_selected_ref_change<R>(
    read: &R,
    repository_root_id: ObjectId,
    snapshot: &BranchSnapshotV1,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let Some(ref_id) = snapshot.latest_ref_change_object_id else {
        return Ok(());
    };
    let bytes = load_object_bytes(read, ref_id).await?;
    let change = ChangeObjectV1::decode(ref_id, &bytes)?;
    let ChangeObjectV1::BranchRef {
        branch_id,
        after_semantic_head_commit_object_id,
        ..
    } = &change
    else {
        return Err(corruption(
            "selected RefChange edge names a semantic Change",
        ));
    };
    if *branch_id != snapshot.branch_id
        || *after_semantic_head_commit_object_id != Some(snapshot.semantic_head_commit_object_id)
    {
        return Err(corruption(
            "selected RefChange does not match its branch and semantic head",
        ));
    }
    let change_catalog_root = repository_change_catalog(read, repository_root_id).await?;
    validate_retained_ref_change(read, change_catalog_root, ref_id, &change).await
}

async fn validate_catalog_claims<R>(
    read: &R,
    commits: &[(CommitId, CommitCatalogEntry)],
    changes: &[(ChangeId, ChangeCatalogEntry)],
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    for (key, entry) in commits {
        let bytes = load_object_bytes(read, entry.commit_object_id).await?;
        let commit = CommitObjectV1::decode(entry.commit_object_id, &bytes)?;
        if commit.commit_id != *key {
            return Err(corruption("CommitCatalog key/object identity mismatch"));
        }
    }
    for (key, entry) in changes {
        let bytes = load_object_bytes(read, entry.change_object_id).await?;
        let change = ChangeObjectV1::decode(entry.change_object_id, &bytes)?;
        if change.change_id() != *key {
            return Err(corruption("ChangeCatalog key/object identity mismatch"));
        }
        match (entry.owner, &change) {
            (
                ChangeCatalogOwner::CommitMember {
                    commit_object_id,
                    ordinal,
                },
                ChangeObjectV1::Semantic { .. },
            ) => {
                let bytes = load_object_bytes(read, commit_object_id).await?;
                let commit = CommitObjectV1::decode(commit_object_id, &bytes)?;
                if commit.member_change_object_ids.get(ordinal as usize)
                    != Some(&entry.change_object_id)
                {
                    return Err(corruption(
                        "ChangeCatalog commit ordinal back-edge mismatch",
                    ));
                }
            }
            (
                ChangeCatalogOwner::BranchRef {
                    ref_change_object_id,
                    branch_id,
                },
                ChangeObjectV1::BranchRef {
                    branch_id: object_branch,
                    ..
                },
            ) if ref_change_object_id == entry.change_object_id && branch_id == *object_branch => {}
            _ => return Err(corruption("ChangeCatalog owner/back-edge mismatch")),
        }
    }
    Ok(())
}

async fn validate_chunk_sequence<R>(
    read: &R,
    chunks: &[super::model::BlobChunkRefV1],
    expected_digest: [u8; 32],
    claim: &str,
) -> Result<(), StorageError>
where
    R: StorageAdapterRead + ?Sized,
{
    let mut hasher = blake3::Hasher::new();
    let mut declarations = BTreeMap::new();
    for chunk in chunks {
        if declarations
            .insert(chunk.chunk_object_id, chunk.declared_len)
            .is_some_and(|existing| existing != chunk.declared_len)
        {
            return Err(corruption(format!(
                "{claim} repeats a chunk with conflicting declared lengths"
            )));
        }
        let bytes = load_object_bytes(read, chunk.chunk_object_id).await?;
        let value = BlobChunkV1::decode(chunk.chunk_object_id, &bytes)?;
        if value.bytes.len() as u64 != chunk.declared_len {
            return Err(corruption(format!(
                "{claim} chunk bytes do not match declared length"
            )));
        }
        hasher.update(&value.bytes);
    }
    if hasher.finalize().as_bytes() != &expected_digest {
        return Err(corruption(format!(
            "{claim} digest does not match authenticated chunk bytes"
        )));
    }
    Ok(())
}

async fn commit_progress<S, R>(
    storage: &S,
    snapshot: GcSnapshot<R>,
    mut progress: GcProgressV2,
    mut edit: MaintenanceEdit,
    sweep: SweepBatch,
    finish: bool,
) -> Result<(), StorageError>
where
    S: Storage,
    R: StorageAdapterRead,
{
    let next_global = snapshot.global.rotated()?;
    let raw_next_global = next_global.encode()?;
    progress.expected_global_digest = global_digest(&raw_next_global);
    progress.expected_global_epoch = next_global.epoch;
    let old_selector = snapshot.progress_selector;
    let old_progress_id = old_selector.map(|selector| selector.progress_object_id);
    let next_generation = old_selector.map_or(Ok(1), |selector| {
        selector
            .selector_generation
            .checked_add(1)
            .ok_or_else(|| corruption("GC progress selector generation overflowed"))
    })?;
    let (progress_id, raw_progress) = progress.encode()?;
    if !finish {
        edit.stage(progress_id, raw_progress)?;
    }
    if let Some(old) = old_progress_id {
        edit.supersede(old, (!finish).then_some(progress_id));
    }
    let mut preconditions = vec![Precondition::KeyValueEquals {
        space: SELECTOR_SPACE,
        key: Key(global_selector_key()),
        expected: snapshot.raw_global,
    }];
    preconditions.push(match snapshot.raw_progress_selector {
        Some(expected) => Precondition::KeyValueEquals {
            space: SELECTOR_SPACE,
            key: Key(gc_progress_selector_key()),
            expected,
        },
        None => Precondition::KeyAbsent {
            space: SELECTOR_SPACE,
            key: Key(gc_progress_selector_key()),
        },
    });
    let mut write = storage
        .begin_write(WriteOptions {
            preconditions,
            ..WriteOptions::default()
        })
        .await?;
    let object_entries = edit
        .puts()
        .map(|(id, bytes)| PutEntry {
            key: object_key(id),
            value: StoredValue {
                bytes: bytes.clone(),
            },
        })
        .collect::<Vec<_>>();
    if !object_entries.is_empty() {
        write
            .put_many(
                OBJECT_SPACE,
                PutBatch {
                    entries: object_entries,
                },
            )
            .await?;
    }
    let mut selector_entries = vec![PutEntry {
        key: Key(global_selector_key()),
        value: StoredValue {
            bytes: raw_next_global,
        },
    }];
    if !finish {
        selector_entries.push(PutEntry {
            key: Key(gc_progress_selector_key()),
            value: StoredValue {
                bytes: GcProgressSelectorV2 {
                    cycle_id: progress.cycle_id,
                    progress_object_id: progress_id,
                    selector_generation: next_generation,
                }
                .encode()?,
            },
        });
    }
    write
        .put_many(
            SELECTOR_SPACE,
            PutBatch {
                entries: selector_entries,
            },
        )
        .await?;
    if finish {
        write
            .delete_many(SELECTOR_SPACE, &[Key(gc_progress_selector_key())])
            .await?;
    }
    let object_deletes = edit
        .deletes()
        .map(object_key)
        .chain(sweep.keys())
        .collect::<Vec<_>>();
    if !object_deletes.is_empty() {
        write.delete_many(OBJECT_SPACE, &object_deletes).await?;
    }
    write.commit().await?;
    Ok(())
}

fn maintenance_cycle(
    domain: ObjectDomain,
    id: ObjectId,
    bytes: &[u8],
) -> Result<Option<[u8; 16]>, StorageError> {
    Ok(match domain {
        ObjectDomain::GcMarkPackV2 => Some(super::model::GcMarkPackV2::decode(id, bytes)?.cycle_id),
        ObjectDomain::GcProgressV2 => Some(GcProgressV2::decode(id, bytes)?.cycle_id),
        ObjectDomain::GcRadixNodeV1 => {
            Some(super::model::GcRadixNodeV1::decode(id, bytes)?.cycle_id)
        }
        ObjectDomain::GcQueuePackV1 => {
            Some(super::model::GcQueuePackV1::decode(id, bytes)?.cycle_id)
        }
        ObjectDomain::GcLiveBranchPackV1 => {
            Some(super::model::GcLiveBranchPackV1::decode(id, bytes)?.cycle_id)
        }
        _ => None,
    })
}

fn status(progress: &GcProgressV2) -> GcStepStatus {
    GcStepStatus::Advanced {
        phase: progress.phase,
        marked: progress.marked_count,
        validated: progress.validated_count,
        reclaimed: progress.reclaimed_count,
    }
}

fn derive_cycle_id(raw_global: &[u8], next_generation: u64) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key(GC_CYCLE_DOMAIN);
    hasher.update(raw_global);
    hasher.update(&next_generation.to_be_bytes());
    hasher.finalize().as_bytes()[..16]
        .try_into()
        .expect("BLAKE3 output has sixteen prefix bytes")
}

fn global_digest(raw: &[u8]) -> [u8; 32] {
    *blake3::hash(raw).as_bytes()
}

fn live_branch_digest(branch_id: super::model::CanonicalBranchId) -> [u8; 32] {
    keyed_hash(LIVE_BRANCH_DIGEST_DOMAIN, branch_id.as_bytes())
}

fn typed(id: ObjectId, domain: ObjectDomain) -> TypedEdge {
    TypedEdge { id, domain }
}

fn object_key(id: ObjectId) -> Key {
    Key(Bytes::copy_from_slice(id.as_bytes()))
}

fn object_id_from_key(key: &Key) -> Result<ObjectId, StorageError> {
    Ok(ObjectId::from_bytes(key.0.as_ref().try_into().map_err(
        |_| corruption("object-space key is not a 32-byte ObjectId"),
    )?))
}

fn required_full(value: Option<ProjectedValue>, missing: &str) -> Result<Bytes, StorageError> {
    match value {
        Some(ProjectedValue::FullValue(bytes)) => Ok(bytes),
        Some(ProjectedValue::KeyOnly) => Err(corruption("full projection returned key-only data")),
        None => Err(corruption(missing)),
    }
}

fn full_value<'a>(value: &'a ProjectedValue, message: &str) -> Result<&'a Bytes, StorageError> {
    match value {
        ProjectedValue::FullValue(bytes) => Ok(bytes),
        ProjectedValue::KeyOnly => Err(corruption(message)),
    }
}

fn unbounded_range() -> KeyRange {
    KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    }
}
