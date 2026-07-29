use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    ScanPlan, StorageAdapter, StorageAdapterRead, StorageCoreProjection, StoragePrefix,
    StorageProjectedValue, StorageScanOptions, StorageWriteOptions, StorageWriteSet,
    StorageWriteSetError,
};
use crate::{ReadOptions, WriteOptions};

static TRANSACTION_ROWS_STAGED: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_UNTRACKED_ROWS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_VALIDATION_BRANCHS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_LOADS: AtomicU64 = AtomicU64::new(0);
static TRANSACTION_SCHEMA_CATALOG_COMPILES: AtomicU64 = AtomicU64::new(0);
static JSON_STORE_STAGE_BYTES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BinaryCasWriteAccounting {
    pub chunk_lookup_count: u64,
    pub chunk_lookup_batch_count: u64,
    pub chunk_lookup_hit_count: u64,
    pub chunk_lookup_miss_count: u64,
    pub chunk_lookup_elapsed_ns: u64,
    pub transaction_duplicate_chunk_count: u64,
}

/// Result of one benchmark-only historical tracked-state diff.
///
/// The durable-root flags prove which physical diff path the benchmark used:
/// the intended populated case is a checkpoint on the left and a rootless
/// ordinary commit on the right.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrackedHistoricalDiffBenchResult {
    pub entries: usize,
    pub left_has_durable_root: bool,
    pub right_has_durable_root: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointCommitScanBenchMode {
    Materialize,
    Stream,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CheckpointCommitScanBenchResult {
    pub commits: usize,
    pub pages: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepositoryGcBenchResult {
    pub live_commits: usize,
    pub swept_commits: usize,
    pub swept_standalone_changes: usize,
    pub swept_payloads: usize,
    pub staged_puts: u64,
    pub staged_deletes: u64,
    pub staged_written_bytes: u64,
    pub delete_descriptors: usize,
    pub delete_descriptor_capacity: usize,
    pub key_inline_bytes: usize,
    pub key_inline_capacity: usize,
    pub key_shared_buffers: usize,
    pub key_shared_bytes: usize,
    pub key_shared_capacity: usize,
    pub root_discovery_us: u64,
    pub changelog_us: u64,
    pub tracked_root_stage_us: u64,
    pub total_us: u64,
}

/// Plans production repository GC without committing its staged sweep.
///
/// The returned arena sizes expose the planner's retained mutation footprint;
/// dropping this function's local write set leaves the fixture unchanged for
/// repeatable measurements.
#[inline(never)]
pub async fn plan_repository_gc_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
) -> Result<RepositoryGcBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = crate::storage_adapter::SharedStorageAdapterRead::new(
        storage.begin_read(ReadOptions::default()).await?,
    );
    let mut writes = storage.new_write_set();
    let plan = crate::gc::stage_repository_gc(read, &mut writes).await?;
    let stats = writes.stats();
    let arena = writes.arena_stats();
    Ok(RepositoryGcBenchResult {
        live_commits: plan.changelog.live.commits.len(),
        swept_commits: plan.changelog.sweep.commits.len(),
        swept_standalone_changes: plan.changelog.sweep.changes.len(),
        swept_payloads: plan.changelog.sweep.json_payloads.len(),
        staged_puts: stats.staged_puts,
        staged_deletes: stats.staged_deletes,
        staged_written_bytes: stats.written_bytes,
        delete_descriptors: arena.delete_descriptors,
        delete_descriptor_capacity: arena.delete_descriptor_capacity,
        key_inline_bytes: arena.key_inline_bytes,
        key_inline_capacity: arena.key_inline_capacity,
        key_shared_buffers: arena.key_shared_buffers,
        key_shared_bytes: arena.key_shared_bytes,
        key_shared_capacity: arena.key_shared_capacity,
        root_discovery_us: plan.profile.root_discovery_us,
        changelog_us: plan.profile.changelog_us,
        tracked_root_stage_us: plan.profile.tracked_root_stage_us,
        total_us: plan.profile.total_us,
    })
}

/// Scans public commit facts through the two checkpoint-history strategies.
///
/// `Materialize` is the current production path for unbounded checkpoint
/// history. `Stream` is a bounded-memory baseline over the same storage rows,
/// page size, codec, and read snapshot.
#[inline(never)]
pub async fn scan_checkpoint_commits_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    mode: CheckpointCommitScanBenchMode,
) -> Result<CheckpointCommitScanBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    const PAGE_SIZE: usize = 1_024;

    let read = storage.begin_read(ReadOptions::default()).await?;
    match mode {
        CheckpointCommitScanBenchMode::Materialize => {
            let records = crate::checkpoint::scan_checkpoint_commit_records(read).await?;
            Ok(CheckpointCommitScanBenchResult {
                commits: records.len(),
                pages: records.len().div_ceil(PAGE_SIZE),
            })
        }
        CheckpointCommitScanBenchMode::Stream => {
            let mut reader = crate::changelog::ChangelogContext::new().reader(read);
            let mut commits = 0usize;
            let mut pages = 0usize;
            let mut start_after = None::<String>;
            loop {
                let batch = crate::changelog::ChangelogReader::scan_commits(
                    &mut reader,
                    crate::changelog::CommitScanRequest {
                        start_after: start_after.as_deref(),
                        limit: Some(PAGE_SIZE),
                    },
                )
                .await?;
                commits = commits.checked_add(batch.entries.len()).ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        "checkpoint benchmark commit count overflow",
                    )
                })?;
                pages = pages.checked_add(1).ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        "checkpoint benchmark page count overflow",
                    )
                })?;
                let Some(next) = batch.next_start_after else {
                    break;
                };
                start_after = Some(next.to_string());
            }
            Ok(CheckpointCommitScanBenchResult { commits, pages })
        }
    }
}

/// Diffs two tracked commits through the production historical reader.
///
/// This is compiled only with `storage-benches`; it intentionally provides a
/// narrow measurement bridge without expanding the normal engine API.
#[inline(never)]
pub async fn diff_tracked_commits_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    left_commit_id: &str,
    right_commit_id: &str,
) -> Result<TrackedHistoricalDiffBenchResult, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut reader = crate::tracked_state::TrackedStateContext::new().reader(read);
    let left_has_durable_root = reader.has_durable_commit_root(left_commit_id).await?;
    let right_has_durable_root = reader.has_durable_commit_root(right_commit_id).await?;
    let entries = reader
        .diff_commits(
            left_commit_id,
            right_commit_id,
            &crate::tracked_state::TrackedStateDiffRequest::default(),
        )
        .await?
        .entries
        .len();
    Ok(TrackedHistoricalDiffBenchResult {
        entries,
        left_has_durable_root,
        right_has_durable_root,
    })
}

pub fn reset_binary_cas_write_accounting() {
    crate::binary_cas::metrics::reset_binary_cas_write_metrics();
}

pub fn binary_cas_write_accounting() -> BinaryCasWriteAccounting {
    let metrics = crate::binary_cas::metrics::binary_cas_write_metrics_snapshot();
    BinaryCasWriteAccounting {
        chunk_lookup_count: metrics.chunk_lookup_count,
        chunk_lookup_batch_count: metrics.chunk_lookup_batch_count,
        chunk_lookup_hit_count: metrics.chunk_lookup_hit_count,
        chunk_lookup_miss_count: metrics.chunk_lookup_miss_count,
        chunk_lookup_elapsed_ns: metrics.chunk_lookup_elapsed_ns,
        transaction_duplicate_chunk_count: metrics.transaction_duplicate_chunk_count,
    }
}

/// Writes one payload through the production binary CAS and commits the
/// resulting canonical write set. This is intentionally available only to
/// storage benchmarks so they can isolate CAS layout costs from SQL planning,
/// validation, tracked state, and changelog work.
pub async fn write_binary_cas_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    bytes: &[u8],
) -> Result<String, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let mut writes = storage.new_write_set();
    let receipt = crate::binary_cas::BinaryCasContext::new()
        .writer_skipping_existing_chunks(&read, &mut writes)
        .stage_payload(&crate::binary_cas::BlobPayload::from_bytes(bytes.to_vec()))
        .await?;
    storage
        .commit_write_set(writes, WriteOptions::default())
        .await?;
    Ok(receipt.hash.to_hex())
}

/// Reads one payload through the production binary CAS. See
/// [`write_binary_cas_for_bench`] for why this feature-gated helper exists.
pub async fn read_binary_cas_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    hash_hex: &str,
) -> Result<Option<Vec<u8>>, crate::LixError>
where
    StorageImpl: Storage,
{
    let read = storage.begin_read(ReadOptions::default()).await?;
    let hash = crate::binary_cas::BlobHash::from_hex(hash_hex)?;
    let mut entries = crate::binary_cas::BinaryCasContext::new()
        .reader(read)
        .load_bytes_many(&[hash])
        .await?
        .into_vec();
    Ok(entries.pop().flatten())
}

pub(crate) fn record_transaction_rows_staged(count: usize) {
    TRANSACTION_ROWS_STAGED.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_untracked_rows(count: usize) {
    TRANSACTION_UNTRACKED_ROWS.fetch_add(count as u64, Ordering::Relaxed);
}

pub(crate) fn record_transaction_validation_branch() {
    TRANSACTION_VALIDATION_BRANCHS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_transaction_schema_catalog_load() {
    TRANSACTION_SCHEMA_CATALOG_LOADS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_transaction_schema_catalog_compile() {
    TRANSACTION_SCHEMA_CATALOG_COMPILES.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn record_json_store_stage_bytes(hash: [u8; 32]) {
    JSON_STORE_STAGE_BYTES.fetch_add(hash.len() as u64, Ordering::Relaxed);
}

#[derive(Clone, Copy, Debug)]
pub struct StorageLayoutAccounting {
    pub space_id: u32,
    pub space: &'static str,
    pub rows: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
}

pub(crate) async fn commit_write_set_for_bench<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    writes: StorageWriteSet,
) -> Result<crate::storage_adapter::StorageWriteSetStats, StorageWriteSetError>
where
    StorageImpl: Storage,
{
    let (_commit, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await?;
    Ok(stats)
}

pub async fn layout_accounting<R>(read: &R) -> Vec<StorageLayoutAccounting>
where
    R: StorageAdapterRead,
{
    let mut accounting = Vec::with_capacity(native_storage_spaces().len());
    for space in native_storage_spaces() {
        accounting.push(scan_layout_space(read, *space).await);
    }
    accounting
}

/// Per-row (key, value bytes) inventory of one space.
///
/// Equivalence tests compare these inventories byte-for-byte, so the scan
/// must be complete; the function asserts it observed every row.
pub async fn space_inventory<R>(read: &R, space_name: &str) -> Vec<(Vec<u8>, Vec<u8>)>
where
    R: StorageAdapterRead,
{
    let space = *native_storage_spaces()
        .iter()
        .find(|space| space.name == space_name)
        .expect("space name should exist");
    scan_layout_entries(read, space)
        .await
        .iter()
        .map(|entry| {
            (
                entry.key.0.to_vec(),
                match &entry.value {
                    StorageProjectedValue::KeyOnly => Vec::new(),
                    StorageProjectedValue::FullValue(value) => value.to_vec(),
                },
            )
        })
        .collect()
}

fn native_storage_spaces() -> &'static [crate::storage_adapter::StorageSpace] {
    &[
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        crate::branch::BRANCH_HEAD_CONTROL_SPACE,
        crate::live_state::HOT_ROW_SPACE,
        crate::live_state::HOT_FILE_SPACE,
        crate::live_state::HOT_DIFF_SPACE,
        crate::live_state::TRACKED_WORKING_DIFF_MARKER_SPACE,
        crate::json_store::store::JSON_SPACE,
        crate::json_store::UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE,
        crate::tracked_state::TRACKED_STATE_TREE_CHUNK_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_ROOT_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_MANIFEST_SPACE,
        crate::tracked_state::TRACKED_STATE_COMMIT_DELTA_SEGMENT_SPACE,
        crate::tracked_state::TRACKED_STATE_CHANGE_LOCATOR_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_CHUNK_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_PRESENCE_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE,
        crate::changelog::COMMIT_SPACE,
        crate::changelog::CHANGE_SPACE,
        crate::changelog::COMMIT_CHANGE_ID_SPACE,
    ]
}

async fn scan_layout_space<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> StorageLayoutAccounting
where
    R: StorageAdapterRead,
{
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut accounting = StorageLayoutAccounting {
        space_id: space.id.0,
        space: space.name,
        rows: 0,
        key_bytes: 0,
        value_bytes: 0,
    };
    let mut resume_after = None;
    loop {
        let result = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after,
                    ..StorageScanOptions::default()
                },
            )
            .await
            .expect("scan complete storage bench layout space");
        let has_more = result.value.has_more;
        resume_after = result.value.entries.last().map(|entry| entry.key.clone());
        for entry in result.value.entries {
            accounting.rows = accounting
                .rows
                .checked_add(1)
                .expect("storage layout row count should not overflow");
            accounting.key_bytes = accounting
                .key_bytes
                .checked_add(entry.key.0.len() as u64 + 4)
                .expect("storage layout key bytes should not overflow");
            accounting.value_bytes = accounting
                .value_bytes
                .checked_add(match entry.value {
                    StorageProjectedValue::KeyOnly => 0,
                    StorageProjectedValue::FullValue(value) => value.len() as u64,
                })
                .expect("storage layout value bytes should not overflow");
        }
        if !has_more {
            return accounting;
        }
        assert!(
            resume_after.is_some(),
            "storage scan reported more rows without a resume key"
        );
    }
}

async fn scan_layout_entries<R>(
    read: &R,
    space: crate::storage_adapter::StorageSpace,
) -> Vec<crate::storage_adapter::StorageReadEntry>
where
    R: StorageAdapterRead,
{
    let plan = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    );
    let mut entries = Vec::new();
    let mut resume_after = None;
    loop {
        let result = plan
            .collect(
                read,
                StorageScanOptions {
                    projection: StorageCoreProjection::FullValue,
                    resume_after,
                    ..StorageScanOptions::default()
                },
            )
            .await
            .expect("scan complete storage bench layout space");
        let has_more = result.value.has_more;
        resume_after = result.value.entries.last().map(|entry| entry.key.clone());
        entries.extend(result.value.entries);
        if !has_more {
            return entries;
        }
        assert!(
            resume_after.is_some(),
            "storage scan reported more rows without a resume key"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CheckpointCommitScanBenchMode, plan_repository_gc_for_bench,
        scan_checkpoint_commits_for_bench,
    };
    use crate::Engine;
    use crate::changelog::bench::{append_ordered_commits, stage_append_once};
    use crate::storage_adapter::{Memory, StorageAdapter};

    #[tokio::test]
    async fn checkpoint_commit_scan_baseline_matches_materialized_records_across_pages() {
        let storage = Memory::new();
        let append = append_ordered_commits(0, 1_025).expect("build commit fixture");
        stage_append_once(storage.clone(), &append)
            .await
            .expect("stage commit fixture");
        let adapter = StorageAdapter::new(storage);

        let materialized =
            scan_checkpoint_commits_for_bench(&adapter, CheckpointCommitScanBenchMode::Materialize)
                .await
                .expect("materialize checkpoint commit records");
        let streamed =
            scan_checkpoint_commits_for_bench(&adapter, CheckpointCommitScanBenchMode::Stream)
                .await
                .expect("stream checkpoint commit records");

        assert_eq!(materialized.commits, 1_025);
        assert_eq!(materialized.pages, 2);
        assert_eq!(streamed, materialized);
    }

    #[tokio::test]
    async fn streamed_layout_accounting_matches_full_space_inventory() {
        let storage = Memory::new();
        let append = append_ordered_commits(0, 1_025).expect("build commit fixture");
        stage_append_once(storage.clone(), &append)
            .await
            .expect("stage commit fixture");
        let adapter = StorageAdapter::new(storage);
        let read = adapter
            .begin_read(crate::ReadOptions::default())
            .await
            .expect("begin layout accounting read");

        let inventory = super::space_inventory(&read, crate::changelog::COMMIT_SPACE.name).await;
        let accounting = super::layout_accounting(&read)
            .await
            .into_iter()
            .find(|space| space.space == crate::changelog::COMMIT_SPACE.name)
            .expect("commit space is accounted");

        assert_eq!(accounting.rows, inventory.len() as u64);
        assert_eq!(
            accounting.key_bytes,
            inventory
                .iter()
                .map(|(key, _)| key.len() as u64 + 4)
                .sum::<u64>()
        );
        assert_eq!(
            accounting.value_bytes,
            inventory
                .iter()
                .map(|(_, value)| value.len() as u64)
                .sum::<u64>()
        );
    }

    #[tokio::test]
    async fn repository_gc_benchmark_plans_unreachable_commits_without_mutating() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("initialize engine");
        let append = append_ordered_commits(100, 10).expect("build unreachable commit fixture");
        stage_append_once(storage.clone(), &append)
            .await
            .expect("stage unreachable commit fixture");
        let adapter = StorageAdapter::new(storage);

        let first = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("plan repository gc");
        let second = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("repeat repository gc plan");

        assert_eq!(first.swept_commits, 10);
        // Each unreachable commit removes its changelog record, exact-ID
        // locator, and derived tracked-root record.
        assert_eq!(first.staged_deletes, 30);
        assert_eq!(first.key_shared_buffers, 3);
        assert_eq!(first.key_shared_bytes, 30 * 16);
        assert_eq!(second.swept_commits, first.swept_commits);
        assert_eq!(second.staged_deletes, first.staged_deletes);
    }
}
