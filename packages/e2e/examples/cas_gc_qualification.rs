use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use lix::Value;
use lix::storage::{
    BeginScanOptions, CoreProjection, KeyRange, MAX_SCAN_PAGE_ROWS, ReadOptions, Storage,
    StorageRead,
};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    CrudPhysicalWriteAccounting, RepositoryGcCommitBenchResult, collect_repository_gc_for_bench,
    read_binary_cas_for_bench, take_crud_physical_write_accounting,
};
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const PART_BYTES: usize = 16 * 1024 * 1024;
const MANIFEST_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0001);
const MANIFEST_CHUNK_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0002);
const PAYLOAD_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0003);
const PRESENCE_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0004);
const FILE_PATH: &str = "/qualification/blob.bin";
const RETAINED_BRANCH_ID: &str = "01990000-0000-7000-8000-00000000005a";

fn observe_unreclaimed_baseline() -> bool {
    std::env::var_os("LIX_CAS_GC_OBSERVE_UNRECLAIMED").is_some()
}

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AllocationSnapshot {
    alloc_calls: u64,
    alloc_bytes: u64,
    dealloc_calls: u64,
    dealloc_bytes: u64,
    realloc_calls: u64,
}

impl AllocationSnapshot {
    fn now() -> Self {
        Self {
            alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
            alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
            dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
            dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
            realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
        }
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            alloc_calls: self.alloc_calls.saturating_sub(earlier.alloc_calls),
            alloc_bytes: self.alloc_bytes.saturating_sub(earlier.alloc_bytes),
            dealloc_calls: self.dealloc_calls.saturating_sub(earlier.dealloc_calls),
            dealloc_bytes: self.dealloc_bytes.saturating_sub(earlier.dealloc_bytes),
            realloc_calls: self.realloc_calls.saturating_sub(earlier.realloc_calls),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SpaceStats {
    rows: u64,
    value_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct CpuSnapshot {
    user_us: u64,
    system_us: u64,
}

impl CpuSnapshot {
    #[cfg(unix)]
    fn now() -> Self {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        assert_eq!(status, 0, "getrusage should succeed");
        let usage = unsafe { usage.assume_init() };
        Self {
            user_us: timeval_micros(usage.ru_utime),
            system_us: timeval_micros(usage.ru_stime),
        }
    }

    #[cfg(not(unix))]
    fn now() -> Self {
        Self::default()
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            user_us: self.user_us.saturating_sub(earlier.user_us),
            system_us: self.system_us.saturating_sub(earlier.system_us),
        }
    }
}

#[cfg(unix)]
fn timeval_micros(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(value.tv_usec).unwrap_or(0))
}

struct PhaseMeasurement {
    wall_ms: u128,
    cpu: CpuSnapshot,
    allocations: AllocationSnapshot,
    physical_writes: CrudPhysicalWriteAccounting,
    slate_io: SlateDBIoSnapshot,
    peak_rss_bytes: u64,
    settled_rss_bytes: u64,
    gc: Option<RepositoryGcCommitBenchResult>,
}

impl std::fmt::Debug for PhaseMeasurement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PhaseMeasurement")
            .field("wall_ms", &self.wall_ms)
            .field("cpu", &self.cpu)
            .field("allocations", &self.allocations)
            .field("physical_writes", &self.physical_writes)
            .field("slate_io", &self.slate_io)
            .field("peak_rss_bytes", &self.peak_rss_bytes)
            .field("settled_rss_bytes", &self.settled_rss_bytes)
            .field("gc", &self.gc)
            .finish()
    }
}

struct PhaseStart {
    started: Instant,
    cpu: CpuSnapshot,
    allocations: AllocationSnapshot,
    slate_io: SlateDBIoSnapshot,
}

impl PhaseStart {
    fn begin(slate_io: Option<&SlateDBIoCounters>) -> Self {
        let _ = take_crud_physical_write_accounting();
        Self {
            started: Instant::now(),
            cpu: CpuSnapshot::now(),
            allocations: AllocationSnapshot::now(),
            slate_io: slate_io.map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot),
        }
    }

    fn finish(
        self,
        slate_io: Option<&SlateDBIoCounters>,
        gc: Option<RepositoryGcCommitBenchResult>,
    ) -> PhaseMeasurement {
        PhaseMeasurement {
            wall_ms: self.started.elapsed().as_millis(),
            cpu: CpuSnapshot::now().saturating_sub(self.cpu),
            allocations: AllocationSnapshot::now().saturating_sub(self.allocations),
            physical_writes: take_crud_physical_write_accounting(),
            slate_io: slate_io
                .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
                .saturating_sub(self.slate_io),
            peak_rss_bytes: peak_rss_bytes(),
            settled_rss_bytes: settled_rss_bytes(),
            gc,
        }
    }
}

struct PreparedFixture {
    branch_id: String,
    root_a: String,
    root_b: String,
    blob_hash: String,
    content_hash: String,
    expected_size: usize,
    upload: PhaseMeasurement,
    retained_gc: PhaseMeasurement,
    before_gc: [SpaceStats; 4],
    retained: [SpaceStats; 4],
    physical_bytes: u64,
}

struct ReleasedFixture {
    cold_reopen: PhaseMeasurement,
    final_release_gc: PhaseMeasurement,
    released: [SpaceStats; 4],
    physical_bytes: u64,
}

struct RunResult {
    size_mib: usize,
    retention_checkpoints: usize,
    root_a: String,
    root_b: String,
    blob_hash: String,
    content_hash: String,
    upload: PhaseMeasurement,
    retained_gc: PhaseMeasurement,
    cold_reopen: PhaseMeasurement,
    final_release_gc: PhaseMeasurement,
    before_gc: [SpaceStats; 4],
    retained: [SpaceStats; 4],
    released: [SpaceStats; 4],
    final_state: [SpaceStats; 4],
    physical_bytes_after_prepare: u64,
    physical_bytes_after_release: u64,
    physical_bytes_final: u64,
}

impl std::fmt::Debug for RunResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RunResult")
            .field("size_mib", &self.size_mib)
            .field("retention_checkpoints", &self.retention_checkpoints)
            .field("root_a", &self.root_a)
            .field("root_b", &self.root_b)
            .field("blob_hash", &self.blob_hash)
            .field("content_hash", &self.content_hash)
            .field("upload", &self.upload)
            .field("retained_gc", &self.retained_gc)
            .field("cold_reopen", &self.cold_reopen)
            .field("final_release_gc", &self.final_release_gc)
            .field("before_gc", &self.before_gc)
            .field("retained", &self.retained)
            .field("released", &self.released)
            .field("final_state", &self.final_state)
            .field(
                "physical_bytes_after_prepare",
                &self.physical_bytes_after_prepare,
            )
            .field(
                "physical_bytes_after_release",
                &self.physical_bytes_after_release,
            )
            .field("physical_bytes_final", &self.physical_bytes_final)
            .finish()
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let path = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("/tmp/lix-cas-gc-qualification");
    let size_mib = args
        .get(3)
        .map(|value| value.parse::<usize>().expect("size must be MiB"))
        .unwrap_or(64);
    let retention_checkpoints = args
        .get(4)
        .map(|value| value.parse::<usize>().expect("retention must be a count"))
        .unwrap_or(64);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create qualification runtime");
    runtime.block_on(async move {
        let result = match backend {
            "rocksdb" => {
                let storage = RocksDB::open(path).expect("open RocksDB preparation");
                let prepared = prepare_retained_fixture(
                    storage.clone(),
                    path,
                    size_mib,
                    retention_checkpoints,
                    None,
                )
                .await;
                storage.flush().expect("flush RocksDB preparation");
                drop(storage);

                let storage = RocksDB::open(path).expect("cold reopen RocksDB retained fixture");
                let released =
                    verify_retained_and_release(storage.clone(), path, &prepared, None).await;
                storage.flush().expect("flush RocksDB release");
                drop(storage);

                let storage = RocksDB::open(path).expect("final reopen RocksDB fixture");
                let final_state = verify_final_state(storage.clone(), &prepared).await;
                storage.flush().expect("flush final RocksDB fixture");
                drop(storage);
                finish_result(
                    path,
                    size_mib,
                    retention_checkpoints,
                    prepared,
                    released,
                    final_state,
                )
            }
            "slatedb" => {
                let counters = SlateDBIoCounters::default();
                let storage = SlateDB::open_with_io_counters(path, counters.clone())
                    .expect("open SlateDB preparation");
                let prepared = prepare_retained_fixture(
                    storage.clone(),
                    path,
                    size_mib,
                    retention_checkpoints,
                    Some(&counters),
                )
                .await;
                storage.flush().await.expect("flush SlateDB preparation");
                drop(storage);

                let storage = SlateDB::open_with_io_counters(path, counters.clone())
                    .expect("cold reopen SlateDB retained fixture");
                let released =
                    verify_retained_and_release(storage.clone(), path, &prepared, Some(&counters))
                        .await;
                storage.flush().await.expect("flush SlateDB release");
                drop(storage);

                let storage = SlateDB::open_with_io_counters(path, counters)
                    .expect("final reopen SlateDB fixture");
                let final_state = verify_final_state(storage.clone(), &prepared).await;
                storage.flush().await.expect("flush final SlateDB fixture");
                drop(storage);
                finish_result(
                    path,
                    size_mib,
                    retention_checkpoints,
                    prepared,
                    released,
                    final_state,
                )
            }
            other => panic!("backend must be rocksdb or slatedb, got {other}"),
        };
        println!("cas_gc_retained_release,backend={backend},path={path},result={result:?}");
    });
}

async fn prepare_retained_fixture<S>(
    storage: S,
    path: &str,
    size_mib: usize,
    retention_checkpoints: usize,
    slate_io: Option<&SlateDBIoCounters>,
) -> PreparedFixture
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let session = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open retained repository");
    let total_size = size_mib
        .checked_mul(1024 * 1024)
        .expect("qualification size should fit usize");
    let upload_started = PhaseStart::begin(slate_io);
    let mut expected_hasher = blake3::Hasher::new();
    let mut payload = Vec::with_capacity(total_size);
    let mut offset = 0_usize;
    let mut part = 0_u64;
    while offset < total_size {
        let part_len = (total_size - offset).min(PART_BYTES);
        let bytes = random_part(part, part_len);
        expected_hasher.update(&bytes);
        payload.extend_from_slice(&bytes);
        offset += part_len;
        part = part.saturating_add(1);
    }
    session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
             ON CONFLICT (path) DO UPDATE SET content = excluded.content",
            &[
                Value::Text(FILE_PATH.to_owned()),
                Value::Blob(payload.into()),
            ],
        )
        .await
        .expect("write retained qualification payload through SQL");
    let upload = upload_started.finish(slate_io, None);
    let expected_content_hash = expected_hasher.finalize().to_hex().to_string();
    let uploaded = session
        .execute(
            "SELECT content, content_identity FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        )
        .await
        .expect("read uploaded qualification payload")
        .rows()
        .first()
        .expect("uploaded qualification payload should exist")
        .clone();
    let uploaded_content = uploaded
        .get::<Vec<u8>>("content")
        .expect("uploaded content should decode");
    assert_eq!(uploaded_content.len(), total_size);
    assert_eq!(
        blake3::hash(&uploaded_content).to_hex().to_string(),
        expected_content_hash,
    );
    let blob_hash = uploaded
        .get::<String>("content_identity")
        .expect("uploaded content identity should decode");
    let root_a = branch_commit(&session).await;
    session
        .execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        )
        .await
        .expect("delete payload from advancing branch");
    let root_b = branch_commit(&session).await;
    let branch = session
        .create_branch(lix::CreateBranchOptions {
            id: Some(RETAINED_BRANCH_ID.to_owned()),
            name: "cas-gc-retained-owner".to_owned(),
            from_commit_id: Some(root_b.clone()),
        })
        .await
        .expect("create retained history owner branch");
    assert_eq!(branch.id, RETAINED_BRANCH_ID);
    for revision in 0..retention_checkpoints {
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                 ON CONFLICT (path) DO UPDATE SET content = excluded.content",
                &[
                    Value::Text("/qualification/retention.json".to_owned()),
                    Value::Blob(format!("{{\"revision\":{revision}}}").into_bytes().into()),
                ],
            )
            .await
            .expect("publish retention commit");
        session
            .create_checkpoint()
            .await
            .expect("publish retention checkpoint");
    }

    let before_gc = cas_stats(&storage).await;
    let adapter = StorageAdapter::new(storage.clone());
    let gc_started = PhaseStart::begin(slate_io);
    let sweep = collect_repository_gc_for_bench(&adapter)
        .await
        .expect("retained-owner GC should commit");
    let retained_gc = gc_started.finish(slate_io, Some(sweep));
    let retained = cas_stats(&storage).await;
    println!(
        "cas_gc_retained_observation,root_a={},root_b={},blob_hash={},before={:?},retained={:?},gc={:?}",
        root_a, root_b, blob_hash, before_gc, retained, retained_gc.gc,
    );
    let bytes = read_binary_cas_for_bench(&adapter, &blob_hash)
        .await
        .expect("retained payload CAS lookup should succeed")
        .expect("retained payload must survive GC");
    assert_eq!(bytes.len(), total_size);
    assert_eq!(
        blake3::hash(&bytes).to_hex().to_string(),
        expected_content_hash,
    );
    drop(bytes);
    drop(adapter);
    drop(session);
    drop(storage);
    PreparedFixture {
        branch_id: branch.id,
        root_a,
        root_b,
        blob_hash,
        content_hash: expected_content_hash,
        expected_size: total_size,
        upload,
        retained_gc,
        before_gc,
        retained,
        physical_bytes: directory_bytes(Path::new(path)),
    }
}

async fn verify_retained_and_release<S>(
    storage: S,
    path: &str,
    fixture: &PreparedFixture,
    slate_io: Option<&SlateDBIoCounters>,
) -> ReleasedFixture
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let cold_started = PhaseStart::begin(slate_io);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("cold reopen retained repository");
    let session = lix
        .open_another_session()
        .await
        .expect("cold reopen retained owner branch");
    session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: (fixture.branch_id.clone()).to_string(),
        })
        .await
        .expect("switch session branch");
    let diff = session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) \
             WHERE schema_key = 'lix_binary_blob_ref'",
            &[
                Value::Text(fixture.root_a.clone()),
                Value::Text(fixture.root_b.clone()),
            ],
        )
        .await
        .expect("cold retained diff should remain readable");
    assert_eq!(diff.rows()[0].get::<i64>("entries").unwrap(), 1);
    let absent = session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        )
        .await
        .expect("retained root B file absence should read");
    assert_eq!(absent.rows()[0].get::<i64>("entries").unwrap(), 0);
    session
        .undo()
        .await
        .expect("retained owner undo should succeed");
    let bytes = read_current_file(&session).await;
    assert_eq!(bytes.len(), fixture.expected_size);
    assert_eq!(
        blake3::hash(&bytes).to_hex().to_string(),
        fixture.content_hash,
    );
    drop(bytes);
    session
        .redo()
        .await
        .expect("retained owner redo should succeed");
    let absent = session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        )
        .await
        .expect("redone retained file absence should read");
    assert_eq!(absent.rows()[0].get::<i64>("entries").unwrap(), 0);
    let cold_reopen = cold_started.finish(slate_io, None);
    drop(session);

    let release_started = PhaseStart::begin(slate_io);
    let workspace = lix
        .open_another_session()
        .await
        .expect("open workspace for final owner release");
    workspace
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(fixture.branch_id.clone())],
        )
        .await
        .expect("delete final retained owner branch");
    drop(workspace);
    let adapter = StorageAdapter::new(storage.clone());
    let sweep = collect_repository_gc_for_bench(&adapter)
        .await
        .expect("final owner-release GC should commit");
    let payload_reclaimed = read_binary_cas_for_bench(&adapter, &fixture.blob_hash)
        .await
        .expect("released payload CAS lookup should succeed")
        .is_none();
    println!("cas_gc_reclamation_observation,phase=release,payload_reclaimed={payload_reclaimed}");
    assert!(
        payload_reclaimed || observe_unreclaimed_baseline(),
        "payload must reclaim after the final owner releases it",
    );
    let final_release_gc = release_started.finish(slate_io, Some(sweep));
    let released = cas_stats(&storage).await;
    drop(adapter);
    drop(lix);
    drop(storage);
    ReleasedFixture {
        cold_reopen,
        final_release_gc,
        released,
        physical_bytes: directory_bytes(Path::new(path)),
    }
}

async fn verify_final_state<S>(storage: S, fixture: &PreparedFixture) -> [SpaceStats; 4]
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("final repository should reopen");
    let workspace = lix
        .open_another_session()
        .await
        .expect("final session should open");
    let branches = workspace
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_branch WHERE id = $1",
            &[Value::Text(fixture.branch_id.clone())],
        )
        .await
        .expect("released branch absence should read");
    assert_eq!(branches.rows()[0].get::<i64>("entries").unwrap(), 0);
    let adapter = StorageAdapter::new(storage.clone());
    let payload_reclaimed = read_binary_cas_for_bench(&adapter, &fixture.blob_hash)
        .await
        .expect("final released payload lookup should succeed")
        .is_none();
    println!(
        "cas_gc_reclamation_observation,phase=final_reopen,payload_reclaimed={payload_reclaimed}"
    );
    assert!(payload_reclaimed || observe_unreclaimed_baseline());
    let stats = cas_stats(&storage).await;
    drop(adapter);
    drop(workspace);
    drop(lix);
    drop(storage);
    stats
}

fn finish_result(
    path: &str,
    size_mib: usize,
    retention_checkpoints: usize,
    prepared: PreparedFixture,
    released: ReleasedFixture,
    final_state: [SpaceStats; 4],
) -> RunResult {
    RunResult {
        size_mib,
        retention_checkpoints,
        root_a: prepared.root_a,
        root_b: prepared.root_b,
        blob_hash: prepared.blob_hash,
        content_hash: prepared.content_hash,
        upload: prepared.upload,
        retained_gc: prepared.retained_gc,
        cold_reopen: released.cold_reopen,
        final_release_gc: released.final_release_gc,
        before_gc: prepared.before_gc,
        retained: prepared.retained,
        released: released.released,
        final_state,
        physical_bytes_after_prepare: prepared.physical_bytes,
        physical_bytes_after_release: released.physical_bytes,
        physical_bytes_final: directory_bytes(Path::new(path)),
    }
}

async fn branch_commit<S>(session: &Lix<S>) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let branch_id = session
        .active_branch_id()
        .await
        .expect("active branch id should load");
    session
        .execute(
            "SELECT commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id)],
        )
        .await
        .expect("branch commit should load")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("branch commit should exist")
}

async fn read_current_file<S>(session: &Lix<S>) -> Vec<u8>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(FILE_PATH.to_owned())],
        )
        .await
        .expect("current retained file should read")
        .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("current retained content should exist")
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    u64::try_from(usage.ru_maxrss)
        .unwrap_or(0)
        .saturating_mul(1024)
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn settled_rss_bytes() -> u64 {
    let resident_pages = std::fs::read_to_string("/proc/self/statm")
        .ok()
        .and_then(|value| value.split_whitespace().nth(1)?.parse::<u64>().ok())
        .unwrap_or(0);
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    resident_pages.saturating_mul(u64::try_from(page_size).unwrap_or(0))
}

#[cfg(not(target_os = "linux"))]
fn settled_rss_bytes() -> u64 {
    0
}

fn random_part(part: u64, len: usize) -> Vec<u8> {
    let mut output = vec![0_u8; len];
    for (block, bytes) in output.chunks_mut(32).enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"lix cas gc qualification payload v1");
        hasher.update(&part.to_le_bytes());
        hasher.update(&(block as u64).to_le_bytes());
        let digest = hasher.finalize();
        let count = bytes.len();
        bytes.copy_from_slice(&digest.as_bytes()[..count]);
    }
    output
}

async fn cas_stats<S: Storage>(storage: &S) -> [SpaceStats; 4] {
    [
        space_stats(storage, MANIFEST_SPACE).await,
        space_stats(storage, MANIFEST_CHUNK_SPACE).await,
        space_stats(storage, PAYLOAD_SPACE).await,
        space_stats(storage, PRESENCE_SPACE).await,
    ]
}

async fn space_stats<S: Storage>(storage: &S, space_id: lix::storage::SpaceId) -> SpaceStats {
    // A space id has exactly one value semantics and Lix registry is
    // where it is declared; guessing it here scans a different physical
    // location than Lix wrote.
    let space = lix::storage_bench::storage_space_by_id(space_id.0);
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open stats read");
    let mut stats = SpaceStats {
        rows: 0,
        value_bytes: 0,
    };
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin CAS stats scan");
    loop {
        let (page, page_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan CAS stats")
            .into_parts();
        stats.rows += page.len() as u64;
        stats.value_bytes += page
            .iter()
            .map(|entry| match &entry.value {
                lix::storage::ProjectedValue::FullValue(bytes) => bytes.len() as u64,
                lix::storage::ProjectedValue::KeyOnly => 0,
            })
            .sum::<u64>();
        if !page_has_more {
            break;
        }
    }
    stats
}

fn directory_bytes(path: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let Ok(metadata) = std::fs::metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }
        std::fs::read_dir(path)
            .into_iter()
            .flat_map(|entries| entries.flatten())
            .map(|entry| visit(&entry.path()))
            .sum()
    }
    visit(path)
}
