use std::alloc::GlobalAlloc;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead,
    StorageScanSource, StorageWrite, WriteOptions,
};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    MergeBaseBenchFixture, MergeBaseBenchScenario, merge_base_for_bench, prepare_merge_for_bench,
    seed_merge_base_fixture_for_bench,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

struct CountingAllocator;

static PROFILE_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static PROFILE_ALLOCATION_ENABLED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed) {
            PROFILE_ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(
        &self,
        pointer: *mut u8,
        layout: std::alloc::Layout,
        new_size: usize,
    ) -> *mut u8 {
        let replacement = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !replacement.is_null()
            && new_size >= layout.size()
            && PROFILE_ALLOCATION_ENABLED.load(Ordering::Relaxed)
        {
            PROFILE_ALLOCATED_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
            PROFILE_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        replacement
    }
}

fn begin_allocation_profile() {
    PROFILE_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    PROFILE_ALLOCATION_ENABLED.store(true, Ordering::Relaxed);
}

fn end_allocation_profile() -> (u64, u64) {
    PROFILE_ALLOCATION_ENABLED.store(false, Ordering::Relaxed);
    (
        PROFILE_ALLOCATED_BYTES.load(Ordering::Relaxed),
        PROFILE_ALLOCATION_CALLS.load(Ordering::Relaxed),
    )
}

#[derive(Clone, Debug, Default)]
struct IoStats {
    get_calls: u64,
    get_keys: u64,
    get_values: u64,
    get_value_bytes: u64,
    scan_calls: u64,
    scan_entries: u64,
    scan_value_bytes: u64,
    write_batches: u64,
    write_puts: u64,
    write_deletes: u64,
    write_ranges: u64,
    write_bytes: u64,
}

#[derive(Clone)]
struct CountingStorage<S> {
    inner: S,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingRead<R> {
    inner: R,
    stats: Arc<Mutex<IoStats>>,
}

struct CountingWrite<W> {
    inner: W,
    stats: Arc<Mutex<IoStats>>,
}

impl<S> CountingStorage<S> {
    fn new(inner: S) -> (Self, Arc<Mutex<IoStats>>) {
        let stats = Arc::new(Mutex::new(IoStats::default()));
        (
            Self {
                inner,
                stats: Arc::clone(&stats),
            },
            stats,
        )
    }
}

impl<S> Storage for CountingStorage<S>
where
    S: Storage,
{
    type Read<'a>
        = CountingRead<S::Read<'a>>
    where
        Self: 'a;
    type Write<'a>
        = CountingWrite<S::Write<'a>>
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountingWrite {
            inner: self.inner.begin_write(options).await?,
            stats: Arc::clone(&self.stats),
        })
    }
}

impl<R> StorageRead for CountingRead<R>
where
    R: StorageRead,
{
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.get_calls += 1;
            stats.get_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        let result = self.inner.get_many(requests).await?;
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            for value in result.values.iter().flatten() {
                stats.get_values += 1;
                stats.get_value_bytes += projected_value_len(value) as u64;
            }
        }
        Ok(result)
    }

    async fn begin_scan(
        &self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
        options: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = options.order;
        self.stats.lock().expect("I/O stats mutex").scan_calls += 1;
        let inner = self.inner.begin_scan(space, range.clone(), options).await?;
        ScanCursor::from_source(
            range,
            order,
            CountingScanSource {
                inner,
                stats: Arc::clone(&self.stats),
            },
        )
    }
}

struct CountingScanSource<'a> {
    inner: ScanCursor<'a>,
    stats: Arc<Mutex<IoStats>>,
}

impl StorageScanSource for CountingScanSource<'_> {
    fn next_page(
        &mut self,
        limit_rows: usize,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<ScanChunk, StorageError>> + Send + '_>> {
        Box::pin(async move {
            let (chunk, chunk_has_more) = self.inner.next_page(limit_rows).await?.into_parts();
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.scan_entries += chunk.len() as u64;
            stats.scan_value_bytes += chunk
                .iter()
                .map(|entry| projected_value_len(&entry.value) as u64)
                .sum::<u64>();
            drop(stats);
            Ok(ScanChunk::new(chunk, chunk_has_more))
        })
    }
}

impl<W> StorageWrite for CountingWrite<W>
where
    W: StorageWrite,
{
    async fn put_many(
        &mut self,
        space: lix::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.write_batches += 1;
            stats.write_puts += entries.entries.len() as u64;
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(
        &mut self,
        space: lix::storage::StorageSpace,
        keys: &[Key],
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.write_batches += 1;
            stats.write_deletes += keys.len() as u64;
            stats.write_bytes += keys.iter().map(|key| key.0.len() as u64).sum::<u64>();
        }
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("I/O stats mutex");
            stats.write_batches += 1;
            stats.write_ranges += 1;
        }
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError>
    where
        Self: Sized,
    {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError>
    where
        Self: Sized,
    {
        self.inner.rollback().await
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Operation {
    MergeBase,
    Prepare,
}

impl Operation {
    fn parse(value: &str) -> Self {
        match value {
            "merge_base" => Self::MergeBase,
            "prepare" => Self::Prepare,
            other => panic!("unknown operation '{other}'"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::MergeBase => "merge_base",
            Self::Prepare => "prepare",
        }
    }
}

#[derive(Clone, Copy)]
struct Parameters {
    scenario: MergeBaseBenchScenario,
    ancestry: usize,
    operation: Operation,
    samples: usize,
    warmups: usize,
    iterations: usize,
}

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create merge-base benchmark runtime")
        .block_on(run());
}

async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let parameters = Parameters {
        scenario: parse_scenario(args.get(2).map(String::as_str).unwrap_or("ancestor")),
        ancestry: parse_positive(args.get(3), "ancestry", 1_000),
        operation: Operation::parse(args.get(4).map(String::as_str).unwrap_or("merge_base")),
        samples: parse_positive(args.get(5), "samples", 7),
        warmups: parse_nonnegative(args.get(6), "warmups", 2),
        iterations: parse_positive(args.get(7), "iterations", 1),
    };
    match backend {
        "rocksdb" => run_rocksdb(parameters).await,
        "slatedb" => run_slatedb(parameters).await,
        other => panic!("unknown backend '{other}'"),
    }
}

async fn run_rocksdb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create RocksDB benchmark directory");
    let database = RocksDB::open(directory.path()).expect("open RocksDB benchmark storage");
    let (counting, stats) = CountingStorage::new(database.clone());
    let storage = StorageAdapter::new(counting);
    let seed_started = Instant::now();
    let fixture =
        seed_merge_base_fixture_for_bench(&storage, parameters.ancestry, parameters.scenario)
            .await
            .expect("seed merge-base fixture");
    database.flush().expect("flush RocksDB benchmark storage");
    let seed = take_stats(&stats);
    print_seed(
        "rocksdb",
        parameters,
        &fixture,
        seed_started.elapsed().as_secs_f64() * 1_000.0,
        &seed,
        directory_bytes(directory.path()),
        SlateDBIoSnapshot::default(),
    );
    measure(
        "rocksdb",
        parameters,
        &storage,
        &fixture,
        &stats,
        directory.path(),
        None,
    )
    .await;
}

async fn run_slatedb(parameters: Parameters) {
    let directory = tempfile::tempdir().expect("create SlateDB benchmark directory");
    let counters = SlateDBIoCounters::default();
    let database = SlateDB::open_with_io_counters(directory.path(), counters.clone())
        .expect("open SlateDB benchmark storage");
    let (counting, stats) = CountingStorage::new(database.clone());
    let storage = StorageAdapter::new(counting);
    let physical_before = counters.snapshot();
    let seed_started = Instant::now();
    let fixture =
        seed_merge_base_fixture_for_bench(&storage, parameters.ancestry, parameters.scenario)
            .await
            .expect("seed merge-base fixture");
    database
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush SlateDB benchmark storage");
    let seed = take_stats(&stats);
    print_seed(
        "slatedb",
        parameters,
        &fixture,
        seed_started.elapsed().as_secs_f64() * 1_000.0,
        &seed,
        directory_bytes(directory.path()),
        counters.snapshot().saturating_sub(physical_before),
    );
    measure(
        "slatedb",
        parameters,
        &storage,
        &fixture,
        &stats,
        directory.path(),
        Some(&counters),
    )
    .await;
}

async fn measure<S>(
    backend: &str,
    parameters: Parameters,
    storage: &StorageAdapter<CountingStorage<S>>,
    fixture: &MergeBaseBenchFixture,
    stats: &Arc<Mutex<IoStats>>,
    database_path: &std::path::Path,
    physical_counters: Option<&SlateDBIoCounters>,
) where
    S: Storage,
{
    for _ in 0..parameters.warmups {
        run_operation(storage, fixture, parameters.operation).await;
    }
    let ticks_per_second = process_clock_ticks_per_second();
    for sample in 1..=parameters.samples {
        let _ = take_stats(stats);
        let disk_before = directory_bytes(database_path);
        let physical_before = physical_counters.map(SlateDBIoCounters::snapshot);
        let rss_before = process_resident_bytes();
        let cpu_before = process_cpu_ticks();
        begin_allocation_profile();
        let started = Instant::now();
        for _ in 0..parameters.iterations {
            run_operation(storage, fixture, parameters.operation).await;
        }
        let wall_us = started.elapsed().as_secs_f64() * 1_000_000.0;
        let (allocated_bytes, allocation_calls) = end_allocation_profile();
        let cpu_ticks = process_cpu_ticks().saturating_sub(cpu_before);
        let rss_after = process_resident_bytes();
        let disk_after = directory_bytes(database_path);
        let io = take_stats(stats);
        let physical = physical_before.map_or_else(SlateDBIoSnapshot::default, |before| {
            physical_counters
                .expect("physical counters exist")
                .snapshot()
                .saturating_sub(before)
        });
        let iterations = parameters.iterations as f64;
        let cpu_us = cpu_ticks as f64 * 1_000_000.0 / ticks_per_second as f64;
        println!(
            "merge_base_scale,sample={sample},backend={backend},scenario={},operation={},ancestry={},commits={},iterations={},wall_us_per_op={:.3},cpu_us_per_op={:.3},alloc_bytes_per_op={:.1},alloc_calls_per_op={:.1},rss_before_bytes={rss_before},rss_after_bytes={rss_after},get_calls_per_op={:.1},get_keys_per_op={:.1},get_values_per_op={:.1},get_value_bytes_per_op={:.1},scan_calls_per_op={:.1},scan_entries_per_op={:.1},write_batches_per_op={:.1},write_bytes_per_op={:.1},disk_before_bytes={disk_before},disk_after_bytes={disk_after},slate_read_objects_per_op={:.1},slate_read_bytes_per_op={:.1},slate_write_objects_per_op={:.1},slate_write_bytes_per_op={:.1}",
            scenario_name(parameters.scenario),
            parameters.operation.name(),
            parameters.ancestry,
            fixture.commits,
            parameters.iterations,
            wall_us / iterations,
            cpu_us / iterations,
            allocated_bytes as f64 / iterations,
            allocation_calls as f64 / iterations,
            io.get_calls as f64 / iterations,
            io.get_keys as f64 / iterations,
            io.get_values as f64 / iterations,
            io.get_value_bytes as f64 / iterations,
            io.scan_calls as f64 / iterations,
            io.scan_entries as f64 / iterations,
            io.write_batches as f64 / iterations,
            io.write_bytes as f64 / iterations,
            physical.read_objects as f64 / iterations,
            physical.read_bytes as f64 / iterations,
            physical.write_objects as f64 / iterations,
            physical.write_bytes as f64 / iterations,
        );
    }
}

async fn run_operation<S>(
    storage: &StorageAdapter<CountingStorage<S>>,
    fixture: &MergeBaseBenchFixture,
    operation: Operation,
) where
    S: Storage,
{
    match operation {
        Operation::MergeBase => match &fixture.expected_base {
            Some(expected) => {
                let base = merge_base_for_bench(storage, &fixture.left_head, &fixture.right_head)
                    .await
                    .expect("merge base should resolve");
                assert_eq!(&base, expected);
            }
            None => {
                let error = merge_base_for_bench(storage, &fixture.left_head, &fixture.right_head)
                    .await
                    .expect_err("criss-cross merge base should stay ambiguous");
                assert_eq!(error.code, lix::LixError::CODE_AMBIGUOUS_MERGE_BASE);
            }
        },
        Operation::Prepare => {
            let expected = fixture
                .expected_base
                .as_ref()
                .expect("ambiguous histories do not have merge preparation");
            let result = prepare_merge_for_bench(storage, &fixture.left_head, &fixture.right_head)
                .await
                .expect("merge preparation should succeed");
            assert_eq!(&result.base_commit_id, expected);
            assert_eq!(result.target_entries, 0);
            assert_eq!(result.source_entries, 0);
        }
    }
}

fn print_seed(
    backend: &str,
    parameters: Parameters,
    fixture: &MergeBaseBenchFixture,
    seed_wall_ms: f64,
    io: &IoStats,
    disk_bytes: u64,
    physical: SlateDBIoSnapshot,
) {
    println!(
        "merge_base_seed,backend={backend},scenario={},ancestry={},commits={},wall_ms={seed_wall_ms:.3},get_calls={},get_keys={},write_batches={},write_puts={},write_bytes={},disk_bytes={disk_bytes},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={}",
        scenario_name(parameters.scenario),
        parameters.ancestry,
        fixture.commits,
        io.get_calls,
        io.get_keys,
        io.write_batches,
        io.write_puts,
        io.write_bytes,
        physical.read_objects,
        physical.read_bytes,
        physical.write_objects,
        physical.write_bytes,
    );
}

fn parse_scenario(value: &str) -> MergeBaseBenchScenario {
    match value {
        "equal" => MergeBaseBenchScenario::EqualHeads,
        "ancestor" => MergeBaseBenchScenario::AncestorDescendant,
        "recent" => MergeBaseBenchScenario::RecentFork,
        "deep" => MergeBaseBenchScenario::DeepFork,
        "criss_cross" => MergeBaseBenchScenario::CrissCross,
        other => panic!("unknown scenario '{other}'"),
    }
}

fn scenario_name(scenario: MergeBaseBenchScenario) -> &'static str {
    match scenario {
        MergeBaseBenchScenario::EqualHeads => "equal",
        MergeBaseBenchScenario::AncestorDescendant => "ancestor",
        MergeBaseBenchScenario::RecentFork => "recent",
        MergeBaseBenchScenario::DeepFork => "deep",
        MergeBaseBenchScenario::CrissCross => "criss_cross",
    }
}

fn projected_value_len(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
    }
}

fn take_stats(stats: &Arc<Mutex<IoStats>>) -> IoStats {
    std::mem::take(&mut *stats.lock().expect("I/O stats mutex"))
}

fn process_resident_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmRSS:"))
                .and_then(|value| value.split_whitespace().next())
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map_or(0, |kilobytes| kilobytes.saturating_mul(1024))
}

fn process_clock_ticks_per_second() -> u64 {
    std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(100)
}

fn process_cpu_ticks() -> u64 {
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0;
    };
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return 0;
    };
    let fields = fields.split_whitespace().collect::<Vec<_>>();
    fields
        .get(11)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_add(
            fields
                .get(12)
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
        )
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries.flatten().fold(0, |total, entry| {
        let path = entry.path();
        let bytes = if path.is_dir() {
            directory_bytes(&path)
        } else {
            entry.metadata().map(|metadata| metadata.len()).unwrap_or(0)
        };
        total.saturating_add(bytes)
    })
}

fn parse_positive(value: Option<&String>, name: &str, default: usize) -> usize {
    let parsed = parse_nonnegative(value, name, default);
    assert!(parsed > 0, "{name} must be positive");
    parsed
}

fn parse_nonnegative(value: Option<&String>, name: &str, default: usize) -> usize {
    value.map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {name} '{value}': {error}"))
    })
}
