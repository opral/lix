use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::hint::black_box;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lix::changelog::bench as changelog_bench;
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, ProjectedValue,
    PutBatch, ReadOptions, ScanChunk, ScanCursor, Storage, StorageError, StorageRead,
    StorageScanSource, StorageSpace, StorageWrite, WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};
use tempfile::TempDir;

const BACKENDS: &[Backend] = &[Backend::Rocks, Backend::Slate];
const OPERATIONS: &[Operation] = &[Operation::UniqueAppend, Operation::DuplicateRejected];
const DEFAULT_HISTORY_COMMITS: &[usize] = &[1_000, 5_000];
const DEFAULT_WARMUPS: usize = 5;
const DEFAULT_SAMPLES: usize = 31;
const DEFAULT_SEED_BATCH_COMMITS: usize = 100_000;

#[derive(Clone, Copy, Debug)]
enum Backend {
    Rocks,
    Slate,
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rocks => formatter.write_str("rocksdb"),
            Self::Slate => formatter.write_str("slatedb"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    UniqueAppend,
    DuplicateRejected,
}

#[derive(Clone, Copy, Debug)]
enum Lifecycle {
    Visible,
    WalFlushed,
    MemtableFlushed,
}

impl Display for Lifecycle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Visible => formatter.write_str("visible"),
            Self::WalFlushed => formatter.write_str("wal_flushed"),
            Self::MemtableFlushed => formatter.write_str("memtable_flushed"),
        }
    }
}

impl Display for Operation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UniqueAppend => formatter.write_str("unique_append"),
            Self::DuplicateRejected => formatter.write_str("duplicate_change_id_rejected"),
        }
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create changelog history append benchmark runtime");
    runtime.block_on(run());
}

async fn run() {
    let histories = env_usizes("LIX_CHANGELOG_HISTORY_COMMITS", DEFAULT_HISTORY_COMMITS);
    let warmups = env_usize("LIX_CHANGELOG_HISTORY_WARMUPS", DEFAULT_WARMUPS);
    let samples = env_usize("LIX_CHANGELOG_HISTORY_SAMPLES", DEFAULT_SAMPLES).max(1);
    let seed_batch_commits = env_usize(
        "LIX_CHANGELOG_HISTORY_SEED_BATCH_COMMITS",
        DEFAULT_SEED_BATCH_COMMITS,
    )
    .max(1);
    let lifecycle = env_lifecycle();
    let settle_ms = env_usize("LIX_CHANGELOG_HISTORY_SETTLE_MS", 0);

    for &backend in BACKENDS {
        if !selected("LIX_CHANGELOG_HISTORY_BACKENDS", &backend.to_string()) {
            continue;
        }
        for &history_commits in &histories {
            for &operation in OPERATIONS {
                if !selected("LIX_CHANGELOG_HISTORY_OPERATIONS", &operation.to_string()) {
                    continue;
                }
                run_case(
                    backend,
                    history_commits,
                    operation,
                    warmups,
                    samples,
                    seed_batch_commits,
                    lifecycle,
                    settle_ms,
                )
                .await;
            }
        }
    }
}

async fn run_case(
    backend: Backend,
    history_commits: usize,
    operation: Operation,
    warmups: usize,
    samples: usize,
    seed_batch_commits: usize,
    lifecycle: Lifecycle,
    settle_ms: usize,
) {
    let mut fixture = Fixture::new(backend, history_commits, seed_batch_commits).await;
    fixture.apply_lifecycle(lifecycle).await;
    tokio::time::sleep(Duration::from_millis(
        settle_ms
            .try_into()
            .expect("settle milliseconds should fit in u64"),
    ))
    .await;
    for _ in 0..warmups {
        let append = fixture.prepare(operation);
        black_box(fixture.execute(operation, append).await);
    }

    fixture.reset_io();
    let physical_io_before = fixture.physical_io();
    let mut stage_timings = Vec::with_capacity(samples);
    let mut commit_timings = Vec::with_capacity(samples);
    let mut total_timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let append = fixture.prepare(operation);
        let total_started = Instant::now();
        let timing = fixture.execute(operation, append).await;
        total_timings.push(total_started.elapsed());
        stage_timings.push(timing.stage_elapsed);
        commit_timings.push(timing.commit_elapsed);
    }
    let io = fixture.io();
    let physical_io = fixture.physical_io().saturating_sub(physical_io_before);
    stage_timings.sort_unstable();
    commit_timings.sort_unstable();
    total_timings.sort_unstable();
    let sample_count = u64::try_from(samples).expect("benchmark sample count should fit in u64");

    println!(
        "changelog_history_append,backend={backend},operation={operation},history_commits={history_commits},\
         seed_batch_commits={seed_batch_commits},lifecycle={lifecycle},settle_ms={settle_ms},\
         warmups={warmups},samples={samples},\
         stage_p50_us={},stage_p95_us={},stage_p99_us={},stage_mean_us={},\
         commit_p50_us={},commit_p95_us={},commit_p99_us={},commit_mean_us={},\
         total_p50_us={},total_p95_us={},total_p99_us={},total_mean_us={},\
         get_many_calls_per_op={},get_many_keys_per_op={},\
         scan_calls_per_op={},scan_rows_per_op={},scan_value_bytes_per_op={},\
         put_batches_per_op={},puts_per_op={},write_bytes_per_op={},\
         physical_read_objects={},physical_read_bytes={},\
         physical_manifest_read_objects={},physical_manifest_read_bytes={},\
         physical_compacted_read_objects={},physical_compacted_read_bytes={},\
         physical_other_read_objects={},physical_other_read_bytes={},\
         physical_reader_read_requests={},physical_main_read_requests={}",
        micros(percentile(&stage_timings, 50, 100)),
        micros(percentile(&stage_timings, 95, 100)),
        micros(percentile(&stage_timings, 99, 100)),
        micros(mean(&stage_timings)),
        micros(percentile(&commit_timings, 50, 100)),
        micros(percentile(&commit_timings, 95, 100)),
        micros(percentile(&commit_timings, 99, 100)),
        micros(mean(&commit_timings)),
        micros(percentile(&total_timings, 50, 100)),
        micros(percentile(&total_timings, 95, 100)),
        micros(percentile(&total_timings, 99, 100)),
        micros(mean(&total_timings)),
        io.get_many_calls / sample_count,
        io.get_many_keys / sample_count,
        io.scan_calls / sample_count,
        io.scan_rows / sample_count,
        io.scan_value_bytes / sample_count,
        io.put_batches / sample_count,
        io.puts / sample_count,
        io.write_bytes / sample_count,
        physical_io.read_objects,
        physical_io.read_bytes,
        physical_io.manifest.read_objects,
        physical_io.manifest.read_bytes,
        physical_io.compacted.read_objects,
        physical_io.compacted.read_bytes,
        physical_io.other.read_objects,
        physical_io.other.read_bytes,
        physical_io.reader.read_requests,
        physical_io.main.read_requests,
    );
}

fn micros(duration: Duration) -> u128 {
    duration.as_micros()
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn mean(timings: &[Duration]) -> Duration {
    let count = u32::try_from(timings.len()).expect("benchmark sample count should fit in u32");
    timings.iter().sum::<Duration>() / count
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_usizes(name: &str, default: &[usize]) -> Vec<usize> {
    let Some(value) = std::env::var(name).ok() else {
        return default.to_vec();
    };
    let values = value
        .split(',')
        .filter_map(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    if values.is_empty() {
        default.to_vec()
    } else {
        values
    }
}

fn env_lifecycle() -> Lifecycle {
    match std::env::var("LIX_CHANGELOG_HISTORY_LIFECYCLE").as_deref() {
        Ok("wal_flushed") => Lifecycle::WalFlushed,
        Ok("memtable_flushed") => Lifecycle::MemtableFlushed,
        Ok("visible") | Err(_) => Lifecycle::Visible,
        Ok(value) => panic!(
            "LIX_CHANGELOG_HISTORY_LIFECYCLE must be visible, wal_flushed, or memtable_flushed; got {value}"
        ),
    }
}

fn selected(variable: &str, candidate: &str) -> bool {
    std::env::var(variable).map_or(true, |selection| {
        selection
            .split(',')
            .map(str::trim)
            .any(|value| value == candidate)
    })
}

#[derive(Clone, Copy, Debug, Default)]
struct IoStats {
    get_many_calls: u64,
    get_many_keys: u64,
    scan_calls: u64,
    scan_rows: u64,
    scan_value_bytes: u64,
    put_batches: u64,
    puts: u64,
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

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(opts).await?,
            stats: Arc::clone(&self.stats),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountingWrite {
            inner: self.inner.begin_write(opts).await?,
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
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.get_many_calls += 1;
            stats.get_many_keys += requests
                .iter()
                .map(|request| request.keys.len() as u64)
                .sum::<u64>();
        }
        self.inner.get_many(requests).await
    }

    async fn begin_scan(
        &self,
        space: StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        let order = opts.order;
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.scan_calls += 1;
        }
        let inner = self.inner.begin_scan(space, range.clone(), opts).await?;
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
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.scan_rows += chunk.len() as u64;
            stats.scan_value_bytes += chunk
                .iter()
                .map(|entry| projected_value_bytes(&entry.value) as u64)
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
        space: StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        {
            let mut stats = self.stats.lock().expect("io stats mutex");
            stats.put_batches += 1;
            stats.puts += entries.entries.len() as u64;
            stats.write_bytes += entries
                .entries
                .iter()
                .map(|entry| (entry.key.0.len() + entry.value.bytes.len()) as u64)
                .sum::<u64>();
        }
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: StorageSpace, keys: &[Key]) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

fn projected_value_bytes(value: &ProjectedValue) -> usize {
    match value {
        ProjectedValue::KeyOnly => 0,
        ProjectedValue::FullValue(value) => value.len(),
    }
}

struct BackendFixture<S>
where
    S: Storage + Clone,
{
    store: changelog_bench::BenchStore<CountingStorage<S>>,
    storage: S,
    _temp_dir: TempDir,
    stats: Arc<Mutex<IoStats>>,
    physical_io: Option<SlateDBIoCounters>,
    history_commit_id: String,
    version: u64,
}

impl<S> BackendFixture<S>
where
    S: Storage + Clone,
{
    async fn create(
        storage: S,
        temp_dir: TempDir,
        history_commits: usize,
        seed_batch_commits: usize,
        physical_io: Option<SlateDBIoCounters>,
    ) -> Self {
        let lifecycle_storage = storage.clone();
        let (storage, stats) = CountingStorage::new(storage);
        let history_name = "changelog-history";
        let first_batch_name = format!("{history_name}-batch-0");
        let first_batch_commits = history_commits.min(seed_batch_commits);
        let first_batch = changelog_bench::append_with_shape(
            &first_batch_name,
            first_batch_commits,
            first_batch_commits,
        )
        .expect("build first changelog history seed batch");
        let store = changelog_bench::prepare_store(storage, &first_batch)
            .await
            .expect("seed first changelog history batch");
        drop(first_batch);
        let mut seeded_commits = first_batch_commits;
        let mut batch_index = 1usize;
        while seeded_commits < history_commits {
            let batch_commits = (history_commits - seeded_commits).min(seed_batch_commits);
            let batch = changelog_bench::append_with_shape(
                &format!("{history_name}-batch-{batch_index}"),
                batch_commits,
                batch_commits,
            )
            .expect("build changelog history seed batch");
            changelog_bench::stage_append_to_store(&store, &batch)
                .await
                .expect("seed changelog history batch");
            seeded_commits += batch_commits;
            batch_index += 1;
        }
        *stats.lock().expect("io stats mutex") = IoStats::default();
        Self {
            store,
            storage: lifecycle_storage,
            _temp_dir: temp_dir,
            stats,
            physical_io,
            history_commit_id: format!("{first_batch_name}-commit-0"),
            version: 0,
        }
    }

    fn prepare(&mut self, operation: Operation) -> changelog_bench::BenchAppend {
        let name = format!("changelog-history-probe-{}", self.version);
        self.version += 1;
        match operation {
            Operation::UniqueAppend => {
                changelog_bench::append_with_shape(&name, 1, 1).expect("build unique append")
            }
            Operation::DuplicateRejected => {
                changelog_bench::append_1c_with_commit_id(&name, &self.history_commit_id)
                    .expect("build duplicate derived change-id append")
            }
        }
    }

    async fn execute(
        &self,
        operation: Operation,
        append: changelog_bench::BenchAppend,
    ) -> changelog_bench::BenchAppendTiming {
        match operation {
            Operation::UniqueAppend => {
                changelog_bench::stage_append_timed_to_store(&self.store, &append)
                    .await
                    .expect("stage unique append")
            }
            Operation::DuplicateRejected => {
                let stage_started = Instant::now();
                let error = changelog_bench::stage_append_to_store(&self.store, &append)
                    .await
                    .expect_err("duplicate derived change id must be rejected");
                assert!(error.message.contains("already exists"), "{error:?}");
                changelog_bench::BenchAppendTiming {
                    stage_elapsed: stage_started.elapsed(),
                    ..changelog_bench::BenchAppendTiming::default()
                }
            }
        }
    }

    fn reset_io(&self) {
        *self.stats.lock().expect("io stats mutex") = IoStats::default();
    }

    fn io(&self) -> IoStats {
        *self.stats.lock().expect("io stats mutex")
    }

    fn physical_io(&self) -> SlateDBIoSnapshot {
        self.physical_io
            .as_ref()
            .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
    }
}

enum Fixture {
    Rocks(BackendFixture<RocksDB>),
    Slate(BackendFixture<SlateDB>),
}

impl Fixture {
    async fn new(backend: Backend, history_commits: usize, seed_batch_commits: usize) -> Self {
        let temp_dir = tempfile::tempdir().expect("create changelog history benchmark directory");
        let database_path = temp_dir.path().join("database");
        match backend {
            Backend::Rocks => Self::Rocks(
                BackendFixture::create(
                    RocksDB::open(&database_path).expect("open benchmark RocksDB"),
                    temp_dir,
                    history_commits,
                    seed_batch_commits,
                    None,
                )
                .await,
            ),
            Backend::Slate => {
                let physical_io = SlateDBIoCounters::default();
                Self::Slate(
                    BackendFixture::create(
                        SlateDB::open_with_io_counters(&database_path, physical_io.clone())
                            .expect("open benchmark SlateDB"),
                        temp_dir,
                        history_commits,
                        seed_batch_commits,
                        Some(physical_io),
                    )
                    .await,
                )
            }
        }
    }

    async fn apply_lifecycle(&self, lifecycle: Lifecycle) {
        match (self, lifecycle) {
            (_, Lifecycle::Visible) => {}
            (Self::Rocks(fixture), Lifecycle::WalFlushed) => fixture
                .storage
                .flush_wal_for_diagnostics()
                .expect("flush changelog-history RocksDB WAL"),
            (Self::Rocks(fixture), Lifecycle::MemtableFlushed) => fixture
                .storage
                .flush()
                .expect("flush changelog-history RocksDB memtable"),
            (Self::Slate(fixture), Lifecycle::WalFlushed) => fixture
                .storage
                .flush()
                .await
                .expect("flush changelog-history SlateDB WAL"),
            (Self::Slate(fixture), Lifecycle::MemtableFlushed) => fixture
                .storage
                .flush_memtable_for_diagnostics()
                .await
                .expect("flush changelog-history SlateDB memtable"),
        }
    }

    fn prepare(&mut self, operation: Operation) -> changelog_bench::BenchAppend {
        match self {
            Self::Rocks(fixture) => fixture.prepare(operation),
            Self::Slate(fixture) => fixture.prepare(operation),
        }
    }

    async fn execute(
        &self,
        operation: Operation,
        append: changelog_bench::BenchAppend,
    ) -> changelog_bench::BenchAppendTiming {
        match self {
            Self::Rocks(fixture) => fixture.execute(operation, append).await,
            Self::Slate(fixture) => fixture.execute(operation, append).await,
        }
    }

    fn reset_io(&self) {
        match self {
            Self::Rocks(fixture) => fixture.reset_io(),
            Self::Slate(fixture) => fixture.reset_io(),
        }
    }

    fn io(&self) -> IoStats {
        match self {
            Self::Rocks(fixture) => fixture.io(),
            Self::Slate(fixture) => fixture.io(),
        }
    }

    fn physical_io(&self) -> SlateDBIoSnapshot {
        match self {
            Self::Rocks(fixture) => fixture.physical_io(),
            Self::Slate(fixture) => fixture.physical_io(),
        }
    }
}
