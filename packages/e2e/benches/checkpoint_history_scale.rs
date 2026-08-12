use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::time::{Duration, Instant};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use lix::changelog::bench::{append_ordered_commits, append_with_shape, stage_append_once};
use lix::integration::Engine;
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{CheckpointCommitScanBenchMode, scan_checkpoint_commits_for_bench};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const DEFAULT_BATCH_COMMITS: usize = 100_000;
const DEFAULT_SAMPLES: usize = 5;
const DEFAULT_WARMUPS: usize = 1;

#[derive(Clone, Copy, Debug)]
enum IdShape {
    Ordered,
    Hashed,
}

impl IdShape {
    fn from_env() -> Self {
        match std::env::var("LIX_CHECKPOINT_HISTORY_ID_SHAPE").as_deref() {
            Ok("hashed") => Self::Hashed,
            Ok("ordered") | Err(_) => Self::Ordered,
            Ok(value) => {
                panic!("checkpoint-history ID shape must be ordered or hashed, got {value}")
            }
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Ordered => "ordered",
            Self::Hashed => "hashed",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDB,
    SlateDB,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::RocksDB,
            "slatedb" => Self::SlateDB,
            _ => panic!("backend must be rocksdb or slatedb, got '{value}'"),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RocksDB => formatter.write_str("rocksdb"),
            Self::SlateDB => formatter.write_str("slatedb"),
        }
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create checkpoint-history benchmark runtime");
    runtime.block_on(run());
}

async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    let Some(command) = args.get(1).map(String::as_str) else {
        print_usage();
        return;
    };
    let Some(backend) = args.get(2).map(|value| Backend::parse(value)) else {
        print_usage();
        return;
    };
    let Some(path) = args.get(3).map(String::as_str) else {
        print_usage();
        return;
    };
    let history_changes = parse_positive(args.get(4), "history changes");
    let commit_width = parse_positive(args.get(5), "commit width");
    assert!(
        history_changes.is_multiple_of(commit_width),
        "history changes must divide evenly by commit width"
    );
    let expected_commits = history_changes / commit_width;
    let id_shape = IdShape::from_env();
    let query_checkpoint_count =
        env_nonnegative_usize("LIX_CHECKPOINT_HISTORY_QUERY_CHECKPOINTS", 0);

    match command {
        "setup" | "setup-query" => {
            let initialize_repository = command == "setup-query";
            assert!(
                !Path::new(path).exists(),
                "refusing to overwrite checkpoint-history fixture {path}"
            );
            let batch_commits = args
                .get(6)
                .map_or(DEFAULT_BATCH_COMMITS, |value| {
                    value
                        .parse::<usize>()
                        .expect("batch commits must be a positive integer")
                })
                .max(1);
            match backend {
                Backend::RocksDB => {
                    let storage = RocksDB::open(path).expect("open checkpoint-history RocksDB");
                    if initialize_repository {
                        initialize_query_fixture(storage.clone(), query_checkpoint_count).await;
                    }
                    let seed = seed_public_commits(
                        storage.clone(),
                        history_changes,
                        commit_width,
                        batch_commits,
                        id_shape,
                    )
                    .await;
                    storage.flush().expect("flush checkpoint-history RocksDB");
                    print_setup(
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        batch_commits,
                        id_shape,
                        initialize_repository,
                        query_checkpoint_count,
                        seed,
                        SlateDBIoSnapshot::default(),
                        false,
                        0,
                    );
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    let storage = SlateDB::open_with_io_counters(path, counters.clone())
                        .expect("open checkpoint-history SlateDB");
                    let before = counters.snapshot();
                    if initialize_repository {
                        initialize_query_fixture(storage.clone(), query_checkpoint_count).await;
                    }
                    let seed = seed_public_commits(
                        storage.clone(),
                        history_changes,
                        commit_width,
                        batch_commits,
                        id_shape,
                    )
                    .await;
                    let memtable_flushed = env_bool("LIX_CHECKPOINT_HISTORY_MEMTABLE_FLUSH", false);
                    if memtable_flushed {
                        storage
                            .flush_memtable_for_diagnostics()
                            .await
                            .expect("flush checkpoint-history SlateDB memtable");
                    } else {
                        storage
                            .flush()
                            .await
                            .expect("flush checkpoint-history SlateDB WAL");
                    }
                    let settle_ms = env_nonnegative_u64("LIX_CHECKPOINT_HISTORY_SETTLE_MS", 0);
                    if settle_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
                    }
                    print_setup(
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        batch_commits,
                        id_shape,
                        initialize_repository,
                        query_checkpoint_count,
                        seed,
                        counters.snapshot().saturating_sub(before),
                        memtable_flushed,
                        settle_ms,
                    );
                }
            }
        }
        "measure" => {
            let mode = match args.get(6).map(String::as_str).unwrap_or("materialize") {
                "materialize" => CheckpointCommitScanBenchMode::Materialize,
                "stream" => CheckpointCommitScanBenchMode::Stream,
                value => panic!("scan mode must be materialize or stream, got '{value}'"),
            };
            let samples = args
                .get(7)
                .map_or(DEFAULT_SAMPLES, |value| {
                    value
                        .parse::<usize>()
                        .expect("samples must be a positive integer")
                })
                .max(1);
            let warmups = args.get(8).map_or(DEFAULT_WARMUPS, |value| {
                value
                    .parse::<usize>()
                    .expect("warmups must be a non-negative integer")
            });
            match backend {
                Backend::RocksDB => {
                    measure(
                        RocksDB::open(path).expect("open checkpoint-history RocksDB"),
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        expected_commits,
                        mode,
                        samples,
                        warmups,
                        None,
                    )
                    .await;
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    measure(
                        SlateDB::open_with_io_counters(path, counters.clone())
                            .expect("open checkpoint-history SlateDB"),
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        expected_commits,
                        mode,
                        samples,
                        warmups,
                        Some(counters),
                    )
                    .await;
                }
            }
        }
        "measure-query" => {
            let samples = args
                .get(6)
                .map_or(DEFAULT_SAMPLES, |value| {
                    value
                        .parse::<usize>()
                        .expect("samples must be a positive integer")
                })
                .max(1);
            let warmups = args.get(7).map_or(DEFAULT_WARMUPS, |value| {
                value
                    .parse::<usize>()
                    .expect("warmups must be a non-negative integer")
            });
            match backend {
                Backend::RocksDB => {
                    measure_query(
                        RocksDB::open(path).expect("open checkpoint-query RocksDB"),
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        samples,
                        warmups,
                        query_checkpoint_count + 1,
                        None,
                    )
                    .await;
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    measure_query(
                        SlateDB::open_with_io_counters(path, counters.clone())
                            .expect("open checkpoint-query SlateDB"),
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        samples,
                        warmups,
                        query_checkpoint_count + 1,
                        Some(counters),
                    )
                    .await;
                }
            }
        }
        _ => print_usage(),
    }
}

#[derive(Clone, Copy, Debug)]
struct SeedResult {
    elapsed: Duration,
    puts: usize,
    written_bytes: usize,
}

async fn seed_public_commits<StorageImpl>(
    storage: StorageImpl,
    history_changes: usize,
    commit_width: usize,
    batch_commits: usize,
    id_shape: IdShape,
) -> SeedResult
where
    StorageImpl: Storage + Clone + Sync,
{
    let commit_count = history_changes / commit_width;
    let started = Instant::now();
    let mut puts = 0usize;
    let mut written_bytes = 0usize;
    for batch_start in (0..commit_count).step_by(batch_commits) {
        let count = (commit_count - batch_start).min(batch_commits);
        let append = match id_shape {
            IdShape::Ordered => append_ordered_commits(batch_start, count),
            IdShape::Hashed => append_with_shape(
                &format!("checkpoint-history-{history_changes}-{commit_width}-{batch_start}"),
                count,
                0,
            ),
        }
        .expect("build checkpoint-history commit batch");
        let stats = stage_append_once(storage.clone(), &append)
            .await
            .expect("stage checkpoint-history commit batch");
        puts = puts.saturating_add(stats.puts);
        written_bytes = written_bytes.saturating_add(stats.bytes_written);
    }
    SeedResult {
        elapsed: started.elapsed(),
        puts,
        written_bytes,
    }
}

async fn initialize_query_fixture<StorageImpl>(storage: StorageImpl, additional_checkpoints: usize)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize checkpoint-query fixture");
    if additional_checkpoints == 0 {
        return;
    }
    let engine = Engine::new(storage)
        .await
        .expect("open checkpoint-query setup engine");
    let session = engine
        .open_session()
        .await
        .expect("open checkpoint-query setup session");
    for _ in 0..additional_checkpoints {
        session
            .create_checkpoint()
            .await
            .expect("create checkpoint-query fixture checkpoint");
    }
}

#[expect(clippy::too_many_arguments)]
async fn measure<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &str,
    history_changes: usize,
    commit_width: usize,
    expected_commits: usize,
    mode: CheckpointCommitScanBenchMode,
    samples: usize,
    warmups: usize,
    counters: Option<SlateDBIoCounters>,
) where
    StorageImpl: Storage,
{
    let adapter = StorageAdapter::new(storage);
    for _ in 0..warmups {
        let result = scan_checkpoint_commits_for_bench(&adapter, mode)
            .await
            .expect("warm checkpoint commit scan");
        assert_eq!(result.commits, expected_commits);
    }
    let io_before = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let mut timings = Vec::with_capacity(samples);
    let mut pages = 0usize;
    for _ in 0..samples {
        let started = Instant::now();
        let result = scan_checkpoint_commits_for_bench(&adapter, mode)
            .await
            .expect("measure checkpoint commit scan");
        timings.push(started.elapsed());
        assert_eq!(result.commits, expected_commits);
        pages = result.pages;
    }
    timings.sort_unstable();
    let io = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(io_before);
    println!(
        "checkpoint_history_scale,phase=measure,backend={backend},mode={},\
         history_changes={history_changes},commit_width={commit_width},commits={expected_commits},\
         pages={pages},warmups={warmups},samples={samples},\
         p50_ms={},p95_ms={},p99_ms={},\
         read_objects={},read_bytes={},write_objects={},write_bytes={},\
         list_operations={},listed_objects={},backend_bytes={}",
        mode_name(mode),
        millis(percentile(&timings, 50)),
        millis(percentile(&timings, 95)),
        millis(percentile(&timings, 99)),
        io.read_objects,
        io.read_bytes,
        io.write_objects,
        io.write_bytes,
        io.list_operations,
        io.listed_objects,
        directory_bytes(Path::new(path)),
    );
}

#[expect(clippy::too_many_arguments)]
async fn measure_query<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &str,
    history_changes: usize,
    commit_width: usize,
    samples: usize,
    warmups: usize,
    expected_checkpoints: usize,
    counters: Option<SlateDBIoCounters>,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let engine = Engine::new(storage)
        .await
        .expect("open checkpoint-query engine");
    let session = engine
        .open_session()
        .await
        .expect("open checkpoint-query session");
    for _ in 0..warmups {
        let rows = session
            .execute("SELECT commit_id FROM lix_checkpoint", &[])
            .await
            .expect("warm checkpoint query");
        assert_eq!(rows.len(), expected_checkpoints);
    }
    let io_before = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        let rows = session
            .execute("SELECT commit_id FROM lix_checkpoint", &[])
            .await
            .expect("measure checkpoint query");
        timings.push(started.elapsed());
        assert_eq!(rows.len(), expected_checkpoints);
    }
    timings.sort_unstable();
    let io = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(io_before);
    println!(
        "checkpoint_history_scale,phase=measure_query,backend={backend},\
         history_changes={history_changes},commit_width={commit_width},\
         requested_checkpoints={expected_checkpoints},\
         warmups={warmups},samples={samples},\
         p50_ms={},p95_ms={},p99_ms={},\
         read_objects={},read_bytes={},write_objects={},write_bytes={},\
         list_operations={},listed_objects={},backend_bytes={}",
        millis(percentile(&timings, 50)),
        millis(percentile(&timings, 95)),
        millis(percentile(&timings, 99)),
        io.read_objects,
        io.read_bytes,
        io.write_objects,
        io.write_bytes,
        io.list_operations,
        io.listed_objects,
        directory_bytes(Path::new(path)),
    );
}

#[expect(clippy::too_many_arguments)]
fn print_setup(
    backend: Backend,
    path: &str,
    history_changes: usize,
    commit_width: usize,
    batch_commits: usize,
    id_shape: IdShape,
    initialized_repository: bool,
    query_checkpoints: usize,
    seed: SeedResult,
    io: SlateDBIoSnapshot,
    memtable_flushed: bool,
    settle_ms: u64,
) {
    println!(
        "checkpoint_history_scale,phase=setup,backend={backend},\
         history_changes={history_changes},commit_width={commit_width},commits={},\
         batch_commits={batch_commits},id_shape={},initialized_repository={initialized_repository},\
         query_checkpoints={},seed_ms={},ingest_changes_per_second={:.1},\
         staged_puts={},logical_written_bytes={},memtable_flushed={memtable_flushed},\
         settle_ms={settle_ms},read_objects={},read_bytes={},write_objects={},write_bytes={},\
         list_operations={},listed_objects={},backend_bytes={}",
        history_changes / commit_width,
        id_shape.name(),
        query_checkpoints + usize::from(initialized_repository),
        seed.elapsed.as_millis(),
        history_changes as f64 / seed.elapsed.as_secs_f64(),
        seed.puts,
        seed.written_bytes,
        io.read_objects,
        io.read_bytes,
        io.write_objects,
        io.write_bytes,
        io.list_operations,
        io.listed_objects,
        directory_bytes(Path::new(path)),
    );
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

const fn mode_name(mode: CheckpointCommitScanBenchMode) -> &'static str {
    match mode {
        CheckpointCommitScanBenchMode::Materialize => "materialize",
        CheckpointCommitScanBenchMode::Stream => "stream",
    }
}

fn parse_positive(value: Option<&String>, label: &str) -> usize {
    value
        .unwrap_or_else(|| panic!("missing {label}"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{label} must be a positive integer"))
        .max(1)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name).map_or(default, |value| {
        matches!(value.as_str(), "1" | "true" | "yes")
    })
}

fn env_nonnegative_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_nonnegative_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a non-negative integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            panic!("{name} must contain valid UTF-8")
        }
    }
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                entry.metadata().map_or(0, |metadata| {
                    if metadata.is_dir() {
                        directory_bytes(&entry.path())
                    } else {
                        metadata.len()
                    }
                })
            })
            .sum()
    })
}

fn print_usage() {
    eprintln!(
        "usage:\n  checkpoint_history_scale setup <rocksdb|slatedb> <path> \
         <history-changes> <commit-width> [batch-commits]\n  \
         checkpoint_history_scale setup-query <rocksdb|slatedb> <path> \
         <history-changes> <commit-width> [batch-commits]\n  \
         checkpoint_history_scale measure <rocksdb|slatedb> <path> \
         <history-changes> <commit-width> <materialize|stream> [samples] [warmups]\n  \
         checkpoint_history_scale measure-query <rocksdb|slatedb> <path> \
         <history-changes> <commit-width> [samples] [warmups]"
    );
}
