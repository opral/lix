use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::time::{Duration, Instant};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use lix_engine::Engine;
use lix_engine::changelog::bench::{append_ordered_commits, stage_append_once};
use lix_engine::storage::Storage;
use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::storage_bench::{RepositoryGcBenchResult, plan_repository_gc_for_bench};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const DEFAULT_BATCH_COMMITS: usize = 100_000;
const DEFAULT_SAMPLES: usize = 5;
const DEFAULT_WARMUPS: usize = 1;

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
        .expect("create repository-GC benchmark runtime");
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
    let expected_swept_commits = history_changes / commit_width;

    match command {
        "setup" => {
            assert!(
                !Path::new(path).exists(),
                "refusing to overwrite repository-GC fixture {path}"
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
                    let storage = RocksDB::open(path).expect("open repository-GC RocksDB");
                    Engine::initialize(storage.clone())
                        .await
                        .expect("initialize repository-GC RocksDB fixture");
                    let seed = seed_unreachable_commits(
                        storage.clone(),
                        expected_swept_commits,
                        batch_commits,
                    )
                    .await;
                    storage.flush().expect("flush repository-GC RocksDB");
                    print_setup(
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        batch_commits,
                        seed,
                        SlateDBIoSnapshot::default(),
                        false,
                        0,
                    );
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    let storage = SlateDB::open_with_io_counters(path, counters.clone())
                        .expect("open repository-GC SlateDB");
                    let before = counters.snapshot();
                    Engine::initialize(storage.clone())
                        .await
                        .expect("initialize repository-GC SlateDB fixture");
                    let seed = seed_unreachable_commits(
                        storage.clone(),
                        expected_swept_commits,
                        batch_commits,
                    )
                    .await;
                    let memtable_flushed = env_bool("LIX_REPOSITORY_GC_MEMTABLE_FLUSH", false);
                    if memtable_flushed {
                        storage
                            .flush_memtable_for_diagnostics()
                            .await
                            .expect("flush repository-GC SlateDB memtable");
                    } else {
                        storage
                            .flush()
                            .await
                            .expect("flush repository-GC SlateDB WAL");
                    }
                    let settle_ms = env_nonnegative_u64("LIX_REPOSITORY_GC_SETTLE_MS", 0);
                    if settle_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
                    }
                    print_setup(
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        batch_commits,
                        seed,
                        counters.snapshot().saturating_sub(before),
                        memtable_flushed,
                        settle_ms,
                    );
                }
            }
        }
        "measure" => {
            let samples = args
                .get(6)
                .map_or(DEFAULT_SAMPLES, |value| {
                    value.parse::<usize>().expect("samples must be positive")
                })
                .max(1);
            let warmups = args.get(7).map_or(DEFAULT_WARMUPS, |value| {
                value
                    .parse::<usize>()
                    .expect("warmups must be non-negative")
            });
            match backend {
                Backend::RocksDB => {
                    measure(
                        RocksDB::open(path).expect("open repository-GC RocksDB"),
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        expected_swept_commits,
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
                            .expect("open repository-GC SlateDB"),
                        backend,
                        path,
                        history_changes,
                        commit_width,
                        expected_swept_commits,
                        samples,
                        warmups,
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

async fn seed_unreachable_commits<StorageImpl>(
    storage: StorageImpl,
    commit_count: usize,
    batch_commits: usize,
) -> SeedResult
where
    StorageImpl: Storage + Clone + Sync,
{
    let started = Instant::now();
    let mut puts = 0usize;
    let mut written_bytes = 0usize;
    for batch_start in (0..commit_count).step_by(batch_commits) {
        let count = (commit_count - batch_start).min(batch_commits);
        let append =
            append_ordered_commits(batch_start, count).expect("build repository-GC commit batch");
        let stats = stage_append_once(storage.clone(), &append)
            .await
            .expect("stage repository-GC commit batch");
        puts = puts.saturating_add(stats.puts);
        written_bytes = written_bytes.saturating_add(stats.bytes_written);
    }
    SeedResult {
        elapsed: started.elapsed(),
        puts,
        written_bytes,
    }
}

#[expect(clippy::too_many_arguments)]
async fn measure<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &str,
    history_changes: usize,
    commit_width: usize,
    expected_swept_commits: usize,
    samples: usize,
    warmups: usize,
    counters: Option<SlateDBIoCounters>,
) where
    StorageImpl: Storage,
{
    let adapter = StorageAdapter::new(storage);
    for _ in 0..warmups {
        let result = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("warm repository-GC plan");
        assert_eq!(result.swept_commits, expected_swept_commits);
    }
    let io_before = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let mut timings = Vec::with_capacity(samples);
    let mut last = RepositoryGcBenchResult::default();
    for _ in 0..samples {
        let started = Instant::now();
        last = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("measure repository-GC plan");
        timings.push(started.elapsed());
        assert_eq!(last.swept_commits, expected_swept_commits);
    }
    timings.sort_unstable();
    let io = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(io_before);
    println!(
        "repository_gc_scale,phase=measure,backend={backend},\
         history_changes={history_changes},commit_width={commit_width},\
         swept_commits={},live_commits={},swept_standalone_changes={},swept_payloads={},\
         samples={samples},warmups={warmups},p50_ms={},p95_ms={},p99_ms={},\
         root_discovery_us={},changelog_us={},tracked_root_stage_us={},gc_total_us={},\
         staged_puts={},staged_deletes={},staged_written_bytes={},\
         delete_descriptors={},delete_descriptor_capacity={},\
         key_inline_bytes={},key_inline_capacity={},\
         key_shared_buffers={},key_shared_bytes={},key_shared_capacity={},\
         read_objects={},read_bytes={},write_objects={},write_bytes={},\
         list_operations={},listed_objects={},backend_bytes={}",
        last.swept_commits,
        last.live_commits,
        last.swept_standalone_changes,
        last.swept_payloads,
        millis(percentile(&timings, 50)),
        millis(percentile(&timings, 95)),
        millis(percentile(&timings, 99)),
        last.root_discovery_us,
        last.changelog_us,
        last.tracked_root_stage_us,
        last.total_us,
        last.staged_puts,
        last.staged_deletes,
        last.staged_written_bytes,
        last.delete_descriptors,
        last.delete_descriptor_capacity,
        last.key_inline_bytes,
        last.key_inline_capacity,
        last.key_shared_buffers,
        last.key_shared_bytes,
        last.key_shared_capacity,
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
    seed: SeedResult,
    io: SlateDBIoSnapshot,
    memtable_flushed: bool,
    settle_ms: u64,
) {
    println!(
        "repository_gc_scale,phase=setup,backend={backend},history_changes={history_changes},\
         commit_width={commit_width},commits={},batch_commits={batch_commits},\
         seed_ms={},ingest_changes_per_second={:.1},staged_puts={},logical_written_bytes={},\
         memtable_flushed={memtable_flushed},settle_ms={settle_ms},\
         read_objects={},read_bytes={},write_objects={},write_bytes={},\
         list_operations={},listed_objects={},backend_bytes={}",
        history_changes / commit_width,
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

fn parse_positive(value: Option<&String>, label: &str) -> usize {
    value
        .unwrap_or_else(|| panic!("missing {label}"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{label} must be positive"))
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
        "usage:\n  repository_gc_scale setup <rocksdb|slatedb> <path> \
         <history-changes> <commit-width> [batch-commits]\n  \
         repository_gc_scale measure <rocksdb|slatedb> <path> \
         <history-changes> <commit-width> [samples] [warmups]"
    );
}
