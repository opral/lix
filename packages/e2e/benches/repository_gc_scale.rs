use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::time::{Duration, Instant};

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use lix::open_lix;
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    RepositoryGcBenchResult, audit_repository_gc_standalone_for_bench, plan_repository_gc_for_bench,
};
use lix::{CreateBranchOptions, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

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
                    open_lix()
                        .with_storage(storage.clone())
                        .await
                        .expect("initialize repository-GC RocksDB fixture");
                    let seed = seed_unreachable_commits(
                        storage.clone(),
                        expected_swept_commits,
                        commit_width,
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
                    open_lix()
                        .with_storage(storage.clone())
                        .await
                        .expect("initialize repository-GC SlateDB fixture");
                    let seed = seed_unreachable_commits(
                        storage.clone(),
                        expected_swept_commits,
                        commit_width,
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
    changes_per_commit: usize,
) -> SeedResult
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let started = Instant::now();
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open repository-GC lix");
    let main = lix
        .open_another_session()
        .await
        .expect("open repository-GC main session");
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "repository_gc_fixture",
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false },
        ],
        "primary_key": ["path"],
    });
    main.execute(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
        &[Value::Text(schema.to_string())],
    )
    .await
    .expect("register repository-GC schema");
    let branch = main
        .create_branch(CreateBranchOptions {
            id: Some("01990000-0000-7000-8000-000000000001".to_owned()),
            name: "repository-gc-unreachable".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create repository-GC disposable branch");
    let branch_session = lix
        .open_another_session()
        .await
        .expect("open repository-GC disposable branch");
    branch_session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: (branch.id.clone()).to_string(),
        })
        .await
        .expect("switch session branch");
    for commit_index in 0..commit_count {
        let mut transaction = branch_session
            .begin_transaction()
            .await
            .expect("begin repository-GC fixture transaction");
        for row_index in 0..changes_per_commit {
            let row = commit_index
                .checked_mul(changes_per_commit)
                .and_then(|base| base.checked_add(row_index))
                .expect("repository-GC fixture row index overflow");
            transaction
                .execute(
                    "INSERT INTO repository_gc_fixture (path, value) VALUES ($1, CAST($2 AS JSONB))",
                    &[
                        Value::Text(format!("/row/{row:08}")),
                        Value::Text(format!(r#"{{"commit":{commit_index},"row":{row}}}"#)),
                    ],
                )
                .await
                .expect("stage repository-GC fixture row");
        }
        transaction
            .commit()
            .await
            .expect("publish repository-GC fixture commit");
    }
    main.execute(
        "DELETE FROM lix_branch WHERE id = $1",
        &[Value::Text(branch.id)],
    )
    .await
    .expect("delete repository-GC disposable branch");
    SeedResult {
        elapsed: started.elapsed(),
        puts: commit_count.saturating_mul(changes_per_commit),
        written_bytes: 0,
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
    let standalone_audit = audit_repository_gc_standalone_for_bench(&adapter)
        .await
        .expect("audit repository-GC standalone facts");
    println!(
        "repository_gc_standalone_audit,backend={backend},history_changes={history_changes},\
         commit_width={commit_width},entries={}",
        standalone_audit.join("|")
    );
    for _ in 0..warmups {
        let result = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("warm repository-GC plan");
        assert!(
            result.swept_commits <= expected_swept_commits,
            "GC planner swept more commits than the fixture created"
        );
    }
    let io_before = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let mut timings = Vec::with_capacity(samples);
    let mut results = Vec::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        let result = plan_repository_gc_for_bench(&adapter)
            .await
            .expect("measure repository-GC plan");
        timings.push(started.elapsed());
        assert!(
            result.swept_commits <= expected_swept_commits,
            "GC planner swept more commits than the fixture created"
        );
        results.push(result);
    }
    timings.sort_unstable();
    let last = results.last().expect("repository-GC samples are positive");
    let phase_percentiles = |select: fn(&RepositoryGcBenchResult) -> u64| {
        let mut values = results.iter().map(select).collect::<Vec<_>>();
        values.sort_unstable();
        (
            percentile_u64(&values, 50),
            percentile_u64(&values, 95),
            percentile_u64(&values, 99),
        )
    };
    let root_discovery = phase_percentiles(|result| result.root_discovery_us);
    let changelog = phase_percentiles(|result| result.changelog_us);
    let tracked_root_stage = phase_percentiles(|result| result.tracked_root_stage_us);
    let gc_total = phase_percentiles(|result| result.total_us);
    let io = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(io_before);
    println!(
        "repository_gc_scale,phase=measure,backend={backend},\
         history_changes={history_changes},commit_width={commit_width},\
         swept_commits={},live_commits={},swept_standalone_changes={},standalone_swept_ids={},swept_payloads={},\
         samples={samples},warmups={warmups},p50_ms={},p95_ms={},p99_ms={},\
         root_discovery_p50_us={},root_discovery_p95_us={},root_discovery_p99_us={},\
         changelog_p50_us={},changelog_p95_us={},changelog_p99_us={},\
         tracked_root_stage_p50_us={},tracked_root_stage_p95_us={},tracked_root_stage_p99_us={},\
         gc_total_p50_us={},gc_total_p95_us={},gc_total_p99_us={},\
         staged_puts={},staged_deletes={},staged_written_bytes={},\
         delete_descriptors={},delete_descriptor_capacity={},\
         key_inline_bytes={},key_inline_capacity={},\
         key_shared_buffers={},key_shared_bytes={},key_shared_capacity={},\
         read_objects={},read_bytes={},write_objects={},write_bytes={},\
         list_operations={},listed_objects={},backend_bytes={}",
        last.swept_commits,
        last.live_commits,
        last.swept_standalone_changes,
        last.standalone_swept_ids.join("|"),
        last.swept_payloads,
        millis(percentile(&timings, 50)),
        millis(percentile(&timings, 95)),
        millis(percentile(&timings, 99)),
        root_discovery.0,
        root_discovery.1,
        root_discovery.2,
        changelog.0,
        changelog.1,
        changelog.2,
        tracked_root_stage.0,
        tracked_root_stage.1,
        tracked_root_stage.2,
        gc_total.0,
        gc_total.1,
        gc_total.2,
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
    print_io_categories("measure", backend, history_changes, commit_width, io);
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
    print_io_categories("setup", backend, history_changes, commit_width, io);
}

fn print_io_categories(
    phase: &str,
    backend: Backend,
    history_changes: usize,
    commit_width: usize,
    io: SlateDBIoSnapshot,
) {
    println!(
        "repository_gc_scale_io,phase={phase},backend={backend},\
         history_changes={history_changes},commit_width={commit_width},\
         wal_read_objects={},wal_read_bytes={},wal_write_objects={},wal_write_bytes={},\
         compacted_read_objects={},compacted_read_bytes={},\
         compacted_write_objects={},compacted_write_bytes={},\
         manifest_read_objects={},manifest_read_bytes={},\
         manifest_write_objects={},manifest_write_bytes={},\
         compactions_read_objects={},compactions_read_bytes={},\
         compactions_write_objects={},compactions_write_bytes={},\
         other_read_objects={},other_read_bytes={},\
         other_write_objects={},other_write_bytes={},\
         main_read_requests={},main_write_requests={},\
         reader_read_requests={},reader_write_requests={},\
         compactor_read_requests={},compactor_write_requests={},\
         gc_read_requests={},gc_write_requests={}",
        io.wal.read_objects,
        io.wal.read_bytes,
        io.wal.write_objects,
        io.wal.write_bytes,
        io.compacted.read_objects,
        io.compacted.read_bytes,
        io.compacted.write_objects,
        io.compacted.write_bytes,
        io.manifest.read_objects,
        io.manifest.read_bytes,
        io.manifest.write_objects,
        io.manifest.write_bytes,
        io.compactions.read_objects,
        io.compactions.read_bytes,
        io.compactions.write_objects,
        io.compactions.write_bytes,
        io.other.read_objects,
        io.other.read_bytes,
        io.other.write_objects,
        io.other.write_bytes,
        io.main.read_requests,
        io.main.write_requests,
        io.reader.read_requests,
        io.reader.write_requests,
        io.compactor.read_requests,
        io.compactor.write_requests,
        io.gc.read_requests,
        io.gc.write_requests,
    );
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn percentile_u64(sorted: &[u64], percentile: usize) -> u64 {
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
