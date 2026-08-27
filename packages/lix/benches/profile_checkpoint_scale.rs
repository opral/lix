#![allow(clippy::large_futures)]

//! Reproducible scale/profile harness for checkpoint compaction.
//!
//! The default workload models a 10,000-file workspace and 1,000 checkpoints.
//! Each interval changes 10 files across five auto-commits before checkpointing.
//! Setup and run are split so the same prepared fixture can be copied and used
//! for before/after comparisons:
//! GC is identified from the engine's post-collection tracing event, without
//! assuming a maintenance cadence or classifying by latency.
//!
//! cargo bench --manifest-path packages/e2e/Cargo.toml \
//!   --bench profile_checkpoint_scale --features storage-benches,slatedb -- \
//!   setup rocksdb /tmp/checkpoint-rocks-seed 10000
//! cp -a /tmp/checkpoint-rocks-seed /tmp/checkpoint-rocks-run
//! cargo bench --manifest-path packages/e2e/Cargo.toml \
//!   --bench profile_checkpoint_scale --features storage-benches,slatedb -- \
//!   run rocksdb /tmp/checkpoint-rocks-run 1000 10 5

use async_trait::async_trait;
use lix::storage::Storage;
use lix::storage_bench::{
    CheckpointForegroundAccounting, checkpoint_foreground_is_active, measure_checkpoint_foreground,
};
use lix::{ExecuteBatchStatement, Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::future::IntoFuture;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;

const DEFAULT_FILE_COUNT: usize = 10_000;
const DEFAULT_CHECKPOINT_COUNT: usize = 1_000;
const DEFAULT_FILES_PER_CHECKPOINT: usize = 10;
const DEFAULT_AUTO_COMMITS_PER_CHECKPOINT: usize = 5;
const SEED_BATCH_SIZE: usize = 100;
const FILE_BYTES: usize = 256;
// Collection is asynchronous by design, so give the final scheduled sweep a
// bounded opportunity to run before validating/reopening the fixture. This is
// outside the measured checkpoint latency window.
const BACKGROUND_GC_SETTLE_MILLIS: u64 = 5_000;

// These are architectural ceilings, not values derived from a particular
// fixture. Any scale dimension that leaks into publication should break an
// operation, payload, or allocation gate before timing noise can hide it.
const MAX_FOREGROUND_READ_VIEWS: u64 = 8;
const MAX_FOREGROUND_POINT_BATCHES: u64 = 80;
const MAX_FOREGROUND_POINT_KEYS: u64 = 80;
const MAX_FOREGROUND_SCAN_STARTS: u64 = 4;
const MAX_FOREGROUND_SCAN_PAGES: u64 = 4;
const MAX_FOREGROUND_SCAN_ROWS: u64 = 48;
const MAX_FOREGROUND_WRITE_TRANSACTIONS: u64 = 1;
const MAX_FOREGROUND_WRITE_CALLS: u64 = 20;
const MAX_FOREGROUND_WRITTEN_RECORDS: u64 = 32;
const MAX_FOREGROUND_WRITTEN_BYTES: u64 = 64 * 1024;
const MAX_FOREGROUND_ALLOCATIONS: u64 = 24_576;
const MAX_FOREGROUND_ALLOCATED_BYTES: u64 = 3 * 1024 * 1024;
const MAX_CHECKPOINT_P99_MILLIS: f64 = 25.0;
const MAX_CHECKPOINT_MILLIS: f64 = 100.0;
const MAX_INTERVAL_WRITE_P99_MILLIS: f64 = 100.0;
const MAX_INTERVAL_WRITE_MILLIS: f64 = 500.0;
const MAX_DEPTH_P95_RATIO: f64 = 2.0;
const MAX_DEPTH_P95_ADDITIVE_MILLIS: f64 = 2.0;

struct CountingAllocator;

thread_local! {
    static ALLOCATION_CALLS: Cell<u64> = const { Cell::new(0) };
    static ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
}

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if checkpoint_foreground_is_active() {
            ALLOCATION_CALLS.with(|calls| calls.set(calls.get() + 1));
            ALLOCATED_BYTES.with(|bytes| bytes.set(bytes.get() + layout.size() as u64));
        }
        // SAFETY: this allocator delegates the unchanged layout to `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` came from the delegated `System` allocation above.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if checkpoint_foreground_is_active() {
            ALLOCATION_CALLS.with(|calls| calls.set(calls.get() + 1));
            ALLOCATED_BYTES.with(|bytes| bytes.set(bytes.get() + new_size as u64));
        }
        // SAFETY: arguments are forwarded unchanged to `System`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AllocationAccounting {
    calls: u64,
    bytes: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct ForegroundAccounting {
    storage: CheckpointForegroundAccounting,
    allocations: AllocationAccounting,
}

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDb,
    SlateDb,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::RocksDb,
            "slatedb" => Self::SlateDb,
            _ => panic!("backend must be 'rocksdb' or 'slatedb', got '{value}'"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::SlateDb => "slatedb",
        }
    }
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let Some(mode) = args.get(1).map(String::as_str) else {
        print_usage();
        return;
    };
    let Some(backend) = args.get(2).map(|value| Backend::parse(value)) else {
        print_usage();
        return;
    };
    let Some(path) = args.get(3).map(PathBuf::from) else {
        print_usage();
        return;
    };
    let gc_observer = GcObserver::install();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("checkpoint profile runtime should build");

    match mode {
        "setup" => {
            let file_count = parse_usize(args.get(4), DEFAULT_FILE_COUNT, "file count");
            let branch_count = parse_usize(args.get(5), 0, "branch count");
            runtime.block_on(setup_backend(backend, &path, file_count, branch_count));
        }
        "run" => {
            let checkpoint_count =
                parse_usize(args.get(4), DEFAULT_CHECKPOINT_COUNT, "checkpoint count");
            let files_per_checkpoint = parse_usize(
                args.get(5),
                DEFAULT_FILES_PER_CHECKPOINT,
                "files per checkpoint",
            );
            let auto_commits_per_checkpoint = parse_usize(
                args.get(6),
                DEFAULT_AUTO_COMMITS_PER_CHECKPOINT,
                "auto commits per checkpoint",
            );
            runtime.block_on(run_backend(
                backend,
                &path,
                checkpoint_count,
                files_per_checkpoint,
                auto_commits_per_checkpoint,
                gc_observer,
            ));
        }
        "stats" => print_storage_report(backend, &path),
        "surfaces" => runtime.block_on(print_surface_backend(backend, &path)),
        _ => print_usage(),
    }
}

fn print_usage() {
    eprintln!(
        "usage:\n  profile_checkpoint_scale setup <rocksdb|slatedb> <storage-dir> [files] [branches]\n  \
         profile_checkpoint_scale run <rocksdb|slatedb> <storage-dir> \
         [checkpoints] [files-per-checkpoint] \
         [auto-commits-per-checkpoint]\n  \
         profile_checkpoint_scale stats <rocksdb|slatedb> <storage-dir>\n  \
         profile_checkpoint_scale surfaces <rocksdb|slatedb> <storage-dir>"
    );
}

fn parse_usize(value: Option<&String>, default: usize, label: &str) -> usize {
    value.map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|_| panic!("{label} must be a positive integer"))
    })
}

#[async_trait]
trait BenchmarkStorage: Storage + Clone + Send + Sync + 'static {
    const NAME: &'static str;

    fn open_for_benchmark(path: &Path) -> Self;
    async fn flush_for_benchmark(&self);
}

#[async_trait]
impl BenchmarkStorage for RocksDB {
    const NAME: &'static str = "rocksdb";

    fn open_for_benchmark(path: &Path) -> Self {
        Self::open(path).expect("open checkpoint benchmark RocksDB")
    }

    async fn flush_for_benchmark(&self) {
        self.flush().expect("flush checkpoint benchmark RocksDB");
    }
}

#[async_trait]
impl BenchmarkStorage for SlateDB {
    const NAME: &'static str = "slatedb";

    fn open_for_benchmark(path: &Path) -> Self {
        Self::open(path).expect("open checkpoint benchmark SlateDB")
    }

    async fn flush_for_benchmark(&self) {
        self.flush()
            .await
            .expect("flush checkpoint benchmark SlateDB");
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct GcObservation {
    swept_commits: u64,
    swept_changes: u64,
    swept_tracked_roots: u64,
    root_discovery_us: u64,
    changelog_us: u64,
    tracked_root_stage_us: u64,
    gc_total_us: u64,
}

#[derive(Clone, Default)]
struct GcObserver {
    events: Arc<Mutex<Vec<GcObservation>>>,
}

impl GcObserver {
    fn install() -> Self {
        let observer = Self::default();
        let subscriber = tracing_subscriber::registry().with(GcObservationLayer {
            observer: observer.clone(),
        });
        tracing::subscriber::set_global_default(subscriber)
            .expect("install checkpoint benchmark tracing observer");
        observer
    }

    fn clear(&self) {
        self.events
            .lock()
            .expect("lock checkpoint GC observations")
            .clear();
    }

    fn drain(&self) -> Vec<GcObservation> {
        std::mem::take(&mut *self.events.lock().expect("lock checkpoint GC observations"))
    }
}

struct GcObservationLayer {
    observer: GcObserver,
}

impl<S> Layer<S> for GcObservationLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut visitor = GcObservationVisitor::default();
        event.record(&mut visitor);
        if visitor.saw_swept_commits {
            self.observer
                .events
                .lock()
                .expect("lock checkpoint GC observations")
                .push(visitor.observation);
        }
    }
}

#[derive(Default)]
struct GcObservationVisitor {
    observation: GcObservation,
    saw_swept_commits: bool,
}

impl GcObservationVisitor {
    fn record_number(&mut self, field: &Field, value: u64) {
        match field.name() {
            "swept_commits" => {
                self.observation.swept_commits = value;
                self.saw_swept_commits = true;
            }
            "swept_changes" => self.observation.swept_changes = value,
            "swept_tracked_roots" => self.observation.swept_tracked_roots = value,
            "root_discovery_us" => self.observation.root_discovery_us = value,
            "changelog_us" => self.observation.changelog_us = value,
            "tracked_root_stage_us" => self.observation.tracked_root_stage_us = value,
            "gc_total_us" => self.observation.gc_total_us = value,
            _ => {}
        }
    }
}

impl Visit for GcObservationVisitor {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_number(field, value);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if let Ok(value) = u64::try_from(value) {
            self.record_number(field, value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if let Ok(value) = format!("{value:?}").parse::<u64>() {
            self.record_number(field, value);
        }
    }
}

async fn setup_backend(backend: Backend, path: &Path, file_count: usize, branch_count: usize) {
    assert!(
        !path.exists(),
        "refusing to overwrite existing fixture {}",
        path.display()
    );
    match backend {
        Backend::RocksDb => setup_fixture::<RocksDB>(path, file_count, branch_count).await,
        Backend::SlateDb => setup_fixture::<SlateDB>(path, file_count, branch_count).await,
    }
}

async fn setup_fixture<S>(path: &Path, file_count: usize, branch_count: usize)
where
    S: BenchmarkStorage,
{
    assert!(file_count > 0, "file count must be positive");
    let storage = S::open_for_benchmark(path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open checkpoint profile lix");

    let setup_start = Instant::now();
    for batch_start in (0..file_count).step_by(SEED_BATCH_SIZE) {
        let batch_end = (batch_start + SEED_BATCH_SIZE).min(file_count);
        insert_file_batch(&lix, batch_start, batch_end).await;
    }
    let starting_branch_count =
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_branch").await;
    for batch_start in (0..branch_count).step_by(SEED_BATCH_SIZE) {
        let batch_end = (batch_start + SEED_BATCH_SIZE).min(branch_count);
        insert_branch_batch(&lix, batch_start, batch_end).await;
    }
    assert_eq!(
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_branch").await,
        starting_branch_count + branch_count,
    );
    let seed_elapsed = setup_start.elapsed();
    let checkpoint_start = Instant::now();
    let initial_foreground = profile_checkpoint_phase(&lix).await;
    let initial_checkpoint_elapsed = checkpoint_start.elapsed();
    assert_eq!(
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_file").await,
        file_count
    );
    let checkpoint_commit_id = nearest_checkpoint_commit_id(&lix).await;
    assert_eq!(
        working_file_diff_count(&lix, &checkpoint_commit_id).await,
        0
    );
    lix.close().await.expect("close checkpoint setup lix");
    drop(lix);
    let flush_start = Instant::now();
    storage.flush_for_benchmark().await;
    let flush_elapsed = flush_start.elapsed();
    let after_flush = physical_stats(path);
    drop(storage);
    let after_close = physical_stats(path);
    println!(
        "setup backend={} files={file_count} branches={branch_count} seed_ms={:.3} \
         initial_checkpoint_ms={:.3} backend_flush_ms={:.3} \
         foreground={} storage_bytes_after_flush={} storage_files_after_flush={} \
         storage_bytes_after_close={} storage_files_after_close={}",
        S::NAME,
        millis(seed_elapsed),
        millis(initial_checkpoint_elapsed),
        millis(flush_elapsed),
        format_foreground(initial_foreground),
        after_flush.storage_bytes,
        after_flush.storage_files,
        after_close.storage_bytes,
        after_close.storage_files,
    );
}

async fn insert_branch_batch<S>(lix: &Lix<S>, start: usize, end: usize)
where
    S: BenchmarkStorage,
{
    let row_count = end - start;
    let mut sql = String::from("INSERT INTO lix_branch (id, name) VALUES ");
    let mut params = Vec::with_capacity(row_count * 2);
    for (offset, branch_index) in (start..end).enumerate() {
        if offset > 0 {
            sql.push(',');
        }
        let parameter = offset * 2;
        write!(sql, "(${}, ${})", parameter + 1, parameter + 2)
            .expect("write branch parameter placeholders");
        params.push(Value::Text(format!(
            "10000000-0000-7000-8000-{branch_index:012x}"
        )));
        params.push(Value::Text(format!("benchmark-branch-{branch_index:05}")));
    }
    lix.execute(&sql, &params)
        .await
        .expect("insert checkpoint benchmark branches");
}

async fn insert_file_batch<S>(lix: &Lix<S>, start: usize, end: usize)
where
    S: BenchmarkStorage,
{
    let row_count = end - start;
    let mut sql = String::from("INSERT INTO lix_file (id, path, content) VALUES ");
    let mut params = Vec::with_capacity(row_count * 3);
    for (offset, file_index) in (start..end).enumerate() {
        if offset > 0 {
            sql.push(',');
        }
        let parameter = offset * 3;
        write!(
            sql,
            "(${}, ${}, ${})",
            parameter + 1,
            parameter + 2,
            parameter + 3
        )
        .expect("write insert parameter placeholders");
        params.push(Value::Text(benchmark_file_id(file_index)));
        params.push(Value::Text(format!("/files/{file_index:05}.bin")));
        params.push(Value::Blob(payload(0, file_index, FILE_BYTES).into()));
    }
    lix.execute(&sql, &params)
        .await
        .expect("insert checkpoint benchmark files");
}

async fn run_backend(
    backend: Backend,
    path: &Path,
    checkpoint_count: usize,
    files_per_checkpoint: usize,
    auto_commits_per_checkpoint: usize,
    gc_observer: GcObserver,
) {
    match backend {
        Backend::RocksDb => {
            run_workload::<RocksDB>(
                path,
                checkpoint_count,
                files_per_checkpoint,
                auto_commits_per_checkpoint,
                gc_observer,
            )
            .await;
        }
        Backend::SlateDb => {
            run_workload::<SlateDB>(
                path,
                checkpoint_count,
                files_per_checkpoint,
                auto_commits_per_checkpoint,
                gc_observer,
            )
            .await;
        }
    }
}

async fn run_workload<S>(
    path: &Path,
    checkpoint_count: usize,
    files_per_checkpoint: usize,
    auto_commits_per_checkpoint: usize,
    gc_observer: GcObserver,
) where
    S: BenchmarkStorage,
{
    assert!(path.exists(), "fixture {} does not exist", path.display());
    assert!(checkpoint_count > 0, "checkpoint count must be positive");
    assert!(
        files_per_checkpoint > 0,
        "files per checkpoint must be positive"
    );
    assert!(
        auto_commits_per_checkpoint > 0
            && files_per_checkpoint.is_multiple_of(auto_commits_per_checkpoint),
        "files per checkpoint must divide evenly across auto commits"
    );

    let storage = S::open_for_benchmark(path);
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open checkpoint run lix");
    let file_count = scalar_count(&lix, "SELECT count(*) AS count FROM lix_file").await;
    assert!(file_count >= files_per_checkpoint);
    let initial_payloads = load_file_payloads(&lix).await;
    assert_eq!(
        initial_payloads.len(),
        file_count,
        "initial payload snapshot must contain every live file"
    );
    let starting_checkpoint_count =
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_checkpoint").await;
    let files_per_auto_commit = files_per_checkpoint / auto_commits_per_checkpoint;
    let mut write_latencies = Vec::with_capacity(checkpoint_count);
    let mut checkpoint_latencies = Vec::with_capacity(checkpoint_count);
    let mut contention_probe_latencies = Vec::with_capacity(checkpoint_count);
    let mut foreground_accounting = Vec::with_capacity(checkpoint_count * 2);
    let mut observed_background_gc_latencies = Vec::new();
    let mut backend_flush_latencies = Vec::new();
    gc_observer.clear();
    let mut peak_sampled_storage_bytes = physical_stats(path).storage_bytes;
    let total_start = Instant::now();

    println!(
        "milestone,backend,files,checkpoints,write_ms,checkpoint_ms,\
         backend_flush_ms,storage_bytes,storage_files"
    );
    for checkpoint_index in 1..=checkpoint_count {
        let write_start = Instant::now();
        for auto_commit_index in 0..auto_commits_per_checkpoint {
            update_file_group(
                &lix,
                file_count,
                checkpoint_index,
                auto_commit_index,
                files_per_auto_commit,
            )
            .await;
        }
        let write_elapsed = write_start.elapsed();
        let checkpoint_start = Instant::now();
        let foreground = profile_checkpoint_phase(&lix).await;
        let checkpoint_elapsed = checkpoint_start.elapsed();
        write_latencies.push(write_elapsed);
        checkpoint_latencies.push(checkpoint_elapsed);
        foreground_accounting.push(foreground);
        // Let maintenance scheduled by the just-published checkpoint start
        // outside its timer. The direct probe below measures real adapter and
        // commit-coordinator contention before any ordinary write can absorb
        // the wait.
        tokio::task::yield_now().await;
        // Start a checkpoint directly after maintenance receives a scheduling
        // opportunity. Ordinary interval writes must not absorb an exclusive
        // adapter-writer wait before the checkpoint timer starts.
        let contention_probe_start = Instant::now();
        let contention_foreground = profile_checkpoint_phase(&lix).await;
        contention_probe_latencies.push(contention_probe_start.elapsed());
        foreground_accounting.push(contention_foreground);
        for gc in gc_observer.drain() {
            observed_background_gc_latencies.push(Duration::from_micros(gc.gc_total_us));
            println!(
                "observed_background_gc backend={} observed_after_checkpoint_index={checkpoint_index} \
                visible_checkpoint_count={} gc_total_ms={:.3} \
                 swept_commits={} swept_changes={} swept_tracked_roots={} \
                 root_discovery_ms={:.3} changelog_ms={:.3} \
                 tracked_root_stage_ms={:.3} gc_total_ms={:.3}",
                S::NAME,
                starting_checkpoint_count + checkpoint_index * 2,
                micros_to_millis(gc.gc_total_us),
                gc.swept_commits,
                gc.swept_changes,
                gc.swept_tracked_roots,
                micros_to_millis(gc.root_discovery_us),
                micros_to_millis(gc.changelog_us),
                micros_to_millis(gc.tracked_root_stage_us),
                micros_to_millis(gc.gc_total_us),
            );
            println!(
                "foreground checkpoint_index={checkpoint_index} {}",
                format_foreground(foreground)
            );
        }
        if is_milestone(checkpoint_index, checkpoint_count) {
            let flush_start = Instant::now();
            storage.flush_for_benchmark().await;
            let flush_elapsed = flush_start.elapsed();
            backend_flush_latencies.push(flush_elapsed);
            let physical = physical_stats(path);
            peak_sampled_storage_bytes = peak_sampled_storage_bytes.max(physical.storage_bytes);
            println!(
                "{checkpoint_index},{},{file_count},{},{:.3},{:.3},{:.3},{},{}",
                S::NAME,
                starting_checkpoint_count + checkpoint_index * 2,
                millis(write_elapsed),
                millis(checkpoint_elapsed),
                millis(flush_elapsed),
                physical.storage_bytes,
                physical.storage_files,
            );
        }
    }
    let total_elapsed = total_start.elapsed();

    // A long-lived application keeps the runtime alive after publication. Do
    // the same here so a GC scheduled by the final checkpoint gets a chance
    // to complete; do not include this idle wait in checkpoint throughput.
    tokio::time::sleep(Duration::from_millis(BACKGROUND_GC_SETTLE_MILLIS)).await;
    for gc in gc_observer.drain() {
        observed_background_gc_latencies.push(Duration::from_micros(gc.gc_total_us));
        println!(
            "observed_background_gc backend={} observed_after_workload=true \
             gc_total_ms={:.3} swept_commits={} swept_changes={} \
             swept_tracked_roots={} \
             root_discovery_ms={:.3} changelog_ms={:.3} \
             tracked_root_stage_ms={:.3}",
            S::NAME,
            micros_to_millis(gc.gc_total_us),
            gc.swept_commits,
            gc.swept_changes,
            gc.swept_tracked_roots,
            micros_to_millis(gc.root_discovery_us),
            micros_to_millis(gc.changelog_us),
            micros_to_millis(gc.tracked_root_stage_us),
        );
    }

    let mut expected_payloads = initial_payloads;
    for checkpoint_index in 1..=checkpoint_count {
        for auto_commit_index in 0..auto_commits_per_checkpoint {
            for offset in 0..files_per_auto_commit {
                let file_index =
                    benchmark_file_index(file_count, checkpoint_index, auto_commit_index, offset);
                expected_payloads.insert(
                    benchmark_file_id(file_index),
                    payload(checkpoint_index, file_index, FILE_BYTES),
                );
            }
        }
    }
    assert_eq!(
        load_file_payloads(&lix).await,
        expected_payloads,
        "checkpoint run must preserve the deterministic final file contents"
    );
    let checkpoint_commit_id = nearest_checkpoint_commit_id(&lix).await;
    let working_diff_query_start = Instant::now();
    let remaining_working_diffs = working_file_diff_count(&lix, &checkpoint_commit_id).await;
    let working_diff_query_elapsed = working_diff_query_start.elapsed();
    assert_eq!(
        remaining_working_diffs, 0,
        "checkpoint run must leave no working diffs"
    );
    let checkpoint_history_query_start = Instant::now();
    let visible_checkpoint_count =
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_checkpoint").await;
    let checkpoint_history_query_elapsed = checkpoint_history_query_start.elapsed();
    assert_eq!(
        visible_checkpoint_count,
        starting_checkpoint_count + checkpoint_count * 2,
        "every requested checkpoint must remain visible"
    );
    let live_commits = scalar_count(&lix, "SELECT count(*) AS count FROM lix_commit").await;
    lix.close().await.expect("close checkpoint run lix");
    drop(lix);
    let after_flush = physical_stats(path);
    peak_sampled_storage_bytes = peak_sampled_storage_bytes.max(after_flush.storage_bytes);
    drop(storage);
    let after_close = physical_stats(path);
    peak_sampled_storage_bytes = peak_sampled_storage_bytes.max(after_close.storage_bytes);
    let reopen_start = Instant::now();
    let reopened_storage = S::open_for_benchmark(path);
    let reopened = open_lix()
        .with_storage(reopened_storage.clone())
        .await
        .expect("reopen checkpoint run lix");
    let reopened_checkpoint_count =
        scalar_count(&reopened, "SELECT count(*) AS count FROM lix_checkpoint").await;
    let reopen_and_history_elapsed = reopen_start.elapsed();
    assert_eq!(
        reopened_checkpoint_count, visible_checkpoint_count,
        "checkpoint history must survive reopen"
    );
    reopened
        .close()
        .await
        .expect("close reopened checkpoint run lix");
    drop(reopened);
    reopened_storage.flush_for_benchmark().await;
    drop(reopened_storage);

    print_latency_summary("interval_write", &write_latencies);
    print_latency_summary("create_checkpoint", &checkpoint_latencies);
    print_latency_summary(
        "create_checkpoint_after_gc_yield",
        &contention_probe_latencies,
    );
    if !observed_background_gc_latencies.is_empty() {
        print_latency_summary("background_gc", &observed_background_gc_latencies);
    }
    print_latency_summary("backend_flush_sample", &backend_flush_latencies);
    print_depth_bands(&checkpoint_latencies);
    print_foreground_summary(&foreground_accounting);
    assert_latency_bounds(&checkpoint_latencies);
    assert_latency_bounds(&contention_probe_latencies);
    assert_operation_latency_bounds(
        &write_latencies,
        MAX_INTERVAL_WRITE_P99_MILLIS,
        MAX_INTERVAL_WRITE_MILLIS,
        "interval write",
    );
    let first_window = checkpoint_latencies.len().min(100);
    let last_window_start = checkpoint_latencies.len().saturating_sub(100);
    println!(
        "summary backend={} files={file_count} checkpoints={checkpoint_count} \
         files_per_checkpoint={files_per_checkpoint} \
        auto_commits_per_checkpoint={auto_commits_per_checkpoint} \
         checkpoint_publications={} total_s={:.3} intervals_per_s={:.3} \
         live_commits={live_commits} \
         first_100_checkpoint_mean_ms={:.3} last_100_checkpoint_mean_ms={:.3} \
         storage_bytes_after_flush={} storage_files_after_flush={} \
         storage_bytes_after_close={} storage_files_after_close={} \
         peak_sampled_storage_bytes={}",
        S::NAME,
        checkpoint_count * 2,
        total_elapsed.as_secs_f64(),
        f64::from(u32::try_from(checkpoint_count).expect("checkpoint count should fit u32"))
            / total_elapsed.as_secs_f64(),
        mean_millis(&checkpoint_latencies[..first_window]),
        mean_millis(&checkpoint_latencies[last_window_start..]),
        after_flush.storage_bytes,
        after_flush.storage_files,
        after_close.storage_bytes,
        after_close.storage_files,
        peak_sampled_storage_bytes,
    );
    println!(
        "surface working_diff_ms={:.3} checkpoint_history_ms={:.3} \
         reopen_and_checkpoint_history_ms={:.3}",
        millis(working_diff_query_elapsed),
        millis(checkpoint_history_query_elapsed),
        millis(reopen_and_history_elapsed),
    );
}

async fn update_file_group<S>(
    lix: &Lix<S>,
    file_count: usize,
    checkpoint_index: usize,
    auto_commit_index: usize,
    files_per_auto_commit: usize,
) where
    S: BenchmarkStorage,
{
    let mut statements = Vec::with_capacity(files_per_auto_commit);
    for offset in 0..files_per_auto_commit {
        let file_index =
            benchmark_file_index(file_count, checkpoint_index, auto_commit_index, offset);
        statements.push(ExecuteBatchStatement {
            label: None,
            sql: "UPDATE lix_file SET content = $1 WHERE id = $2".to_string(),
            params: vec![
                Value::Blob(payload(checkpoint_index, file_index, FILE_BYTES).into()),
                Value::Text(benchmark_file_id(file_index)),
            ],
        });
    }

    lix.execute_batch(&statements)
        .await
        .expect("update checkpoint benchmark file group");
}

fn benchmark_file_index(
    file_count: usize,
    checkpoint_index: usize,
    auto_commit_index: usize,
    offset: usize,
) -> usize {
    (checkpoint_index * 97 + auto_commit_index * 17 + offset * 31) % file_count
}

fn benchmark_file_id(file_index: usize) -> String {
    format!("00000000-0000-0000-0000-{file_index:012x}")
}

#[inline(never)]
async fn profile_checkpoint_phase<S>(lix: &Lix<S>) -> ForegroundAccounting
where
    S: BenchmarkStorage,
{
    ALLOCATION_CALLS.with(|calls| calls.set(0));
    ALLOCATED_BYTES.with(|bytes| bytes.set(0));
    let checkpoint = IntoFuture::into_future(
        lix.execute("SELECT commit_id FROM lix_create_checkpoint()", &[]),
    );
    let (result, storage) = measure_checkpoint_foreground(checkpoint).await;
    result.expect("create benchmark checkpoint");
    let accounting = ForegroundAccounting {
        storage,
        allocations: AllocationAccounting {
            calls: ALLOCATION_CALLS.with(Cell::get),
            bytes: ALLOCATED_BYTES.with(Cell::get),
        },
    };
    assert_foreground_bounds(accounting);
    accounting
}

fn assert_foreground_bounds(accounting: ForegroundAccounting) {
    let storage = accounting.storage;
    assert!(
        storage.read_views <= MAX_FOREGROUND_READ_VIEWS,
        "{accounting:?}"
    );
    assert!(
        storage.point_batches <= MAX_FOREGROUND_POINT_BATCHES,
        "{accounting:?}"
    );
    assert!(
        storage.point_keys <= MAX_FOREGROUND_POINT_KEYS,
        "{accounting:?}"
    );
    assert!(
        storage.scan_starts <= MAX_FOREGROUND_SCAN_STARTS,
        "{accounting:?}"
    );
    assert!(
        storage.scan_pages <= MAX_FOREGROUND_SCAN_PAGES,
        "{accounting:?}"
    );
    assert!(
        storage.scan_rows <= MAX_FOREGROUND_SCAN_ROWS,
        "{accounting:?}"
    );
    assert!(
        storage.write_transactions <= MAX_FOREGROUND_WRITE_TRANSACTIONS,
        "{accounting:?}"
    );
    assert!(
        storage.write_calls <= MAX_FOREGROUND_WRITE_CALLS,
        "{accounting:?}"
    );
    assert!(
        storage.written_records <= MAX_FOREGROUND_WRITTEN_RECORDS,
        "{accounting:?}"
    );
    assert!(
        storage.written_bytes <= MAX_FOREGROUND_WRITTEN_BYTES,
        "{accounting:?}"
    );
    assert!(
        accounting.allocations.calls <= MAX_FOREGROUND_ALLOCATIONS,
        "{accounting:?}"
    );
    assert!(
        accounting.allocations.bytes <= MAX_FOREGROUND_ALLOCATED_BYTES,
        "{accounting:?}"
    );
}

fn format_foreground(accounting: ForegroundAccounting) -> String {
    let storage = accounting.storage;
    format!(
        "read_views={} point_batches={} point_keys={} scan_starts={} scan_pages={} \
         scan_rows={} write_transactions={} write_calls={} written_records={} \
         written_bytes={} allocations={} allocated_bytes={}",
        storage.read_views,
        storage.point_batches,
        storage.point_keys,
        storage.scan_starts,
        storage.scan_pages,
        storage.scan_rows,
        storage.write_transactions,
        storage.write_calls,
        storage.written_records,
        storage.written_bytes,
        accounting.allocations.calls,
        accounting.allocations.bytes,
    )
}

fn print_foreground_summary(samples: &[ForegroundAccounting]) {
    let max = |select: fn(&ForegroundAccounting) -> u64| {
        samples.iter().map(select).max().unwrap_or_default()
    };
    println!(
        "foreground_max read_views={} point_batches={} point_keys={} scan_starts={} \
         scan_pages={} scan_rows={} write_transactions={} write_calls={} \
         written_records={} written_bytes={} allocations={} allocated_bytes={}",
        max(|sample| sample.storage.read_views),
        max(|sample| sample.storage.point_batches),
        max(|sample| sample.storage.point_keys),
        max(|sample| sample.storage.scan_starts),
        max(|sample| sample.storage.scan_pages),
        max(|sample| sample.storage.scan_rows),
        max(|sample| sample.storage.write_transactions),
        max(|sample| sample.storage.write_calls),
        max(|sample| sample.storage.written_records),
        max(|sample| sample.storage.written_bytes),
        max(|sample| sample.allocations.calls),
        max(|sample| sample.allocations.bytes),
    );
}

async fn scalar_count<S>(lix: &Lix<S>, sql: &str) -> usize
where
    S: BenchmarkStorage,
{
    let result = lix.execute(sql, &[]).await.expect("execute count query");
    let count = result
        .rows()
        .first()
        .expect("count query should return one row")
        .get::<i64>("count")
        .expect("count should be an integer");
    usize::try_from(count).expect("count should be non-negative")
}

async fn nearest_checkpoint_commit_id<S>(lix: &Lix<S>) -> String
where
    S: BenchmarkStorage,
{
    lix.execute(
        "SELECT checkpoint.commit_id AS commit_id \
         FROM lix_checkpoint AS checkpoint \
         JOIN lix_commit_ancestry() AS ancestry \
           ON ancestry.commit_id = checkpoint.commit_id \
         ORDER BY ancestry.depth, checkpoint.commit_id \
         LIMIT 1",
        &[],
    )
    .await
    .expect("read the nearest checkpoint reachable from the active branch")
    .rows()
    .first()
    .expect("checkpoint benchmark requires a reachable checkpoint")
    .get::<String>("commit_id")
    .expect("reachable checkpoint commit ID should be text")
}

async fn working_file_diff_count<S>(lix: &Lix<S>, checkpoint_commit_id: &str) -> usize
where
    S: BenchmarkStorage,
{
    let result = lix
        .execute(
            "SELECT count(*) AS count \
             FROM lix_diff('lix_file', $1, lix_active_branch_commit_id())",
            &[Value::Text(checkpoint_commit_id.to_owned())],
        )
        .await
        .expect("execute checkpoint-to-head file diff count query");
    let count = result
        .rows()
        .first()
        .expect("file diff count query should return one row")
        .get::<i64>("count")
        .expect("file diff count should be an integer");
    usize::try_from(count).expect("file diff count should be non-negative")
}

async fn load_file_payloads<S>(lix: &Lix<S>) -> BTreeMap<String, Vec<u8>>
where
    S: BenchmarkStorage,
{
    lix.execute("SELECT id, content FROM lix_file ORDER BY id", &[])
        .await
        .expect("load checkpoint benchmark file payloads")
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id")
                    .expect("benchmark file id should be text"),
                row.get::<Vec<u8>>("content")
                    .expect("benchmark file data should be a blob"),
            )
        })
        .collect()
}

fn payload(checkpoint_index: usize, file_index: usize, bytes: usize) -> Vec<u8> {
    let prefix = format!("checkpoint={checkpoint_index:05};file={file_index:05};");
    let mut payload = Vec::with_capacity(bytes);
    while payload.len() < bytes {
        payload.extend_from_slice(prefix.as_bytes());
    }
    payload.truncate(bytes);
    payload
}

fn is_milestone(index: usize, total: usize) -> bool {
    index == 1
        || index == 10
        || index == 100
        || index == 250
        || index == 500
        || index == 750
        || index == 1_000
        || index == 2_000
        || index == 5_000
        || index == total
}

fn print_latency_summary(label: &str, latencies: &[Duration]) {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    println!(
        "latency phase={label} count={} mean_ms={:.3} p50_ms={:.3} p95_ms={:.3} \
         p99_ms={:.3} max_ms={:.3}",
        sorted.len(),
        mean_millis(&sorted),
        millis(percentile(&sorted, 50, 100)),
        millis(percentile(&sorted, 95, 100)),
        millis(percentile(&sorted, 99, 100)),
        millis(*sorted.last().expect("latencies must not be empty"))
    );
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    assert!(denominator > 0 && numerator <= denominator);
    let scaled = (sorted.len() - 1)
        .checked_mul(numerator)
        .expect("percentile index should fit usize");
    let index = (scaled + denominator / 2) / denominator;
    sorted[index]
}

fn print_depth_bands(latencies: &[Duration]) {
    for (band_index, band) in latencies.chunks(100).enumerate() {
        let mut sorted = band.to_vec();
        sorted.sort_unstable();
        println!(
            "depth_band start={} end={} mean_ms={:.3} p95_ms={:.3} max_ms={:.3}",
            band_index * 100 + 1,
            band_index * 100 + band.len(),
            mean_millis(band),
            millis(percentile(&sorted, 95, 100)),
            millis(*sorted.last().expect("depth band must not be empty")),
        );
    }
}

fn assert_latency_bounds(latencies: &[Duration]) {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let p99 = millis(percentile(&sorted, 99, 100));
    let max = millis(*sorted.last().expect("latencies must not be empty"));
    assert!(
        p99 <= MAX_CHECKPOINT_P99_MILLIS,
        "checkpoint p99 {p99:.3}ms exceeded {MAX_CHECKPOINT_P99_MILLIS:.3}ms"
    );
    assert!(
        max <= MAX_CHECKPOINT_MILLIS,
        "checkpoint max {max:.3}ms exceeded {MAX_CHECKPOINT_MILLIS:.3}ms"
    );
    if latencies.len() >= 200 {
        let mut first = latencies[..100].to_vec();
        let mut last = latencies[latencies.len() - 100..].to_vec();
        first.sort_unstable();
        last.sort_unstable();
        let first_p95 = millis(percentile(&first, 95, 100));
        let last_p95 = millis(percentile(&last, 95, 100));
        let allowed = first_p95 * MAX_DEPTH_P95_RATIO + MAX_DEPTH_P95_ADDITIVE_MILLIS;
        assert!(
            last_p95 <= allowed,
            "checkpoint depth p95 drifted from {first_p95:.3}ms to {last_p95:.3}ms; \
             allowed {allowed:.3}ms"
        );
    }
}

fn assert_operation_latency_bounds(
    latencies: &[Duration],
    max_p99_millis: f64,
    max_millis: f64,
    label: &str,
) {
    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let p99 = millis(percentile(&sorted, 99, 100));
    let max = millis(*sorted.last().expect("latencies must not be empty"));
    assert!(
        p99 <= max_p99_millis,
        "{label} p99 {p99:.3}ms exceeded {max_p99_millis:.3}ms"
    );
    assert!(
        max <= max_millis,
        "{label} max {max:.3}ms exceeded {max_millis:.3}ms"
    );
}

fn mean_millis(durations: &[Duration]) -> f64 {
    durations
        .iter()
        .map(|duration| millis(*duration))
        .sum::<f64>()
        / f64::from(u32::try_from(durations.len()).expect("duration count should fit u32"))
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn micros_to_millis(micros: u64) -> f64 {
    millis(Duration::from_micros(micros))
}

#[derive(Default)]
struct PhysicalStats {
    storage_bytes: u64,
    storage_files: u64,
    groups: BTreeMap<String, StorageGroup>,
}

#[derive(Clone, Copy, Default)]
struct StorageGroup {
    bytes: u64,
    files: u64,
}

fn physical_stats(path: &Path) -> PhysicalStats {
    let mut stats = PhysicalStats::default();
    collect_physical_stats(path, path, &mut stats);
    stats
}

fn collect_physical_stats(root: &Path, path: &Path, stats: &mut PhysicalStats) {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return;
    };
    if metadata.is_file() {
        let bytes = metadata.len();
        stats.storage_bytes = stats.storage_bytes.saturating_add(bytes);
        stats.storage_files = stats.storage_files.saturating_add(1);
        let relative = path.strip_prefix(root).unwrap_or(path);
        let group = storage_group(relative);
        let entry = stats.groups.entry(group).or_default();
        entry.bytes = entry.bytes.saturating_add(bytes);
        entry.files = entry.files.saturating_add(1);
        return;
    }
    if !metadata.is_dir() {
        return;
    }
    for entry in fs::read_dir(path).expect("read checkpoint benchmark storage directory") {
        collect_physical_stats(
            root,
            &entry
                .expect("read checkpoint benchmark directory entry")
                .path(),
            stats,
        );
    }
}

fn storage_group(relative: &Path) -> String {
    for component in relative.components() {
        let component = component.as_os_str().to_string_lossy();
        if matches!(
            component.as_ref(),
            "wal" | "compacted" | "manifest" | "compactions"
        ) {
            return component.into_owned();
        }
    }
    match relative
        .extension()
        .and_then(|extension| extension.to_str())
    {
        Some("sst") => "sst".to_string(),
        Some("blob") => "blob".to_string(),
        Some("log") => "log".to_string(),
        Some(extension) => format!("extension:{extension}"),
        None => "other".to_string(),
    }
}

fn print_storage_report(backend: Backend, path: &Path) {
    assert!(path.exists(), "fixture {} does not exist", path.display());
    let physical = physical_stats(path);
    println!(
        "storage backend={} storage_bytes={} storage_files={}",
        backend.as_str(),
        physical.storage_bytes,
        physical.storage_files,
    );
    for (group, values) in physical.groups {
        println!(
            "storage_group backend={} group={group} bytes={} files={}",
            backend.as_str(),
            values.bytes,
            values.files,
        );
    }
}

async fn print_surface_backend(backend: Backend, path: &Path) {
    match backend {
        Backend::RocksDb => print_surface_report::<RocksDB>(path).await,
        Backend::SlateDb => print_surface_report::<SlateDB>(path).await,
    }
}

async fn print_surface_report<S>(path: &Path)
where
    S: BenchmarkStorage,
{
    const SURFACE_REPETITIONS: usize = 7;

    assert!(path.exists(), "fixture {} does not exist", path.display());
    let storage = S::open_for_benchmark(path);
    let open_start = Instant::now();
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open checkpoint surface lix");
    let open_elapsed = open_start.elapsed();

    let checkpoint_commit_id = nearest_checkpoint_commit_id(&lix).await;
    let working_start = Instant::now();
    let working_count = working_file_diff_count(&lix, &checkpoint_commit_id).await;
    let working_elapsed = working_start.elapsed();
    let limited_sql = "SELECT commit_id FROM lix_checkpoint LIMIT 20";
    let medium_sql = "SELECT commit_id FROM lix_checkpoint LIMIT 128";
    let full_sql = "SELECT commit_id FROM lix_checkpoint";
    let count_sql = "SELECT count(*) AS count FROM lix_checkpoint";
    let limited_checkpoint_count = row_count(&lix, limited_sql).await;
    let medium_checkpoint_count = row_count(&lix, medium_sql).await;
    let checkpoint_count = row_count(&lix, full_sql).await;
    assert_eq!(
        scalar_count(&lix, count_sql).await,
        checkpoint_count,
        "checkpoint count and full history must agree"
    );
    let mut limited_history_latencies = Vec::with_capacity(SURFACE_REPETITIONS);
    let mut medium_history_latencies = Vec::with_capacity(SURFACE_REPETITIONS);
    let mut full_history_latencies = Vec::with_capacity(SURFACE_REPETITIONS);
    let mut count_history_latencies = Vec::with_capacity(SURFACE_REPETITIONS);
    for repetition in 0..SURFACE_REPETITIONS {
        if repetition.is_multiple_of(2) {
            limited_history_latencies.push(timed_row_count(&lix, limited_sql).await.1);
            medium_history_latencies.push(timed_row_count(&lix, medium_sql).await.1);
            full_history_latencies.push(timed_row_count(&lix, full_sql).await.1);
            count_history_latencies.push(timed_scalar_count(&lix, count_sql).await.1);
        } else {
            count_history_latencies.push(timed_scalar_count(&lix, count_sql).await.1);
            full_history_latencies.push(timed_row_count(&lix, full_sql).await.1);
            medium_history_latencies.push(timed_row_count(&lix, medium_sql).await.1);
            limited_history_latencies.push(timed_row_count(&lix, limited_sql).await.1);
        }
    }
    lix.close().await.expect("close checkpoint surface lix");
    drop(lix);
    storage.flush_for_benchmark().await;
    drop(storage);

    println!(
        "surface backend={} open_ms={:.3} working_diff_ms={:.3} working_diffs={} \
         checkpoint_history_limit_20_ms={:.3} limited_checkpoints={} \
         checkpoint_history_limit_128_ms={:.3} medium_checkpoints={} \
         checkpoint_history_full_ms={:.3} checkpoint_history_count_ms={:.3} \
         checkpoints={} repetitions={SURFACE_REPETITIONS}",
        S::NAME,
        millis(open_elapsed),
        millis(working_elapsed),
        working_count,
        median_millis(&limited_history_latencies),
        limited_checkpoint_count,
        median_millis(&medium_history_latencies),
        medium_checkpoint_count,
        median_millis(&full_history_latencies),
        median_millis(&count_history_latencies),
        checkpoint_count,
    );
}

async fn row_count<S>(lix: &Lix<S>, sql: &str) -> usize
where
    S: BenchmarkStorage,
{
    lix.execute(sql, &[])
        .await
        .expect("execute row-count query")
        .rows()
        .len()
}

async fn timed_row_count<S>(lix: &Lix<S>, sql: &str) -> (usize, Duration)
where
    S: BenchmarkStorage,
{
    let start = Instant::now();
    let count = row_count(lix, sql).await;
    (count, start.elapsed())
}

async fn timed_scalar_count<S>(lix: &Lix<S>, sql: &str) -> (usize, Duration)
where
    S: BenchmarkStorage,
{
    let start = Instant::now();
    let count = scalar_count(lix, sql).await;
    (count, start.elapsed())
}

fn median_millis(durations: &[Duration]) -> f64 {
    let mut sorted = durations.to_vec();
    sorted.sort_unstable();
    millis(sorted[sorted.len() / 2])
}
