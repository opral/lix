//! Reproducible scale/profile harness for checkpoint compaction.
//!
//! The default workload models a 10,000-file workspace and 1,000 checkpoints.
//! Each interval changes 10 files across five auto-commits before checkpointing.
//! Setup and run are split so the same prepared fixture can be copied and used
//! for before/after comparisons:
//! GC is identified from the engine's post-collection tracing event, without
//! assuming a maintenance cadence or classifying by latency.
//!
//! cargo bench -p lix_sdk --bench profile_checkpoint_scale \
//!   --features checkpoint_backends -- \
//!   setup rocksdb /tmp/checkpoint-rocks-seed 10000
//! cp -a /tmp/checkpoint-rocks-seed /tmp/checkpoint-rocks-run
//! cargo bench -p lix_sdk --bench profile_checkpoint_scale \
//!   --features checkpoint_backends -- \
//!   run rocksdb /tmp/checkpoint-rocks-run 1000 10 5

use async_trait::async_trait;
use lix_rocksdb_storage::RocksDB;
use lix_sdk::{ExecuteBatchStatement, Lix, OpenLixOptions, Storage, Value, open_lix};
use lix_slatedb_storage::SlateDB;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
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
            runtime.block_on(setup_backend(backend, &path, file_count));
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
        "usage:\n  profile_checkpoint_scale setup <rocksdb|slatedb> <storage-dir> [files]\n  \
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

async fn setup_backend(backend: Backend, path: &Path, file_count: usize) {
    assert!(
        !path.exists(),
        "refusing to overwrite existing fixture {}",
        path.display()
    );
    match backend {
        Backend::RocksDb => setup_fixture::<RocksDB>(path, file_count).await,
        Backend::SlateDb => setup_fixture::<SlateDB>(path, file_count).await,
    }
}

async fn setup_fixture<S>(path: &Path, file_count: usize)
where
    S: BenchmarkStorage,
{
    assert!(file_count > 0, "file count must be positive");
    let storage = S::open_for_benchmark(path);
    let lix = open_lix(OpenLixOptions::new(storage.clone()))
        .await
        .expect("open checkpoint profile lix");

    let setup_start = Instant::now();
    for batch_start in (0..file_count).step_by(SEED_BATCH_SIZE) {
        let batch_end = (batch_start + SEED_BATCH_SIZE).min(file_count);
        insert_file_batch(&lix, batch_start, batch_end).await;
    }
    let seed_elapsed = setup_start.elapsed();
    let checkpoint_start = Instant::now();
    lix.create_checkpoint()
        .await
        .expect("compact initial checkpoint");
    let initial_checkpoint_elapsed = checkpoint_start.elapsed();
    assert_eq!(
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_file").await,
        file_count
    );
    assert_eq!(
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_working_change").await,
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
        "setup backend={} files={file_count} seed_ms={:.3} \
         initial_checkpoint_ms={:.3} backend_flush_ms={:.3} \
         storage_bytes_after_flush={} storage_files_after_flush={} \
         storage_bytes_after_close={} storage_files_after_close={}",
        S::NAME,
        millis(seed_elapsed),
        millis(initial_checkpoint_elapsed),
        millis(flush_elapsed),
        after_flush.storage_bytes,
        after_flush.storage_files,
        after_close.storage_bytes,
        after_close.storage_files,
    );
}

async fn insert_file_batch<S>(lix: &Lix<S>, start: usize, end: usize)
where
    S: BenchmarkStorage,
{
    let row_count = end - start;
    let mut sql = String::from("INSERT INTO lix_file (id, path, data) VALUES ");
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
        params.push(Value::Text(format!("benchmark-file-{file_index:05}")));
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
    let lix = open_lix(OpenLixOptions::new(storage.clone()))
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
        profile_checkpoint_phase(&lix).await;
        let checkpoint_elapsed = checkpoint_start.elapsed();
        write_latencies.push(write_elapsed);
        checkpoint_latencies.push(checkpoint_elapsed);
        for gc in gc_observer.drain() {
            observed_background_gc_latencies.push(Duration::from_micros(gc.gc_total_us));
            println!(
                "observed_background_gc backend={} observed_after_checkpoint_index={checkpoint_index} \
                 visible_checkpoint_count={} gc_total_ms={:.3} \
                 swept_commits={} swept_changes={} swept_tracked_roots={} \
                 root_discovery_ms={:.3} changelog_ms={:.3} \
                 tracked_root_stage_ms={:.3} gc_total_ms={:.3}",
                S::NAME,
                starting_checkpoint_count + checkpoint_index,
                micros_to_millis(gc.gc_total_us),
                gc.swept_commits,
                gc.swept_changes,
                gc.swept_tracked_roots,
                micros_to_millis(gc.root_discovery_us),
                micros_to_millis(gc.changelog_us),
                micros_to_millis(gc.tracked_root_stage_us),
                micros_to_millis(gc.gc_total_us),
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
                starting_checkpoint_count + checkpoint_index,
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
                    format!("benchmark-file-{file_index:05}"),
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
    let working_change_query_start = Instant::now();
    let remaining_working_changes =
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_working_change").await;
    let working_change_query_elapsed = working_change_query_start.elapsed();
    assert_eq!(
        remaining_working_changes, 0,
        "checkpoint run must leave no working changes"
    );
    let checkpoint_history_query_start = Instant::now();
    let visible_checkpoint_count =
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_checkpoint").await;
    let checkpoint_history_query_elapsed = checkpoint_history_query_start.elapsed();
    assert_eq!(
        visible_checkpoint_count,
        starting_checkpoint_count + checkpoint_count,
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
    let reopened = open_lix(OpenLixOptions::new(reopened_storage.clone()))
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
    if !observed_background_gc_latencies.is_empty() {
        print_latency_summary("background_gc", &observed_background_gc_latencies);
    }
    print_latency_summary("backend_flush_sample", &backend_flush_latencies);
    print_depth_bands(&checkpoint_latencies);
    let first_window = checkpoint_latencies.len().min(100);
    let last_window_start = checkpoint_latencies.len().saturating_sub(100);
    println!(
        "summary backend={} files={file_count} checkpoints={checkpoint_count} \
         files_per_checkpoint={files_per_checkpoint} \
         auto_commits_per_checkpoint={auto_commits_per_checkpoint} \
         total_s={:.3} checkpoints_per_s={:.3} live_commits={live_commits} \
         first_100_checkpoint_mean_ms={:.3} last_100_checkpoint_mean_ms={:.3} \
         storage_bytes_after_flush={} storage_files_after_flush={} \
         storage_bytes_after_close={} storage_files_after_close={} \
         peak_sampled_storage_bytes={}",
        S::NAME,
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
        "surface working_change_ms={:.3} checkpoint_history_ms={:.3} \
         reopen_and_checkpoint_history_ms={:.3}",
        millis(working_change_query_elapsed),
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
            sql: "UPDATE lix_file SET data = $1 WHERE id = $2".to_string(),
            params: vec![
                Value::Blob(payload(checkpoint_index, file_index, FILE_BYTES).into()),
                Value::Text(format!("benchmark-file-{file_index:05}")),
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

#[inline(never)]
async fn profile_checkpoint_phase<S>(lix: &Lix<S>)
where
    S: BenchmarkStorage,
{
    lix.create_checkpoint()
        .await
        .expect("create benchmark checkpoint");
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

async fn load_file_payloads<S>(lix: &Lix<S>) -> BTreeMap<String, Vec<u8>>
where
    S: BenchmarkStorage,
{
    lix.execute("SELECT id, data FROM lix_file ORDER BY id", &[])
        .await
        .expect("load checkpoint benchmark file payloads")
        .rows()
        .iter()
        .map(|row| {
            (
                row.get::<String>("id")
                    .expect("benchmark file id should be text"),
                row.get::<Vec<u8>>("data")
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
    let lix = open_lix(OpenLixOptions::new(storage.clone()))
        .await
        .expect("open checkpoint surface lix");
    let open_elapsed = open_start.elapsed();

    let working_start = Instant::now();
    let working_count =
        scalar_count(&lix, "SELECT count(*) AS count FROM lix_working_change").await;
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
        "surface backend={} open_ms={:.3} working_change_ms={:.3} working_changes={} \
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
