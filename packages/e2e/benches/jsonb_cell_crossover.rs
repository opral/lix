//! Declared-JSONB-cell physical encoding crossover.
//!
//! This benchmark deliberately keeps ordinary row columns native and varies
//! only the payload of the declared `jsonb` column. Each invocation owns one
//! fresh backend path, excludes setup from sampled phases, settles storage,
//! drops every handle, and cold-reopens before its final digest check.

#![allow(clippy::large_futures)]

use std::future::Future;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lix::storage::{ReadOptions, Storage};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{layout_accounting, take_crud_physical_write_accounting};
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const INSERT_BATCH: usize = 5_000;

#[derive(Clone, Copy, Debug)]
enum Shape {
    Absent,
    Sparse,
    Dense,
}

impl Shape {
    fn parse(value: &str) -> Self {
        match value {
            "absent" => Self::Absent,
            "sparse" => Self::Sparse,
            "dense" => Self::Dense,
            other => panic!("unknown shape {other:?}; expected absent, sparse, or dense"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        }
    }

    fn initial_payload(self, ordinal: usize) -> Option<String> {
        match self {
            Self::Absent => None,
            Self::Sparse if !ordinal.is_multiple_of(10) => None,
            Self::Sparse | Self::Dense => Some(payload(ordinal, 0)),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Distribution {
    p50: Duration,
    p95: Duration,
}

#[derive(Clone, Copy, Default)]
struct ProcessSample {
    wall: Duration,
    cpu: Duration,
    peak_rss_bytes: u64,
}

#[derive(Clone, Copy, Default)]
struct Phase {
    wall: Distribution,
    cpu: Distribution,
    peak_rss_bytes: u64,
}

struct HotResult {
    update: Phase,
    exact: Phase,
    scan: Phase,
    digest: String,
    logical_bytes: u64,
    logical_rows: u64,
}

fn main() {
    let backend = env("LIX_JSONB_CELL_BACKEND", "rocksdb");
    let path = PathBuf::from(env("LIX_JSONB_CELL_PATH", "/tmp/lix-jsonb-cell-crossover"));
    let rows = env_usize("LIX_JSONB_CELL_ROWS", 1_000);
    let changes = parse_changes(rows, &env("LIX_JSONB_CELL_CHANGES", "1"));
    let shape = Shape::parse(&env("LIX_JSONB_CELL_SHAPE", "dense"));
    let warmups = env_usize("LIX_JSONB_CELL_WARMUPS", 3);
    let samples = env_usize("LIX_JSONB_CELL_SAMPLES", 11).max(1);
    assert!(rows > 0 && changes > 0 && changes <= rows);
    assert!(!path.exists(), "benchmark path must be fresh: {}", path.display());

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create JSONB crossover runtime");
    runtime.block_on(async {
        match backend.as_str() {
            "rocksdb" => run_rocks(&path, rows, changes, shape, warmups, samples).await,
            "slatedb" => run_slate(&path, rows, changes, shape, warmups, samples).await,
            other => panic!("unknown backend {other:?}; expected rocksdb or slatedb"),
        }
    });
}

async fn run_rocks(
    path: &Path,
    rows: usize,
    changes: usize,
    shape: Shape,
    warmups: usize,
    samples: usize,
) {
    let storage = RocksDB::open(path).expect("open JSONB crossover RocksDB");
    let hot = run_hot(storage.clone(), rows, changes, shape, warmups, samples).await;
    storage.flush().expect("flush JSONB crossover RocksDB");
    let settled_bytes = directory_bytes(path);
    drop(storage);

    let cold_started = Instant::now();
    let reopened = RocksDB::open(path).expect("cold reopen JSONB crossover RocksDB");
    let cold_digest = query_digest(reopened.clone(), rows).await;
    let cold = cold_started.elapsed();
    assert_eq!(cold_digest, hot.digest, "RocksDB cold-reopen digest");
    print_result(
        "rocksdb",
        rows,
        changes,
        shape,
        warmups,
        samples,
        hot,
        cold,
        settled_bytes,
        SlateDBIoSnapshot::default(),
    );
}

async fn run_slate(
    path: &Path,
    rows: usize,
    changes: usize,
    shape: Shape,
    warmups: usize,
    samples: usize,
) {
    let counters = SlateDBIoCounters::default();
    let storage = SlateDB::open_with_io_counters(path, counters.clone())
        .expect("open JSONB crossover SlateDB");
    let io_before = counters.snapshot();
    let hot = run_hot(storage.clone(), rows, changes, shape, warmups, samples).await;
    storage
        .flush_memtable_for_diagnostics()
        .await
        .expect("flush JSONB crossover SlateDB");
    let settled_bytes = directory_bytes(path);
    drop(storage);

    let cold_started = Instant::now();
    let reopened = SlateDB::open_with_io_counters(path, counters.clone())
        .expect("cold reopen JSONB crossover SlateDB");
    let cold_digest = query_digest(reopened.clone(), rows).await;
    let cold = cold_started.elapsed();
    assert_eq!(cold_digest, hot.digest, "SlateDB cold-reopen digest");
    let io = counters.snapshot().saturating_sub(io_before);
    print_result(
        "slatedb",
        rows,
        changes,
        shape,
        warmups,
        samples,
        hot,
        cold,
        settled_bytes,
        io,
    );
}

async fn run_hot<S>(
    storage: S,
    rows: usize,
    changes: usize,
    shape: Shape,
    warmups: usize,
    samples: usize,
) -> HotResult
where
    S: Storage + Clone + Send + Sync + 'static,
{
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize JSONB crossover repository");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open JSONB crossover repository");
    let session = lix
        .open_another_session()
        .await
        .expect("open JSONB crossover session");
    register_schema(&session).await;
    seed(&session, rows, shape).await;

    for warmup in 0..warmups {
        update(&session, rows, changes, 10_000 + warmup).await;
        black_box(exact_read(&session, rows / 2).await);
        black_box(scan_digest(&session, rows).await);
    }

    let mut update_samples = Vec::with_capacity(samples);
    let mut exact_samples = Vec::with_capacity(samples);
    let mut scan_samples = Vec::with_capacity(samples);
    for sample in 0..samples {
        update_samples.push(
            measure(update(&session, rows, changes, 20_000 + sample)).await,
        );
        exact_samples.push(measure(exact_read(&session, rows / 2)).await);
        scan_samples.push(measure(scan_digest(&session, rows)).await);
    }
    let digest = scan_digest(&session, rows).await;
    drop(session);
    drop(lix);

    let adapter = StorageAdapter::new(storage);
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .expect("open JSONB crossover layout read");
    let layout = layout_accounting(&read).await;
    let logical_rows = layout.iter().map(|entry| entry.rows).sum();
    let logical_bytes = layout
        .iter()
        .map(|entry| entry.key_bytes + entry.value_bytes)
        .sum();
    HotResult {
        update: summarize(update_samples),
        exact: summarize(exact_samples),
        scan: summarize(scan_samples),
        digest,
        logical_bytes,
        logical_rows,
    }
}

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "jsonb_cell_crossover",
        "columns": [
            { "name": "id", "type": "text", "nullable": false },
            { "name": "ordinal", "type": "int8", "nullable": false },
            { "name": "active", "type": "boolean", "nullable": false },
            { "name": "payload", "type": "jsonb", "nullable": true }
        ],
        "primary_key": ["id"]
    });
    let result = session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register JSONB crossover schema");
    assert_eq!(result.rows_affected(), 1);
}

async fn seed<S>(session: &Lix<S>, rows: usize, shape: Shape)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    for start in (0..rows).step_by(INSERT_BATCH) {
        let end = (start + INSERT_BATCH).min(rows);
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin JSONB crossover seed");
        for ordinal in start..end {
            let id = row_id(ordinal);
            if let Some(payload) = shape.initial_payload(ordinal) {
                transaction
                    .execute(
                        "INSERT INTO jsonb_cell_crossover (id, ordinal, active, payload) VALUES ($1, $2, $3, CAST($4 AS JSONB))",
                        &[
                            Value::Text(id),
                            Value::Integer(ordinal as i64),
                            Value::Boolean(ordinal.is_multiple_of(2)),
                            Value::Text(payload),
                        ],
                    )
                    .await
                    .expect("stage JSONB crossover seed row");
            } else {
                transaction
                    .execute(
                        "INSERT INTO jsonb_cell_crossover (id, ordinal, active) VALUES ($1, $2, $3)",
                        &[
                            Value::Text(id),
                            Value::Integer(ordinal as i64),
                            Value::Boolean(ordinal.is_multiple_of(2)),
                        ],
                    )
                    .await
                    .expect("stage absent JSONB crossover seed row");
            }
        }
        transaction
            .commit()
            .await
            .expect("commit JSONB crossover seed batch");
    }
}

async fn update<S>(session: &Lix<S>, rows: usize, changes: usize, generation: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let _ = take_crud_physical_write_accounting();
    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin JSONB crossover update");
    for offset in 0..changes {
        let ordinal = spread_ordinal(offset, changes, rows);
        transaction
            .execute(
                "UPDATE jsonb_cell_crossover SET payload = CAST($1 AS JSONB) WHERE id = $2",
                &[
                    Value::Text(payload(ordinal, generation)),
                    Value::Text(row_id(ordinal)),
                ],
            )
            .await
            .expect("stage JSONB crossover update");
    }
    transaction
        .commit()
        .await
        .expect("commit JSONB crossover update");
}

async fn exact_read<S>(session: &Lix<S>, ordinal: usize) -> usize
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT payload FROM jsonb_cell_crossover WHERE id = $1",
            &[Value::Text(row_id(ordinal))],
        )
        .await
        .expect("exact JSONB crossover read");
    black_box(result.rows().len())
}

async fn scan_digest<S>(session: &Lix<S>, rows: usize) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let result = session
        .execute(
            "SELECT id, ordinal, active, payload FROM jsonb_cell_crossover ORDER BY id",
            &[],
        )
        .await
        .expect("scan JSONB crossover rows");
    assert_eq!(result.rows().len(), rows);
    let mut digest = blake3::Hasher::new();
    for row in result.rows() {
        digest.update(format!("{row:?}\n").as_bytes());
    }
    digest.finalize().to_hex().to_string()
}

async fn query_digest<S>(storage: S, rows: usize) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("cold-open JSONB crossover repository");
    let session = lix
        .open_another_session()
        .await
        .expect("cold-open JSONB crossover session");
    scan_digest(&session, rows).await
}

async fn measure<F, T>(future: F) -> ProcessSample
where
    F: Future<Output = T>,
{
    let running = Arc::new(AtomicBool::new(true));
    let peak = Arc::new(AtomicU64::new(current_rss_bytes()));
    let sampler_running = running.clone();
    let sampler_peak = peak.clone();
    let sampler = std::thread::spawn(move || {
        while sampler_running.load(Ordering::Relaxed) {
            sampler_peak.fetch_max(current_rss_bytes(), Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(1));
        }
        sampler_peak.fetch_max(current_rss_bytes(), Ordering::Relaxed);
    });
    let cpu_before = process_cpu();
    let started = Instant::now();
    black_box(future.await);
    let wall = started.elapsed();
    let cpu = process_cpu().saturating_sub(cpu_before);
    running.store(false, Ordering::Relaxed);
    sampler.join().expect("join RSS sampler");
    ProcessSample {
        wall,
        cpu,
        peak_rss_bytes: peak.load(Ordering::Relaxed),
    }
}

fn summarize(samples: Vec<ProcessSample>) -> Phase {
    let mut wall: Vec<_> = samples.iter().map(|sample| sample.wall).collect();
    let mut cpu: Vec<_> = samples.iter().map(|sample| sample.cpu).collect();
    wall.sort_unstable();
    cpu.sort_unstable();
    Phase {
        wall: Distribution {
            p50: percentile(&wall, 50),
            p95: percentile(&wall, 95),
        },
        cpu: Distribution {
            p50: percentile(&cpu, 50),
            p95: percentile(&cpu, 95),
        },
        peak_rss_bytes: samples
            .iter()
            .map(|sample| sample.peak_rss_bytes)
            .max()
            .unwrap_or(0),
    }
}

fn percentile(sorted: &[Duration], percent: usize) -> Duration {
    sorted[sorted.len().saturating_mul(percent).div_ceil(100) - 1]
}

#[allow(clippy::too_many_arguments)]
fn print_result(
    backend: &str,
    rows: usize,
    changes: usize,
    shape: Shape,
    warmups: usize,
    samples: usize,
    hot: HotResult,
    cold: Duration,
    settled_bytes: u64,
    io: SlateDBIoSnapshot,
) {
    println!(
        "jsonb_cell_crossover,backend={backend},encoding={},shape={},rows={rows},changes={changes},warmups={warmups},samples={samples},update_p50_us={:.3},update_p95_us={:.3},update_cpu_p50_us={:.3},update_cpu_p95_us={:.3},exact_p50_us={:.3},exact_p95_us={:.3},exact_cpu_p50_us={:.3},exact_cpu_p95_us={:.3},scan_p50_us={:.3},scan_p95_us={:.3},scan_cpu_p50_us={:.3},scan_cpu_p95_us={:.3},peak_rss_bytes={},cold_reopen_us={:.3},logical_rows={},logical_bytes={},settled_bytes={settled_bytes},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},digest={},verified=true",
        option_env!("LIX_JSONB_CELL_ENCODING").unwrap_or("canonical-text-v1"),
        shape.label(),
        micros(hot.update.wall.p50),
        micros(hot.update.wall.p95),
        micros(hot.update.cpu.p50),
        micros(hot.update.cpu.p95),
        micros(hot.exact.wall.p50),
        micros(hot.exact.wall.p95),
        micros(hot.exact.cpu.p50),
        micros(hot.exact.cpu.p95),
        micros(hot.scan.wall.p50),
        micros(hot.scan.wall.p95),
        micros(hot.scan.cpu.p50),
        micros(hot.scan.cpu.p95),
        hot.update
            .peak_rss_bytes
            .max(hot.exact.peak_rss_bytes)
            .max(hot.scan.peak_rss_bytes),
        micros(cold),
        hot.logical_rows,
        hot.logical_bytes,
        io.read_objects,
        io.read_bytes,
        io.write_objects,
        io.write_bytes,
        hot.digest,
    );
}

fn payload(ordinal: usize, generation: usize) -> String {
    match ordinal % 7 {
        0 => "null".to_owned(),
        1 => ((ordinal + generation) as i64).to_string(),
        2 => serde_json::to_string(&format!("value-{ordinal}-{generation}"))
            .expect("render JSON string"),
        3 => format!("[null,true,{ordinal},{{\"generation\":{generation}}}]") ,
        4 => format!(
            "{{\"a\":{ordinal},\"nested\":{{\"active\":true,\"generation\":{generation}}}}}"
        ),
        5 => format!(
            "{{\"array\":[0,1,2,3,4,5,6,7],\"ordinal\":{ordinal},\"generation\":{generation}}}"
        ),
        _ => ((ordinal + generation).is_multiple_of(2)).to_string(),
    }
}

fn spread_ordinal(offset: usize, changes: usize, rows: usize) -> usize {
    if changes == 1 {
        rows / 2
    } else {
        offset.saturating_mul(rows - 1) / (changes - 1)
    }
}

fn row_id(ordinal: usize) -> String {
    format!("row-{ordinal:08}")
}

fn parse_changes(rows: usize, value: &str) -> usize {
    value.strip_suffix('%').map_or_else(
        || value.parse().expect("changes must be an integer or percent"),
        |percent| {
            let percent: usize = percent.parse().expect("percent changes must be an integer");
            rows.saturating_mul(percent).div_ceil(100).max(1)
        },
    )
}

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000_000.0
}

fn process_cpu() -> Duration {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(status, 0, "getrusage failed");
    let usage = unsafe { usage.assume_init() };
    timeval_duration(usage.ru_utime) + timeval_duration(usage.ru_stime)
}

fn timeval_duration(value: libc::timeval) -> Duration {
    Duration::from_secs(value.tv_sec as u64)
        + Duration::from_micros(value.tv_usec.try_into().expect("nonnegative timeval"))
}

fn current_rss_bytes() -> u64 {
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
        .saturating_mul(1024)
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.metadata() {
            Ok(metadata) if metadata.is_dir() => directory_bytes(&entry.path()),
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        })
        .sum()
}
