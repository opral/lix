#![allow(clippy::large_futures)]
#![recursion_limit = "256"]

use std::fmt::{self, Display, Formatter};
use std::hint::black_box;
use std::path::Path;
use std::time::{Duration, Instant};

use lix::Value;
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::tracked_state::bench::seed_packed_history;
use lix::{Lix, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const DEFAULT_HISTORY: &[usize] = &[100_000, 1_000_000, 10_000_000];
const DEFAULT_WIDTHS: &[usize] = &[1, 10, 100, 10_000];
const DEFAULT_LIVE_ROWS: usize = 100;
const DEFAULT_WARMUPS: usize = 3;
const DEFAULT_SAMPLES: usize = 31;

#[derive(Clone, Copy)]
enum Backend {
    RocksDB,
    SlateDB,
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::RocksDB => formatter.write_str("rocksdb"),
            Self::SlateDB => formatter.write_str("slatedb"),
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    ExactRead,
    EnumerateLive,
    UpdateOne,
    InsertOne,
    DeleteOne,
}

impl Display for Operation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExactRead => formatter.write_str("exact_read"),
            Self::EnumerateLive => formatter.write_str("enumerate_live"),
            Self::UpdateOne => formatter.write_str("update_one"),
            Self::InsertOne => formatter.write_str("insert_one"),
            Self::DeleteOne => formatter.write_str("delete_one"),
        }
    }
}

struct Fixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session: Lix<S>,
    live_rows: usize,
    sequence: usize,
    delete_paths: Vec<String>,
}

fn main() {
    let threads = env_usize("LIX_FIXED_LIVE_RUNTIME_THREADS", 1);
    let mut runtime = if threads == 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(threads);
        builder
    };
    runtime
        .enable_all()
        .build()
        .expect("create fixed-live benchmark runtime")
        .block_on(run());
}

async fn run() {
    let histories = env_usizes("LIX_FIXED_LIVE_HISTORY", DEFAULT_HISTORY);
    let widths = env_usizes("LIX_FIXED_LIVE_WIDTHS", DEFAULT_WIDTHS);
    let live_rows = env_usize("LIX_FIXED_LIVE_ROWS", DEFAULT_LIVE_ROWS);
    let warmups = env_usize("LIX_FIXED_LIVE_WARMUPS", DEFAULT_WARMUPS);
    let samples = env_usize("LIX_FIXED_LIVE_SAMPLES", DEFAULT_SAMPLES);
    let settle = Duration::from_millis(env_nonnegative_usize("LIX_FIXED_LIVE_SETTLE_MS", 0) as u64);

    for backend in [Backend::RocksDB, Backend::SlateDB] {
        if !selected("LIX_FIXED_LIVE_BACKENDS", &backend.to_string()) {
            continue;
        }
        for &history in &histories {
            for &width in &widths {
                if !history.is_multiple_of(width) {
                    continue;
                }
                match backend {
                    Backend::RocksDB => {
                        let directory = tempfile::tempdir().expect("create RocksDB directory");
                        let storage = RocksDB::open(directory.path()).expect("open RocksDB");
                        let (mut fixture, seed) =
                            Fixture::prepare(storage.clone(), live_rows, history, width).await;
                        storage.flush().expect("flush RocksDB");
                        run_case(
                            backend,
                            history,
                            width,
                            warmups,
                            samples,
                            seed,
                            SlateDBIoSnapshot::default(),
                            SlateDBIoSnapshot::default(),
                            directory.path(),
                            None,
                            &mut fixture,
                        )
                        .await;
                    }
                    Backend::SlateDB => {
                        let directory = tempfile::tempdir().expect("create SlateDB directory");
                        let io_counters = SlateDBIoCounters::default();
                        let storage =
                            SlateDB::open_with_io_counters(directory.path(), io_counters.clone())
                                .expect("open SlateDB");
                        let seed_io_before = io_counters.snapshot();
                        let (mut fixture, seed) =
                            Fixture::prepare(storage.clone(), live_rows, history, width).await;
                        let seed_io_after = io_counters.snapshot();
                        if env_bool("LIX_FIXED_LIVE_MEMTABLE_FLUSH", false) {
                            storage
                                .flush_memtable_for_diagnostics()
                                .await
                                .expect("flush SlateDB memtable");
                        } else {
                            storage.flush().await.expect("flush SlateDB WAL");
                        }
                        tokio::time::sleep(settle).await;
                        let lifecycle_io_after = io_counters.snapshot();
                        run_case(
                            backend,
                            history,
                            width,
                            warmups,
                            samples,
                            seed,
                            seed_io_after.saturating_sub(seed_io_before),
                            lifecycle_io_after.saturating_sub(seed_io_after),
                            directory.path(),
                            Some(&io_counters),
                            &mut fixture,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct SeedStats {
    elapsed: Duration,
    staged_puts: u64,
    written_bytes: u64,
}

impl<S> Fixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn prepare(
        storage: S,
        live_rows: usize,
        history: usize,
        width: usize,
    ) -> (Self, SeedStats) {
        open_lix()
            .with_storage(storage.clone())
            .await
            .expect("initialize repository");
        let lix = open_lix()
            .with_storage(storage.clone())
            .await
            .expect("open lix");
        let session = lix.open_another_session().await.expect("open workspace");
        register_schema(&session).await;
        seed_live_rows(&session, live_rows).await;

        let started = Instant::now();
        let accounting =
            seed_packed_history(&StorageAdapter::new(storage), history, width, 100_000).await;
        let seed = SeedStats {
            elapsed: started.elapsed(),
            staged_puts: accounting.staged_puts,
            written_bytes: accounting.written_bytes,
        };
        (
            Self {
                session,
                live_rows,
                sequence: 0,
                delete_paths: Vec::new(),
            },
            seed,
        )
    }

    async fn execute(&mut self, operation: Operation) {
        self.sequence += 1;
        match operation {
            Operation::ExactRead => {
                let result = self
                    .session
                    .execute(
                        "SELECT path, value FROM fixed_live WHERE path = $1",
                        &[Value::Text(live_path(self.live_rows / 2))],
                    )
                    .await
                    .expect("exact read");
                assert_eq!(black_box(result.rows().len()), 1);
            }
            Operation::EnumerateLive => {
                let result = self
                    .session
                    .execute("SELECT path, value FROM fixed_live ORDER BY path", &[])
                    .await
                    .expect("enumerate live rows");
                assert_eq!(black_box(result.rows().len()), self.live_rows);
            }
            Operation::UpdateOne => {
                let result = self
                    .session
                    .execute(
                        "UPDATE fixed_live SET value = CAST($1 AS JSONB) WHERE path = $2",
                        &[
                            Value::Text(format!(r#"{{"update":{}}}"#, self.sequence)),
                            Value::Text(live_path(self.live_rows / 2)),
                        ],
                    )
                    .await
                    .expect("update one");
                assert_eq!(black_box(result.rows_affected()), 1);
            }
            Operation::InsertOne => {
                let path = format!("/insert/{:08}", self.sequence);
                let result = self
                    .session
                    .execute(
                        "INSERT INTO fixed_live (path, value) VALUES ($1, CAST($2 AS JSONB))",
                        &[
                            Value::Text(path),
                            Value::Text(format!(r#"{{"insert":{}}}"#, self.sequence)),
                        ],
                    )
                    .await
                    .expect("insert one");
                assert_eq!(black_box(result.rows_affected()), 1);
            }
            Operation::DeleteOne => {
                let path = self.delete_paths.pop().expect("prepared delete row");
                let result = self
                    .session
                    .execute(
                        "DELETE FROM fixed_live WHERE path = $1",
                        &[Value::Text(path)],
                    )
                    .await
                    .expect("delete one");
                assert_eq!(black_box(result.rows_affected()), 1);
            }
        }
    }

    async fn prepare_deletes(&mut self, count: usize) {
        for index in 0..count {
            let path = format!("/delete/{index:08}");
            self.session
                .execute(
                    "INSERT INTO fixed_live (path, value) VALUES ($1, CAST('null' AS JSONB))",
                    &[Value::Text(path.clone())],
                )
                .await
                .expect("prepare delete row");
            self.delete_paths.push(path);
        }
    }
}

async fn run_case<S>(
    backend: Backend,
    history: usize,
    width: usize,
    warmups: usize,
    samples: usize,
    seed: SeedStats,
    seed_io: SlateDBIoSnapshot,
    lifecycle_io: SlateDBIoSnapshot,
    storage_path: &Path,
    io_counters: Option<&SlateDBIoCounters>,
    fixture: &mut Fixture<S>,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    for operation in [
        Operation::ExactRead,
        Operation::EnumerateLive,
        Operation::UpdateOne,
        Operation::InsertOne,
        Operation::DeleteOne,
    ] {
        if !selected("LIX_FIXED_LIVE_OPERATIONS", &operation.to_string()) {
            continue;
        }
        if matches!(operation, Operation::DeleteOne) {
            fixture.prepare_deletes(warmups + samples + 1).await;
        }
        let io_before = io_counters.map_or_else(SlateDBIoSnapshot::default, |c| c.snapshot());
        let cold_started = Instant::now();
        fixture.execute(operation).await;
        let cold = cold_started.elapsed();
        for _ in 0..warmups {
            fixture.execute(operation).await;
        }
        let mut timings = Vec::with_capacity(samples);
        for _ in 0..samples {
            let started = Instant::now();
            fixture.execute(operation).await;
            timings.push(started.elapsed());
        }
        timings.sort_unstable();
        let io = io_counters
            .map_or_else(SlateDBIoSnapshot::default, |c| c.snapshot())
            .saturating_sub(io_before);
        println!(
            "fixed_live_history,backend={backend},operation={operation},history_changes={history},\
             commit_width={width},live_rows={},cold_us={},p50_us={},p95_us={},p99_us={},\
             seed_ms={},ingest_changes_per_second={:.1},staged_puts={},logical_written_bytes={},\
             seed_object_reads={},seed_object_read_bytes={},seed_object_writes={},\
             seed_object_write_bytes={},flush_object_reads={},flush_object_read_bytes={},\
             flush_object_writes={},flush_object_write_bytes={},\
             physical_bytes={},physical_objects={},object_reads={},object_read_bytes={},\
             object_writes={},object_write_bytes={},list_operations={},listed_objects={},\
             deleted_objects={},copied_objects={},runtime_threads={},memtable_flush={},settle_ms={}",
            fixture.live_rows,
            cold.as_micros(),
            percentile(&timings, 50).as_micros(),
            percentile(&timings, 95).as_micros(),
            percentile(&timings, 99).as_micros(),
            seed.elapsed.as_millis(),
            history as f64 / seed.elapsed.as_secs_f64(),
            seed.staged_puts,
            seed.written_bytes,
            seed_io.read_objects,
            seed_io.read_bytes,
            seed_io.write_objects,
            seed_io.write_bytes,
            lifecycle_io.read_objects,
            lifecycle_io.read_bytes,
            lifecycle_io.write_objects,
            lifecycle_io.write_bytes,
            directory_bytes(storage_path),
            directory_objects(storage_path),
            io.read_objects,
            io.read_bytes,
            io.write_objects,
            io.write_bytes,
            io.list_operations,
            io.listed_objects,
            io.deleted_objects,
            io.copied_objects,
            env_usize("LIX_FIXED_LIVE_RUNTIME_THREADS", 1),
            env_bool("LIX_FIXED_LIVE_MEMTABLE_FLUSH", false),
            env_nonnegative_usize("LIX_FIXED_LIVE_SETTLE_MS", 0),
        );
    }
}

async fn register_schema<S>(session: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "fixed_live",
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false },
        ],
        "primary_key": ["path"],
    });
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .expect("register schema");
}

async fn seed_live_rows<S>(session: &Lix<S>, live_rows: usize)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mut transaction = session.begin_transaction().await.expect("begin live seed");
    for index in 0..live_rows {
        transaction
            .execute(
                "INSERT INTO fixed_live (path, value) VALUES ($1, CAST($2 AS JSONB))",
                &[
                    Value::Text(live_path(index)),
                    Value::Text(format!(r#"{{"seed":{index}}}"#)),
                ],
            )
            .await
            .expect("seed live row");
    }
    transaction.commit().await.expect("commit live seed");
}

fn live_path(index: usize) -> String {
    format!("/live/{index:08}")
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                entry.metadata().map_or(0, |metadata| {
                    if metadata.is_dir() {
                        directory_bytes(&path)
                    } else {
                        metadata.len()
                    }
                })
            })
            .sum()
    })
}

fn directory_objects(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if entry.metadata().is_ok_and(|metadata| metadata.is_dir()) {
                    directory_objects(&path)
                } else {
                    1
                }
            })
            .sum()
    })
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_nonnegative_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usizes(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .filter(|value| *value > 0)
                .collect()
        })
        .filter(|values: &Vec<_>| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name).map_or(default, |value| {
        value != "0" && !value.eq_ignore_ascii_case("false")
    })
}

fn selected(name: &str, candidate: &str) -> bool {
    std::env::var(name).map_or(true, |selection| {
        selection
            .split(',')
            .map(str::trim)
            .any(|value| value == candidate)
    })
}
