use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hdrhistogram::Histogram;
use lix_engine::{Engine, ExecuteBatchStatement, SessionContext, Value};
use lix_slatedb_storage::SlateDB;
use postgres::{Client, NoTls, Statement};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use tempfile::TempDir;

const INSERT_SQL: &str = "INSERT INTO sbtest1 (id, k, c, pad) VALUES ($1, $2, $3, $4)";
const POINT_SELECT_SQL: &str = "SELECT c FROM sbtest1 WHERE id = $1";
const UPDATE_INDEX_SQL: &str = "UPDATE sbtest1 SET k = k + 1 WHERE id = $1";
const UPDATE_NON_INDEX_SQL: &str = "UPDATE sbtest1 SET c = $1 WHERE id = $2";
const DELETE_SQL: &str = "DELETE FROM sbtest1 WHERE id = $1";
const SYSBENCH_VERSION: &str = "1.0.20";
const PROFILE_NAME: &str = "sysbench-1.0.20-oltp-derived-common-feature";

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
enum EngineKind {
    LixSlatedb,
    Sqlite,
    Postgres,
}

impl FromStr for EngineKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "lix-slatedb" => Ok(Self::LixSlatedb),
            "sqlite" => Ok(Self::Sqlite),
            "postgres" => Ok(Self::Postgres),
            _ => Err(format!(
                "unknown engine '{value}'; expected lix-slatedb, sqlite, or postgres"
            )),
        }
    }
}

impl fmt::Display for EngineKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LixSlatedb => formatter.write_str("lix-slatedb"),
            Self::Sqlite => formatter.write_str("sqlite"),
            Self::Postgres => formatter.write_str("postgres"),
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Workload {
    PointSelect,
    Insert,
    UpdateIndex,
    UpdateNonIndex,
    Delete,
}

impl Workload {
    fn needs_seed(self) -> bool {
        !matches!(self, Self::Insert)
    }
}

impl FromStr for Workload {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "point-select" => Ok(Self::PointSelect),
            "insert" => Ok(Self::Insert),
            "update-index" => Ok(Self::UpdateIndex),
            "update-non-index" => Ok(Self::UpdateNonIndex),
            "delete" => Ok(Self::Delete),
            _ => Err(format!(
                "unknown workload '{value}'; expected point-select, insert, update-index, update-non-index, or delete"
            )),
        }
    }
}

#[derive(Debug)]
struct Config {
    engine: EngineKind,
    workload: Workload,
    table_size: u32,
    clients: usize,
    warmup: Duration,
    measurement: Duration,
    events_per_client: Option<u64>,
    seed: u64,
    load_batch_size: usize,
    settle: Duration,
    database_path: Option<PathBuf>,
    postgres_url: Option<String>,
    output: Option<PathBuf>,
    target_revision: Option<String>,
    target_dirty: bool,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut engine = None;
        let mut workload = None;
        let mut table_size = 10_000_u32;
        let mut clients = 1_usize;
        let mut warmup_seconds = 15_u64;
        let mut time_seconds = 60_u64;
        let mut events_per_client = None;
        let mut seed = 1_u64;
        let mut load_batch_size = 10_000_usize;
        let mut settle_ms = 1_000_u64;
        let mut database_path = None;
        let mut postgres_url = None;
        let mut output = None;
        let mut target_revision = None;
        let mut target_dirty = false;
        let mut arguments = std::env::args().skip(1);
        while let Some(flag) = arguments.next() {
            let mut value = || {
                arguments
                    .next()
                    .ok_or_else(|| format!("{flag} requires a value"))
            };
            match flag.as_str() {
                "--engine" => engine = Some(value()?.parse()?),
                "--workload" => workload = Some(value()?.parse()?),
                "--table-size" => table_size = parse_positive(&flag, &value()?)?,
                "--clients" => clients = parse_positive(&flag, &value()?)?,
                "--warmup-seconds" => warmup_seconds = parse_number(&flag, &value()?)?,
                "--time-seconds" => time_seconds = parse_number(&flag, &value()?)?,
                "--events-per-client" => {
                    events_per_client = Some(parse_positive(&flag, &value()?)?)
                }
                "--seed" => seed = parse_number(&flag, &value()?)?,
                "--load-batch-size" => load_batch_size = parse_positive(&flag, &value()?)?,
                "--settle-ms" => settle_ms = parse_number(&flag, &value()?)?,
                "--database-path" => database_path = Some(PathBuf::from(value()?)),
                "--postgres-url" => postgres_url = Some(value()?),
                "--output" => output = Some(PathBuf::from(value()?)),
                "--target-revision" => target_revision = Some(value()?),
                "--target-dirty" => target_dirty = parse_number(&flag, &value()?)?,
                "--help" | "-h" => return Err(usage()),
                _ => return Err(format!("unknown argument '{flag}'\n\n{}", usage())),
            }
        }
        let engine = engine.ok_or_else(|| format!("--engine is required\n\n{}", usage()))?;
        let workload = workload.ok_or_else(|| format!("--workload is required\n\n{}", usage()))?;
        if events_per_client.is_none() && time_seconds == 0 {
            return Err("--time-seconds must be positive unless --events-per-client is set".into());
        }
        if events_per_client.is_some() {
            warmup_seconds = 0;
            time_seconds = 0;
        }
        if matches!(engine, EngineKind::Postgres) && postgres_url.is_none() {
            return Err("--postgres-url is required for the postgres engine".into());
        }
        Ok(Self {
            engine,
            workload,
            table_size,
            clients,
            warmup: Duration::from_secs(warmup_seconds),
            measurement: Duration::from_secs(time_seconds),
            events_per_client,
            seed,
            load_batch_size,
            settle: Duration::from_millis(settle_ms),
            database_path,
            postgres_url,
            output,
            target_revision,
            target_dirty,
        })
    }
}

fn parse_number<T>(flag: &str, value: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid value for {flag}: {error}"))
}

fn parse_positive<T>(flag: &str, value: &str) -> Result<T, String>
where
    T: FromStr + Default + PartialOrd,
    T::Err: fmt::Display,
{
    let parsed = parse_number(flag, value)?;
    if parsed <= T::default() {
        return Err(format!("{flag} must be positive"));
    }
    Ok(parsed)
}

fn usage() -> String {
    r#"Usage: sysbench_crud --engine <lix-slatedb|sqlite|postgres> \
  --workload <point-select|insert|update-index|update-non-index|delete> [options]

Options:
  --table-size N            Seed rows (default 10000)
  --clients N               Concurrent clients (default 1)
  --warmup-seconds N        Warmup duration (default 15)
  --time-seconds N          Measurement duration (default 60)
  --events-per-client N     Fixed-event qualification mode; disables warmup
  --seed N                  Deterministic 64-bit seed (default 1)
  --load-batch-size N       Rows committed per load transaction (default 10000)
  --database-path PATH      Fresh SQLite file or Lix SlateDB directory
  --postgres-url URL        Dedicated PostgreSQL benchmark database
  --output PATH             Write JSON report to a new file
  --target-revision REV     Revision recorded in the report
  --settle-ms N             Settle before storage snapshots (default 1000)
  --target-dirty BOOL       Dirty checkout state recorded in the report"#
        .to_string()
}

#[derive(Clone)]
enum WorkerFactory {
    Lix(Arc<Engine<SlateDB>>),
    Sqlite(PathBuf),
    Postgres { url: String, schema: String },
}

enum Worker {
    Lix {
        runtime: tokio::runtime::Runtime,
        session: SessionContext<SlateDB>,
    },
    Sqlite(Connection),
    Postgres {
        client: Client,
        statements: PostgresStatements,
    },
}

struct PostgresStatements {
    point_select: Statement,
    insert: Statement,
    update_index: Statement,
    update_non_index: Statement,
    delete: Statement,
}

impl WorkerFactory {
    fn worker(&self) -> Result<Worker, String> {
        match self {
            Self::Lix(engine) => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| format!("create Lix worker runtime: {error}"))?;
                let session = runtime
                    .block_on(engine.open_workspace_session())
                    .map_err(|error| format!("open Lix worker session: {error}"))?;
                Ok(Worker::Lix { runtime, session })
            }
            Self::Sqlite(path) => {
                let connection = Connection::open(path)
                    .map_err(|error| format!("open SQLite worker connection: {error}"))?;
                configure_sqlite(&connection)?;
                connection.set_prepared_statement_cache_capacity(16);
                Ok(Worker::Sqlite(connection))
            }
            Self::Postgres { url, schema } => {
                let mut client = Client::connect(url, NoTls)
                    .map_err(|error| format!("open PostgreSQL worker connection: {error}"))?;
                set_postgres_schema(&mut client, schema)?;
                let statements = PostgresStatements {
                    point_select: client
                        .prepare(POINT_SELECT_SQL)
                        .map_err(|error| format!("prepare PostgreSQL point select: {error}"))?,
                    insert: client
                        .prepare(INSERT_SQL)
                        .map_err(|error| format!("prepare PostgreSQL insert: {error}"))?,
                    update_index: client
                        .prepare(UPDATE_INDEX_SQL)
                        .map_err(|error| format!("prepare PostgreSQL indexed update: {error}"))?,
                    update_non_index: client
                        .prepare(UPDATE_NON_INDEX_SQL)
                        .map_err(|error| format!("prepare PostgreSQL non-index update: {error}"))?,
                    delete: client
                        .prepare(DELETE_SQL)
                        .map_err(|error| format!("prepare PostgreSQL delete: {error}"))?,
                };
                Ok(Worker::Postgres { client, statements })
            }
        }
    }
}

impl Worker {
    fn event(
        &mut self,
        workload: Workload,
        id: i64,
        k: i64,
        c: &str,
        pad: &str,
    ) -> Result<(), String> {
        match self {
            Self::Lix { runtime, session } => runtime.block_on(async {
                let result = match workload {
                    Workload::PointSelect => {
                        session
                            .execute(POINT_SELECT_SQL, &[Value::Integer(id)])
                            .await
                    }
                    Workload::Insert => {
                        session
                            .execute(
                                INSERT_SQL,
                                &[
                                    Value::Integer(id),
                                    Value::Integer(k),
                                    Value::Text(c.to_string()),
                                    Value::Text(pad.to_string()),
                                ],
                            )
                            .await
                    }
                    Workload::UpdateIndex => {
                        session
                            .execute(UPDATE_INDEX_SQL, &[Value::Integer(id)])
                            .await
                    }
                    Workload::UpdateNonIndex => {
                        session
                            .execute(
                                UPDATE_NON_INDEX_SQL,
                                &[Value::Text(c.to_string()), Value::Integer(id)],
                            )
                            .await
                    }
                    Workload::Delete => session.execute(DELETE_SQL, &[Value::Integer(id)]).await,
                }
                .map_err(|error| error.to_string())?;
                if matches!(workload, Workload::PointSelect) && result.rows().len() != 1 {
                    return Err(format!(
                        "point select returned {} rows instead of one",
                        result.rows().len()
                    ));
                }
                Ok(())
            }),
            Self::Sqlite(connection) => {
                match workload {
                    Workload::PointSelect => {
                        let mut statement = connection
                            .prepare_cached("SELECT c FROM sbtest1 WHERE id = ?1")
                            .map_err(|error| error.to_string())?;
                        let value = statement
                            .query_row(params![id], |row| row.get::<_, String>(0))
                            .optional()
                            .map_err(|error| error.to_string())?;
                        if value.is_none() {
                            return Err("point select returned no row".into());
                        }
                    }
                    Workload::Insert => {
                        connection
                            .prepare_cached(
                                "INSERT INTO sbtest1 (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)",
                            )
                            .map_err(|error| error.to_string())?
                            .execute(params![id, k, c, pad])
                            .map_err(|error| error.to_string())?;
                    }
                    Workload::UpdateIndex => {
                        connection
                            .prepare_cached("UPDATE sbtest1 SET k = k + 1 WHERE id = ?1")
                            .map_err(|error| error.to_string())?
                            .execute(params![id])
                            .map_err(|error| error.to_string())?;
                    }
                    Workload::UpdateNonIndex => {
                        connection
                            .prepare_cached("UPDATE sbtest1 SET c = ?1 WHERE id = ?2")
                            .map_err(|error| error.to_string())?
                            .execute(params![c, id])
                            .map_err(|error| error.to_string())?;
                    }
                    Workload::Delete => {
                        connection
                            .prepare_cached("DELETE FROM sbtest1 WHERE id = ?1")
                            .map_err(|error| error.to_string())?
                            .execute(params![id])
                            .map_err(|error| error.to_string())?;
                    }
                }
                Ok(())
            }
            Self::Postgres { client, statements } => {
                match workload {
                    Workload::PointSelect => {
                        let rows = client
                            .query(&statements.point_select, &[&id])
                            .map_err(|error| error.to_string())?;
                        if rows.len() != 1 {
                            return Err(format!(
                                "point select returned {} rows instead of one",
                                rows.len()
                            ));
                        }
                        let _: &str = rows[0].get(0);
                    }
                    Workload::Insert => {
                        client
                            .execute(&statements.insert, &[&id, &k, &c, &pad])
                            .map_err(|error| error.to_string())?;
                    }
                    Workload::UpdateIndex => {
                        client
                            .execute(&statements.update_index, &[&id])
                            .map_err(|error| error.to_string())?;
                    }
                    Workload::UpdateNonIndex => {
                        client
                            .execute(&statements.update_non_index, &[&c, &id])
                            .map_err(|error| error.to_string())?;
                    }
                    Workload::Delete => {
                        client
                            .execute(&statements.delete, &[&id])
                            .map_err(|error| error.to_string())?;
                    }
                }
                Ok(())
            }
        }
    }
}

struct Resources {
    factory: WorkerFactory,
    lix_storage: Option<SlateDB>,
    database_path: Option<PathBuf>,
    _temp_dir: Option<TempDir>,
    postgres_schema: Option<String>,
    engine_version: String,
    storage_bytes_before: u64,
    initial_history_commits: Option<i64>,
}

#[derive(Clone, Copy, Debug)]
struct Window {
    measurement_start: Instant,
    end: Instant,
}

struct WorkerResult {
    successes: u64,
    failures: u64,
    histogram: Histogram<u64>,
    first_error: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    schema_version: u32,
    suite: &'static str,
    sysbench_reference_version: &'static str,
    engine: EngineKind,
    engine_version: String,
    workload: Workload,
    table_size: u32,
    clients: usize,
    warmup_seconds: f64,
    requested_measurement_seconds: f64,
    actual_measurement_seconds: f64,
    events_per_client: Option<u64>,
    seed: u64,
    load_batch_size: usize,
    successful_events: u64,
    failed_events: u64,
    retried_events: u64,
    first_error: Option<String>,
    events_per_second: f64,
    latency_ns: Latency,
    initial_rows: u64,
    final_rows: u64,
    initial_history_commits: Option<i64>,
    final_history_commits: Option<i64>,
    storage_bytes_before: u64,
    storage_bytes_after: u64,
    postgres_schema: Option<String>,
    target_revision: Option<String>,
    target_dirty: bool,
    started_at_unix_ms: u128,
    finished_at_unix_ms: u128,
    durability: &'static str,
    auto_increment: bool,
    secondary_index: bool,
    access_distribution: &'static str,
    settle_ms: u64,
    storage_measurement: &'static str,
    storage_scope: &'static str,
    retry_policy: &'static str,
}

#[derive(Serialize)]
struct Latency {
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
    mean: f64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("sysbench_crud: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), String> {
    let config = Config::parse()?;
    validate_fresh_paths(&config)?;
    let started_at_unix_ms = unix_ms();
    let mut resources = setup(&config)?;
    thread::sleep(config.settle);
    resources.storage_bytes_before = storage_bytes(&resources)?;
    let initial_rows = row_count(&resources)?;
    let expected_initial_rows = if config.workload.needs_seed() {
        u64::from(config.table_size)
    } else {
        0
    };
    if initial_rows != expected_initial_rows {
        return Err(format!(
            "qualification failed: expected {expected_initial_rows} initial rows, found {initial_rows}"
        ));
    }

    let ready = Arc::new(Barrier::new(config.clients + 1));
    let start = Arc::new(Barrier::new(config.clients + 1));
    let window = Arc::new(OnceLock::new());
    let abort = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(config.clients);
    for client_id in 0..config.clients {
        let factory = resources.factory.clone();
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        let window = Arc::clone(&window);
        let abort = Arc::clone(&abort);
        let workload = config.workload;
        let table_size = config.table_size;
        let clients = config.clients;
        let seed = config.seed;
        let events_per_client = config.events_per_client;
        handles.push(thread::spawn(move || {
            let mut worker = match factory.worker() {
                Ok(worker) => worker,
                Err(error) => {
                    abort.store(true, Ordering::Release);
                    ready.wait();
                    start.wait();
                    return failed_worker(error);
                }
            };
            ready.wait();
            start.wait();
            let window = *window.get().expect("coordinator installs benchmark window");
            run_worker(
                &mut worker,
                workload,
                table_size,
                clients,
                client_id,
                seed,
                events_per_client,
                window,
                &abort,
            )
        }));
    }

    ready.wait();
    if abort.load(Ordering::Acquire) {
        window
            .set(Window {
                measurement_start: Instant::now(),
                end: Instant::now(),
            })
            .expect("install failed benchmark window");
    } else {
        let now = Instant::now();
        window
            .set(Window {
                measurement_start: now + config.warmup,
                end: now + config.warmup + config.measurement,
            })
            .expect("install benchmark window");
    }
    let measurement_wall_start = Instant::now() + config.warmup;
    start.wait();

    let mut successes = 0_u64;
    let mut failures = 0_u64;
    let mut histogram = latency_histogram()?;
    let mut first_error = None;
    for handle in handles {
        let result = handle
            .join()
            .map_err(|_| "benchmark worker panicked".to_string())?;
        successes += result.successes;
        failures += result.failures;
        histogram
            .add(&result.histogram)
            .map_err(|error| format!("merge latency histogram: {error}"))?;
        if first_error.is_none() {
            first_error = result.first_error;
        }
    }
    let actual_measurement = Instant::now().saturating_duration_since(measurement_wall_start);
    let final_rows = row_count(&resources)?;
    let final_history_commits = history_count(&resources)?;
    thread::sleep(config.settle);
    let storage_bytes_after = storage_bytes(&resources)?;
    let finished_at_unix_ms = unix_ms();
    let report = Report {
        schema_version: 1,
        suite: PROFILE_NAME,
        sysbench_reference_version: SYSBENCH_VERSION,
        engine: config.engine,
        engine_version: resources.engine_version.clone(),
        workload: config.workload,
        table_size: config.table_size,
        clients: config.clients,
        warmup_seconds: config.warmup.as_secs_f64(),
        requested_measurement_seconds: config.measurement.as_secs_f64(),
        actual_measurement_seconds: actual_measurement.as_secs_f64(),
        events_per_client: config.events_per_client,
        seed: config.seed,
        load_batch_size: config.load_batch_size,
        successful_events: successes,
        failed_events: failures,
        retried_events: 0,
        first_error,
        events_per_second: successes as f64 / actual_measurement.as_secs_f64().max(f64::EPSILON),
        latency_ns: summarize_latency(&histogram),
        initial_rows,
        final_rows,
        initial_history_commits: resources.initial_history_commits,
        final_history_commits,
        storage_bytes_before: resources.storage_bytes_before,
        storage_bytes_after,
        postgres_schema: resources.postgres_schema.clone(),
        target_revision: config.target_revision,
        target_dirty: config.target_dirty,
        started_at_unix_ms,
        finished_at_unix_ms,
        durability: match config.engine {
            EngineKind::LixSlatedb => "lix/slatedb defaults; no relaxation",
            EngineKind::Sqlite => "WAL, synchronous=FULL",
            EngineKind::Postgres => {
                "fsync=on, full_page_writes=on, synchronous_commit=on; all verified"
            }
        },
        auto_increment: false,
        secondary_index: false,
        access_distribution: "uniform",
        settle_ms: config.settle.as_millis() as u64,
        storage_measurement: "settled live storage snapshot; database remains open",
        storage_scope: match config.engine {
            EngineKind::LixSlatedb => "complete SlateDB database directory",
            EngineKind::Sqlite => "database file plus WAL and shared-memory sidecars",
            EngineKind::Postgres => "pg_total_relation_size(sbtest1); excludes server WAL",
        },
        retry_policy: "none; failures are reported",
    };
    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| format!("serialize report: {error}"))?;
    if let Some(path) = &config.output {
        if path.exists() {
            return Err(format!("refusing to overwrite output {}", path.display()));
        }
        fs::write(path, format!("{json}\n"))
            .map_err(|error| format!("write {}: {error}", path.display()))?;
    }
    println!("{json}");
    cleanup_postgres(&mut resources)?;
    if failures > 0 {
        return Err(format!("benchmark completed with {failures} failed events"));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_worker(
    worker: &mut Worker,
    workload: Workload,
    table_size: u32,
    clients: usize,
    client_id: usize,
    seed: u64,
    events_per_client: Option<u64>,
    window: Window,
    abort: &AtomicBool,
) -> WorkerResult {
    let mut rng = SplitMix64::new(seed ^ (client_id as u64).wrapping_mul(0x9e3779b97f4a7c15));
    let mut histogram = latency_histogram().expect("valid histogram bounds");
    let mut successes = 0_u64;
    let mut failures = 0_u64;
    let mut first_error = None;
    let mut event_index = 0_u64;
    loop {
        if abort.load(Ordering::Acquire) {
            break;
        }
        let now = Instant::now();
        let measuring = now >= window.measurement_start;
        if let Some(limit) = events_per_client {
            if measuring && successes + failures >= limit {
                break;
            }
        } else if now >= window.end {
            break;
        }
        let event_start = Instant::now();
        let id = if matches!(workload, Workload::Insert) {
            i64::from(i32::MIN)
                + i64::try_from(client_id).expect("client id fits i64")
                + i64::try_from(event_index).expect("event index fits i64")
                    * i64::try_from(clients).expect("client count fits i64")
        } else {
            i64::from(rng.uniform_u32(1, table_size))
        };
        let k = if matches!(workload, Workload::Insert) {
            i64::from(rng.uniform_u32(1, table_size))
        } else {
            0
        };
        let c = if matches!(workload, Workload::Insert | Workload::UpdateNonIndex) {
            sysbench_string(&mut rng, 10)
        } else {
            String::new()
        };
        let pad = if matches!(workload, Workload::Insert) {
            sysbench_string(&mut rng, 5)
        } else {
            String::new()
        };
        let outcome = worker.event(workload, id, k, &c, &pad);
        let elapsed = event_start.elapsed();
        if measuring {
            let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
            histogram
                .record(nanos.max(1))
                .expect("latency is in histogram range");
            match outcome {
                Ok(()) => successes += 1,
                Err(error) => {
                    failures += 1;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        } else if let Err(error) = outcome {
            abort.store(true, Ordering::Release);
            first_error = Some(format!("warmup failed: {error}"));
            failures += 1;
            break;
        }
        event_index += 1;
    }
    WorkerResult {
        successes,
        failures,
        histogram,
        first_error,
    }
}

fn failed_worker(error: String) -> WorkerResult {
    WorkerResult {
        successes: 0,
        failures: 1,
        histogram: latency_histogram().expect("valid histogram bounds"),
        first_error: Some(error),
    }
}

fn latency_histogram() -> Result<Histogram<u64>, String> {
    Histogram::new_with_bounds(1, 300_000_000_000, 3)
        .map_err(|error| format!("create latency histogram: {error}"))
}

fn summarize_latency(histogram: &Histogram<u64>) -> Latency {
    if histogram.is_empty() {
        return Latency {
            min: 0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
            mean: 0.0,
        };
    }
    Latency {
        min: histogram.min(),
        p50: histogram.value_at_quantile(0.50),
        p95: histogram.value_at_quantile(0.95),
        p99: histogram.value_at_quantile(0.99),
        max: histogram.max(),
        mean: histogram.mean(),
    }
}

fn validate_fresh_paths(config: &Config) -> Result<(), String> {
    if let Some(path) = &config.database_path
        && path.exists()
    {
        return Err(format!(
            "refusing to reuse database path {}; provide a fresh path",
            path.display()
        ));
    }
    if let Some(path) = &config.output
        && path.exists()
    {
        return Err(format!("refusing to overwrite output {}", path.display()));
    }
    Ok(())
}

fn setup(config: &Config) -> Result<Resources, String> {
    match config.engine {
        EngineKind::LixSlatedb => setup_lix(config),
        EngineKind::Sqlite => setup_sqlite(config),
        EngineKind::Postgres => setup_postgres(config),
    }
}

fn setup_path(config: &Config, leaf: &str) -> Result<(PathBuf, Option<TempDir>), String> {
    if let Some(path) = &config.database_path {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create {}: {error}", parent.display()))?;
        }
        return Ok((path.clone(), None));
    }
    let temp_dir =
        tempfile::tempdir().map_err(|error| format!("create temporary path: {error}"))?;
    let path = temp_dir.path().join(leaf);
    Ok((path, Some(temp_dir)))
}

fn setup_lix(config: &Config) -> Result<Resources, String> {
    let (path, temp_dir) = setup_path(config, "lix-slatedb")?;
    let storage = SlateDB::open(&path).map_err(|error| format!("open SlateDB: {error}"))?;
    let accounting_storage = storage.clone();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create Lix setup runtime: {error}"))?;
    let engine = runtime.block_on(async {
        Engine::initialize(storage.clone())
            .await
            .map_err(|error| format!("initialize Lix: {error}"))?;
        let engine = Engine::new(storage)
            .await
            .map_err(|error| format!("open Lix engine: {error}"))?;
        let session = engine
            .open_workspace_session()
            .await
            .map_err(|error| format!("open Lix setup session: {error}"))?;
        register_lix_schema(&session).await?;
        if config.workload.needs_seed() {
            seed_lix(&session, config).await?;
        }
        Ok::<_, String>(engine)
    })?;
    let engine = Arc::new(engine);
    let session = runtime
        .block_on(engine.open_workspace_session())
        .map_err(|error| format!("open Lix accounting session: {error}"))?;
    let initial_history_commits = runtime.block_on(lix_count(&session, "lix_commit"))?;
    let storage_bytes_before = path_size(&path)?;
    Ok(Resources {
        factory: WorkerFactory::Lix(engine),
        lix_storage: Some(accounting_storage),
        database_path: Some(path),
        _temp_dir: temp_dir,
        postgres_schema: None,
        engine_version: format!("lix {}; slatedb 0.14.1", env!("CARGO_PKG_VERSION")),
        storage_bytes_before,
        initial_history_commits: Some(initial_history_commits),
    })
}

async fn register_lix_schema(session: &SessionContext<SlateDB>) -> Result<(), String> {
    let schema = serde_json::json!({
        "x-lix-key": "sbtest1",
        "x-lix-primary-key": ["/id"],
        "type": "object",
        "required": ["id", "k", "c", "pad"],
        "properties": {
            "id": { "type": "integer" },
            "k": { "type": "integer" },
            "c": { "type": "string" },
            "pad": { "type": "string" }
        },
        "additionalProperties": false
    });
    let result = session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
            &[Value::Text(schema.to_string())],
        )
        .await
        .map_err(|error| format!("register Lix sbtest schema: {error}"))?;
    if result.rows_affected() != 1 {
        return Err("registering Lix sbtest schema did not affect one row".into());
    }
    Ok(())
}

async fn seed_lix(session: &SessionContext<SlateDB>, config: &Config) -> Result<(), String> {
    for start in (1..=config.table_size).step_by(config.load_batch_size) {
        let end = config
            .table_size
            .min(start + u32::try_from(config.load_batch_size).unwrap_or(u32::MAX) - 1);
        let statements = (start..=end)
            .map(|id| {
                let row = seed_row(config.seed, id);
                ExecuteBatchStatement {
                    // Setup is outside the timer. Distinct literals avoid the
                    // homogeneous string-PK fast path so integer primary keys
                    // retain their declared type during bulk qualification.
                    sql: format!(
                        "INSERT INTO sbtest1 (id, k, c, pad) VALUES ({id}, {}, '{}', '{}')",
                        row.k, row.c, row.pad
                    ),
                    params: Vec::new(),
                }
            })
            .collect::<Vec<_>>();
        let results = session
            .execute_batch(&statements)
            .await
            .map_err(|error| format!("seed Lix rows {start}..={end}: {error}"))?;
        if results.len() != usize::try_from(end - start + 1).expect("batch size fits usize") {
            return Err(format!(
                "Lix seed returned wrong result count for {start}..={end}"
            ));
        }
    }
    Ok(())
}

fn setup_sqlite(config: &Config) -> Result<Resources, String> {
    let (path, temp_dir) = setup_path(config, "sysbench.sqlite")?;
    let mut connection = Connection::open(&path)
        .map_err(|error| format!("open SQLite setup connection: {error}"))?;
    configure_sqlite(&connection)?;
    connection
        .execute_batch(
            "CREATE TABLE sbtest1 (
                id INTEGER NOT NULL PRIMARY KEY,
                k INTEGER NOT NULL,
                c TEXT NOT NULL,
                pad TEXT NOT NULL
            );",
        )
        .map_err(|error| format!("create SQLite sbtest table: {error}"))?;
    if config.workload.needs_seed() {
        seed_sqlite(&mut connection, config)?;
    }
    drop(connection);
    Ok(Resources {
        factory: WorkerFactory::Sqlite(path.clone()),
        lix_storage: None,
        database_path: Some(path.clone()),
        _temp_dir: temp_dir,
        postgres_schema: None,
        engine_version: rusqlite::version().to_string(),
        storage_bytes_before: sqlite_storage_bytes(&path)?,
        initial_history_commits: None,
    })
}

fn configure_sqlite(connection: &Connection) -> Result<(), String> {
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA busy_timeout=30000;
             PRAGMA foreign_keys=ON;",
        )
        .map_err(|error| format!("configure SQLite: {error}"))?;
    let journal_mode: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .map_err(|error| format!("verify SQLite journal_mode: {error}"))?;
    let synchronous: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .map_err(|error| format!("verify SQLite synchronous: {error}"))?;
    let busy_timeout: i64 = connection
        .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
        .map_err(|error| format!("verify SQLite busy_timeout: {error}"))?;
    if !journal_mode.eq_ignore_ascii_case("wal") || synchronous != 2 || busy_timeout != 30_000 {
        return Err(format!(
            "SQLite durability verification failed: journal_mode={journal_mode}, synchronous={synchronous}, busy_timeout={busy_timeout}"
        ));
    }
    Ok(())
}

fn seed_sqlite(connection: &mut Connection, config: &Config) -> Result<(), String> {
    for start in (1..=config.table_size).step_by(config.load_batch_size) {
        let end = config
            .table_size
            .min(start + u32::try_from(config.load_batch_size).unwrap_or(u32::MAX) - 1);
        let transaction = connection
            .transaction()
            .map_err(|error| format!("begin SQLite load transaction: {error}"))?;
        {
            let mut statement = transaction
                .prepare("INSERT INTO sbtest1 (id, k, c, pad) VALUES (?1, ?2, ?3, ?4)")
                .map_err(|error| format!("prepare SQLite load insert: {error}"))?;
            for id in start..=end {
                let row = seed_row(config.seed, id);
                statement
                    .execute(params![id, row.k, row.c, row.pad])
                    .map_err(|error| format!("seed SQLite row {id}: {error}"))?;
            }
        }
        transaction
            .commit()
            .map_err(|error| format!("commit SQLite load transaction: {error}"))?;
    }
    Ok(())
}

fn setup_postgres(config: &Config) -> Result<Resources, String> {
    let url = config
        .postgres_url
        .as_ref()
        .expect("validated PostgreSQL URL");
    let schema = format!(
        "lix_sysbench_{:016x}_{}_{}",
        config.seed,
        std::process::id(),
        unix_ms()
    );
    let mut client = Client::connect(url, NoTls)
        .map_err(|error| format!("open PostgreSQL setup connection: {error}"))?;
    for setting in ["fsync", "full_page_writes", "synchronous_commit"] {
        let value: String = client
            .query_one(&format!("SHOW {setting}"), &[])
            .map_err(|error| format!("read PostgreSQL {setting}: {error}"))?
            .get(0);
        if value != "on" {
            return Err(format!("PostgreSQL {setting} must be on, got '{value}'"));
        }
    }
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema};
             CREATE TABLE sbtest1 (
                id BIGINT NOT NULL PRIMARY KEY,
                k BIGINT NOT NULL,
                c VARCHAR(120) NOT NULL,
                pad VARCHAR(60) NOT NULL
             );"
        ))
        .map_err(|error| format!("create PostgreSQL benchmark schema: {error}"))?;
    if config.workload.needs_seed() {
        seed_postgres(&mut client, config)?;
    }
    let engine_version: String = client
        .query_one("SHOW server_version", &[])
        .map_err(|error| format!("read PostgreSQL version: {error}"))?
        .get(0);
    let storage_bytes_before = postgres_storage_bytes(&mut client)?;
    Ok(Resources {
        factory: WorkerFactory::Postgres {
            url: url.clone(),
            schema: schema.clone(),
        },
        lix_storage: None,
        database_path: None,
        _temp_dir: None,
        postgres_schema: Some(schema),
        engine_version,
        storage_bytes_before,
        initial_history_commits: None,
    })
}

fn set_postgres_schema(client: &mut Client, schema: &str) -> Result<(), String> {
    client
        .batch_execute(&format!("SET search_path TO {schema}"))
        .map_err(|error| format!("select PostgreSQL benchmark schema: {error}"))
}

fn seed_postgres(client: &mut Client, config: &Config) -> Result<(), String> {
    let statement = client
        .prepare(INSERT_SQL)
        .map_err(|error| format!("prepare PostgreSQL load insert: {error}"))?;
    for start in (1..=config.table_size).step_by(config.load_batch_size) {
        let end = config
            .table_size
            .min(start + u32::try_from(config.load_batch_size).unwrap_or(u32::MAX) - 1);
        let mut transaction = client
            .transaction()
            .map_err(|error| format!("begin PostgreSQL load transaction: {error}"))?;
        for id in start..=end {
            let row = seed_row(config.seed, id);
            transaction
                .execute(
                    &statement,
                    &[&i64::from(id), &i64::from(row.k), &row.c, &row.pad],
                )
                .map_err(|error| format!("seed PostgreSQL row {id}: {error}"))?;
        }
        transaction
            .commit()
            .map_err(|error| format!("commit PostgreSQL load transaction: {error}"))?;
    }
    Ok(())
}

fn row_count(resources: &Resources) -> Result<u64, String> {
    match &resources.factory {
        WorkerFactory::Lix(engine) => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("create Lix accounting runtime: {error}"))?;
            let session = runtime
                .block_on(engine.open_workspace_session())
                .map_err(|error| format!("open Lix accounting session: {error}"))?;
            runtime
                .block_on(lix_count(&session, "sbtest1"))
                .map(|value| value as u64)
        }
        WorkerFactory::Sqlite(path) => {
            let connection = Connection::open(path)
                .map_err(|error| format!("open SQLite accounting connection: {error}"))?;
            let count: i64 = connection
                .query_row("SELECT COUNT(*) FROM sbtest1", [], |row| row.get(0))
                .map_err(|error| format!("count SQLite rows: {error}"))?;
            Ok(count as u64)
        }
        WorkerFactory::Postgres { url, schema } => {
            let mut client = Client::connect(url, NoTls)
                .map_err(|error| format!("open PostgreSQL accounting connection: {error}"))?;
            set_postgres_schema(&mut client, schema)?;
            let count: i64 = client
                .query_one("SELECT COUNT(*) FROM sbtest1", &[])
                .map_err(|error| format!("count PostgreSQL rows: {error}"))?
                .get(0);
            Ok(count as u64)
        }
    }
}

fn history_count(resources: &Resources) -> Result<Option<i64>, String> {
    let WorkerFactory::Lix(engine) = &resources.factory else {
        return Ok(None);
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| format!("create Lix history runtime: {error}"))?;
    let session = runtime
        .block_on(engine.open_workspace_session())
        .map_err(|error| format!("open Lix history session: {error}"))?;
    runtime
        .block_on(lix_count(&session, "lix_commit"))
        .map(Some)
}

async fn lix_count(session: &SessionContext<SlateDB>, table: &str) -> Result<i64, String> {
    let result = session
        .execute(&format!("SELECT COUNT(*) AS count FROM {table}"), &[])
        .await
        .map_err(|error| format!("count Lix {table}: {error}"))?;
    result.rows()[0]
        .get::<i64>("count")
        .map_err(|error| format!("Lix {table} count was not an integer: {error}"))
}

fn storage_bytes(resources: &Resources) -> Result<u64, String> {
    match &resources.factory {
        WorkerFactory::Postgres { url, schema } => {
            let mut client = Client::connect(url, NoTls)
                .map_err(|error| format!("open PostgreSQL storage connection: {error}"))?;
            set_postgres_schema(&mut client, schema)?;
            postgres_storage_bytes(&mut client)
        }
        WorkerFactory::Sqlite(_) => sqlite_storage_bytes(
            resources
                .database_path
                .as_ref()
                .expect("SQLite has a database path"),
        ),
        WorkerFactory::Lix(_) => path_size({
            let storage = resources
                .lix_storage
                .as_ref()
                .expect("Lix has a SlateDB storage handle");
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| format!("create Lix storage accounting runtime: {error}"))?;
            runtime
                .block_on(storage.flush())
                .map_err(|error| format!("flush Lix storage before accounting: {error}"))?;
            resources
                .database_path
                .as_ref()
                .expect("Lix has a database path")
        }),
    }
}

fn sqlite_storage_bytes(path: &Path) -> Result<u64, String> {
    let mut bytes = path_size(path)?;
    let file_name = path
        .file_name()
        .ok_or_else(|| format!("SQLite path has no file name: {}", path.display()))?
        .to_string_lossy();
    for suffix in ["-wal", "-shm"] {
        bytes += path_size(&path.with_file_name(format!("{file_name}{suffix}")))?;
    }
    Ok(bytes)
}

fn postgres_storage_bytes(client: &mut Client) -> Result<u64, String> {
    let bytes: i64 = client
        .query_one("SELECT pg_total_relation_size('sbtest1')", &[])
        .map_err(|error| format!("measure PostgreSQL relation size: {error}"))?
        .get(0);
    Ok(bytes as u64)
}

fn cleanup_postgres(resources: &mut Resources) -> Result<(), String> {
    let WorkerFactory::Postgres { url, schema } = &resources.factory else {
        return Ok(());
    };
    let mut client = Client::connect(url, NoTls)
        .map_err(|error| format!("open PostgreSQL cleanup connection: {error}"))?;
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .map_err(|error| format!("drop PostgreSQL benchmark schema: {error}"))
}

fn path_size(path: &Path) -> Result<u64, String> {
    if path.is_file() {
        return fs::metadata(path)
            .map(|metadata| metadata.len())
            .map_err(|error| format!("stat {}: {error}", path.display()));
    }
    if !path.exists() {
        return Ok(0);
    }
    let mut bytes = 0_u64;
    for entry in fs::read_dir(path).map_err(|error| format!("read {}: {error}", path.display()))? {
        let entry = entry.map_err(|error| format!("read {} entry: {error}", path.display()))?;
        let entry_path = entry.path();
        if entry_path.is_dir() {
            bytes += path_size(&entry_path)?;
        } else {
            bytes += entry
                .metadata()
                .map_err(|error| format!("stat {}: {error}", entry_path.display()))?
                .len();
        }
    }
    Ok(bytes)
}

struct SeedRow {
    k: u32,
    c: String,
    pad: String,
}

fn seed_row(seed: u64, id: u32) -> SeedRow {
    let mut rng = SplitMix64::new(seed ^ u64::from(id).wrapping_mul(0x9e3779b97f4a7c15));
    SeedRow {
        k: rng.uniform_u32(1, u32::MAX),
        c: sysbench_string(&mut rng, 10),
        pad: sysbench_string(&mut rng, 5),
    }
}

fn sysbench_string(rng: &mut SplitMix64, groups: usize) -> String {
    let mut value = String::with_capacity(groups * 11 + groups.saturating_sub(1));
    for group in 0..groups {
        if group > 0 {
            value.push('-');
        }
        for _ in 0..11 {
            value.push(char::from(b'0' + rng.uniform_u32(0, 9) as u8));
        }
    }
    value
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }

    fn uniform_u32(&mut self, min: u32, max: u32) -> u32 {
        assert!(min <= max);
        let width = u64::from(max) - u64::from(min) + 1;
        (u64::from(min) + self.next() % width) as u32
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is after Unix epoch")
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::{SplitMix64, seed_row, sysbench_string};

    #[test]
    fn sysbench_strings_preserve_template_shape() {
        let mut rng = SplitMix64::new(42);
        let c = sysbench_string(&mut rng, 10);
        let pad = sysbench_string(&mut rng, 5);
        assert_eq!(c.len(), 119);
        assert_eq!(pad.len(), 59);
        assert!(c.bytes().enumerate().all(|(index, byte)| {
            if (index + 1) % 12 == 0 {
                byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        }));
    }

    #[test]
    fn seed_rows_are_deterministic_and_identity_specific() {
        let first = seed_row(7, 11);
        let repeated = seed_row(7, 11);
        let next = seed_row(7, 12);
        assert_eq!(first.k, repeated.k);
        assert_eq!(first.c, repeated.c);
        assert_eq!(first.pad, repeated.pad);
        assert_ne!(first.c, next.c);
    }

    #[test]
    fn uniform_generator_honors_closed_bounds() {
        let mut rng = SplitMix64::new(9);
        for _ in 0..1_000 {
            assert!((3..=7).contains(&rng.uniform_u32(3, 7)));
        }
        assert_eq!(rng.uniform_u32(5, 5), 5);
    }
}
