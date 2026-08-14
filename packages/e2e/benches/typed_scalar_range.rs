#![allow(clippy::large_futures)]

use std::fmt::Write as _;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use lix::storage::Storage;
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

const INSERT_ROWS_PER_STATEMENT: usize = 2_048;

#[derive(Clone, Copy, Debug)]
enum Backend {
    Rocks,
    Slate,
}

#[derive(Clone, Copy, Debug)]
enum Scalar {
    Int8,
    Text,
    Uuid,
}

impl Scalar {
    fn parse(value: &str) -> Self {
        match value {
            "int8" => Self::Int8,
            "text" => Self::Text,
            "uuid" => Self::Uuid,
            _ => panic!("scalar must be int8, text, or uuid; got {value}"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Int8 => "int8",
            Self::Text => "text",
            Self::Uuid => "uuid",
        }
    }

    const fn schema_type(self) -> &'static str {
        self.label()
    }

    fn sql_literal(self, ordinal: usize) -> String {
        match self {
            Self::Int8 => ordinal.to_string(),
            Self::Text => format!("'value-{ordinal:012}'"),
            Self::Uuid => format!("'01920000-0000-7000-8000-{ordinal:012x}'"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Config {
    backend: Backend,
    rows: usize,
    scalar: Scalar,
    delta: usize,
    warmups: usize,
    samples: usize,
}

#[derive(Debug)]
struct SampleSummary {
    p50_ns: u128,
    p95_ns: u128,
    cpu_ns: u128,
    rss_bytes: u64,
    result_rows: usize,
    digest: String,
    range_candidates: u64,
    range_engaged: u64,
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    assert_eq!(
        args.len(),
        8,
        "usage: typed_scalar_range <rocksdb|slatedb> <database-dir> <rows> <int8|text|uuid> <delta> <warmups> <samples>"
    );
    let backend = match args[1].as_str() {
        "rocksdb" => Backend::Rocks,
        "slatedb" => Backend::Slate,
        other => panic!("backend must be rocksdb or slatedb; got {other}"),
    };
    let config = Config {
        backend,
        rows: parse_positive(&args[3], "rows"),
        scalar: Scalar::parse(&args[4]),
        delta: args[5].parse().expect("delta must be a nonnegative integer"),
        warmups: args[6].parse().expect("warmups must be a nonnegative integer"),
        samples: parse_positive(&args[7], "samples"),
    };
    assert!(config.delta <= config.rows, "delta must not exceed rows");
    let path = PathBuf::from(&args[2]);
    assert!(!path.exists(), "database path must be fresh: {}", path.display());

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build benchmark runtime")
        .block_on(run(config, path));
}

async fn run(config: Config, path: PathBuf) {
    match config.backend {
        Backend::Rocks => {
            let storage = RocksDB::open(&path).expect("open RocksDB");
            run_storage(config, &path, storage, None).await;
        }
        Backend::Slate => {
            let counters = SlateDBIoCounters::default();
            let storage = SlateDB::open_with_io_counters(&path, counters.clone())
                .expect("open SlateDB");
            run_storage(config, &path, storage, Some(&counters)).await;
        }
    }
}

async fn run_storage<S>(
    config: Config,
    path: &Path,
    storage: S,
    slate_counters: Option<&SlateDBIoCounters>,
) where
    S: BenchStorage,
{
    let session = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open repository");
    register_schema(&session, "range_indexed", config.scalar, true).await;
    register_schema(&session, "range_scan", config.scalar, false).await;
    seed_rows(&session, "range_indexed", config).await;
    seed_rows(&session, "range_scan", config).await;
    apply_delta(&session, "range_indexed", config).await;
    apply_delta(&session, "range_scan", config).await;
    storage.flush_bench().await;

    let settled_before = directory_bytes(path);
    for answer_rows in [1, 100, config.rows.div_ceil(100)] {
        let answer_rows = answer_rows.min(config.rows).max(1);
        let start = config.rows / 2 - answer_rows / 2;
        let indexed_sql = range_sql("range_indexed", config.scalar, start, answer_rows);
        let scan_sql = range_sql("range_scan", config.scalar, start, answer_rows);
        let indexed = measure_query(
            &session,
            &indexed_sql,
            config.warmups,
            config.samples,
            slate_counters,
        )
        .await;
        let scan = measure_query(
            &session,
            &scan_sql,
            config.warmups,
            config.samples,
            slate_counters,
        )
        .await;
        assert_eq!(indexed.digest, scan.digest, "indexed and scan digests differ");
        print_summary(config, "range_indexed", answer_rows, &indexed, settled_before);
        print_summary(config, "range_scan", answer_rows, &scan, settled_before);
    }

    let point = measure_query(
        &session,
        &format!("SELECT id, scalar_value, payload FROM range_indexed WHERE id = 'row-{:012}'", config.rows / 2),
        config.warmups,
        config.samples,
        slate_counters,
    )
    .await;
    print_summary(config, "point", 1, &point, settled_before);

    let full = measure_query(
        &session,
        "SELECT id, scalar_value, payload FROM range_indexed ORDER BY id",
        config.warmups,
        config.samples,
        slate_counters,
    )
    .await;
    print_summary(config, "full", config.rows, &full, settled_before);

    let update_started = Instant::now();
    let result = session
        .execute(
            &format!("UPDATE range_indexed SET payload = 'guardrail-updated' WHERE id = 'row-{:012}'", config.rows / 3),
            &[],
        )
        .await
        .expect("execute update guardrail");
    assert_eq!(result.rows_affected(), 1);
    storage.flush_bench().await;
    println!(
        "RESULT backend={:?} rows={} scalar={} delta={} route=update answer_rows=1 p50_ns={} p95_ns={} cpu_ns=0 rss_bytes={} result_rows=1 digest=na range_candidates=0 range_engaged=0 settled_bytes={}",
        config.backend,
        config.rows,
        config.scalar.label(),
        config.delta,
        update_started.elapsed().as_nanos(),
        update_started.elapsed().as_nanos(),
        peak_rss_bytes(),
        directory_bytes(path),
    );

    // Reopening through a second public handle authenticates the durable route
    // without changing or bypassing the adapter's lifecycle.
    let reopened = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("cold reopen repository");
    let reopen = measure_query(
        &reopened,
        &range_sql("range_indexed", config.scalar, config.rows / 2, 100.min(config.rows)),
        0,
        1,
        slate_counters,
    )
    .await;
    print_summary(config, "reopen", reopen.result_rows, &reopen, directory_bytes(path));
}

async fn register_schema<S: Storage + Clone + Send + Sync + 'static>(
    session: &Lix<S>,
    key: &str,
    scalar: Scalar,
    indexed: bool,
) {
    let unique = if indexed { r#", "unique":[["scalar_value"]]"# } else { "" };
    let schema = format!(
        r#"{{"$schema":"https://lix.dev/schema-v1.json","key":"{key}","columns":[{{"name":"id","type":"text","nullable":false}},{{"name":"scalar_value","type":"{}","nullable":false}},{{"name":"payload","type":"text","nullable":false}}],"primary_key":["id"]{unique}}}"#,
        scalar.schema_type(),
    );
    session
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(schema)],
        )
        .await
        .unwrap_or_else(|error| panic!("register {key}: {error:?}"));
}

async fn seed_rows<S: Storage + Clone + Send + Sync + 'static>(
    session: &Lix<S>,
    table: &str,
    config: Config,
) {
    let mut transaction = session.begin_transaction().await.expect("begin seed transaction");
    for start in (0..config.rows).step_by(INSERT_ROWS_PER_STATEMENT) {
        let end = (start + INSERT_ROWS_PER_STATEMENT).min(config.rows);
        let mut sql = format!("INSERT INTO {table} (id, scalar_value, payload) VALUES ");
        for ordinal in start..end {
            if ordinal != start {
                sql.push(',');
            }
            write!(
                sql,
                "('row-{ordinal:012}', {}, 'payload-{ordinal:012}')",
                config.scalar.sql_literal(ordinal),
            )
            .expect("write seed SQL");
        }
        transaction
            .execute(&sql, &[])
            .await
            .unwrap_or_else(|error| panic!("seed {table} rows {start}..{end}: {error:?}"));
    }
    transaction.commit().await.expect("commit seed transaction");
}

async fn apply_delta<S: Storage + Clone + Send + Sync + 'static>(
    session: &Lix<S>,
    table: &str,
    config: Config,
) {
    if config.delta == 0 {
        return;
    }
    let mut transaction = session.begin_transaction().await.expect("begin delta transaction");
    for ordinal in 0..config.delta {
        transaction
            .execute(
                &format!("UPDATE {table} SET scalar_value = {} WHERE id = 'row-{ordinal:012}'", config.scalar.sql_literal(config.rows + ordinal)),
                &[],
            )
            .await
            .unwrap_or_else(|error| panic!("update {table} row {ordinal}: {error:?}"));
    }
    transaction.commit().await.expect("commit delta transaction");
}

fn range_sql(table: &str, scalar: Scalar, start: usize, rows: usize) -> String {
    let end = start + rows - 1;
    format!(
        "SELECT id, scalar_value, payload FROM {table} WHERE scalar_value BETWEEN {} AND {} ORDER BY id",
        scalar.sql_literal(start),
        scalar.sql_literal(end),
    )
}

async fn measure_query<S: Storage + Clone + Send + Sync + 'static>(
    session: &Lix<S>,
    sql: &str,
    warmups: usize,
    samples: usize,
    slate_counters: Option<&SlateDBIoCounters>,
) -> SampleSummary {
    for _ in 0..warmups {
        black_box(session.execute(sql, &[]).await.expect("execute warmup"));
    }
    let _ = lix::storage_bench::take_hot_index_probe_census();
    let io_before = slate_counters.map(SlateDBIoCounters::snapshot).unwrap_or_default();
    let cpu_before = process_cpu_ns();
    let mut elapsed = Vec::with_capacity(samples);
    let mut digest = None;
    let mut result_rows = 0;
    for _ in 0..samples {
        let started = Instant::now();
        let result = session.execute(sql, &[]).await.expect("execute measured query");
        elapsed.push(started.elapsed());
        result_rows = result.len();
        let current = digest_result(&result);
        assert!(digest.as_ref().is_none_or(|expected| expected == &current));
        digest = Some(current);
        black_box(result);
    }
    let cpu_ns = process_cpu_ns().saturating_sub(cpu_before);
    let io = slate_counters
        .map(|counters| counters.snapshot().saturating_sub(io_before))
        .unwrap_or_default();
    let census = lix::storage_bench::take_hot_index_probe_census();
    elapsed.sort_unstable();
    let p95_index = (elapsed.len() * 95).div_ceil(100).saturating_sub(1);
    println!(
        "IO slate_read_objects={} slate_read_bytes={} slate_write_objects={} slate_write_bytes={}",
        io.read_objects, io.read_bytes, io.write_objects, io.write_bytes,
    );
    SampleSummary {
        p50_ns: elapsed[elapsed.len() / 2].as_nanos(),
        p95_ns: elapsed[p95_index].as_nanos(),
        cpu_ns,
        rss_bytes: peak_rss_bytes(),
        result_rows,
        digest: digest.expect("at least one sample"),
        range_candidates: census.range_probe_candidates,
        range_engaged: census.range_probes_engaged,
    }
}

fn digest_result(result: &lix::ExecuteResult) -> String {
    let mut hasher = blake3::Hasher::new();
    for column in result.columns() {
        hasher.update(column.as_bytes());
        hasher.update(&[0]);
    }
    for row in result.rows() {
        for value in row.values() {
            hasher.update(format!("{value:?}").as_bytes());
            hasher.update(&[0xff]);
        }
    }
    hasher.finalize().to_hex().to_string()
}

fn print_summary(config: Config, route: &str, answer_rows: usize, summary: &SampleSummary, settled: u64) {
    println!(
        "RESULT backend={:?} rows={} scalar={} delta={} route={} answer_rows={} p50_ns={} p95_ns={} cpu_ns={} rss_bytes={} result_rows={} digest={} range_candidates={} range_engaged={} settled_bytes={}",
        config.backend,
        config.rows,
        config.scalar.label(),
        config.delta,
        route,
        answer_rows,
        summary.p50_ns,
        summary.p95_ns,
        summary.cpu_ns,
        summary.rss_bytes,
        summary.result_rows,
        summary.digest,
        summary.range_candidates,
        summary.range_engaged,
        settled,
    );
}

#[async_trait::async_trait]
trait BenchStorage: Storage + Clone + Send + Sync + 'static {
    async fn flush_bench(&self);
}

#[async_trait::async_trait]
impl BenchStorage for RocksDB {
    async fn flush_bench(&self) {
        self.flush().expect("flush RocksDB");
    }
}

#[async_trait::async_trait]
impl BenchStorage for SlateDB {
    async fn flush_bench(&self) {
        self.flush().await.expect("flush SlateDB");
    }
}

fn parse_positive(value: &str, label: &str) -> usize {
    let parsed = value.parse().unwrap_or_else(|_| panic!("{label} must be an integer"));
    assert!(parsed > 0, "{label} must be positive");
    parsed
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else { return 0 };
    entries
        .filter_map(Result::ok)
        .map(|entry| match entry.metadata() {
            Ok(metadata) if metadata.is_dir() => directory_bytes(&entry.path()),
            Ok(metadata) => metadata.len(),
            Err(_) => 0,
        })
        .sum()
}

#[cfg(target_os = "linux")]
fn peak_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| status.lines().find(|line| line.starts_with("VmHWM:")).map(str::to_owned))
        .and_then(|line| line.split_whitespace().nth(1).and_then(|value| value.parse::<u64>().ok()))
        .unwrap_or(0)
        * 1024
}

#[cfg(not(target_os = "linux"))]
fn peak_rss_bytes() -> u64 { 0 }

#[cfg(target_os = "linux")]
fn process_cpu_ns() -> u128 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    assert_eq!(status, 0, "getrusage failed");
    let usage = unsafe { usage.assume_init() };
    let micros = |time: libc::timeval| {
        u128::try_from(time.tv_sec).unwrap_or(0) * 1_000_000
            + u128::try_from(time.tv_usec).unwrap_or(0)
    };
    (micros(usage.ru_utime) + micros(usage.ru_stime)) * 1_000
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ns() -> u128 { 0 }
