//! Manual probe for `execute_batch` read-only performance.
//!
//! Run with `cargo test -p lix_engine --test execute_batch_benchmark --
//! --ignored --nocapture`. The probe reuses one warmed session and reports
//! latency plus storage operations for 1/3/5-statement batches.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lix_engine::{
    Engine, ExecuteBatchStatement, GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead,
    MemoryWrite, ReadOptions, ScanChunk, ScanOptions, SessionContext, SpaceId, Storage,
    StorageError, StorageRead, Value, WriteOptions,
};

const FILE_COUNT_ENV: &str = "LIX_EXECUTE_BATCH_BENCH_FILES";
const ROUNDS_ENV: &str = "LIX_EXECUTE_BATCH_BENCH_ROUNDS";
const WARMUPS_ENV: &str = "LIX_EXECUTE_BATCH_BENCH_WARMUPS";

#[derive(Clone)]
struct CountingStorage {
    inner: Memory,
    counters: Arc<StorageCounters>,
}

struct CountingRead {
    inner: MemoryRead,
    counters: Arc<StorageCounters>,
}

#[derive(Default)]
struct StorageCounters {
    begin_reads: AtomicU64,
    get_many_calls: AtomicU64,
    scan_calls: AtomicU64,
}

#[derive(Clone, Copy, Default)]
struct CounterSnapshot {
    begin_reads: u64,
    get_many_calls: u64,
    scan_calls: u64,
}

struct BenchStatement {
    sql: &'static str,
    params: Vec<Value>,
}

impl CountingStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            counters: Arc::new(StorageCounters::default()),
        }
    }

    fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            begin_reads: self.counters.begin_reads.load(Ordering::Relaxed),
            get_many_calls: self.counters.get_many_calls.load(Ordering::Relaxed),
            scan_calls: self.counters.scan_calls.load(Ordering::Relaxed),
        }
    }
}

impl Storage for CountingStorage {
    type Read<'a>
        = CountingRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.counters.begin_reads.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            counters: Arc::clone(&self.counters),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

impl StorageRead for CountingRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        options: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.counters.get_many_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_many(space, keys, options).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.counters.scan_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.scan(space, range, options).await
    }
}

impl CounterSnapshot {
    fn delta(self, before: Self) -> Self {
        Self {
            begin_reads: self.begin_reads - before.begin_reads,
            get_many_calls: self.get_many_calls - before.get_many_calls,
            scan_calls: self.scan_calls - before.scan_calls,
        }
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual performance probe; run with --ignored --nocapture"]
async fn execute_batch_benchmark_probe() {
    let file_count = parse_usize_env(FILE_COUNT_ENV, 10_000);
    let rounds = parse_usize_env(ROUNDS_ENV, 200);
    let warmups = parse_usize_env(WARMUPS_ENV, 20);
    assert!(file_count > 0);
    assert!(rounds > 0);

    let storage = CountingStorage::new();
    Engine::initialize(storage.clone()).await.unwrap();
    let engine = Engine::new(storage.clone()).await.unwrap();
    let session = engine.open_workspace_session().await.unwrap();
    seed_files(&session, file_count).await;

    for (workload, statements) in [
        ("setup_only", setup_statements()),
        ("branch_file_reads", realistic_statements(file_count)),
    ] {
        for statement_count in [1, 3, 5] {
            run_case(
                &session,
                &storage,
                workload,
                &statements[..statement_count],
                warmups,
                rounds,
            )
            .await;
        }
    }
}

async fn run_case(
    session: &SessionContext<CountingStorage>,
    storage: &CountingStorage,
    workload: &str,
    statements: &[BenchStatement],
    warmups: usize,
    rounds: usize,
) {
    let statements = statements
        .iter()
        .map(|statement| ExecuteBatchStatement {
            sql: statement.sql.to_string(),
            params: statement.params.clone(),
        })
        .collect::<Vec<_>>();

    for _ in 0..warmups {
        black_box(session.execute_batch(&statements).await.unwrap());
    }

    let mut durations = Vec::with_capacity(rounds);
    let mut counters = CounterSnapshot::default();
    for _ in 0..rounds {
        let before = storage.snapshot();
        let started = Instant::now();
        black_box(session.execute_batch(&statements).await.unwrap());
        durations.push(started.elapsed());
        let delta = storage.snapshot().delta(before);
        counters.begin_reads += delta.begin_reads;
        counters.get_many_calls += delta.get_many_calls;
        counters.scan_calls += delta.scan_calls;
    }
    durations.sort_unstable();

    println!(
        "execute_batch_bench workload={workload} files={} statements={} rounds={rounds} warmups={warmups} p50_ns={} p95_ns={} begin_reads_per_op={} get_many_calls_per_op={} scan_calls_per_op={}",
        parse_usize_env(FILE_COUNT_ENV, 10_000),
        statements.len(),
        percentile(&durations, 50).as_nanos(),
        percentile(&durations, 95).as_nanos(),
        format_per_operation(counters.begin_reads, rounds),
        format_per_operation(counters.get_many_calls, rounds),
        format_per_operation(counters.scan_calls, rounds),
    );
}

fn setup_statements() -> Vec<BenchStatement> {
    (0..5)
        .map(|ordinal| BenchStatement {
            sql: "SELECT $1 AS ordinal",
            params: vec![Value::Integer(ordinal)],
        })
        .collect()
}

fn realistic_statements(file_count: usize) -> Vec<BenchStatement> {
    let middle = file_count / 2;
    let last = file_count - 1;
    vec![
        BenchStatement {
            sql: "SELECT id, path FROM lix_file WHERE path = $1",
            params: vec![Value::Text(file_path(middle))],
        },
        BenchStatement {
            sql: "SELECT id, path FROM lix_file WHERE id = $1",
            params: vec![Value::Text(file_id(last))],
        },
        BenchStatement {
            sql: "SELECT id, commit_id FROM lix_branch WHERE id = $1",
            params: vec![Value::Text("main".to_string())],
        },
        BenchStatement {
            sql: "SELECT path FROM lix_directory ORDER BY path",
            params: Vec::new(),
        },
        BenchStatement {
            sql: "SELECT id, path FROM lix_file WHERE path >= $1 ORDER BY path LIMIT 16",
            params: vec![Value::Text(file_path(middle))],
        },
    ]
}

async fn seed_files(session: &SessionContext<CountingStorage>, file_count: usize) {
    let mut sql = String::from("INSERT INTO lix_file (id, path, data) VALUES ");
    for index in 0..file_count {
        if index > 0 {
            sql.push(',');
        }
        sql.push_str("('");
        sql.push_str(&file_id(index));
        sql.push_str("','");
        sql.push_str(&file_path(index));
        sql.push_str("',X'62656E6368')");
    }
    let result = session.execute(&sql, &[]).await.unwrap();
    assert_eq!(
        result.rows_affected(),
        u64::try_from(file_count).expect("file count should fit in u64")
    );
}

fn file_id(index: usize) -> String {
    format!("execute-batch-bench-{index:08}")
}

fn file_path(index: usize) -> String {
    format!("/execute-batch-bench/{index:08}.txt")
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|error| panic!("invalid {name}: {error}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("failed to read {name}: {error}"),
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn format_per_operation(total: u64, rounds: usize) -> String {
    let rounds = u64::try_from(rounds).expect("round count should fit in u64");
    format!(
        "{}.{:03}",
        total / rounds,
        (total % rounds) * 1_000 / rounds
    )
}
