//! Manual, deterministic probe for coherent read-batch setup costs.
//!
//! The probe pairs every method in one warmed session and rotates their order
//! each round to reduce drift bias. The default matrix covers 1k/10k files and
//! 1/2/3/5 statements. Override it with the `LIX_COHERENT_READ_BENCH_*`
//! environment variables declared below; pin the test process to one CPU when
//! collecting numbers.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lix_engine::{
    Engine, ExecuteBatchStatement, GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead,
    MemoryWrite, ReadOptions, ScanChunk, ScanOptions, SessionContext, SpaceId, Storage,
    StorageError, StorageRead, Value, WriteOptions,
};

const FILE_COUNTS_ENV: &str = "LIX_COHERENT_READ_BENCH_FILES";
const STATEMENT_COUNTS_ENV: &str = "LIX_COHERENT_READ_BENCH_STATEMENTS";
const ROUNDS_ENV: &str = "LIX_COHERENT_READ_BENCH_ROUNDS";
const WARMUPS_ENV: &str = "LIX_COHERENT_READ_BENCH_WARMUPS";
const METHODS_ENV: &str = "LIX_COHERENT_READ_BENCH_METHODS";

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
    get_many_keys: AtomicU64,
    scan_calls: AtomicU64,
    scan_rows: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
struct CounterSnapshot {
    begin_reads: u64,
    get_many_calls: u64,
    get_many_keys: u64,
    scan_calls: u64,
    scan_rows: u64,
}

#[derive(Clone, Copy, Debug)]
enum Method {
    SequentialStatements,
    SequentialRevisionEquivalent,
    CoherentBatch,
    ExplicitBranchBatch,
}

impl Method {
    const ALL: [Self; 4] = [
        Self::SequentialStatements,
        Self::SequentialRevisionEquivalent,
        Self::CoherentBatch,
        Self::ExplicitBranchBatch,
    ];

    const fn label(self) -> &'static str {
        match self {
            Self::SequentialStatements => "sequential_statements",
            Self::SequentialRevisionEquivalent => "sequential_revision_equivalent",
            Self::CoherentBatch => "coherent_batch",
            Self::ExplicitBranchBatch => "explicit_branch_batch",
        }
    }

    fn parse(label: &str) -> Self {
        match label {
            "sequential_statements" => Self::SequentialStatements,
            "sequential_revision_equivalent" => Self::SequentialRevisionEquivalent,
            "coherent_batch" => Self::CoherentBatch,
            "explicit_branch_batch" => Self::ExplicitBranchBatch,
            other => panic!("unsupported {METHODS_ENV} value {other:?}"),
        }
    }
}

struct BenchStatement {
    sql: &'static str,
    params: Vec<Value>,
}

#[derive(Default)]
struct Samples {
    durations: Vec<Duration>,
    counters: CounterSnapshot,
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
            get_many_keys: self.counters.get_many_keys.load(Ordering::Relaxed),
            scan_calls: self.counters.scan_calls.load(Ordering::Relaxed),
            scan_rows: self.counters.scan_rows.load(Ordering::Relaxed),
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
        self.counters.get_many_keys.fetch_add(
            u64::try_from(keys.len()).expect("get_many key count should fit in u64"),
            Ordering::Relaxed,
        );
        self.inner.get_many(space, keys, options).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.counters.scan_calls.fetch_add(1, Ordering::Relaxed);
        let chunk = self.inner.scan(space, range, options).await?;
        self.counters.scan_rows.fetch_add(
            u64::try_from(chunk.entries.len()).expect("scan row count should fit in u64"),
            Ordering::Relaxed,
        );
        Ok(chunk)
    }
}

impl CounterSnapshot {
    fn delta(self, before: Self) -> Self {
        Self {
            begin_reads: self.begin_reads - before.begin_reads,
            get_many_calls: self.get_many_calls - before.get_many_calls,
            get_many_keys: self.get_many_keys - before.get_many_keys,
            scan_calls: self.scan_calls - before.scan_calls,
            scan_rows: self.scan_rows - before.scan_rows,
        }
    }

    fn add(&mut self, other: Self) {
        self.begin_reads += other.begin_reads;
        self.get_many_calls += other.get_many_calls;
        self.get_many_keys += other.get_many_keys;
        self.scan_calls += other.scan_calls;
        self.scan_rows += other.scan_rows;
    }
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual performance probe; run with --ignored --nocapture"]
async fn coherent_read_batch_benchmark_probe() {
    let file_counts = parse_list_env(FILE_COUNTS_ENV, &[1_000, 10_000]);
    let statement_counts = parse_list_env(STATEMENT_COUNTS_ENV, &[1, 2, 3, 5]);
    let rounds = parse_usize_env(ROUNDS_ENV, 50);
    let warmups = parse_usize_env(WARMUPS_ENV, 5);
    let methods = parse_methods_env();
    assert!(rounds > 0, "{ROUNDS_ENV} must be greater than zero");

    for file_count in file_counts {
        let storage = CountingStorage::new();
        Engine::initialize(storage.clone())
            .await
            .expect("benchmark storage should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("benchmark engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("benchmark session should open");
        seed_files(&session, file_count).await;
        let branch_id = session
            .active_branch_id()
            .await
            .expect("benchmark branch ID should load");

        let setup_statements = setup_statements();
        let realistic_statements = realistic_statements(file_count);
        for (workload, statements) in [
            ("setup_only", &setup_statements),
            ("branch_file_reads", &realistic_statements),
        ] {
            for &statement_count in &statement_counts {
                assert!(
                    (1..=statements.len()).contains(&statement_count),
                    "{STATEMENT_COUNTS_ENV} values must be between 1 and {}",
                    statements.len()
                );
                run_paired_case(
                    &session,
                    &storage,
                    &branch_id,
                    workload,
                    file_count,
                    &statements[..statement_count],
                    &methods,
                    warmups,
                    rounds,
                )
                .await;
            }
        }
    }
}

async fn run_paired_case(
    session: &SessionContext<CountingStorage>,
    storage: &CountingStorage,
    branch_id: &str,
    workload: &str,
    file_count: usize,
    statements: &[BenchStatement],
    methods: &[Method],
    warmups: usize,
    rounds: usize,
) {
    let statement_refs = statements
        .iter()
        .map(|statement| (statement.sql, statement.params.as_slice()))
        .collect::<Vec<_>>();
    let owned_statements = statements
        .iter()
        .map(|statement| ExecuteBatchStatement {
            sql: statement.sql.to_string(),
            params: statement.params.clone(),
        })
        .collect::<Vec<_>>();

    for warmup in 0..warmups {
        for method in method_order(warmup, methods) {
            let _ = measure_once(
                method,
                session,
                storage,
                branch_id,
                &statement_refs,
                &owned_statements,
            )
            .await;
        }
    }

    let mut samples_by_method = methods
        .iter()
        .copied()
        .map(|method| (method, Samples::default()))
        .collect::<Vec<_>>();
    for round in 0..rounds {
        for method in method_order(round, methods) {
            let (duration, counters) = measure_once(
                method,
                session,
                storage,
                branch_id,
                &statement_refs,
                &owned_statements,
            )
            .await;
            let (_, samples) = samples_by_method
                .iter_mut()
                .find(|(candidate, _)| candidate.label() == method.label())
                .expect("every benchmark method should have samples");
            samples.durations.push(duration);
            samples.counters.add(counters);
        }
    }

    for (method, mut samples) in samples_by_method {
        samples.durations.sort_unstable();
        println!(
            "coherent_read_batch_bench workload={workload} files={file_count} statements={} method={} rounds={rounds} warmups={warmups} order=alternating_same_session p50_ns={} p95_ns={} begin_reads_per_op={} get_many_calls_per_op={} get_many_keys_per_op={} scan_calls_per_op={} scan_rows_per_op={}",
            statements.len(),
            method.label(),
            percentile(&samples.durations, 50).as_nanos(),
            percentile(&samples.durations, 95).as_nanos(),
            format_per_operation(samples.counters.begin_reads, rounds),
            format_per_operation(samples.counters.get_many_calls, rounds),
            format_per_operation(samples.counters.get_many_keys, rounds),
            format_per_operation(samples.counters.scan_calls, rounds),
            format_per_operation(samples.counters.scan_rows, rounds),
        );
    }
}

fn method_order(round: usize, methods: &[Method]) -> Vec<Method> {
    let mut methods = methods.to_vec();
    let method_count = methods.len();
    methods.rotate_left(round % method_count);
    methods
}

async fn measure_once(
    method: Method,
    session: &SessionContext<CountingStorage>,
    storage: &CountingStorage,
    branch_id: &str,
    statements: &[(&str, &[Value])],
    owned_statements: &[ExecuteBatchStatement],
) -> (Duration, CounterSnapshot) {
    let before = storage.snapshot();
    let started = Instant::now();
    match method {
        Method::SequentialStatements => {
            let mut results = Vec::with_capacity(statements.len());
            for &(sql, params) in statements {
                results.push(
                    session
                        .execute(sql, params)
                        .await
                        .expect("sequential benchmark statement should execute"),
                );
            }
            black_box(results);
        }
        Method::SequentialRevisionEquivalent => {
            let active_branch_id = session
                .active_branch_id()
                .await
                .expect("active branch ID should load");
            let active_branch_commit_id = session
                .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
                .await
                .expect("active branch commit ID should load");
            let storage_mutation_revision = session
                .storage_mutation_revision()
                .await
                .expect("storage mutation revision should load");
            let mut results = Vec::with_capacity(statements.len());
            for &(sql, params) in statements {
                results.push(
                    session
                        .execute(sql, params)
                        .await
                        .expect("revision-equivalent benchmark statement should execute"),
                );
            }
            black_box((
                active_branch_id,
                active_branch_commit_id,
                storage_mutation_revision,
                results,
            ));
        }
        Method::CoherentBatch => {
            let batch = session
                .execute_coherent_read_batch(statements)
                .await
                .expect("coherent benchmark batch should execute");
            black_box(batch);
        }
        Method::ExplicitBranchBatch => {
            let batch = session
                .execute_read_batch(branch_id, owned_statements)
                .await
                .expect("explicit-branch benchmark batch should execute");
            black_box(batch);
        }
    }
    let duration = started.elapsed();
    let counters = storage.snapshot().delta(before);
    (duration, counters)
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
    let last = file_count.saturating_sub(1);
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
    assert!(
        file_count > 0,
        "benchmark file count must be greater than zero"
    );
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
    let result = session
        .execute(&sql, &[])
        .await
        .expect("benchmark files should seed in one transaction");
    assert_eq!(
        result.rows_affected(),
        u64::try_from(file_count).expect("file count should fit in u64")
    );
}

fn file_id(index: usize) -> String {
    format!("coherent-read-bench-{index:08}")
}

fn file_path(index: usize) -> String {
    format!("/coherent-read-bench/{index:08}.txt")
}

fn parse_list_env(name: &str, default: &[usize]) -> Vec<usize> {
    match std::env::var(name) {
        Ok(raw) => raw
            .split(',')
            .map(|value| {
                value
                    .trim()
                    .parse::<usize>()
                    .unwrap_or_else(|error| panic!("invalid {name} value {value:?}: {error}"))
            })
            .collect(),
        Err(std::env::VarError::NotPresent) => default.to_vec(),
        Err(error) => panic!("failed to read {name}: {error}"),
    }
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(raw) => raw
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("invalid {name} value {raw:?}: {error}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("failed to read {name}: {error}"),
    }
}

fn parse_methods_env() -> Vec<Method> {
    match std::env::var(METHODS_ENV) {
        Ok(raw) => {
            let methods = raw
                .split(',')
                .map(|value| Method::parse(value.trim()))
                .collect::<Vec<_>>();
            assert!(!methods.is_empty(), "{METHODS_ENV} must not be empty");
            methods
        }
        Err(std::env::VarError::NotPresent) => Method::ALL.to_vec(),
        Err(error) => panic!("failed to read {METHODS_ENV}: {error}"),
    }
}

fn percentile(sorted: &[Duration], percentile: usize) -> Duration {
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn format_per_operation(total: u64, rounds: usize) -> String {
    let rounds = u64::try_from(rounds).expect("round count should fit in u64");
    let whole = total / rounds;
    let thousandths = (total % rounds) * 1_000 / rounds;
    format!("{whole}.{thousandths:03}")
}
