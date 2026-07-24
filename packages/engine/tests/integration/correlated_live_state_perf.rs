use std::collections::BTreeSet;
use std::env;
use std::ffi::OsString;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lix_engine::storage::{
    GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead, MemoryWrite, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, Storage, StorageError, StorageRead, WriteOptions,
};
use lix_engine::{Engine, ExecuteResult, SessionContext, Value};
use serde::Serialize;

const FILES_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_FILES";
const BATCH_SIZES_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_BATCH_SIZES";
const OPERATIONS_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_OPERATIONS";
const WARMUPS_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_WARMUPS";
const SAMPLES_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_SAMPLES";
const SETUP_CHUNK_SIZE_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_SETUP_CHUNK_SIZE";
const SEED_INPUT_PATH_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_SEED_INPUT_PATH";
const SEED_OUTPUT_PATH_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_SEED_OUTPUT_PATH";
const PROFILE_READY_PATH_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_PROFILE_READY_PATH";
const PROFILE_GO_PATH_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_PROFILE_GO_PATH";
const PROFILE_ITERATIONS_ENV: &str = "LIX_CORRELATED_LIVE_STATE_PERF_PROFILE_ITERATIONS";

// Seed input bypasses setup; seed output persists the exact bytes used by the run.
// Setting any profile variable enables SELECT-only profile mode and requires all three.

const DEFAULT_FILES: usize = 10_000;
const DEFAULT_BATCH_SIZES: &str = "1,10,32,33,100";
const DEFAULT_OPERATIONS: &str = "all";
const DEFAULT_WARMUPS: usize = 10;
const DEFAULT_SAMPLES: usize = 100;
const DEFAULT_SETUP_CHUNK_SIZE: usize = 500;

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual release-mode performance harness"]
async fn correlated_live_state_sql_perf() {
    let config = Config::from_env();
    let ids = file_ids(config.files);
    let seed_snapshot = prepare_seed_snapshot(&config, &ids).await;
    let mut measurements = Vec::with_capacity(config.batch_sizes.len() * config.operations.len());
    let profile_measurement = if let Some(profile) = config.profile.as_ref() {
        let batch_size = config.batch_sizes[0];
        let selected_ids = &ids[..batch_size];
        Some(
            profile_select(
                &seed_snapshot,
                selected_ids,
                &select_sql(selected_ids),
                config.warmups,
                profile,
            )
            .await,
        )
    } else {
        for &batch_size in &config.batch_sizes {
            let selected_ids = &ids[..batch_size];
            let select_sql = select_sql(selected_ids);
            let update_sql = update_sql(selected_ids);

            if config.operations.contains(&Operation::Select) {
                measurements.push(
                    measure_select(
                        &seed_snapshot,
                        selected_ids,
                        &select_sql,
                        config.warmups,
                        config.samples,
                    )
                    .await,
                );
            }
            if config.operations.contains(&Operation::Update) {
                measurements.push(
                    measure_update(
                        &seed_snapshot,
                        selected_ids,
                        &select_sql,
                        &update_sql,
                        config.warmups,
                        config.samples,
                    )
                    .await,
                );
            }
        }
        None
    };

    let report = Report {
        benchmark: "correlated_live_state_sql",
        config,
        measurements,
        profile_measurement,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("performance report should serialize")
    );
}

#[derive(Debug, Serialize)]
struct Config {
    files: usize,
    batch_sizes: Vec<usize>,
    operations: Vec<Operation>,
    warmups: usize,
    samples: usize,
    setup_chunk_size: usize,
    seed_input_path: Option<PathBuf>,
    seed_output_path: Option<PathBuf>,
    profile: Option<ProfileConfig>,
}

impl Config {
    fn from_env() -> Self {
        let files = parse_usize_env(FILES_ENV, DEFAULT_FILES);
        let warmups = parse_usize_env(WARMUPS_ENV, DEFAULT_WARMUPS);
        let samples = parse_usize_env(SAMPLES_ENV, DEFAULT_SAMPLES);
        let setup_chunk_size = parse_usize_env(SETUP_CHUNK_SIZE_ENV, DEFAULT_SETUP_CHUNK_SIZE);
        let seed_input_path = optional_path_env(SEED_INPUT_PATH_ENV);
        let seed_output_path = optional_path_env(SEED_OUTPUT_PATH_ENV);
        let profile = ProfileConfig::from_env();
        let batch_sizes = parse_batch_sizes(
            &env::var(BATCH_SIZES_ENV).unwrap_or_else(|_| DEFAULT_BATCH_SIZES.to_string()),
        );
        let operations = parse_operations(
            &env::var(OPERATIONS_ENV).unwrap_or_else(|_| DEFAULT_OPERATIONS.to_string()),
        );

        assert!(files > 0, "{FILES_ENV} must be greater than zero");
        assert!(samples > 0, "{SAMPLES_ENV} must be greater than zero");
        assert!(
            setup_chunk_size > 0,
            "{SETUP_CHUNK_SIZE_ENV} must be greater than zero"
        );
        assert!(
            batch_sizes.iter().all(|&size| size <= files),
            "every {BATCH_SIZES_ENV} value must be at most {FILES_ENV} ({files})"
        );
        if profile.is_some() {
            assert_eq!(
                batch_sizes.len(),
                1,
                "profile mode requires exactly one {BATCH_SIZES_ENV} value"
            );
            assert_eq!(
                operations.as_slice(),
                &[Operation::Select],
                "profile mode requires {OPERATIONS_ENV}=select"
            );
        }

        Self {
            files,
            batch_sizes,
            operations,
            warmups,
            samples,
            setup_chunk_size,
            seed_input_path,
            seed_output_path,
            profile,
        }
    }
}

#[derive(Debug, Serialize)]
struct ProfileConfig {
    ready_path: PathBuf,
    go_path: PathBuf,
    iterations: usize,
}

impl ProfileConfig {
    fn from_env() -> Option<Self> {
        let ready_path = optional_path_env(PROFILE_READY_PATH_ENV);
        let go_path = optional_path_env(PROFILE_GO_PATH_ENV);
        let iterations = env::var_os(PROFILE_ITERATIONS_ENV);

        if ready_path.is_none() && go_path.is_none() && iterations.is_none() {
            return None;
        }

        let ready_path = ready_path.unwrap_or_else(|| {
            panic!("{PROFILE_READY_PATH_ENV} is required when profile mode is enabled")
        });
        let go_path = go_path.unwrap_or_else(|| {
            panic!("{PROFILE_GO_PATH_ENV} is required when profile mode is enabled")
        });
        let iterations = parse_required_usize_env(PROFILE_ITERATIONS_ENV, iterations);

        assert_ne!(
            ready_path, go_path,
            "{PROFILE_READY_PATH_ENV} and {PROFILE_GO_PATH_ENV} must differ"
        );
        assert!(
            iterations > 0,
            "{PROFILE_ITERATIONS_ENV} must be greater than zero"
        );

        Some(Self {
            ready_path,
            go_path,
            iterations,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Select,
    Update,
}

fn parse_usize_env(name: &str, default: usize) -> usize {
    env::var(name).map_or(default, |value| {
        value
            .parse::<usize>()
            .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
    })
}

fn parse_required_usize_env(name: &str, value: Option<OsString>) -> usize {
    let value = value
        .unwrap_or_else(|| panic!("{name} is required when profile mode is enabled"))
        .into_string()
        .unwrap_or_else(|_| panic!("{name} must be valid Unicode"));
    value
        .parse::<usize>()
        .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}"))
}

fn optional_path_env(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn parse_batch_sizes(value: &str) -> Vec<usize> {
    let mut batch_sizes = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<usize>().unwrap_or_else(|error| {
                panic!("{BATCH_SIZES_ENV} contains invalid value {part:?}: {error}")
            })
        })
        .collect::<Vec<_>>();
    assert!(
        !batch_sizes.is_empty(),
        "{BATCH_SIZES_ENV} must contain at least one batch size"
    );
    assert!(
        batch_sizes.iter().all(|&size| size > 0),
        "{BATCH_SIZES_ENV} values must be greater than zero"
    );
    batch_sizes.sort_unstable();
    batch_sizes.dedup();
    batch_sizes
}

fn parse_operations(value: &str) -> Vec<Operation> {
    let mut select = false;
    let mut update = false;
    for operation in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        match operation {
            "all" => {
                select = true;
                update = true;
            }
            "select" => select = true,
            "update" => update = true,
            other => panic!(
                "{OPERATIONS_ENV} contains invalid operation {other:?}; expected select, update, or all"
            ),
        }
    }
    assert!(select || update, "{OPERATIONS_ENV} must not be empty");

    let mut operations = Vec::with_capacity(2);
    if select {
        operations.push(Operation::Select);
    }
    if update {
        operations.push(Operation::Update);
    }
    operations
}

async fn prepare_seed_snapshot(config: &Config, ids: &[String]) -> Vec<u8> {
    let snapshot = if let Some(input_path) = config.seed_input_path.as_ref() {
        let snapshot = fs::read(input_path).unwrap_or_else(|error| {
            panic!(
                "failed to read {SEED_INPUT_PATH_ENV} {}: {error}",
                input_path.display()
            )
        });
        validate_seed_snapshot(&snapshot, config.files).await;
        snapshot
    } else {
        seed_snapshot(config, ids).await
    };

    if let Some(output_path) = config.seed_output_path.as_ref() {
        fs::write(output_path, &snapshot).unwrap_or_else(|error| {
            panic!(
                "failed to write {SEED_OUTPUT_PATH_ENV} {}: {error}",
                output_path.display()
            )
        });
    }

    snapshot
}

async fn seed_snapshot(config: &Config, ids: &[String]) -> Vec<u8> {
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

    for chunk in ids.chunks(config.setup_chunk_size) {
        let values = chunk
            .iter()
            .map(|id| {
                let suffix = id_index(id);
                format!("('{id}', '/bench-{suffix:08}.bin', X'00')")
            })
            .collect::<Vec<_>>()
            .join(",");
        let result = session
            .execute(
                &format!("INSERT INTO lix_file (id, path, data) VALUES {values}"),
                &[],
            )
            .await
            .expect("benchmark files should insert");
        assert_eq!(
            usize::try_from(result.rows_affected()).expect("affected row count should fit usize"),
            chunk.len(),
            "setup insert should affect every requested file"
        );
    }

    let count = session
        .execute("SELECT id FROM lix_file", &[])
        .await
        .expect("setup row count should be readable");
    assert_eq!(count.len(), config.files, "setup should seed every file");
    session.close().await.expect("setup session should close");
    storage
        .export_snapshot()
        .expect("seeded Memory storage should export")
}

async fn validate_seed_snapshot(snapshot: &[u8], expected_files: usize) {
    let (_, session) = open_case(snapshot).await;
    let rows = session
        .execute("SELECT id FROM lix_file", &[])
        .await
        .expect("input seed row count should be readable");
    assert_eq!(
        rows.len(),
        expected_files,
        "{SEED_INPUT_PATH_ENV} should contain exactly {expected_files} files"
    );
    session
        .close()
        .await
        .expect("input seed validation session should close");
}

fn file_ids(count: usize) -> Vec<String> {
    (0..count)
        .map(|index| format!("bench-file-{index:08}"))
        .collect()
}

fn id_index(id: &str) -> usize {
    id.rsplit_once('-')
        .expect("benchmark id should contain a numeric suffix")
        .1
        .parse()
        .expect("benchmark id suffix should be numeric")
}

fn quoted_ids(ids: &[String]) -> String {
    ids.iter()
        .map(|id| format!("'{id}'"))
        .collect::<Vec<_>>()
        .join(",")
}

fn select_sql(ids: &[String]) -> String {
    format!(
        "SELECT id, data FROM lix_file WHERE id IN ({})",
        quoted_ids(ids)
    )
}

fn update_sql(ids: &[String]) -> String {
    format!(
        "UPDATE lix_file SET data = $1 WHERE id IN ({})",
        quoted_ids(ids)
    )
}

async fn open_case(seed_snapshot: &[u8]) -> (CountingStorage, SessionContext<CountingStorage>) {
    let storage = CountingStorage::from_snapshot(seed_snapshot)
        .expect("benchmark seed snapshot should reopen");
    let engine = Engine::new(storage.clone())
        .await
        .expect("benchmark engine should open from seed snapshot");
    let session = engine
        .open_workspace_session()
        .await
        .expect("benchmark session should open from seed snapshot");
    (storage, session)
}

async fn profile_select(
    seed_snapshot: &[u8],
    ids: &[String],
    sql: &str,
    warmups: usize,
    profile: &ProfileConfig,
) -> ProfileMeasurement {
    let (_, session) = open_case(seed_snapshot).await;

    for _ in 0..warmups {
        session
            .execute(sql, &[])
            .await
            .expect("profile select warmup should succeed");
    }
    let validation = session
        .execute(sql, &[])
        .await
        .expect("profile select validation should succeed");
    validate_data(&validation, ids, &[0]);

    assert!(
        !profile.ready_path.exists(),
        "remove stale {PROFILE_READY_PATH_ENV} marker {}",
        profile.ready_path.display()
    );
    assert!(
        !profile.go_path.exists(),
        "remove stale {PROFILE_GO_PATH_ENV} marker {}",
        profile.go_path.display()
    );
    fs::write(&profile.ready_path, format!("{}\n", std::process::id())).unwrap_or_else(|error| {
        panic!(
            "failed to write {PROFILE_READY_PATH_ENV} {}: {error}",
            profile.ready_path.display()
        )
    });
    while !profile.go_path.exists() {
        std::thread::sleep(Duration::from_millis(10));
    }

    let mut final_result = None;
    for _ in 0..profile.iterations {
        final_result = Some(
            session
                .execute(sql, &[])
                .await
                .expect("profile select should succeed"),
        );
    }
    validate_data(
        final_result
            .as_ref()
            .expect("profile mode requires at least one iteration"),
        ids,
        &[0],
    );

    session
        .close()
        .await
        .expect("profile select session should close");
    ProfileMeasurement {
        operation: "select_data_profile",
        batch_size: ids.len(),
        iterations: profile.iterations,
    }
}

async fn measure_select(
    seed_snapshot: &[u8],
    ids: &[String],
    sql: &str,
    warmups: usize,
    samples: usize,
) -> Measurement {
    let (storage, session) = open_case(seed_snapshot).await;
    for _ in 0..warmups {
        let result = session
            .execute(sql, &[])
            .await
            .expect("select warmup should succeed");
        validate_data(&result, ids, &[0]);
    }

    let mut observations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let before = storage.stats();
        let started = Instant::now();
        let result = session
            .execute(sql, &[])
            .await
            .expect("timed select should succeed");
        let duration_ns = elapsed_ns(started);
        let storage_delta = storage.stats().delta_since(before);
        validate_data(&result, ids, &[0]);
        observations.push(Observation {
            duration_ns,
            storage: storage_delta,
        });
    }

    session.close().await.expect("select session should close");
    Measurement::from_observations("select_data", ids.len(), observations)
}

async fn measure_update(
    seed_snapshot: &[u8],
    ids: &[String],
    select_sql: &str,
    update_sql: &str,
    warmups: usize,
    samples: usize,
) -> Measurement {
    let (storage, session) = open_case(seed_snapshot).await;
    for iteration in 0..warmups {
        let data = update_data(iteration);
        let result = session
            .execute(update_sql, &[Value::Blob(data.clone().into())])
            .await
            .expect("update warmup should succeed");
        assert_eq!(
            usize::try_from(result.rows_affected()).expect("affected row count should fit usize"),
            ids.len()
        );
        validate_selected_data(&session, select_sql, ids, &data).await;
    }

    let mut observations = Vec::with_capacity(samples);
    for sample in 0..samples {
        let data = update_data(warmups + sample);
        let before = storage.stats();
        let started = Instant::now();
        let result = session
            .execute(update_sql, &[Value::Blob(data.clone().into())])
            .await
            .expect("timed update should succeed");
        let duration_ns = elapsed_ns(started);
        let storage_delta = storage.stats().delta_since(before);
        assert_eq!(
            usize::try_from(result.rows_affected()).expect("affected row count should fit usize"),
            ids.len(),
            "timed update should affect every requested file"
        );
        validate_selected_data(&session, select_sql, ids, &data).await;
        observations.push(Observation {
            duration_ns,
            storage: storage_delta,
        });
    }

    session.close().await.expect("update session should close");
    Measurement::from_observations("update_data_end_to_end", ids.len(), observations)
}

fn update_data(iteration: usize) -> Vec<u8> {
    vec![u8::try_from(iteration % 2 + 1).expect("update byte should fit")]
}

async fn validate_selected_data(
    session: &SessionContext<CountingStorage>,
    select_sql: &str,
    ids: &[String],
    expected_data: &[u8],
) {
    let result = session
        .execute(select_sql, &[])
        .await
        .expect("updated data should be readable");
    validate_data(&result, ids, expected_data);
}

fn validate_data(result: &ExecuteResult, ids: &[String], expected_data: &[u8]) {
    assert_eq!(
        result.len(),
        ids.len(),
        "select should return every requested file"
    );
    let expected = ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();

    for row in result.rows() {
        let [Value::Text(id), Value::Blob(data)] = row.values() else {
            panic!("expected [text id, blob data], got {:?}", row.values());
        };
        assert!(expected.contains(id.as_str()), "unexpected file id {id}");
        assert_eq!(data, expected_data, "unexpected data for {id}");
        assert!(seen.insert(id.as_str()), "duplicate file id {id}");
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).expect("sample duration should fit in u64")
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Observation {
    duration_ns: u64,
    storage: StorageStats,
}

#[derive(Debug, Serialize)]
struct Report {
    benchmark: &'static str,
    config: Config,
    measurements: Vec<Measurement>,
    profile_measurement: Option<ProfileMeasurement>,
}

#[derive(Debug, Serialize)]
struct ProfileMeasurement {
    operation: &'static str,
    batch_size: usize,
    iterations: usize,
}

#[derive(Debug, Serialize)]
struct Measurement {
    operation: &'static str,
    batch_size: usize,
    duration_ns: Percentiles,
    duration_ns_chronological_halves: Option<ChronologicalPercentiles>,
    duration_ns_per_row: Percentiles,
    storage_per_sample: StoragePercentiles,
    raw_observations: Vec<Observation>,
}

impl Measurement {
    fn from_observations(
        operation: &'static str,
        batch_size: usize,
        observations: Vec<Observation>,
    ) -> Self {
        let duration_ns = observations
            .iter()
            .map(|observation| observation.duration_ns)
            .collect::<Vec<_>>();
        let divisor = u64::try_from(batch_size).expect("batch size should fit in u64");
        let duration_ns_per_row = duration_ns
            .iter()
            .map(|duration| duration / divisor)
            .collect::<Vec<_>>();
        let duration_ns_chronological_halves = chronological_halves(&duration_ns);

        Self {
            operation,
            batch_size,
            duration_ns: percentiles(duration_ns),
            duration_ns_chronological_halves,
            duration_ns_per_row: percentiles(duration_ns_per_row),
            storage_per_sample: StoragePercentiles {
                begin_read_calls: percentiles(
                    observations
                        .iter()
                        .map(|observation| observation.storage.begin_read_calls)
                        .collect(),
                ),
                get_many_calls: percentiles(
                    observations
                        .iter()
                        .map(|observation| observation.storage.get_many_calls)
                        .collect(),
                ),
                get_many_requested_keys: percentiles(
                    observations
                        .iter()
                        .map(|observation| observation.storage.get_many_requested_keys)
                        .collect(),
                ),
                scan_calls: percentiles(
                    observations
                        .iter()
                        .map(|observation| observation.storage.scan_calls)
                        .collect(),
                ),
            },
            raw_observations: observations,
        }
    }
}

#[derive(Debug, Serialize)]
struct StoragePercentiles {
    begin_read_calls: Percentiles,
    get_many_calls: Percentiles,
    get_many_requested_keys: Percentiles,
    scan_calls: Percentiles,
}

#[derive(Debug, Serialize)]
struct Percentiles {
    p50: u64,
    p95: u64,
}

#[derive(Debug, Serialize)]
struct ChronologicalPercentiles {
    first: Percentiles,
    second: Percentiles,
}

fn chronological_halves(values: &[u64]) -> Option<ChronologicalPercentiles> {
    if values.len() < 2 {
        return None;
    }
    let midpoint = values.len().div_ceil(2);
    Some(ChronologicalPercentiles {
        first: percentiles(values[..midpoint].to_vec()),
        second: percentiles(values[midpoint..].to_vec()),
    })
}

fn percentiles(mut values: Vec<u64>) -> Percentiles {
    assert!(
        !values.is_empty(),
        "percentiles require at least one sample"
    );
    values.sort_unstable();
    Percentiles {
        p50: nearest_rank(&values, 50),
        p95: nearest_rank(&values, 95),
    }
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let rank = (sorted.len() * percentile).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

#[derive(Clone, Default)]
struct CountingStorage {
    inner: Memory,
    counters: Arc<StorageCounters>,
}

impl CountingStorage {
    fn new() -> Self {
        Self::default()
    }

    fn from_snapshot(snapshot: &[u8]) -> Result<Self, StorageError> {
        Ok(Self {
            inner: Memory::from_snapshot(snapshot)?,
            counters: Arc::new(StorageCounters::default()),
        })
    }

    fn export_snapshot(&self) -> Result<Vec<u8>, StorageError> {
        self.inner.export_snapshot()
    }

    fn stats(&self) -> StorageStats {
        self.counters.snapshot()
    }
}

struct CountingRead {
    inner: MemoryRead,
    counters: Arc<StorageCounters>,
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

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.counters
            .begin_read_calls
            .fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: self.inner.begin_read(opts).await?,
            counters: Arc::clone(&self.counters),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(opts).await
    }
}

impl StorageRead for CountingRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.counters.get_many_calls.fetch_add(1, Ordering::Relaxed);
        self.counters.get_many_requested_keys.fetch_add(
            u64::try_from(keys.len()).expect("requested key count should fit in u64"),
            Ordering::Relaxed,
        );
        self.inner.get_many(space, keys, opts).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.counters.scan_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.scan(space, range, opts).await
    }
}

#[derive(Default)]
struct StorageCounters {
    begin_read_calls: AtomicU64,
    get_many_calls: AtomicU64,
    get_many_requested_keys: AtomicU64,
    scan_calls: AtomicU64,
}

impl StorageCounters {
    fn snapshot(&self) -> StorageStats {
        StorageStats {
            begin_read_calls: self.begin_read_calls.load(Ordering::Relaxed),
            get_many_calls: self.get_many_calls.load(Ordering::Relaxed),
            get_many_requested_keys: self.get_many_requested_keys.load(Ordering::Relaxed),
            scan_calls: self.scan_calls.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct StorageStats {
    begin_read_calls: u64,
    get_many_calls: u64,
    get_many_requested_keys: u64,
    scan_calls: u64,
}

impl StorageStats {
    fn delta_since(self, before: Self) -> Self {
        Self {
            begin_read_calls: self.begin_read_calls - before.begin_read_calls,
            get_many_calls: self.get_many_calls - before.get_many_calls,
            get_many_requested_keys: self.get_many_requested_keys - before.get_many_requested_keys,
            scan_calls: self.scan_calls - before.scan_calls,
        }
    }
}
