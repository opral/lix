use std::collections::BTreeSet;
use std::env;
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

const PREFIX: &str = "LIX_NATIVE_PK_SELECT_PERF_";
const CANONICAL_SEED_ID: &str = "native-primary-key-select-v1";
const DEFAULT_ROWS: usize = 10_000;
const DEFAULT_WARMUPS: usize = 10;
const DEFAULT_SAMPLES: usize = 100;
const DEFAULT_CHUNK_SIZE: usize = 500;
const FILE_DATA: &[u8] = b"native-primary-key-file-payload";

const CASES: [Case; 9] = [
    Case::new("entity_equality", Surface::Entity, 1),
    Case::new("entity_in_10", Surface::Entity, 10),
    Case::new("entity_in_100", Surface::Entity, 100),
    Case::new("file_descriptor_equality", Surface::FileDescriptor, 1),
    Case::new("file_descriptor_in_10", Surface::FileDescriptor, 10),
    Case::new("file_descriptor_in_100", Surface::FileDescriptor, 100),
    Case::new("file_data_equality", Surface::FileData, 1),
    Case::new("file_data_in_10", Surface::FileData, 10),
    Case::new("file_data_in_100", Surface::FileData, 100),
];

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual release-mode performance harness"]
async fn native_primary_key_select_perf() {
    let config = Config::from_env();
    let cases = selected_cases(&config.case_filter);
    assert!(
        cases.iter().all(|case| case.rows <= config.rows),
        "selected cases must request at most {} rows",
        config.rows
    );
    if config.profile.is_some() {
        assert_eq!(cases.len(), 1, "profile mode requires exactly one case");
    }

    let fixtures = Fixtures::new(config.rows);
    let snapshot = prepare_snapshot(&config, &fixtures).await;
    let seed = SeedMetadata {
        canonical_seed_id: CANONICAL_SEED_ID,
        rows_per_surface: config.rows,
        snapshot_bytes: snapshot.len(),
        snapshot_blake3: blake3::hash(&snapshot).to_hex().to_string(),
    };

    let (measurements, profile) = match config.profile.as_ref() {
        Some(profile) => (
            Vec::new(),
            Some(
                profile_case(
                    &snapshot,
                    &fixtures,
                    config.route,
                    cases[0],
                    config.warmups,
                    profile,
                )
                .await,
            ),
        ),
        None => {
            let mut measurements = Vec::with_capacity(cases.len());
            for case in cases {
                measurements.push(
                    measure_case(
                        &snapshot,
                        &fixtures,
                        config.route,
                        case,
                        config.warmups,
                        config.samples,
                    )
                    .await,
                );
            }
            (measurements, None)
        }
    };

    let report_output_path = config.report_output_path.clone();
    let report = serde_json::to_string_pretty(&Report {
        benchmark: "native_primary_key_select",
        report_version: 1,
        build: BuildMetadata {
            debug_assertions: cfg!(debug_assertions),
            build_id: env_value("BUILD_ID"),
        },
        seed,
        config,
        measurements,
        profile,
    })
    .expect("report should serialize");
    if let Some(path) = report_output_path {
        fs::write(path, &report).expect("performance report should be writable");
    }
    println!("{report}");
}

#[derive(Debug, Serialize)]
struct Config {
    rows: usize,
    case_filter: String,
    route: Route,
    warmups: usize,
    samples: usize,
    setup_chunk_size: usize,
    seed_input_path: Option<PathBuf>,
    seed_output_path: Option<PathBuf>,
    report_output_path: Option<PathBuf>,
    profile: Option<ProfileConfig>,
    allow_debug: bool,
}

impl Config {
    fn from_env() -> Self {
        let allow_debug = env_value("ALLOW_DEBUG").as_deref() == Some("1");
        assert!(
            !cfg!(debug_assertions) || allow_debug,
            "performance measurements require --release; set {PREFIX}ALLOW_DEBUG=1 only for smoke validation"
        );
        let config = Self {
            rows: usize_env("ROWS", DEFAULT_ROWS),
            case_filter: env_value("CASES").unwrap_or_else(|| "all".to_string()),
            route: Route::from_env(),
            warmups: usize_env("WARMUPS", DEFAULT_WARMUPS),
            samples: usize_env("SAMPLES", DEFAULT_SAMPLES),
            setup_chunk_size: usize_env("SETUP_CHUNK_SIZE", DEFAULT_CHUNK_SIZE),
            seed_input_path: path_env("SEED_INPUT_PATH"),
            seed_output_path: path_env("SEED_OUTPUT_PATH"),
            report_output_path: path_env("REPORT_OUTPUT_PATH"),
            profile: ProfileConfig::from_env(),
            allow_debug,
        };
        assert!(config.rows > 0, "{PREFIX}ROWS must be positive");
        assert!(config.samples > 0, "{PREFIX}SAMPLES must be positive");
        assert!(
            config.setup_chunk_size > 0,
            "{PREFIX}SETUP_CHUNK_SIZE must be positive"
        );
        config
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
enum Route {
    #[serde(rename = "auto")]
    Auto,
}

impl Route {
    fn from_env() -> Self {
        match env_value("ROUTE").as_deref().unwrap_or("auto") {
            "auto" => Self::Auto,
            route => panic!("unsupported {PREFIX}ROUTE {route:?}; supported route: auto"),
        }
    }
}

#[derive(Debug, Serialize)]
struct ProfileConfig {
    ready_path: PathBuf,
    go_path: PathBuf,
    done_path: PathBuf,
    acknowledged_path: PathBuf,
    iterations: usize,
}

impl ProfileConfig {
    fn from_env() -> Option<Self> {
        let ready_path = path_env("PROFILE_READY_PATH");
        let go_path = path_env("PROFILE_GO_PATH");
        let done_path = path_env("PROFILE_DONE_PATH");
        let acknowledged_path = path_env("PROFILE_ACKNOWLEDGED_PATH");
        let iterations = env_value("PROFILE_ITERATIONS");
        if ready_path.is_none()
            && go_path.is_none()
            && done_path.is_none()
            && acknowledged_path.is_none()
            && iterations.is_none()
        {
            return None;
        }
        let config = Self {
            ready_path: ready_path.expect("profile mode requires PROFILE_READY_PATH"),
            go_path: go_path.expect("profile mode requires PROFILE_GO_PATH"),
            done_path: done_path.expect("profile mode requires PROFILE_DONE_PATH"),
            acknowledged_path: acknowledged_path
                .expect("profile mode requires PROFILE_ACKNOWLEDGED_PATH"),
            iterations: iterations
                .expect("profile mode requires PROFILE_ITERATIONS")
                .parse()
                .expect("PROFILE_ITERATIONS must be an unsigned integer"),
        };
        assert!(config.iterations > 0, "PROFILE_ITERATIONS must be positive");
        let paths = [
            &config.ready_path,
            &config.go_path,
            &config.done_path,
            &config.acknowledged_path,
        ];
        for (index, left) in paths.iter().enumerate() {
            assert!(
                paths[index + 1..].iter().all(|right| left != right),
                "profile paths must all differ"
            );
        }
        Some(config)
    }
}

fn env_value(suffix: &str) -> Option<String> {
    env::var(format!("{PREFIX}{suffix}"))
        .ok()
        .filter(|value| !value.is_empty())
}

fn usize_env(suffix: &str, default: usize) -> usize {
    env_value(suffix).map_or(default, |value| {
        value
            .parse()
            .unwrap_or_else(|error| panic!("{PREFIX}{suffix} must be an unsigned integer: {error}"))
    })
}

fn path_env(suffix: &str) -> Option<PathBuf> {
    env_value(suffix).map(PathBuf::from)
}

#[derive(Clone, Copy, Debug)]
struct Case {
    name: &'static str,
    surface: Surface,
    rows: usize,
}

impl Case {
    const fn new(name: &'static str, surface: Surface, rows: usize) -> Self {
        Self {
            name,
            surface,
            rows,
        }
    }

    fn query(self, fixtures: &Fixtures) -> Query {
        let all_ids = match self.surface {
            Surface::Entity => &fixtures.entity_ids,
            Surface::FileDescriptor | Surface::FileData => &fixtures.file_ids,
        };
        let ids = spread_ids(all_ids, self.rows);
        let (table, projection) = match self.surface {
            Surface::Entity => ("perf_item", "id, payload"),
            Surface::FileDescriptor => ("lix_file", "id, path"),
            Surface::FileData => ("lix_file", "id, data"),
        };
        let predicate = if self.rows == 1 {
            "id = $1".to_string()
        } else {
            let placeholders = (1..=self.rows)
                .map(|index| format!("${index}"))
                .collect::<Vec<_>>()
                .join(",");
            format!("id IN ({placeholders})")
        };
        Query {
            sql: format!("SELECT {projection} FROM {table} WHERE {predicate}"),
            params: ids.iter().cloned().map(Value::Text).collect(),
            expected_ids: ids,
        }
    }
}

fn spread_ids(ids: &[String], count: usize) -> Vec<String> {
    assert!(count > 0 && count <= ids.len());
    if count == 1 {
        return vec![ids[ids.len() / 2].clone()];
    }
    (0..count)
        .map(|index| ids[index * (ids.len() - 1) / (count - 1)].clone())
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Surface {
    Entity,
    FileDescriptor,
    FileData,
}

fn selected_cases(filter: &str) -> Vec<Case> {
    if filter == "all" {
        return CASES.to_vec();
    }
    let names = filter
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .collect::<BTreeSet<_>>();
    assert!(!names.is_empty(), "{PREFIX}CASES must not be empty");
    let selected = CASES
        .iter()
        .copied()
        .filter(|case| names.contains(case.name))
        .collect::<Vec<_>>();
    assert_eq!(
        selected.len(),
        names.len(),
        "{PREFIX}CASES contains an unknown case; valid cases: {}",
        CASES
            .iter()
            .map(|case| case.name)
            .collect::<Vec<_>>()
            .join(",")
    );
    selected
}

struct Fixtures {
    entity_ids: Vec<String>,
    file_ids: Vec<String>,
}

impl Fixtures {
    fn new(rows: usize) -> Self {
        Self {
            entity_ids: (0..rows)
                .map(|index| format!("perf-entity-{index:08}"))
                .collect(),
            file_ids: (0..rows)
                .map(|index| format!("perf-file-{index:08}"))
                .collect(),
        }
    }
}

struct Query {
    sql: String,
    params: Vec<Value>,
    expected_ids: Vec<String>,
}

async fn prepare_snapshot(config: &Config, fixtures: &Fixtures) -> Vec<u8> {
    let snapshot = match config.seed_input_path.as_ref() {
        Some(path) => fs::read(path).expect("seed snapshot should be readable"),
        None => seed_snapshot(config, fixtures).await,
    };
    validate_snapshot(&snapshot, config.rows).await;
    if let Some(path) = config.seed_output_path.as_ref() {
        fs::write(path, &snapshot).expect("seed snapshot should be writable");
    }
    snapshot
}

async fn seed_snapshot(config: &Config, fixtures: &Fixtures) -> Vec<u8> {
    let storage = CountingStorage::new();
    Engine::initialize(storage.clone())
        .await
        .expect("benchmark storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("session should open");
    session
        .execute(
            "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (\
             lix_json('{\"x-lix-key\":\"perf_item\",\"x-lix-primary-key\":[\"/id\"],\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"payload\":{\"type\":\"string\"}},\"required\":[\"id\",\"payload\"],\"additionalProperties\":false}'),\
             false, false)",
            &[],
        )
        .await
        .expect("perf_item schema should register");

    for ids in fixtures.entity_ids.chunks(config.setup_chunk_size) {
        let values = ids
            .iter()
            .map(|id| format!("('{id}', '{}')", entity_payload(id)))
            .collect::<Vec<_>>()
            .join(",");
        insert_rows(&session, "perf_item", "id, payload", values, ids.len()).await;
    }
    let data_hex = FILE_DATA
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    for ids in fixtures.file_ids.chunks(config.setup_chunk_size) {
        let values = ids
            .iter()
            .map(|id| format!("('{id}', '{}', X'{data_hex}')", file_path(id)))
            .collect::<Vec<_>>()
            .join(",");
        insert_rows(&session, "lix_file", "id, path, data", values, ids.len()).await;
    }
    session.close().await.expect("setup session should close");
    storage.export_snapshot().expect("seed should export")
}

async fn insert_rows(
    session: &SessionContext<CountingStorage>,
    table: &str,
    columns: &str,
    values: String,
    expected: usize,
) {
    let result = session
        .execute(
            &format!("INSERT INTO {table} ({columns}) VALUES {values}"),
            &[],
        )
        .await
        .unwrap_or_else(|error| panic!("{table} setup insert should succeed: {error}"));
    assert_eq!(
        usize::try_from(result.rows_affected()).expect("row count should fit usize"),
        expected
    );
}

async fn validate_snapshot(snapshot: &[u8], expected: usize) {
    let (_, session) = open_case(snapshot).await;
    for table in ["perf_item", "lix_file"] {
        let rows = session
            .execute(&format!("SELECT id FROM {table}"), &[])
            .await
            .unwrap_or_else(|error| panic!("{table} seed should be readable: {error}"));
        assert_eq!(rows.len(), expected, "unexpected {table} seed row count");
    }
    session
        .close()
        .await
        .expect("validation session should close");
}

fn entity_payload(id: &str) -> String {
    format!("payload-{}", suffix(id))
}

fn file_path(id: &str) -> String {
    format!("/bench/{}.bin", suffix(id))
}

fn suffix(id: &str) -> &str {
    id.rsplit_once('-')
        .expect("fixture id should have suffix")
        .1
}

async fn open_case(snapshot: &[u8]) -> (CountingStorage, SessionContext<CountingStorage>) {
    let storage = CountingStorage::from_snapshot(snapshot).expect("seed should reopen");
    let engine = Engine::new(storage.clone())
        .await
        .expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("session should open");
    (storage, session)
}

async fn execute_query(
    session: &SessionContext<CountingStorage>,
    route: Route,
    query: &Query,
) -> ExecuteResult {
    // Future native routes only need another arm here; cases and evidence stay identical.
    match route {
        Route::Auto => session
            .execute(&query.sql, &query.params)
            .await
            .expect("primary-key SELECT should succeed"),
    }
}

async fn measure_case(
    snapshot: &[u8],
    fixtures: &Fixtures,
    route: Route,
    case: Case,
    warmups: usize,
    samples: usize,
) -> Measurement {
    let query = case.query(fixtures);
    let (storage, session) = open_case(snapshot).await;
    for _ in 0..warmups {
        validate(case, &query, &execute_query(&session, route, &query).await);
    }
    let mut observations = Vec::with_capacity(samples);
    for _ in 0..samples {
        let before = storage.stats();
        let started = Instant::now();
        let result = execute_query(&session, route, &query).await;
        let row_count = std::hint::black_box(result.len());
        drop(result);
        let duration_ns = elapsed_ns(started);
        let storage = storage.stats().delta_since(before);
        assert_eq!(row_count, case.rows, "unexpected timed SELECT row count");
        observations.push(Observation {
            duration_ns,
            storage,
        });
    }
    validate(case, &query, &execute_query(&session, route, &query).await);
    session.close().await.expect("case session should close");
    Measurement::new(case, route, query.sql, observations)
}

async fn profile_case(
    snapshot: &[u8],
    fixtures: &Fixtures,
    route: Route,
    case: Case,
    warmups: usize,
    profile: &ProfileConfig,
) -> ProfileMeasurement {
    let query = case.query(fixtures);
    let (storage, session) = open_case(snapshot).await;
    for _ in 0..warmups {
        validate(case, &query, &execute_query(&session, route, &query).await);
    }
    assert!(!profile.ready_path.exists(), "remove stale ready marker");
    assert!(!profile.go_path.exists(), "remove stale go marker");
    assert!(!profile.done_path.exists(), "remove stale done marker");
    assert!(
        !profile.acknowledged_path.exists(),
        "remove stale acknowledged marker"
    );
    fs::write(&profile.ready_path, format!("{}\n", std::process::id()))
        .expect("ready marker should be writable");
    while !profile.go_path.exists() {
        std::thread::sleep(Duration::from_millis(10));
    }

    let before = storage.stats();
    let started = Instant::now();
    let mut row_count = 0;
    for _ in 0..profile.iterations {
        let result = execute_query(&session, route, &query).await;
        row_count = std::hint::black_box(result.len());
        drop(result);
    }
    let duration_ns = elapsed_ns(started);
    let storage = storage.stats().delta_since(before);
    assert_eq!(row_count, case.rows, "unexpected profiled SELECT row count");
    fs::write(&profile.done_path, format!("{}\n", std::process::id()))
        .expect("done marker should be writable");
    while !profile.acknowledged_path.exists() {
        std::thread::sleep(Duration::from_millis(10));
    }
    validate(case, &query, &execute_query(&session, route, &query).await);
    session.close().await.expect("profile session should close");
    ProfileMeasurement {
        case: case.name,
        route,
        sql: query.sql,
        row_count: case.rows,
        iterations: profile.iterations,
        duration_ns,
        storage,
    }
}

fn validate(case: Case, query: &Query, result: &ExecuteResult) {
    assert_eq!(result.len(), case.rows, "unexpected SELECT row count");
    let expected = query
        .expected_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    for row in result.rows() {
        let [Value::Text(id), value] = row.values() else {
            panic!(
                "expected [text id, projected value], got {:?}",
                row.values()
            );
        };
        assert!(expected.contains(id.as_str()), "unexpected id {id}");
        assert!(seen.insert(id.as_str()), "duplicate id {id}");
        let expected_value = match case.surface {
            Surface::Entity => Value::Text(entity_payload(id)),
            Surface::FileDescriptor => Value::Text(file_path(id)),
            Surface::FileData => Value::Blob(FILE_DATA.to_vec()),
        };
        assert_eq!(value, &expected_value, "unexpected value for {id}");
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).expect("duration should fit u64")
}

#[derive(Debug, Serialize)]
struct Report {
    benchmark: &'static str,
    report_version: u32,
    build: BuildMetadata,
    seed: SeedMetadata,
    config: Config,
    measurements: Vec<Measurement>,
    profile: Option<ProfileMeasurement>,
}

#[derive(Debug, Serialize)]
struct BuildMetadata {
    debug_assertions: bool,
    build_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct SeedMetadata {
    canonical_seed_id: &'static str,
    rows_per_surface: usize,
    snapshot_bytes: usize,
    snapshot_blake3: String,
}

#[derive(Clone, Copy, Debug, Serialize)]
struct Observation {
    duration_ns: u64,
    storage: StorageStats,
}

#[derive(Debug, Serialize)]
struct Measurement {
    case: &'static str,
    route: Route,
    sql: String,
    row_count: usize,
    duration_ns: Percentiles,
    storage_per_sample: StoragePercentiles,
    raw_observations: Vec<Observation>,
}

impl Measurement {
    fn new(case: Case, route: Route, sql: String, observations: Vec<Observation>) -> Self {
        Self {
            case: case.name,
            route,
            sql,
            row_count: case.rows,
            duration_ns: percentiles(&observations, |item| item.duration_ns),
            storage_per_sample: StoragePercentiles {
                begin_read_calls: percentiles(&observations, |item| item.storage.begin_read_calls),
                get_many_calls: percentiles(&observations, |item| item.storage.get_many_calls),
                get_many_requested_keys: percentiles(&observations, |item| {
                    item.storage.get_many_requested_keys
                }),
                scan_calls: percentiles(&observations, |item| item.storage.scan_calls),
            },
            raw_observations: observations,
        }
    }
}

#[derive(Debug, Serialize)]
struct ProfileMeasurement {
    case: &'static str,
    route: Route,
    sql: String,
    row_count: usize,
    iterations: usize,
    duration_ns: u64,
    storage: StorageStats,
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

fn percentiles(observations: &[Observation], value: fn(&Observation) -> u64) -> Percentiles {
    let mut values = observations.iter().map(value).collect::<Vec<_>>();
    values.sort_unstable();
    let nearest = |percentile: usize| {
        let rank = (values.len() * percentile).div_ceil(100);
        values[rank.saturating_sub(1)]
    };
    Percentiles {
        p50: nearest(50),
        p95: nearest(95),
    }
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
            u64::try_from(keys.len()).expect("key count should fit u64"),
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
