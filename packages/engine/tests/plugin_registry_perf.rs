#![recursion_limit = "256"]

//! Reproducible current-state plugin registry performance probe.
//!
//! This is intentionally ignored in normal test runs. Example:
//!
//! ```text
//! LIX_PLUGIN_REGISTRY_PERF_MODE=all \
//! LIX_PLUGIN_REGISTRY_PERF_FILES=10000 \
//! LIX_PLUGIN_REGISTRY_PERF_BATCH_SIZE=100 \
//! LIX_PLUGIN_REGISTRY_PERF_WASM_BYTES=1048576 \
//! cargo test -p lix_engine --test plugin_registry_perf --release -- --ignored --nocapture
//! ```
//!
//! Modes are `p0`, `p1_nonmatch`, `p1_zero_state_write`, `p1_state_write`, and
//! `p1_read_render`.

use std::fmt::Write as FmtWrite;
use std::io::{Cursor, Write as IoWrite};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lix_engine::storage::{
    GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead, MemoryWrite, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, Storage, StorageError, StorageRead, WriteOptions,
};
use lix_engine::wasm::{
    WasmComponentInstance, WasmLimits, WasmPluginDetectedChange, WasmPluginEntityState,
    WasmPluginFile, WasmRuntime,
};
use lix_engine::{Engine, LixError, SessionContext, Value};
use serde_json::json;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const MODE_ENV: &str = "LIX_PLUGIN_REGISTRY_PERF_MODE";
const FILE_COUNT_ENV: &str = "LIX_PLUGIN_REGISTRY_PERF_FILES";
const BATCH_SIZE_ENV: &str = "LIX_PLUGIN_REGISTRY_PERF_BATCH_SIZE";
const WARMUPS_ENV: &str = "LIX_PLUGIN_REGISTRY_PERF_WARMUPS";
const SAMPLES_ENV: &str = "LIX_PLUGIN_REGISTRY_PERF_SAMPLES";
const SETUP_CHUNK_ENV: &str = "LIX_PLUGIN_REGISTRY_PERF_SETUP_CHUNK_SIZE";
const WASM_BYTES_ENV: &str = "LIX_PLUGIN_REGISTRY_PERF_WASM_BYTES";

#[tokio::test(flavor = "current_thread")]
#[ignore = "manual plugin registry p50/p95 performance probe"]
async fn plugin_registry_perf_probe() {
    let config = ProbeConfig::from_env();
    for mode in modes_from_env() {
        run_mode(config, mode).await;
    }
}

#[derive(Debug, Clone, Copy)]
struct ProbeConfig {
    file_count: usize,
    batch_size: usize,
    warmups: usize,
    samples: usize,
    setup_chunk_size: usize,
    plugin_wasm_bytes: usize,
}

impl ProbeConfig {
    fn from_env() -> Self {
        let file_count = env_usize(FILE_COUNT_ENV, 1_000);
        let config = Self {
            file_count,
            batch_size: env_usize(BATCH_SIZE_ENV, 1),
            warmups: env_usize(WARMUPS_ENV, 20),
            samples: env_usize(SAMPLES_ENV, 200),
            setup_chunk_size: env_usize(SETUP_CHUNK_ENV, file_count),
            plugin_wasm_bytes: env_usize(WASM_BYTES_ENV, 1024 * 1024),
        };
        assert!(config.file_count > 0, "{FILE_COUNT_ENV} must be positive");
        assert!(config.batch_size > 0, "{BATCH_SIZE_ENV} must be positive");
        assert!(
            config.batch_size <= config.file_count,
            "{BATCH_SIZE_ENV} must not exceed {FILE_COUNT_ENV}"
        );
        assert!(config.warmups > 0, "{WARMUPS_ENV} must be positive");
        assert!(
            config.samples >= 2,
            "{SAMPLES_ENV} must contain at least two samples"
        );
        assert!(
            config.setup_chunk_size > 0,
            "{SETUP_CHUNK_ENV} must be positive"
        );
        assert!(
            config.plugin_wasm_bytes >= 8,
            "{WASM_BYTES_ENV} must be at least 8 bytes"
        );
        config
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeMode {
    P0,
    P1Nonmatch,
    P1ZeroStateWrite,
    P1StateWrite,
    P1ReadRender,
}

impl ProbeMode {
    fn label(self) -> &'static str {
        match self {
            Self::P0 => "p0",
            Self::P1Nonmatch => "p1_nonmatch",
            Self::P1ZeroStateWrite => "p1_zero_state_write",
            Self::P1StateWrite => "p1_state_write",
            Self::P1ReadRender => "p1_read_render",
        }
    }

    fn plugin_glob(self) -> Option<&'static str> {
        match self {
            Self::P0 => None,
            Self::P1Nonmatch => Some("*.owned"),
            Self::P1ZeroStateWrite | Self::P1StateWrite | Self::P1ReadRender => Some("*.bench"),
        }
    }

    fn workload(self) -> &'static str {
        match self {
            Self::P0 => "no_plugin_write_control",
            Self::P1Nonmatch => "installed_nonmatching_plugin_write_control",
            Self::P1ZeroStateWrite => "matched_zero_state_engine_overhead_write",
            Self::P1StateWrite => "matched_stable_one_row_state_write",
            Self::P1ReadRender => "matched_stable_one_row_state_read_render",
        }
    }

    fn timed_operation(self) -> &'static str {
        match self {
            Self::P1ReadRender => "select_rendered_data_existing_files",
            Self::P0 | Self::P1Nonmatch | Self::P1ZeroStateWrite | Self::P1StateWrite => {
                "autocommit_update_existing_files"
            }
        }
    }

    fn timed_operation_appends_layer(self) -> bool {
        self != Self::P1ReadRender
    }

    fn component_behavior(self) -> BenchComponentBehavior {
        match self {
            Self::P1StateWrite | Self::P1ReadRender => BenchComponentBehavior::StableState,
            Self::P0 | Self::P1Nonmatch | Self::P1ZeroStateWrite => {
                BenchComponentBehavior::ZeroState
            }
        }
    }

    fn timed_detects(self) -> bool {
        matches!(self, Self::P1ZeroStateWrite | Self::P1StateWrite)
    }

    fn timed_renders(self) -> bool {
        self == Self::P1ReadRender
    }

    fn primes_state(self) -> bool {
        self == Self::P1ReadRender
    }

    fn initializes_component(self) -> bool {
        matches!(
            self,
            Self::P1ZeroStateWrite | Self::P1StateWrite | Self::P1ReadRender
        )
    }

    fn plugin_count(self) -> u8 {
        match self {
            Self::P0 => 0,
            Self::P1Nonmatch | Self::P1ZeroStateWrite | Self::P1StateWrite | Self::P1ReadRender => {
                1
            }
        }
    }
}

fn modes_from_env() -> Vec<ProbeMode> {
    let raw = std::env::var(MODE_ENV).unwrap_or_else(|_| "all".to_string());
    if raw == "all" {
        return vec![
            ProbeMode::P0,
            ProbeMode::P1Nonmatch,
            ProbeMode::P1ZeroStateWrite,
            ProbeMode::P1StateWrite,
            ProbeMode::P1ReadRender,
        ];
    }
    let modes = raw
        .split(',')
        .map(|value| match value.trim() {
            "p0" => ProbeMode::P0,
            "p1_nonmatch" => ProbeMode::P1Nonmatch,
            "p1_zero_state_write" => ProbeMode::P1ZeroStateWrite,
            "p1_state_write" => ProbeMode::P1StateWrite,
            "p1_read_render" => ProbeMode::P1ReadRender,
            other => {
                panic!(
                    "{MODE_ENV} entries must be p0, p1_nonmatch, p1_zero_state_write, \
                     p1_state_write, or p1_read_render; got {other:?}"
                )
            }
        })
        .collect::<Vec<_>>();
    assert!(
        !modes.is_empty(),
        "{MODE_ENV} must select at least one mode"
    );
    modes
}

async fn run_mode(config: ProbeConfig, mode: ProbeMode) {
    let storage = CountingStorage::new();
    Engine::initialize(storage.clone())
        .await
        .expect("benchmark storage should initialize");
    let runtime = Arc::new(BenchWasmRuntime::new(mode.component_behavior()));
    let engine = Engine::new_with_wasm_runtime(storage.clone(), runtime.clone())
        .await
        .expect("benchmark engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("benchmark workspace session should open");

    let files = seed_files(config);
    let setup_commit_count = insert_seed_files(&session, &files, config.setup_chunk_size).await;
    let plugin_install_commit_count = usize::from(mode.plugin_glob().is_some());
    if let Some(path_glob) = mode.plugin_glob() {
        install_benchmark_plugin(&session, path_glob, config.plugin_wasm_bytes).await;
    }
    let update_sql = update_sql(&files[..config.batch_size]);
    let read_sql = read_sql(&files[..config.batch_size]);

    let state_prime_commit_count = if mode.primes_state() {
        execute_update(&session, &update_sql, update_payload(0), config.batch_size).await;
        1
    } else {
        0
    };

    for sequence in 0..config.warmups {
        if mode.timed_renders() {
            execute_read_render(&session, &read_sql, config.batch_size).await;
        } else {
            execute_update(
                &session,
                &update_sql,
                update_payload(sequence),
                config.batch_size,
            )
            .await;
        }
    }
    let expected_pre_timing_detects = usize::from(mode.primes_state())
        .checked_add(if mode.timed_detects() {
            config.warmups
        } else {
            0
        })
        .and_then(|calls| calls.checked_mul(config.batch_size))
        .expect("pre-timing detect count should fit usize");
    assert_eq!(
        runtime.detect_calls.load(Ordering::Relaxed),
        expected_pre_timing_detects,
        "setup and warmup plugin matching did not execute the expected files"
    );
    let expected_warmup_renders = if mode.timed_renders() {
        config
            .warmups
            .checked_mul(config.batch_size)
            .expect("warmup render count should fit usize")
    } else {
        0
    };
    assert_eq!(
        runtime.render_calls.load(Ordering::Relaxed),
        expected_warmup_renders,
        "warmup plugin rendering did not execute the expected files"
    );
    if mode.initializes_component() {
        assert!(
            runtime.init_calls.load(Ordering::Relaxed) > 0,
            "matching mode must initialize the fake component"
        );
    } else {
        assert_eq!(
            runtime.init_calls.load(Ordering::Relaxed),
            0,
            "a missing or nonmatching plugin must not initialize a component"
        );
    }

    runtime.detect_calls.store(0, Ordering::Relaxed);
    runtime.render_calls.store(0, Ordering::Relaxed);
    runtime.detect_input_state_rows.store(0, Ordering::Relaxed);
    runtime.detect_output_state_rows.store(0, Ordering::Relaxed);
    runtime.render_input_state_rows.store(0, Ordering::Relaxed);
    storage.reset_read_counts();
    let reads_before = storage.read_counts();
    assert_eq!(
        reads_before,
        PhysicalReadCounts::default(),
        "physical read counters must reset immediately before timed samples"
    );
    let split_index = config.samples / 2;
    let mut durations = Vec::with_capacity(config.samples);
    let mut reads_at_split = None;
    for sample in 0..config.samples {
        let sequence = config
            .warmups
            .checked_add(sample)
            .expect("update sequence should fit usize");
        durations.push(if mode.timed_renders() {
            execute_read_render(&session, &read_sql, config.batch_size).await
        } else {
            execute_update(
                &session,
                &update_sql,
                update_payload(sequence),
                config.batch_size,
            )
            .await
        });
        if sample + 1 == split_index {
            reads_at_split = Some(storage.read_counts());
        }
    }
    let reads_after = storage.read_counts();
    let timed_read_delta = reads_after.delta_since(reads_before);
    let reads_at_split = reads_at_split.expect("sample split must be observed");
    let first_half_read_delta = reads_at_split.delta_since(reads_before);
    let second_half_read_delta = reads_after.delta_since(reads_at_split);

    let expected_measured_detects = if mode.timed_detects() {
        config
            .samples
            .checked_mul(config.batch_size)
            .expect("measured detect count should fit usize")
    } else {
        0
    };
    let measured_detects = runtime.detect_calls.load(Ordering::Relaxed);
    assert_eq!(
        measured_detects, expected_measured_detects,
        "measured plugin matching did not execute the expected files"
    );
    let expected_measured_renders = if mode.timed_renders() {
        config
            .samples
            .checked_mul(config.batch_size)
            .expect("measured render count should fit usize")
    } else {
        0
    };
    let measured_renders = runtime.render_calls.load(Ordering::Relaxed);
    assert_eq!(
        measured_renders, expected_measured_renders,
        "measured plugin rendering did not execute the expected files"
    );
    let expected_detect_state_rows = if mode == ProbeMode::P1StateWrite {
        expected_measured_detects
    } else {
        0
    };
    let measured_detect_input_state_rows = runtime.detect_input_state_rows.load(Ordering::Relaxed);
    let measured_detect_output_state_rows =
        runtime.detect_output_state_rows.load(Ordering::Relaxed);
    assert_eq!(
        measured_detect_input_state_rows, expected_detect_state_rows,
        "stateful timed writes must read exactly one stable state row per file"
    );
    assert_eq!(
        measured_detect_output_state_rows, expected_detect_state_rows,
        "stateful timed writes must replace exactly one stable state row per file"
    );
    let measured_render_input_state_rows = runtime.render_input_state_rows.load(Ordering::Relaxed);
    assert_eq!(
        measured_render_input_state_rows, expected_measured_renders,
        "timed renders must receive exactly one stable state row per file"
    );

    let mut first_half_durations = durations[..split_index].to_vec();
    let mut second_half_durations = durations[split_index..].to_vec();
    first_half_durations.sort_unstable();
    second_half_durations.sort_unstable();
    durations.sort_unstable();
    let known_layer_appends_before_timing = setup_commit_count
        + plugin_install_commit_count
        + state_prime_commit_count
        + if mode.timed_operation_appends_layer() {
            config.warmups
        } else {
            0
        };
    let timed_layer_appends = if mode.timed_operation_appends_layer() {
        config.samples
    } else {
        0
    };
    let first_half_layer_appends = if mode.timed_operation_appends_layer() {
        split_index
    } else {
        0
    };
    let result = json!({
        "probe": "lix_plugin_registry_perf",
        "format_version": 1,
        "mode": mode.label(),
        "workload": mode.workload(),
        "plugin_count": mode.plugin_count(),
        "plugin_matches_timed_files": mode.initializes_component(),
        "storage": "memory",
        "tokio_runtime": "current_thread",
        "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
        "timed_operation": mode.timed_operation(),
        "timed_operation_appends_layer": mode.timed_operation_appends_layer(),
        "selector": "id_in",
        "ordinary_file_count": config.file_count,
        "batch_size": config.batch_size,
        "setup_chunk_size": config.setup_chunk_size,
        "setup_commit_count": setup_commit_count,
        "setup_commit_scope": "ordinary_files",
        "plugin_install_commit_count": plugin_install_commit_count,
        "state_prime_commit_count": state_prime_commit_count,
        "plugin_wasm_payload_bytes": config.plugin_wasm_bytes,
        "warmups": config.warmups,
        "samples": config.samples,
        "successful_timed_operations": config.samples,
        "successful_timed_updates": if mode.timed_operation_appends_layer() { config.samples } else { 0 },
        "successful_timed_reads": if mode.timed_renders() { config.samples } else { 0 },
        "update_payload_bytes": if mode.timed_operation_appends_layer() { 8 } else { 0 },
        "stable_state_rows_per_matched_file": i32::from(mode.component_behavior() == BenchComponentBehavior::StableState),
        "component_init_calls_total": runtime.init_calls.load(Ordering::Relaxed),
        "component_detect_calls_timed": measured_detects,
        "component_render_calls_timed": measured_renders,
        "component_detect_input_state_rows_timed": measured_detect_input_state_rows,
        "component_detect_output_state_rows_timed": measured_detect_output_state_rows,
        "component_render_input_state_rows_timed": measured_render_input_state_rows,
        "layer_context": {
            "backend": "layered_memory",
            "engine_initialization_layer_depth": "not_observed",
            "known_benchmark_layer_appends_before_timing": known_layer_appends_before_timing,
            "known_benchmark_layer_appends_during_timing": timed_layer_appends,
            "known_benchmark_layer_appends_after_timing": known_layer_appends_before_timing + timed_layer_appends,
            "interpretation": if mode.timed_operation_appends_layer() {
                "write samples deepen Memory; compare chronological sample halves"
            } else {
                "read samples do not append layers; setup depth remains fixed"
            },
        },
        "physical_reads": {
            "begin_read": {
                "timed_delta": timed_read_delta.begin_read,
                "first_half_delta": first_half_read_delta.begin_read,
                "second_half_delta": second_half_read_delta.begin_read,
                "total_after_reset": reads_after.begin_read,
            },
            "get_many": {
                "timed_delta": timed_read_delta.get_many,
                "first_half_delta": first_half_read_delta.get_many,
                "second_half_delta": second_half_read_delta.get_many,
                "total_after_reset": reads_after.get_many,
            },
            "scan": {
                "timed_delta": timed_read_delta.scan,
                "first_half_delta": first_half_read_delta.scan,
                "second_half_delta": second_half_read_delta.scan,
                "total_after_reset": reads_after.scan,
            },
        },
        "first_half": {
            "sample_start_inclusive": 0,
            "sample_end_exclusive": split_index,
            "sample_count": first_half_durations.len(),
            "known_layer_appends_before_half": known_layer_appends_before_timing,
            "known_layer_appends_after_half": known_layer_appends_before_timing + first_half_layer_appends,
            "p50_ns": percentile_ns(&first_half_durations, 50),
            "p95_ns": percentile_ns(&first_half_durations, 95),
        },
        "second_half": {
            "sample_start_inclusive": split_index,
            "sample_end_exclusive": config.samples,
            "sample_count": second_half_durations.len(),
            "known_layer_appends_before_half": known_layer_appends_before_timing + first_half_layer_appends,
            "known_layer_appends_after_half": known_layer_appends_before_timing + timed_layer_appends,
            "p50_ns": percentile_ns(&second_half_durations, 50),
            "p95_ns": percentile_ns(&second_half_durations, 95),
        },
        "time_unit": "ns",
        "min_ns": duration_ns(durations[0]),
        "p50_ns": percentile_ns(&durations, 50),
        "p95_ns": percentile_ns(&durations, 95),
        "max_ns": duration_ns(*durations.last().expect("samples are nonempty")),
    });

    session
        .close()
        .await
        .expect("benchmark workspace session should close");
    let serialized = serde_json::to_string(&result).expect("benchmark result should serialize");
    println!("{serialized}");
}

#[derive(Debug)]
struct SeedFile {
    id: String,
    path: String,
}

fn seed_files(config: ProbeConfig) -> Vec<SeedFile> {
    (0..config.file_count)
        .map(|index| {
            let path = if index < config.batch_size {
                format!("/perf/target-{index:08}.bench")
            } else {
                format!("/perf/filler-{index:08}.txt")
            };
            SeedFile {
                id: format!("perf-file-{index:08}"),
                path,
            }
        })
        .collect()
}

async fn insert_seed_files(
    session: &SessionContext<CountingStorage>,
    files: &[SeedFile],
    chunk_size: usize,
) -> usize {
    let mut commit_count = 0;
    for chunk in files.chunks(chunk_size) {
        let mut sql = String::from("INSERT INTO lix_file (id, path, data) VALUES ");
        for (index, file) in chunk.iter().enumerate() {
            if index != 0 {
                sql.push_str(", ");
            }
            write!(
                &mut sql,
                "('{id}', '{path}', X'00')",
                id = file.id,
                path = file.path
            )
            .expect("writing benchmark setup SQL should succeed");
        }
        let result = session.execute(&sql, &[]).await;
        let result = result.expect("benchmark file setup chunk should commit");
        assert_eq!(
            result.rows_affected(),
            u64::try_from(chunk.len()).expect("setup chunk length should fit u64"),
            "benchmark setup must insert every requested file"
        );
        commit_count += 1;
    }
    commit_count
}

async fn install_benchmark_plugin(
    session: &SessionContext<CountingStorage>,
    path_glob: &str,
    wasm_bytes: usize,
) {
    let result = session
        .execute(
            "INSERT INTO lix_file (path, data) VALUES ($1, $2)",
            &[
                Value::Text("/.lix/plugins/plugin_perf.lixplugin".to_string()),
                Value::Blob(benchmark_plugin_archive(path_glob, wasm_bytes)),
            ],
        )
        .await;
    let result = result.expect("benchmark plugin should install");
    assert_eq!(result.rows_affected(), 1);
}

fn update_sql(files: &[SeedFile]) -> String {
    let ids = files
        .iter()
        .map(|file| format!("'{id}'", id = file.id))
        .collect::<Vec<_>>()
        .join(", ");
    format!("UPDATE lix_file SET data = $1 WHERE id IN ({ids})")
}

fn read_sql(files: &[SeedFile]) -> String {
    let ids = files
        .iter()
        .map(|file| format!("'{id}'", id = file.id))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT data FROM lix_file WHERE id IN ({ids})")
}

async fn execute_update(
    session: &SessionContext<CountingStorage>,
    sql: &str,
    payload: Vec<u8>,
    expected_rows: usize,
) -> Duration {
    let params = [Value::Blob(payload)];
    let started = Instant::now();
    let result = session.execute(sql, &params).await;
    let elapsed = started.elapsed();
    let result = result.expect("timed autocommit update should succeed");
    assert_eq!(
        result.rows_affected(),
        u64::try_from(expected_rows).expect("batch size should fit u64"),
        "timed update must affect the configured batch"
    );
    elapsed
}

async fn execute_read_render(
    session: &SessionContext<CountingStorage>,
    sql: &str,
    expected_rows: usize,
) -> Duration {
    let started = Instant::now();
    let result = session.execute(sql, &[]).await;
    let elapsed = started.elapsed();
    let result = result.expect("timed rendered read should succeed");
    assert_eq!(
        result.len(),
        expected_rows,
        "timed rendered read must return the configured batch"
    );
    for row in result.rows() {
        assert_eq!(
            row.values(),
            &[Value::Blob(BENCH_RENDERED_BYTES.to_vec())],
            "timed rendered read must return deterministic component bytes"
        );
    }
    elapsed
}

fn update_payload(sequence: usize) -> Vec<u8> {
    u64::try_from(sequence)
        .expect("update sequence should fit u64")
        .to_le_bytes()
        .to_vec()
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .unwrap_or_else(|error| panic!("{name} must be an unsigned integer: {error}")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("{name} must be valid Unicode: {error}"),
    }
}

fn percentile_ns(samples: &[Duration], percentile: usize) -> u64 {
    assert!(!samples.is_empty());
    assert!(percentile <= 100);
    let index = (samples.len() - 1) * percentile / 100;
    duration_ns(samples[index])
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("benchmark duration nanoseconds should fit u64")
}

#[derive(Clone, Default)]
struct CountingStorage {
    inner: Memory,
    counters: Arc<PhysicalReadCounters>,
}

impl CountingStorage {
    fn new() -> Self {
        Self::default()
    }

    fn reset_read_counts(&self) {
        self.counters.begin_read.store(0, Ordering::Relaxed);
        self.counters.get_many.store(0, Ordering::Relaxed);
        self.counters.scan.store(0, Ordering::Relaxed);
    }

    fn read_counts(&self) -> PhysicalReadCounts {
        PhysicalReadCounts {
            begin_read: self.counters.begin_read.load(Ordering::Relaxed),
            get_many: self.counters.get_many.load(Ordering::Relaxed),
            scan: self.counters.scan.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct PhysicalReadCounters {
    begin_read: AtomicUsize,
    get_many: AtomicUsize,
    scan: AtomicUsize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PhysicalReadCounts {
    begin_read: usize,
    get_many: usize,
    scan: usize,
}

impl PhysicalReadCounts {
    fn delta_since(self, before: Self) -> Self {
        Self {
            begin_read: self
                .begin_read
                .checked_sub(before.begin_read)
                .expect("begin_read counter must be monotonic"),
            get_many: self
                .get_many
                .checked_sub(before.get_many)
                .expect("get_many counter must be monotonic"),
            scan: self
                .scan
                .checked_sub(before.scan)
                .expect("scan counter must be monotonic"),
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

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.counters.begin_read.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: self.inner.begin_read(opts).await?,
            counters: Arc::clone(&self.counters),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(opts).await
    }
}

#[derive(Clone)]
struct CountingRead {
    inner: MemoryRead,
    counters: Arc<PhysicalReadCounters>,
}

impl StorageRead for CountingRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.counters.get_many.fetch_add(1, Ordering::Relaxed);
        self.inner.get_many(space, keys, opts).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.counters.scan.fetch_add(1, Ordering::Relaxed);
        self.inner.scan(space, range, opts).await
    }
}

const BENCH_SCHEMA_KEY: &str = "plugin_perf_note";
const BENCH_ENTITY_ID: &str = "document";
const BENCH_STATE_BODY: &str =
    "stable deterministic benchmark state payload used to exercise validation and storage";
const BENCH_RENDERED_BYTES: &[u8] = b"deterministic-rendered-plugin-file";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchComponentBehavior {
    ZeroState,
    StableState,
}

struct BenchWasmRuntime {
    behavior: BenchComponentBehavior,
    init_calls: AtomicUsize,
    detect_calls: Arc<AtomicUsize>,
    render_calls: Arc<AtomicUsize>,
    detect_input_state_rows: Arc<AtomicUsize>,
    detect_output_state_rows: Arc<AtomicUsize>,
    render_input_state_rows: Arc<AtomicUsize>,
}

impl BenchWasmRuntime {
    fn new(behavior: BenchComponentBehavior) -> Self {
        Self {
            behavior,
            init_calls: AtomicUsize::new(0),
            detect_calls: Arc::new(AtomicUsize::new(0)),
            render_calls: Arc::new(AtomicUsize::new(0)),
            detect_input_state_rows: Arc::new(AtomicUsize::new(0)),
            detect_output_state_rows: Arc::new(AtomicUsize::new(0)),
            render_input_state_rows: Arc::new(AtomicUsize::new(0)),
        }
    }
}

struct BenchWasmComponent {
    behavior: BenchComponentBehavior,
    detect_calls: Arc<AtomicUsize>,
    render_calls: Arc<AtomicUsize>,
    detect_input_state_rows: Arc<AtomicUsize>,
    detect_output_state_rows: Arc<AtomicUsize>,
    render_input_state_rows: Arc<AtomicUsize>,
}

#[async_trait]
impl WasmRuntime for BenchWasmRuntime {
    async fn init_component(
        &self,
        _bytes: Vec<u8>,
        _limits: WasmLimits,
    ) -> Result<Arc<dyn WasmComponentInstance>, LixError> {
        self.init_calls.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(BenchWasmComponent {
            behavior: self.behavior,
            detect_calls: Arc::clone(&self.detect_calls),
            render_calls: Arc::clone(&self.render_calls),
            detect_input_state_rows: Arc::clone(&self.detect_input_state_rows),
            detect_output_state_rows: Arc::clone(&self.detect_output_state_rows),
            render_input_state_rows: Arc::clone(&self.render_input_state_rows),
        }))
    }
}

#[async_trait]
impl WasmComponentInstance for BenchWasmComponent {
    async fn detect_changes(
        &self,
        state: Vec<WasmPluginEntityState>,
        file: WasmPluginFile,
    ) -> Result<Vec<WasmPluginDetectedChange>, LixError> {
        self.detect_calls.fetch_add(1, Ordering::Relaxed);
        self.detect_input_state_rows
            .fetch_add(state.len(), Ordering::Relaxed);
        match self.behavior {
            BenchComponentBehavior::ZeroState => {
                assert!(state.is_empty(), "zero-state control must stay state-free");
                Ok(Vec::new())
            }
            BenchComponentBehavior::StableState => {
                assert!(
                    state.len() <= 1,
                    "stateful benchmark expects at most one row per file"
                );
                for entity in &state {
                    assert_valid_benchmark_state(entity);
                }
                let revision_bytes: [u8; 8] = file
                    .data
                    .as_slice()
                    .try_into()
                    .expect("stateful benchmark writes must contain one u64 revision");
                let filename = file
                    .filename
                    .expect("stateful benchmark detect must receive the descriptor filename");
                let snapshot_content = json!({
                    "id": BENCH_ENTITY_ID,
                    "revision": u64::from_le_bytes(revision_bytes),
                    "filename": filename,
                    "body": BENCH_STATE_BODY,
                })
                .to_string();
                self.detect_output_state_rows
                    .fetch_add(1, Ordering::Relaxed);
                Ok(vec![WasmPluginDetectedChange {
                    entity_pk: vec![BENCH_ENTITY_ID.to_string()],
                    schema_key: BENCH_SCHEMA_KEY.to_string(),
                    snapshot_content: Some(snapshot_content),
                    metadata: Some("{\"source\":\"plugin_registry_perf\"}".to_string()),
                }])
            }
        }
    }

    async fn render(&self, state: Vec<WasmPluginEntityState>) -> Result<Vec<u8>, LixError> {
        self.render_calls.fetch_add(1, Ordering::Relaxed);
        self.render_input_state_rows
            .fetch_add(state.len(), Ordering::Relaxed);
        assert_eq!(
            self.behavior,
            BenchComponentBehavior::StableState,
            "only the stateful benchmark component may render"
        );
        assert_eq!(
            state.len(),
            1,
            "stateful render must receive one row per file"
        );
        assert_valid_benchmark_state(&state[0]);
        Ok(BENCH_RENDERED_BYTES.to_vec())
    }
}

fn assert_valid_benchmark_state(state: &WasmPluginEntityState) {
    assert_eq!(state.entity_pk, [BENCH_ENTITY_ID.to_string()]);
    assert_eq!(state.schema_key, BENCH_SCHEMA_KEY);
    let snapshot: serde_json::Value = serde_json::from_str(&state.snapshot_content)
        .expect("benchmark state snapshot must be valid JSON");
    assert_eq!(snapshot.get("id"), Some(&json!(BENCH_ENTITY_ID)));
    assert!(
        snapshot
            .get("revision")
            .and_then(serde_json::Value::as_u64)
            .is_some()
    );
    assert!(
        snapshot
            .get("filename")
            .and_then(|value| value.as_str())
            .is_some()
    );
    assert_eq!(snapshot.get("body"), Some(&json!(BENCH_STATE_BODY)));
}

fn benchmark_plugin_archive(path_glob: &str, wasm_bytes: usize) -> Vec<u8> {
    const SCHEMA: &[u8] = br#"{
        "x-lix-key":"plugin_perf_note",
        "x-lix-primary-key":["/id"],
        "type":"object",
        "properties":{
            "id":{"type":"string"},
            "revision":{"type":"integer","minimum":0},
            "filename":{"type":"string"},
            "body":{"type":"string"}
        },
        "required":["id","revision","filename","body"],
        "additionalProperties":false
    }"#;
    const WASM_HEADER: &[u8; 8] = b"\0asm\x01\0\0\0";
    assert!(wasm_bytes >= WASM_HEADER.len());
    let mut wasm = vec![0; wasm_bytes];
    wasm[..WASM_HEADER.len()].copy_from_slice(WASM_HEADER);
    let manifest = format!(
        r#"{{
            "key":"plugin_perf",
            "runtime":"wasm-component-v1",
            "api_version":"0.1.0",
            "match":{{"path_glob":"{path_glob}"}},
            "entry":"plugin.wasm",
            "schemas":["schema/plugin_perf_note.json"]
        }}"#
    );

    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    let options = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
    for (path, bytes) in [
        ("manifest.json", manifest.as_bytes()),
        ("schema/plugin_perf_note.json", SCHEMA),
        ("plugin.wasm", wasm.as_slice()),
    ] {
        writer
            .start_file(path, options)
            .expect("benchmark plugin ZIP entry should start");
        writer
            .write_all(bytes)
            .expect("benchmark plugin ZIP entry should write");
    }
    writer
        .finish()
        .expect("benchmark plugin ZIP should finish")
        .into_inner()
}
