use lix_engine::{boot, BootArgs, ExecuteOptions, LixError, Value};
use serde::Serialize;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

#[path = "../tests/support/wasmtime_runtime.rs"]
mod bench_wasmtime_runtime;

mod support;
use support::sqlite_backend::BenchSqliteBackend;
use support::storage_metrics::{
    collect_binary_chunk_diagnostics, collect_storage_metrics, BinaryChunkDiagnostics,
    StorageMetrics,
};

const DEFAULT_FILES_PER_CLASS: usize = 32;
const DEFAULT_BASE_BLOB_BYTES: usize = 64 * 1024;
const DEFAULT_UPDATE_ROUNDS: usize = 2;
const DEFAULT_POINT_READ_OPS: usize = 500;
const DEFAULT_SCAN_READ_OPS: usize = 8;
const DEFAULT_BENCH_PROFILE: &str = "binary_4mb_focus";
const PROFILE_4MB_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum BlobClass {
    Random,
    MediaLike,
    AppendFriendly,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum BenchProfile {
    SmallDocs,
    Binary4MbFocus,
}

impl BenchProfile {
    fn parse(raw: &str) -> Result<Self, LixError> {
        match raw {
            "small_docs" => Ok(Self::SmallDocs),
            "binary_4mb_focus" => Ok(Self::Binary4MbFocus),
            _ => Err(LixError {
                message: format!(
                    "LIX_BINARY_STORAGE_PROFILE must be one of: small_docs, binary_4mb_focus; got '{raw}'"
                ),
            }),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::SmallDocs => "small_docs",
            Self::Binary4MbFocus => "binary_4mb_focus",
        }
    }

    fn max_blob_bytes(self, _base_blob_bytes: usize) -> usize {
        match self {
            Self::SmallDocs => usize::MAX,
            Self::Binary4MbFocus => PROFILE_4MB_MAX_BYTES,
        }
    }
}

#[derive(Debug, Clone)]
struct FileSpec {
    id: String,
    path: String,
    class: BlobClass,
    data: Vec<u8>,
    seed: u64,
    media_ext: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkloadMetrics {
    name: String,
    operations: u64,
    bytes_written: u64,
    bytes_read: u64,
    wall_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    ops_per_sec: f64,
}

#[derive(Debug, Clone, Serialize)]
struct BenchConfig {
    profile: String,
    files_per_class: usize,
    base_blob_bytes: usize,
    max_blob_bytes: usize,
    update_rounds: usize,
    point_read_ops: usize,
    scan_read_ops: usize,
}

#[derive(Debug, Clone, Serialize)]
struct DatasetSummary {
    total_files: usize,
    total_bytes: u64,
    p50_file_bytes: usize,
    p80_file_bytes: usize,
    p95_file_bytes: usize,
    max_file_bytes: usize,
    files_le_64k: usize,
    files_le_256k: usize,
    files_le_1m: usize,
    files_le_4m: usize,
}

#[derive(Debug, Clone, Serialize)]
struct StorageSummary {
    baseline: StorageMetrics,
    after_ingest: StorageMetrics,
    after_update: StorageMetrics,
    after_reads: StorageMetrics,
    ingest_write_amp: f64,
    update_write_amp: f64,
    storage_amp_after_update: f64,
}

#[derive(Debug, Clone, Serialize)]
struct BenchmarkReport {
    generated_unix_ms: u128,
    db_path: String,
    config: BenchConfig,
    dataset: DatasetSummary,
    workloads: Vec<WorkloadMetrics>,
    storage: StorageSummary,
    chunk_diagnostics: Option<BinaryChunkDiagnostics>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("binary_storage bench failed");
        eprintln!("{}", error.message);
        std::process::exit(1);
    }
}

fn run() -> Result<(), LixError> {
    let runtime = Runtime::new().map_err(|error| LixError {
        message: format!("failed to initialize tokio runtime: {error}"),
    })?;

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let results_dir = manifest_dir.join("benches").join("results");
    std::fs::create_dir_all(&results_dir).map_err(|error| LixError {
        message: format!(
            "failed to create benchmark results directory {}: {error}",
            results_dir.display()
        ),
    })?;

    let unix_ms = now_unix_ms();
    let default_db_path = results_dir.join(format!("binary-storage-{unix_ms}.sqlite"));
    let db_path = std::env::var("LIX_BINARY_STORAGE_BENCH_DB_PATH")
        .map(PathBuf::from)
        .unwrap_or(default_db_path);
    cleanup_db_artifacts(&db_path)?;

    let files_per_class = env_usize(
        "LIX_BINARY_STORAGE_FILES_PER_CLASS",
        DEFAULT_FILES_PER_CLASS,
    )?;
    let profile = env_profile("LIX_BINARY_STORAGE_PROFILE", DEFAULT_BENCH_PROFILE)?;
    let base_blob_bytes = env_usize(
        "LIX_BINARY_STORAGE_BASE_BLOB_BYTES",
        DEFAULT_BASE_BLOB_BYTES,
    )?;
    let max_blob_bytes = profile.max_blob_bytes(base_blob_bytes);
    let update_rounds = env_usize("LIX_BINARY_STORAGE_UPDATE_ROUNDS", DEFAULT_UPDATE_ROUNDS)?;
    let point_read_ops = env_usize("LIX_BINARY_STORAGE_POINT_READ_OPS", DEFAULT_POINT_READ_OPS)?;
    let scan_read_ops = env_usize("LIX_BINARY_STORAGE_SCAN_READ_OPS", DEFAULT_SCAN_READ_OPS)?;

    let backend = Box::new(BenchSqliteBackend::file_backed(&db_path)?);
    let wasm_runtime = Arc::new(bench_wasmtime_runtime::TestWasmtimeRuntime::new().map_err(
        |error| LixError {
            message: format!(
                "failed to initialize bench wasmtime runtime: {}",
                error.message
            ),
        },
    )?);
    let mut boot_args = BootArgs::new(backend);
    boot_args.wasm_runtime = Some(wasm_runtime);
    let engine = boot(boot_args);
    runtime.block_on(engine.init())?;

    let mut dataset = build_dataset(files_per_class, base_blob_bytes, profile);
    let dataset_summary = summarize_dataset(&dataset);
    let total_files = dataset.len();
    println!(
        "[binary-storage] dataset prepared: profile={}, files={total_files}, total_bytes={}, p80_file_bytes={}, max_file_bytes={}",
        profile.as_str(),
        dataset_summary.total_bytes,
        dataset_summary.p80_file_bytes,
        dataset_summary.max_file_bytes
    );

    let baseline_storage = runtime.block_on(collect_storage_metrics(&db_path))?;
    println!("[binary-storage] workload ingest_binary_cold");
    let ingest = run_ingest_workload(&runtime, &engine, &dataset)?;
    let after_ingest_storage = runtime.block_on(collect_storage_metrics(&db_path))?;

    println!("[binary-storage] workload update_binary_hot");
    let update = run_update_workload(
        &runtime,
        &engine,
        &mut dataset,
        update_rounds,
        profile,
        max_blob_bytes,
    )?;
    let after_update_storage = runtime.block_on(collect_storage_metrics(&db_path))?;

    println!("[binary-storage] workload read_point_binary");
    let read_point = run_read_point_workload(&runtime, &engine, &dataset, point_read_ops)?;
    println!("[binary-storage] workload read_scan_binary");
    let read_scan = run_read_scan_workload(&runtime, &engine, &dataset, scan_read_ops)?;
    let after_reads_storage = runtime.block_on(collect_storage_metrics(&db_path))?;
    let chunk_diagnostics = runtime.block_on(collect_binary_chunk_diagnostics(&db_path))?;

    let baseline_total = total_storage_bytes(&baseline_storage);
    let ingest_total = total_storage_bytes(&after_ingest_storage);
    let update_total = total_storage_bytes(&after_update_storage);

    let ingest_write_amp = if ingest.bytes_written > 0 {
        ingest_total.saturating_sub(baseline_total) as f64 / ingest.bytes_written as f64
    } else {
        0.0
    };
    let update_write_amp = if update.bytes_written > 0 {
        update_total.saturating_sub(ingest_total) as f64 / update.bytes_written as f64
    } else {
        0.0
    };
    let storage_amp_after_update = if after_update_storage.file_data_cache_bytes > 0 {
        update_total as f64 / after_update_storage.file_data_cache_bytes as f64
    } else {
        0.0
    };

    let report = BenchmarkReport {
        generated_unix_ms: unix_ms,
        db_path: db_path.display().to_string(),
        config: BenchConfig {
            profile: profile.as_str().to_string(),
            files_per_class,
            base_blob_bytes,
            max_blob_bytes,
            update_rounds,
            point_read_ops,
            scan_read_ops,
        },
        dataset: dataset_summary,
        workloads: vec![ingest, update, read_point, read_scan],
        storage: StorageSummary {
            baseline: baseline_storage,
            after_ingest: after_ingest_storage,
            after_update: after_update_storage,
            after_reads: after_reads_storage,
            ingest_write_amp,
            update_write_amp,
            storage_amp_after_update,
        },
        chunk_diagnostics,
    };

    let report_path = std::env::var("LIX_BINARY_STORAGE_REPORT_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| results_dir.join("binary-storage-report.json"));
    let report_json = serde_json::to_string_pretty(&report).map_err(|error| LixError {
        message: format!("failed to serialize benchmark report: {error}"),
    })?;
    std::fs::write(&report_path, report_json).map_err(|error| LixError {
        message: format!(
            "failed to write benchmark report {}: {error}",
            report_path.display()
        ),
    })?;

    println!(
        "[binary-storage] done: report={}, db={}",
        report_path.display(),
        db_path.display()
    );
    println!(
        "[binary-storage] storage_amp_after_update={:.3}, ingest_write_amp={:.3}, update_write_amp={:.3}",
        report.storage.storage_amp_after_update,
        report.storage.ingest_write_amp,
        report.storage.update_write_amp
    );
    if let Some(chunk) = report.chunk_diagnostics.as_ref() {
        println!(
            "[binary-storage] chunk_reuse_rate={:.3}, avg_chunks_per_blob={:.3}, bytes_dedup_saved={}",
            chunk.chunk_reuse_rate,
            chunk.avg_chunks_per_blob,
            chunk.bytes_dedup_saved
        );
    }
    Ok(())
}

fn env_usize(name: &str, default_value: usize) -> Result<usize, LixError> {
    let Some(raw) = std::env::var_os(name) else {
        return Ok(default_value);
    };
    let parsed = raw
        .to_string_lossy()
        .parse::<usize>()
        .map_err(|error| LixError {
            message: format!(
                "{name} must be a positive integer, got '{}': {error}",
                raw.to_string_lossy()
            ),
        })?;
    if parsed == 0 {
        return Err(LixError {
            message: format!("{name} must be > 0"),
        });
    }
    Ok(parsed)
}

fn env_profile(name: &str, default_value: &str) -> Result<BenchProfile, LixError> {
    let raw = std::env::var(name).unwrap_or_else(|_| default_value.to_string());
    BenchProfile::parse(raw.trim())
}

fn run_ingest_workload(
    runtime: &Runtime,
    engine: &lix_engine::Engine,
    dataset: &[FileSpec],
) -> Result<WorkloadMetrics, LixError> {
    run_measured("ingest_binary_cold", dataset.len(), |index| {
        let spec = &dataset[index];
        runtime.block_on(engine.execute(
            "INSERT INTO lix_file (id, path, data) VALUES (?, ?, ?)",
            &[
                Value::Text(spec.id.clone()),
                Value::Text(spec.path.clone()),
                Value::Blob(spec.data.clone()),
            ],
            ExecuteOptions::default(),
        ))?;
        Ok(spec.data.len() as u64)
    })
}

fn run_update_workload(
    runtime: &Runtime,
    engine: &lix_engine::Engine,
    dataset: &mut [FileSpec],
    rounds: usize,
    profile: BenchProfile,
    max_blob_bytes: usize,
) -> Result<WorkloadMetrics, LixError> {
    let operations = dataset.len() * rounds;
    run_measured("update_binary_hot", operations, |op_index| {
        let file_index = op_index % dataset.len();
        let round = op_index / dataset.len();
        let spec = &mut dataset[file_index];
        let next = mutate_blob(spec, round, op_index, profile, max_blob_bytes);

        runtime.block_on(engine.execute(
            "UPDATE lix_file SET data = ? WHERE id = ?",
            &[Value::Blob(next.clone()), Value::Text(spec.id.clone())],
            ExecuteOptions::default(),
        ))?;

        spec.data = next;
        Ok(spec.data.len() as u64)
    })
}

fn run_read_point_workload(
    runtime: &Runtime,
    engine: &lix_engine::Engine,
    dataset: &[FileSpec],
    operations: usize,
) -> Result<WorkloadMetrics, LixError> {
    run_measured("read_point_binary", operations, |index| {
        let dataset_index = (index * 17) % dataset.len();
        let spec = &dataset[dataset_index];
        let result = runtime.block_on(engine.execute(
            "SELECT data FROM lix_file WHERE id = ?",
            &[Value::Text(spec.id.clone())],
            ExecuteOptions::default(),
        ))?;
        let row_count = result.rows.len();
        black_box(row_count);
        Ok(spec.data.len() as u64)
    })
}

fn run_read_scan_workload(
    runtime: &Runtime,
    engine: &lix_engine::Engine,
    dataset: &[FileSpec],
    operations: usize,
) -> Result<WorkloadMetrics, LixError> {
    let bytes_per_scan = dataset
        .iter()
        .map(|entry| entry.data.len() as u64)
        .sum::<u64>();
    run_measured("read_scan_binary", operations, |_index| {
        let result = runtime.block_on(engine.execute(
            "SELECT path, data FROM lix_file ORDER BY path",
            &[],
            ExecuteOptions::default(),
        ))?;
        black_box(result.rows.len());
        Ok(bytes_per_scan)
    })
}

fn run_measured<F>(name: &str, operations: usize, mut op: F) -> Result<WorkloadMetrics, LixError>
where
    F: FnMut(usize) -> Result<u64, LixError>,
{
    let mut samples_ms = Vec::with_capacity(operations);
    let mut bytes_total = 0_u64;
    let started = Instant::now();

    for index in 0..operations {
        let op_started = Instant::now();
        let bytes = op(index)?;
        let elapsed_ms = op_started.elapsed().as_secs_f64() * 1_000.0;
        samples_ms.push(elapsed_ms);
        bytes_total = bytes_total.saturating_add(bytes);
    }

    let wall_ms = started.elapsed().as_secs_f64() * 1_000.0;
    samples_ms.sort_by(|left, right| left.total_cmp(right));
    let p50_ms = percentile_ms(&samples_ms, 0.50);
    let p95_ms = percentile_ms(&samples_ms, 0.95);
    let ops_per_sec = if wall_ms > 0.0 {
        operations as f64 / (wall_ms / 1_000.0)
    } else {
        0.0
    };

    Ok(WorkloadMetrics {
        name: name.to_string(),
        operations: operations as u64,
        bytes_written: if name.starts_with("read_") {
            0
        } else {
            bytes_total
        },
        bytes_read: if name.starts_with("read_") {
            bytes_total
        } else {
            0
        },
        wall_ms,
        p50_ms,
        p95_ms,
        ops_per_sec,
    })
}

fn percentile_ms(sorted_samples_ms: &[f64], percentile: f64) -> f64 {
    if sorted_samples_ms.is_empty() {
        return 0.0;
    }
    let last = sorted_samples_ms.len() - 1;
    let position = ((last as f64) * percentile).round() as usize;
    sorted_samples_ms[position.min(last)]
}

fn build_dataset(
    files_per_class: usize,
    base_blob_bytes: usize,
    profile: BenchProfile,
) -> Vec<FileSpec> {
    let mut out = Vec::with_capacity(files_per_class * 3);
    let total_slots = files_per_class * 3;
    let mut slot = 0usize;

    for index in 0..files_per_class {
        let seed = 0xA11CE_u64 ^ (index as u64 * 0x9E37_79B9);
        let size = pick_initial_size(profile, slot, total_slots, base_blob_bytes, seed);
        slot += 1;
        out.push(FileSpec {
            id: format!("bench-random-{index:05}"),
            path: format!("/bench/random/{:02}/file-{index:05}.bin", index % 16),
            class: BlobClass::Random,
            data: pseudo_random_bytes(size, seed),
            seed,
            media_ext: None,
        });
    }

    let media_ext = ["gif", "png", "mp4", "jpg"];
    for index in 0..files_per_class {
        let seed = 0xB10B_u64 ^ (index as u64 * 0x517C_C1B7);
        let ext = media_ext[index % media_ext.len()];
        let size = pick_initial_size(profile, slot, total_slots, base_blob_bytes, seed);
        slot += 1;
        out.push(FileSpec {
            id: format!("bench-media-{index:05}"),
            path: format!("/bench/media/{:02}/file-{index:05}.{ext}", index % 16),
            class: BlobClass::MediaLike,
            data: media_like_bytes(size, seed, ext),
            seed,
            media_ext: Some(ext),
        });
    }

    for index in 0..files_per_class {
        let seed = 0x5EED_u64 ^ (index as u64 * 0x6A09_E667);
        let size = pick_initial_size(profile, slot, total_slots, base_blob_bytes, seed);
        slot += 1;
        out.push(FileSpec {
            id: format!("bench-append-{index:05}"),
            path: format!("/bench/append/{:02}/file-{index:05}.dat", index % 16),
            class: BlobClass::AppendFriendly,
            data: append_friendly_bytes(size, seed),
            seed,
            media_ext: None,
        });
    }

    out
}

fn mutate_blob(
    spec: &FileSpec,
    round: usize,
    op_index: usize,
    profile: BenchProfile,
    max_blob_bytes: usize,
) -> Vec<u8> {
    let selector = deterministic_mix_u64(
        spec.seed
            ^ ((round as u64 + 1) * 0x9E37_79B9_7F4A_7C15)
            ^ (op_index as u64 * 0xBF58_476D_1CE4_E5B9),
    ) % 100;
    if selector < 60 {
        mutate_localized(spec, round)
    } else if selector < 85 {
        mutate_append(spec, round, max_blob_bytes)
    } else {
        mutate_full_rewrite(spec, round, profile)
    }
}

fn mutate_full_rewrite(spec: &FileSpec, round: usize, profile: BenchProfile) -> Vec<u8> {
    let max_blob = profile.max_blob_bytes(spec.data.len());
    let next_len = spec.data.len().min(max_blob);
    let seed = spec.seed ^ ((round as u64 + 1) * 0xFF51_1AFD);
    match spec.class {
        BlobClass::Random => pseudo_random_bytes(next_len, seed),
        BlobClass::MediaLike => media_like_bytes(next_len, seed, spec.media_ext.unwrap_or("bin")),
        BlobClass::AppendFriendly => append_friendly_bytes(next_len, seed),
    }
}

fn mutate_localized(spec: &FileSpec, round: usize) -> Vec<u8> {
    let mut next = spec.data.clone();
    if next.is_empty() {
        return next;
    }
    let min_window = next.len().min(4 * 1024).max(1);
    let max_window = next.len().min(64 * 1024).max(min_window);
    let width_seed = deterministic_mix_u64(spec.seed ^ (round as u64 * 0x94D0_49BB_1331_11EB));
    let span = max_window.saturating_sub(min_window);
    let window = if span == 0 {
        min_window
    } else {
        min_window + (width_seed as usize % (span + 1))
    };
    let max_start = next.len().saturating_sub(window);
    let start_seed = deterministic_mix_u64(spec.seed ^ (round as u64 * 0xD2B7_4407_B1CE_6E93));
    let start = if max_start == 0 {
        0
    } else {
        start_seed as usize % (max_start + 1)
    };

    let mut state = deterministic_mix_u64(spec.seed ^ ((round as u64 + 1) * 0x9E37_79B9));
    for offset in 0..window {
        state = deterministic_mix_u64(state ^ offset as u64);
        let idx = start + offset;
        next[idx] ^= (state & 0xFF) as u8;
    }
    next
}

fn mutate_append(spec: &FileSpec, round: usize, max_blob_bytes: usize) -> Vec<u8> {
    if max_blob_bytes <= spec.data.len() {
        return mutate_localized(spec, round);
    }
    let mut next = spec.data.clone();
    let append_seed = deterministic_mix_u64(spec.seed ^ ((round as u64 + 1) * 0x8CB9_2BA7));
    let min_append = 32 * 1024;
    let max_append = 256 * 1024;
    let planned = min_append + (append_seed as usize % (max_append - min_append + 1));
    let available = max_blob_bytes.saturating_sub(next.len());
    let append_len = planned.min(available);
    if append_len == 0 {
        return mutate_localized(spec, round);
    }

    let suffix = match spec.class {
        BlobClass::Random => pseudo_random_bytes(append_len, append_seed),
        BlobClass::MediaLike => pseudo_random_bytes(append_len, append_seed ^ 0xA24B_1C62),
        BlobClass::AppendFriendly => append_friendly_bytes(append_len, append_seed),
    };
    next.extend_from_slice(&suffix);
    next
}

fn pick_initial_size(
    profile: BenchProfile,
    slot: usize,
    total_slots: usize,
    base_blob_bytes: usize,
    seed: u64,
) -> usize {
    match profile {
        BenchProfile::SmallDocs => base_blob_bytes,
        BenchProfile::Binary4MbFocus => {
            let percentile = if total_slots == 0 {
                0.0
            } else {
                slot as f64 / total_slots as f64
            };
            if percentile < 0.20 {
                sample_size_inclusive(8 * 1024, 64 * 1024, seed ^ 0xA11C_EA55)
            } else if percentile < 0.55 {
                sample_size_inclusive(64 * 1024 + 1, 256 * 1024, seed ^ 0xB10B_CAFE)
            } else if percentile < 0.85 {
                sample_size_inclusive(256 * 1024 + 1, 1024 * 1024, seed ^ 0x5EED_FACE)
            } else {
                sample_size_inclusive(1024 * 1024 + 1, PROFILE_4MB_MAX_BYTES, seed ^ 0xC0DE_4D2B)
            }
        }
    }
}

fn sample_size_inclusive(min_bytes: usize, max_bytes: usize, seed: u64) -> usize {
    if max_bytes <= min_bytes {
        return min_bytes;
    }
    let mixed = deterministic_mix_u64(seed);
    min_bytes + (mixed as usize % (max_bytes - min_bytes + 1))
}

fn deterministic_mix_u64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn summarize_dataset(dataset: &[FileSpec]) -> DatasetSummary {
    let mut sizes = dataset
        .iter()
        .map(|entry| entry.data.len())
        .collect::<Vec<_>>();
    sizes.sort_unstable();
    let total_files = sizes.len();
    let total_bytes = sizes.iter().map(|size| *size as u64).sum::<u64>();
    DatasetSummary {
        total_files,
        total_bytes,
        p50_file_bytes: percentile_size(&sizes, 0.50),
        p80_file_bytes: percentile_size(&sizes, 0.80),
        p95_file_bytes: percentile_size(&sizes, 0.95),
        max_file_bytes: sizes.last().copied().unwrap_or(0),
        files_le_64k: sizes.iter().filter(|size| **size <= 64 * 1024).count(),
        files_le_256k: sizes.iter().filter(|size| **size <= 256 * 1024).count(),
        files_le_1m: sizes.iter().filter(|size| **size <= 1024 * 1024).count(),
        files_le_4m: sizes
            .iter()
            .filter(|size| **size <= PROFILE_4MB_MAX_BYTES)
            .count(),
    }
}

fn percentile_size(sorted_sizes: &[usize], percentile: f64) -> usize {
    if sorted_sizes.is_empty() {
        return 0;
    }
    let last = sorted_sizes.len() - 1;
    let index = ((last as f64) * percentile).round() as usize;
    sorted_sizes[index.min(last)]
}

fn pseudo_random_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state ^= state << 7;
        state ^= state >> 9;
        state ^= state << 8;
        out.push((state & 0xFF) as u8);
    }
    out
}

fn media_like_bytes(len: usize, seed: u64, ext: &str) -> Vec<u8> {
    let mut out = pseudo_random_bytes(len, seed);
    match ext {
        "gif" => patch_prefix(&mut out, b"GIF89a"),
        "png" => patch_prefix(&mut out, &[0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A]),
        "jpg" => patch_prefix(
            &mut out,
            &[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F'],
        ),
        "mp4" => patch_prefix(
            &mut out,
            &[
                0x00, 0x00, 0x00, 0x1C, b'f', b't', b'y', b'p', b'm', b'p', b'4', b'2',
            ],
        ),
        _ => {}
    }
    out
}

fn append_friendly_bytes(len: usize, seed: u64) -> Vec<u8> {
    let pattern = format!("record-{seed:016x}|");
    let bytes = pattern.as_bytes();
    let mut out = Vec::with_capacity(len);
    for index in 0..len {
        out.push(bytes[index % bytes.len()]);
    }
    out
}

fn patch_prefix(target: &mut [u8], prefix: &[u8]) {
    for (index, byte) in prefix.iter().enumerate() {
        if index >= target.len() {
            return;
        }
        target[index] = *byte;
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn total_storage_bytes(metrics: &StorageMetrics) -> u64 {
    metrics
        .db_file_bytes
        .saturating_add(metrics.wal_file_bytes)
        .saturating_add(metrics.shm_file_bytes)
}

fn cleanup_db_artifacts(path: &Path) -> Result<(), LixError> {
    let mut paths = vec![path.to_path_buf()];
    let mut wal = path.as_os_str().to_os_string();
    wal.push("-wal");
    paths.push(PathBuf::from(wal));
    let mut shm = path.as_os_str().to_os_string();
    shm.push("-shm");
    paths.push(PathBuf::from(shm));

    for item in paths {
        if item.exists() {
            std::fs::remove_file(&item).map_err(|error| LixError {
                message: format!(
                    "failed to remove existing benchmark artifact {}: {error}",
                    item.display()
                ),
            })?;
        }
    }
    Ok(())
}
