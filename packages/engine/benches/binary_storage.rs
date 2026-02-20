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
use support::storage_metrics::{collect_storage_metrics, StorageMetrics};

const DEFAULT_FILES_PER_CLASS: usize = 32;
const DEFAULT_BASE_BLOB_BYTES: usize = 64 * 1024;
const DEFAULT_UPDATE_ROUNDS: usize = 2;
const DEFAULT_POINT_READ_OPS: usize = 500;
const DEFAULT_SCAN_READ_OPS: usize = 8;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum BlobClass {
    Random,
    MediaLike,
    AppendFriendly,
}

#[derive(Debug, Clone)]
struct FileSpec {
    id: String,
    path: String,
    class: BlobClass,
    data: Vec<u8>,
    seed: u64,
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
    files_per_class: usize,
    base_blob_bytes: usize,
    update_rounds: usize,
    point_read_ops: usize,
    scan_read_ops: usize,
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
    workloads: Vec<WorkloadMetrics>,
    storage: StorageSummary,
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
    let base_blob_bytes = env_usize(
        "LIX_BINARY_STORAGE_BASE_BLOB_BYTES",
        DEFAULT_BASE_BLOB_BYTES,
    )?;
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

    let mut dataset = build_dataset(files_per_class, base_blob_bytes);
    let total_files = dataset.len();
    println!(
        "[binary-storage] dataset prepared: files={total_files}, base_blob_bytes={base_blob_bytes}"
    );

    let baseline_storage = runtime.block_on(collect_storage_metrics(&db_path))?;
    println!("[binary-storage] workload ingest_binary_cold");
    let ingest = run_ingest_workload(&runtime, &engine, &dataset)?;
    let after_ingest_storage = runtime.block_on(collect_storage_metrics(&db_path))?;

    println!("[binary-storage] workload update_binary_hot");
    let update = run_update_workload(&runtime, &engine, &mut dataset, update_rounds)?;
    let after_update_storage = runtime.block_on(collect_storage_metrics(&db_path))?;

    println!("[binary-storage] workload read_point_binary");
    let read_point = run_read_point_workload(&runtime, &engine, &dataset, point_read_ops)?;
    println!("[binary-storage] workload read_scan_binary");
    let read_scan = run_read_scan_workload(&runtime, &engine, &dataset, scan_read_ops)?;
    let after_reads_storage = runtime.block_on(collect_storage_metrics(&db_path))?;

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
            files_per_class,
            base_blob_bytes,
            update_rounds,
            point_read_ops,
            scan_read_ops,
        },
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
) -> Result<WorkloadMetrics, LixError> {
    let operations = dataset.len() * rounds;
    run_measured("update_binary_hot", operations, |op_index| {
        let file_index = op_index % dataset.len();
        let round = op_index / dataset.len();
        let spec = &mut dataset[file_index];
        let next = mutate_blob(spec, round);

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

fn build_dataset(files_per_class: usize, base_blob_bytes: usize) -> Vec<FileSpec> {
    let mut out = Vec::with_capacity(files_per_class * 3);

    for index in 0..files_per_class {
        let seed = 0xA11CE_u64 ^ (index as u64 * 0x9E37_79B9);
        out.push(FileSpec {
            id: format!("bench-random-{index:05}"),
            path: format!("/bench/random/{:02}/file-{index:05}.bin", index % 16),
            class: BlobClass::Random,
            data: pseudo_random_bytes(base_blob_bytes, seed),
            seed,
        });
    }

    let media_ext = ["gif", "png", "mp4", "jpg"];
    for index in 0..files_per_class {
        let seed = 0xB10B_u64 ^ (index as u64 * 0x517C_C1B7);
        let ext = media_ext[index % media_ext.len()];
        out.push(FileSpec {
            id: format!("bench-media-{index:05}"),
            path: format!("/bench/media/{:02}/file-{index:05}.{ext}", index % 16),
            class: BlobClass::MediaLike,
            data: media_like_bytes(base_blob_bytes, seed, ext),
            seed,
        });
    }

    for index in 0..files_per_class {
        let seed = 0x5EED_u64 ^ (index as u64 * 0x6A09_E667);
        out.push(FileSpec {
            id: format!("bench-append-{index:05}"),
            path: format!("/bench/append/{:02}/file-{index:05}.dat", index % 16),
            class: BlobClass::AppendFriendly,
            data: append_friendly_bytes(base_blob_bytes, seed),
            seed,
        });
    }

    out
}

fn mutate_blob(spec: &FileSpec, round: usize) -> Vec<u8> {
    match spec.class {
        BlobClass::Random => {
            pseudo_random_bytes(spec.data.len(), spec.seed ^ ((round as u64 + 1) * 0xFF51))
        }
        BlobClass::MediaLike => {
            let mut next = spec.data.clone();
            let flips = (next.len() / 100).max(32);
            for step in 0..flips {
                let idx = ((step as u64 * 1_103_515_245 + spec.seed + round as u64) as usize)
                    % next.len();
                next[idx] ^= ((round + step + 1) % 251) as u8;
            }
            next
        }
        BlobClass::AppendFriendly => {
            let mut next = spec.data.clone();
            let suffix = append_friendly_bytes(4 * 1024, spec.seed ^ (round as u64 * 0x9E37));
            next.extend_from_slice(&suffix);
            next
        }
    }
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
