// Cold-open profiling harness for the filesystem backend.
//
// Usage:
//   cargo run --release --example profile_fs_open --features rocksdb -- \
//     --backend rocksdb-blob <src_dir>
//
// Copies <src_dir> (sans any existing .lix) into a fresh temp dir, then times
// FsBackend::open on the cold workspace. Pass --keep-workspace to preserve the
// copied temp workspace for inspection.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lix_engine::{
    Backend as _, BackendRead as _, BinaryCasStorageStats, BinaryCasWriteMetrics, ReadOptions,
    Value, binary_cas_write_metrics_snapshot, collect_binary_cas_storage_stats,
    reset_binary_cas_write_metrics,
};
use lix_fs_backend::RocksDbBlobOptions;
use lix_sdk::{FsBackend, FsBackendFilter, open_lix_with_backend};

const DEFAULT_BLOB_MIN_SIZE: u64 = 32 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileBackend {
    RocksDb,
    RocksDbBlob { min_blob_size: u64 },
}

#[derive(Debug)]
struct Args {
    backend: ProfileBackend,
    compact_before_stats: bool,
    in_place: bool,
    json: bool,
    keep_workspace: bool,
    read_bench: bool,
    src: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ProfileStats {
    corpus_file_count: u64,
    corpus_bytes: u64,
    lix_total_bytes: u64,
    rocksdb_total_bytes: u64,
    rocksdb_sst_bytes: u64,
    rocksdb_blob_bytes: u64,
    rocksdb_wal_bytes: u64,
    rocksdb_log_bytes: u64,
    rocksdb_manifest_bytes: u64,
    rocksdb_options_bytes: u64,
    rocksdb_other_bytes: u64,
    binary_cas_manifest_rows: u64,
    binary_cas_empty_blob_rows: u64,
    binary_cas_single_chunk_blob_rows: u64,
    binary_cas_chunked_blob_rows: u64,
    binary_cas_manifest_chunk_rows: u64,
    binary_cas_chunk_rows: u64,
    binary_cas_total_chunk_refs: u64,
    binary_cas_logical_blob_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ReadBenchStats {
    all_files_ms: u128,
    all_files_count: u64,
    all_files_bytes: u64,
    largest_files_ms: u128,
    largest_files_repeat_ms: u128,
    largest_files_count: u64,
    largest_files_bytes: u64,
    small_sample_ms: u128,
    small_sample_count: u64,
    small_sample_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BenchFile {
    lix_path: String,
    size_bytes: u64,
}

impl ProfileBackend {
    fn name(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::RocksDbBlob { .. } => "rocksdb-blob",
        }
    }

    fn blob_min_size(self) -> Option<u64> {
        match self {
            Self::RocksDbBlob { min_blob_size } => Some(min_blob_size),
            Self::RocksDb => None,
        }
    }

    async fn open(self, path: &Path) -> FsBackend {
        match self {
            Self::RocksDb => FsBackend::open_with_rocksdb_blob_options(
                path,
                FsBackendFilter::default(),
                RocksDbBlobOptions::Disabled,
            )
            .await
            .unwrap(),
            Self::RocksDbBlob {
                min_blob_size: DEFAULT_BLOB_MIN_SIZE,
            } => FsBackend::open(path).await.unwrap(),
            Self::RocksDbBlob { min_blob_size } => FsBackend::open_with_rocksdb_blob_options(
                path,
                FsBackendFilter::default(),
                RocksDbBlobOptions::Enabled {
                    min_blob_size,
                    blob_file_size: 256 * 1024 * 1024,
                    enable_gc: true,
                    gc_age_cutoff: 0.25,
                },
            )
            .await
            .unwrap(),
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name == ".lix" {
            continue;
        }
        let from = entry.path();
        let to = dst.join(&name);
        if from.is_dir() {
            copy_dir(&from, &to);
        } else {
            std::fs::copy(&from, &to).unwrap();
        }
    }
}

fn parse_args() -> Args {
    let mut raw = std::env::args().skip(1);
    let mut backend = ProfileBackend::RocksDbBlob {
        min_blob_size: DEFAULT_BLOB_MIN_SIZE,
    };
    let mut compact_before_stats = false;
    let mut in_place = false;
    let mut json = false;
    let mut keep_workspace = false;
    let mut read_bench = false;
    let mut src = None;

    while let Some(arg) = raw.next() {
        match arg.as_str() {
            "--backend" => {
                let value = raw.next().expect("--backend requires a value");
                backend = match value.as_str() {
                    "rocksdb" => ProfileBackend::RocksDb,
                    "rocksdb-blob" => ProfileBackend::RocksDbBlob {
                        min_blob_size: DEFAULT_BLOB_MIN_SIZE,
                    },
                    other => panic!("unknown backend '{other}'"),
                };
            }
            "--blob-min" => {
                let value = raw.next().expect("--blob-min requires a value");
                let min_blob_size = parse_size(&value);
                backend = ProfileBackend::RocksDbBlob { min_blob_size };
            }
            "--compact-before-stats" => compact_before_stats = true,
            "--in-place" => in_place = true,
            "--json" => json = true,
            "--keep-workspace" => keep_workspace = true,
            "--read-bench" => read_bench = true,
            _ if arg.starts_with("--") => panic!("unknown option '{arg}'"),
            _ => {
                if src.replace(arg).is_some() {
                    panic!("profile_fs_open accepts exactly one source directory");
                }
            }
        }
    }

    Args {
        backend,
        compact_before_stats,
        in_place,
        json,
        keep_workspace,
        read_bench,
        src: src.expect(
            "usage: profile_fs_open [--json] [--in-place] [--keep-workspace] [--read-bench] [--backend rocksdb|rocksdb-blob] [--blob-min bytes] <src_dir>",
        ),
    }
}

fn parse_size(value: &str) -> u64 {
    let Some(last_digit) = value.rfind(|ch: char| ch.is_ascii_digit()) else {
        panic!("size must include digits");
    };
    let (digits, suffix) = value.split_at(last_digit + 1);
    let base = digits.parse::<u64>().expect("size digits must parse");
    match suffix.to_ascii_lowercase().as_str() {
        "" => base,
        "k" | "kb" | "kib" => base * 1024,
        "m" | "mb" | "mib" => base * 1024 * 1024,
        other => panic!("unknown size suffix '{other}'"),
    }
}

fn duration_ms(duration: Duration) -> u128 {
    duration.as_micros() / 1000
}

fn metric_duration_us(ns: u64) -> u64 {
    ns / 1000
}

async fn open_with_metrics(
    backend: ProfileBackend,
    path: &Path,
) -> (FsBackend, Duration, BinaryCasWriteMetrics) {
    reset_binary_cas_write_metrics();
    let started = Instant::now();
    let opened = backend.open(path).await;
    let elapsed = started.elapsed();
    let metrics = binary_cas_write_metrics_snapshot();
    (opened, elapsed, metrics)
}

fn compact_backend_if_requested(args: &Args, backend: &FsBackend) -> Option<Duration> {
    if !args.compact_before_stats {
        return None;
    }
    #[cfg(feature = "rocksdb")]
    {
        let t_compact = Instant::now();
        backend
            .compact_rocksdb()
            .expect("profile RocksDB compact should succeed");
        Some(t_compact.elapsed())
    }
    #[cfg(not(feature = "rocksdb"))]
    {
        let _ = backend;
        panic!("profile_fs_open was built without the rocksdb feature")
    }
}

fn collect_profile_stats(workspace: &Path) -> ProfileStats {
    let mut stats = ProfileStats::default();
    collect_corpus_stats(workspace, &mut stats);
    collect_lix_stats(workspace, &mut stats);
    stats
}

fn collect_backend_profile_stats(backend: &FsBackend, stats: &mut ProfileStats) {
    let read = backend
        .begin_read(ReadOptions::default())
        .expect("profile backend read should open");
    let binary_cas =
        collect_binary_cas_storage_stats(&read).expect("profile binary CAS stats should collect");
    read.close().expect("profile backend read should close");
    apply_binary_cas_stats(stats, binary_cas);
}

async fn run_read_benchmark(backend: &FsBackend, workspace: &Path) -> ReadBenchStats {
    let files = collect_bench_files(workspace);
    let largest = files.iter().take(4).collect::<Vec<_>>();
    let small_sample = select_small_sample(&files, 16);
    let lix = open_lix_with_backend(backend.clone())
        .await
        .expect("profile read benchmark should open lix");

    let all_started = Instant::now();
    let all_files = lix
        .execute("SELECT path, data FROM lix_file ORDER BY path", &[])
        .await
        .expect("profile read benchmark should read all files");
    let all_files_ms = duration_ms(all_started.elapsed());
    let mut all_files_count = 0u64;
    let mut all_files_bytes = 0u64;
    for row in all_files.rows() {
        let _path = row
            .get::<String>("path")
            .expect("profile read benchmark path should decode");
        let data = row
            .get::<Vec<u8>>("data")
            .expect("profile read benchmark data should decode");
        all_files_count += 1;
        all_files_bytes += data.len() as u64;
    }

    let (largest_files_ms, largest_files_bytes) = time_read_paths(&lix, &largest).await;
    let (largest_files_repeat_ms, repeat_largest_bytes) = time_read_paths(&lix, &largest).await;
    assert_eq!(
        largest_files_bytes, repeat_largest_bytes,
        "profile read benchmark repeated largest reads should return the same bytes"
    );
    let (small_sample_ms, small_sample_bytes) = time_read_paths(&lix, &small_sample).await;
    lix.close()
        .await
        .expect("profile read benchmark should close lix");

    ReadBenchStats {
        all_files_ms,
        all_files_count,
        all_files_bytes,
        largest_files_ms,
        largest_files_repeat_ms,
        largest_files_count: largest.len() as u64,
        largest_files_bytes,
        small_sample_ms,
        small_sample_count: small_sample.len() as u64,
        small_sample_bytes,
    }
}

async fn time_read_paths(lix: &lix_sdk::Lix<FsBackend>, files: &[&BenchFile]) -> (u128, u64) {
    let started = Instant::now();
    let mut bytes = 0u64;
    for file in files {
        let result = lix
            .execute(
                "SELECT data FROM lix_file WHERE path = $1",
                &[Value::Text(file.lix_path.clone())],
            )
            .await
            .expect("profile read benchmark should read file");
        let row = result
            .rows()
            .first()
            .unwrap_or_else(|| panic!("missing lix_file row for {}", file.lix_path));
        let data = row
            .get::<Vec<u8>>("data")
            .expect("profile read benchmark data should decode");
        assert_eq!(
            data.len() as u64,
            file.size_bytes,
            "profile read benchmark byte count mismatch for {}",
            file.lix_path
        );
        bytes += data.len() as u64;
    }
    (duration_ms(started.elapsed()), bytes)
}

fn collect_bench_files(workspace: &Path) -> Vec<BenchFile> {
    let mut files = Vec::new();
    collect_bench_files_recursive(workspace, workspace, &mut files);
    files.sort_by(|left, right| {
        right
            .size_bytes
            .cmp(&left.size_bytes)
            .then_with(|| left.lix_path.cmp(&right.lix_path))
    });
    files
}

fn collect_bench_files_recursive(root: &Path, dir: &Path, files: &mut Vec<BenchFile>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let entry = entry.expect("profile read benchmark should read directory entry");
        if entry.file_name() == ".lix" {
            continue;
        }
        let path = entry.path();
        let metadata = entry
            .metadata()
            .expect("profile read benchmark should read metadata");
        if metadata.is_dir() {
            collect_bench_files_recursive(root, &path, files);
        } else if metadata.is_file() {
            files.push(BenchFile {
                lix_path: local_path_to_lix_path(root, &path),
                size_bytes: metadata.len(),
            });
        }
    }
}

fn local_path_to_lix_path(root: &Path, path: &Path) -> String {
    let relative = path
        .strip_prefix(root)
        .expect("profile read benchmark file should be under workspace");
    let mut lix_path = String::from("/");
    for (index, component) in relative.components().enumerate() {
        if index > 0 {
            lix_path.push('/');
        }
        match component {
            std::path::Component::Normal(segment) => {
                lix_path.push_str(&segment.to_string_lossy());
            }
            _ => panic!("profile read benchmark only supports normal path components"),
        }
    }
    lix_path
}

fn select_small_sample(files: &[BenchFile], count: usize) -> Vec<&BenchFile> {
    let mut small = files
        .iter()
        .filter(|file| file.size_bytes <= 64 * 1024)
        .collect::<Vec<_>>();
    small.sort_by(|left, right| {
        stable_path_hash(&left.lix_path)
            .cmp(&stable_path_hash(&right.lix_path))
            .then_with(|| left.lix_path.cmp(&right.lix_path))
    });
    small.truncate(count);
    small
}

fn stable_path_hash(path: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in path.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn apply_binary_cas_stats(stats: &mut ProfileStats, binary_cas: BinaryCasStorageStats) {
    stats.binary_cas_manifest_rows = binary_cas.manifest_rows;
    stats.binary_cas_empty_blob_rows = binary_cas.empty_blob_rows;
    stats.binary_cas_single_chunk_blob_rows = binary_cas.single_chunk_blob_rows;
    stats.binary_cas_chunked_blob_rows = binary_cas.chunked_blob_rows;
    stats.binary_cas_manifest_chunk_rows = binary_cas.manifest_chunk_rows;
    stats.binary_cas_chunk_rows = binary_cas.chunk_rows;
    stats.binary_cas_total_chunk_refs = binary_cas.total_chunk_refs;
    stats.binary_cas_logical_blob_bytes = binary_cas.logical_blob_bytes;
}

fn collect_corpus_stats(root: &Path, stats: &mut ProfileStats) {
    let Ok(entries) = std::fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();
        if entry.file_name() == ".lix" {
            continue;
        }
        let metadata = entry.metadata().unwrap();
        if metadata.is_dir() {
            collect_corpus_stats(&path, stats);
        } else if metadata.is_file() {
            stats.corpus_file_count += 1;
            stats.corpus_bytes += metadata.len();
        }
    }
}

fn collect_lix_stats(workspace: &Path, stats: &mut ProfileStats) {
    let lix_dir = workspace.join(".lix");
    if !lix_dir.exists() {
        return;
    }
    let rocksdb_dir = lix_dir.join(".internal/rocksdb");
    collect_lix_stats_recursive(&lix_dir, &rocksdb_dir, stats);
}

fn collect_lix_stats_recursive(dir: &Path, rocksdb_dir: &Path, stats: &mut ProfileStats) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();
        let metadata = entry.metadata().unwrap();
        if metadata.is_dir() {
            collect_lix_stats_recursive(&path, rocksdb_dir, stats);
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        let bytes = metadata.len();
        stats.lix_total_bytes += bytes;
        if path.strip_prefix(rocksdb_dir).is_ok() {
            classify_rocksdb_file(&path, bytes, stats);
        }
    }
}

fn classify_rocksdb_file(path: &Path, bytes: u64, stats: &mut ProfileStats) {
    stats.rocksdb_total_bytes += bytes;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("sst") => stats.rocksdb_sst_bytes += bytes,
        Some("blob") => stats.rocksdb_blob_bytes += bytes,
        Some("log") => stats.rocksdb_wal_bytes += bytes,
        _ if file_name.starts_with("LOG") => stats.rocksdb_log_bytes += bytes,
        _ if file_name.starts_with("MANIFEST") => stats.rocksdb_manifest_bytes += bytes,
        _ if file_name.starts_with("OPTIONS") => stats.rocksdb_options_bytes += bytes,
        _ => stats.rocksdb_other_bytes += bytes,
    }
}

fn json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => {
                use std::fmt::Write as _;
                write!(&mut out, "\\u{:04x}", ch as u32).unwrap();
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

fn print_result(
    args: &Args,
    copy_elapsed: Option<Duration>,
    open_elapsed: Duration,
    open_metrics: &BinaryCasWriteMetrics,
    warm_elapsed: Option<Duration>,
    warm_metrics: Option<&BinaryCasWriteMetrics>,
    compact_elapsed: Option<Duration>,
    read_bench: Option<&ReadBenchStats>,
    stats: &ProfileStats,
    workspace_path: Option<&Path>,
) {
    if args.json {
        let blob_min_json = args
            .backend
            .blob_min_size()
            .map_or("null".to_string(), |size| size.to_string());
        let workspace_json = workspace_path.map_or("null".to_string(), |path| {
            json_string(&path.display().to_string())
        });
        let compact_ms_json = compact_elapsed
            .map(|duration| duration_ms(duration).to_string())
            .unwrap_or_else(|| "null".to_string());
        let read_bench = read_bench.copied().unwrap_or_default();
        match (copy_elapsed, warm_elapsed) {
            (Some(copy_elapsed), Some(warm_elapsed)) => println!(
                concat!(
                    "{{",
                    "\"backend\":\"{}\",",
                    "\"blob_min_size\":{},",
                    "\"copy_ms\":{},",
                    "\"cold_open_ms\":{},",
                    "\"warm_reopen_ms\":{},",
                    "\"cold_binary_cas_chunk_lookup_count\":{},",
                    "\"cold_binary_cas_chunk_lookup_batch_count\":{},",
                    "\"cold_binary_cas_chunk_lookup_hit_count\":{},",
                    "\"cold_binary_cas_chunk_lookup_miss_count\":{},",
                    "\"cold_binary_cas_chunk_lookup_time_us\":{},",
                    "\"cold_binary_cas_transaction_duplicate_chunk_count\":{},",
                    "\"warm_binary_cas_chunk_lookup_count\":{},",
                    "\"warm_binary_cas_chunk_lookup_batch_count\":{},",
                    "\"warm_binary_cas_chunk_lookup_hit_count\":{},",
                    "\"warm_binary_cas_chunk_lookup_miss_count\":{},",
                    "\"warm_binary_cas_chunk_lookup_time_us\":{},",
                    "\"warm_binary_cas_transaction_duplicate_chunk_count\":{},",
                    "\"read_all_files_ms\":{},",
                    "\"read_all_files_count\":{},",
                    "\"read_all_files_bytes\":{},",
                    "\"read_largest_files_ms\":{},",
                    "\"read_largest_files_repeat_ms\":{},",
                    "\"read_largest_files_count\":{},",
                    "\"read_largest_files_bytes\":{},",
                    "\"read_small_sample_ms\":{},",
                    "\"read_small_sample_count\":{},",
                    "\"read_small_sample_bytes\":{},",
                    "\"compact_ms\":{},",
                    "\"workspace_path\":{},",
                    "\"corpus_file_count\":{},",
                    "\"corpus_bytes\":{},",
                    "\"lix_total_bytes\":{},",
                    "\"rocksdb_total_bytes\":{},",
                    "\"rocksdb_sst_bytes\":{},",
                    "\"rocksdb_blob_bytes\":{},",
                    "\"rocksdb_wal_bytes\":{},",
                    "\"rocksdb_log_bytes\":{},",
                    "\"rocksdb_manifest_bytes\":{},",
                    "\"rocksdb_options_bytes\":{},",
                    "\"rocksdb_other_bytes\":{},",
                    "\"binary_cas_manifest_rows\":{},",
                    "\"binary_cas_empty_blob_rows\":{},",
                    "\"binary_cas_single_chunk_blob_rows\":{},",
                    "\"binary_cas_chunked_blob_rows\":{},",
                    "\"binary_cas_manifest_chunk_rows\":{},",
                    "\"binary_cas_chunk_rows\":{},",
                    "\"binary_cas_total_chunk_refs\":{},",
                    "\"binary_cas_logical_blob_bytes\":{}",
                    "}}"
                ),
                args.backend.name(),
                blob_min_json,
                duration_ms(copy_elapsed),
                duration_ms(open_elapsed),
                duration_ms(warm_elapsed),
                open_metrics.chunk_lookup_count,
                open_metrics.chunk_lookup_batch_count,
                open_metrics.chunk_lookup_hit_count,
                open_metrics.chunk_lookup_miss_count,
                metric_duration_us(open_metrics.chunk_lookup_elapsed_ns),
                open_metrics.transaction_duplicate_chunk_count,
                warm_metrics
                    .map(|metrics| metrics.chunk_lookup_count)
                    .unwrap_or_default(),
                warm_metrics
                    .map(|metrics| metrics.chunk_lookup_batch_count)
                    .unwrap_or_default(),
                warm_metrics
                    .map(|metrics| metrics.chunk_lookup_hit_count)
                    .unwrap_or_default(),
                warm_metrics
                    .map(|metrics| metrics.chunk_lookup_miss_count)
                    .unwrap_or_default(),
                warm_metrics
                    .map(|metrics| metric_duration_us(metrics.chunk_lookup_elapsed_ns))
                    .unwrap_or_default(),
                warm_metrics
                    .map(|metrics| metrics.transaction_duplicate_chunk_count)
                    .unwrap_or_default(),
                read_bench.all_files_ms,
                read_bench.all_files_count,
                read_bench.all_files_bytes,
                read_bench.largest_files_ms,
                read_bench.largest_files_repeat_ms,
                read_bench.largest_files_count,
                read_bench.largest_files_bytes,
                read_bench.small_sample_ms,
                read_bench.small_sample_count,
                read_bench.small_sample_bytes,
                compact_ms_json,
                workspace_json,
                stats.corpus_file_count,
                stats.corpus_bytes,
                stats.lix_total_bytes,
                stats.rocksdb_total_bytes,
                stats.rocksdb_sst_bytes,
                stats.rocksdb_blob_bytes,
                stats.rocksdb_wal_bytes,
                stats.rocksdb_log_bytes,
                stats.rocksdb_manifest_bytes,
                stats.rocksdb_options_bytes,
                stats.rocksdb_other_bytes,
                stats.binary_cas_manifest_rows,
                stats.binary_cas_empty_blob_rows,
                stats.binary_cas_single_chunk_blob_rows,
                stats.binary_cas_chunked_blob_rows,
                stats.binary_cas_manifest_chunk_rows,
                stats.binary_cas_chunk_rows,
                stats.binary_cas_total_chunk_refs,
                stats.binary_cas_logical_blob_bytes
            ),
            _ => println!(
                concat!(
                    "{{",
                    "\"backend\":\"{}\",",
                    "\"blob_min_size\":{},",
                    "\"open_ms\":{},",
                    "\"open_binary_cas_chunk_lookup_count\":{},",
                    "\"open_binary_cas_chunk_lookup_batch_count\":{},",
                    "\"open_binary_cas_chunk_lookup_hit_count\":{},",
                    "\"open_binary_cas_chunk_lookup_miss_count\":{},",
                    "\"open_binary_cas_chunk_lookup_time_us\":{},",
                    "\"open_binary_cas_transaction_duplicate_chunk_count\":{},",
                    "\"read_all_files_ms\":{},",
                    "\"read_all_files_count\":{},",
                    "\"read_all_files_bytes\":{},",
                    "\"read_largest_files_ms\":{},",
                    "\"read_largest_files_repeat_ms\":{},",
                    "\"read_largest_files_count\":{},",
                    "\"read_largest_files_bytes\":{},",
                    "\"read_small_sample_ms\":{},",
                    "\"read_small_sample_count\":{},",
                    "\"read_small_sample_bytes\":{},",
                    "\"compact_ms\":{},",
                    "\"workspace_path\":{},",
                    "\"corpus_file_count\":{},",
                    "\"corpus_bytes\":{},",
                    "\"lix_total_bytes\":{},",
                    "\"rocksdb_total_bytes\":{},",
                    "\"rocksdb_sst_bytes\":{},",
                    "\"rocksdb_blob_bytes\":{},",
                    "\"rocksdb_wal_bytes\":{},",
                    "\"rocksdb_log_bytes\":{},",
                    "\"rocksdb_manifest_bytes\":{},",
                    "\"rocksdb_options_bytes\":{},",
                    "\"rocksdb_other_bytes\":{},",
                    "\"binary_cas_manifest_rows\":{},",
                    "\"binary_cas_empty_blob_rows\":{},",
                    "\"binary_cas_single_chunk_blob_rows\":{},",
                    "\"binary_cas_chunked_blob_rows\":{},",
                    "\"binary_cas_manifest_chunk_rows\":{},",
                    "\"binary_cas_chunk_rows\":{},",
                    "\"binary_cas_total_chunk_refs\":{},",
                    "\"binary_cas_logical_blob_bytes\":{}",
                    "}}"
                ),
                args.backend.name(),
                blob_min_json,
                duration_ms(open_elapsed),
                open_metrics.chunk_lookup_count,
                open_metrics.chunk_lookup_batch_count,
                open_metrics.chunk_lookup_hit_count,
                open_metrics.chunk_lookup_miss_count,
                metric_duration_us(open_metrics.chunk_lookup_elapsed_ns),
                open_metrics.transaction_duplicate_chunk_count,
                read_bench.all_files_ms,
                read_bench.all_files_count,
                read_bench.all_files_bytes,
                read_bench.largest_files_ms,
                read_bench.largest_files_repeat_ms,
                read_bench.largest_files_count,
                read_bench.largest_files_bytes,
                read_bench.small_sample_ms,
                read_bench.small_sample_count,
                read_bench.small_sample_bytes,
                compact_ms_json,
                workspace_json,
                stats.corpus_file_count,
                stats.corpus_bytes,
                stats.lix_total_bytes,
                stats.rocksdb_total_bytes,
                stats.rocksdb_sst_bytes,
                stats.rocksdb_blob_bytes,
                stats.rocksdb_wal_bytes,
                stats.rocksdb_log_bytes,
                stats.rocksdb_manifest_bytes,
                stats.rocksdb_options_bytes,
                stats.rocksdb_other_bytes,
                stats.binary_cas_manifest_rows,
                stats.binary_cas_empty_blob_rows,
                stats.binary_cas_single_chunk_blob_rows,
                stats.binary_cas_chunked_blob_rows,
                stats.binary_cas_manifest_chunk_rows,
                stats.binary_cas_chunk_rows,
                stats.binary_cas_total_chunk_refs,
                stats.binary_cas_logical_blob_bytes
            ),
        }
    } else if args.in_place {
        println!("OPEN_MS={}", duration_ms(open_elapsed));
        print_open_metrics("OPEN", open_metrics);
        if let Some(compact_elapsed) = compact_elapsed {
            println!("COMPACT_MS={}", duration_ms(compact_elapsed));
        }
        if let Some(read_bench) = read_bench {
            print_read_bench(read_bench);
        }
        print_text_stats(stats, workspace_path);
    } else {
        println!("COLD_OPEN_MS={}", duration_ms(open_elapsed));
        print_open_metrics("COLD", open_metrics);
        if let (Some(warm_elapsed), Some(warm_metrics)) = (warm_elapsed, warm_metrics) {
            println!("WARM_REOPEN_MS={}", duration_ms(warm_elapsed));
            print_open_metrics("WARM", warm_metrics);
        }
        if let Some(compact_elapsed) = compact_elapsed {
            println!("COMPACT_MS={}", duration_ms(compact_elapsed));
        }
        if let Some(read_bench) = read_bench {
            print_read_bench(read_bench);
        }
        print_text_stats(stats, workspace_path);
    }
}

fn print_open_metrics(prefix: &str, metrics: &BinaryCasWriteMetrics) {
    println!(
        "{prefix}_BINARY_CAS_CHUNK_LOOKUP_COUNT={}",
        metrics.chunk_lookup_count
    );
    println!(
        "{prefix}_BINARY_CAS_CHUNK_LOOKUP_BATCH_COUNT={}",
        metrics.chunk_lookup_batch_count
    );
    println!(
        "{prefix}_BINARY_CAS_CHUNK_LOOKUP_HIT_COUNT={}",
        metrics.chunk_lookup_hit_count
    );
    println!(
        "{prefix}_BINARY_CAS_CHUNK_LOOKUP_MISS_COUNT={}",
        metrics.chunk_lookup_miss_count
    );
    println!(
        "{prefix}_BINARY_CAS_CHUNK_LOOKUP_TIME_US={}",
        metric_duration_us(metrics.chunk_lookup_elapsed_ns)
    );
    println!(
        "{prefix}_BINARY_CAS_TRANSACTION_DUPLICATE_CHUNK_COUNT={}",
        metrics.transaction_duplicate_chunk_count
    );
}

fn print_read_bench(read_bench: &ReadBenchStats) {
    println!("READ_ALL_FILES_MS={}", read_bench.all_files_ms);
    println!("READ_ALL_FILES_COUNT={}", read_bench.all_files_count);
    println!("READ_ALL_FILES_BYTES={}", read_bench.all_files_bytes);
    println!("READ_LARGEST_FILES_MS={}", read_bench.largest_files_ms);
    println!(
        "READ_LARGEST_FILES_REPEAT_MS={}",
        read_bench.largest_files_repeat_ms
    );
    println!(
        "READ_LARGEST_FILES_COUNT={}",
        read_bench.largest_files_count
    );
    println!(
        "READ_LARGEST_FILES_BYTES={}",
        read_bench.largest_files_bytes
    );
    println!("READ_SMALL_SAMPLE_MS={}", read_bench.small_sample_ms);
    println!("READ_SMALL_SAMPLE_COUNT={}", read_bench.small_sample_count);
    println!("READ_SMALL_SAMPLE_BYTES={}", read_bench.small_sample_bytes);
}

fn print_text_stats(stats: &ProfileStats, workspace_path: Option<&Path>) {
    if let Some(workspace_path) = workspace_path {
        println!("WORKSPACE_PATH={}", workspace_path.display());
    }
    println!("CORPUS_FILE_COUNT={}", stats.corpus_file_count);
    println!("CORPUS_BYTES={}", stats.corpus_bytes);
    println!("LIX_TOTAL_BYTES={}", stats.lix_total_bytes);
    println!("ROCKSDB_TOTAL_BYTES={}", stats.rocksdb_total_bytes);
    println!("ROCKSDB_SST_BYTES={}", stats.rocksdb_sst_bytes);
    println!("ROCKSDB_BLOB_BYTES={}", stats.rocksdb_blob_bytes);
    println!("ROCKSDB_WAL_BYTES={}", stats.rocksdb_wal_bytes);
    println!("ROCKSDB_LOG_BYTES={}", stats.rocksdb_log_bytes);
    println!("ROCKSDB_MANIFEST_BYTES={}", stats.rocksdb_manifest_bytes);
    println!("ROCKSDB_OPTIONS_BYTES={}", stats.rocksdb_options_bytes);
    println!("ROCKSDB_OTHER_BYTES={}", stats.rocksdb_other_bytes);
    println!(
        "BINARY_CAS_MANIFEST_ROWS={}",
        stats.binary_cas_manifest_rows
    );
    println!(
        "BINARY_CAS_EMPTY_BLOB_ROWS={}",
        stats.binary_cas_empty_blob_rows
    );
    println!(
        "BINARY_CAS_SINGLE_CHUNK_BLOB_ROWS={}",
        stats.binary_cas_single_chunk_blob_rows
    );
    println!(
        "BINARY_CAS_CHUNKED_BLOB_ROWS={}",
        stats.binary_cas_chunked_blob_rows
    );
    println!(
        "BINARY_CAS_MANIFEST_CHUNK_ROWS={}",
        stats.binary_cas_manifest_chunk_rows
    );
    println!("BINARY_CAS_CHUNK_ROWS={}", stats.binary_cas_chunk_rows);
    println!(
        "BINARY_CAS_TOTAL_CHUNK_REFS={}",
        stats.binary_cas_total_chunk_refs
    );
    println!(
        "BINARY_CAS_LOGICAL_BLOB_BYTES={}",
        stats.binary_cas_logical_blob_bytes
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = parse_args();
    let src = Path::new(&args.src);

    if args.in_place {
        let (backend, open_elapsed, open_metrics) = open_with_metrics(args.backend, src).await;
        eprintln!("{} in-place open: {open_elapsed:?}", args.backend.name());
        let read_bench = if args.read_bench {
            Some(run_read_benchmark(&backend, src).await)
        } else {
            None
        };
        let compact_elapsed = compact_backend_if_requested(&args, &backend);
        let mut stats = collect_profile_stats(src);
        collect_backend_profile_stats(&backend, &mut stats);
        drop(backend);
        print_result(
            &args,
            None,
            open_elapsed,
            &open_metrics,
            None,
            None,
            compact_elapsed,
            read_bench.as_ref(),
            &stats,
            Some(src),
        );
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path().join("workspace");

    let t_copy = Instant::now();
    copy_dir(src, &work);
    let copy_elapsed = t_copy.elapsed();
    eprintln!("copy: {copy_elapsed:?}");

    let repeat: usize = std::env::var("REPEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    let (backend, open_elapsed, open_metrics) = open_with_metrics(args.backend, &work).await;
    eprintln!("{} cold open: {open_elapsed:?}", args.backend.name());
    drop(backend);

    // Warm reopen (now .lix exists).
    let (backend, warm_elapsed, warm_metrics) = open_with_metrics(args.backend, &work).await;
    eprintln!("{} warm reopen: {warm_elapsed:?}", args.backend.name());
    let read_bench = if args.read_bench {
        Some(run_read_benchmark(&backend, &work).await)
    } else {
        None
    };
    let compact_elapsed = compact_backend_if_requested(&args, &backend);
    let mut stats = collect_profile_stats(&work);
    collect_backend_profile_stats(&backend, &mut stats);
    drop(backend);

    // Repeated cold opens into fresh temp dirs for profiling sample density.
    for i in 0..repeat {
        let tmp_i = tempfile::tempdir().unwrap();
        let work_i = tmp_i.path().join("workspace");
        copy_dir(src, &work_i);
        let t = Instant::now();
        let backend = args.backend.open(&work_i).await;
        if i == repeat - 1 {
            let elapsed = t.elapsed();
            eprintln!(
                "{} cold open (repeat {repeat}): {elapsed:?}",
                args.backend.name()
            );
        }
        drop(backend);
    }

    let workspace_path = args.keep_workspace.then_some(work.as_path());
    print_result(
        &args,
        Some(copy_elapsed),
        open_elapsed,
        &open_metrics,
        Some(warm_elapsed),
        Some(&warm_metrics),
        compact_elapsed,
        read_bench.as_ref(),
        &stats,
        workspace_path,
    );
    if args.keep_workspace {
        let _kept_root: PathBuf = tmp.keep();
        eprintln!("kept workspace: {}", work.display());
    }
}
