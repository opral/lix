// Cold-open profiling harness for the filesystem backend.
//
// Usage:
//   cargo run --release --example profile_fs_open --features sqlite,rocksdb -- \
//     --backend sqlite <src_dir>
//
// Copies <src_dir> (sans any existing .lix) into a fresh temp dir, then times
// FsBackend::open on the cold workspace. Pass --keep-workspace to preserve the
// copied temp workspace for inspection.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use lix_engine::{
    Backend as _, BackendRead as _, BinaryCasStorageStats, ReadOptions,
    collect_binary_cas_storage_stats,
};
use lix_sdk::FsBackend;
#[cfg(feature = "rocksdb")]
use lix_sdk::{FsBackendFilter, RocksDbBlobOptions};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileBackend {
    Sqlite,
    #[cfg(feature = "rocksdb")]
    RocksDb,
    #[cfg(feature = "rocksdb")]
    RocksDbBlob {
        min_blob_size: u64,
    },
}

#[derive(Debug)]
struct Args {
    backend: ProfileBackend,
    in_place: bool,
    json: bool,
    keep_workspace: bool,
    src: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ProfileStats {
    corpus_file_count: u64,
    corpus_bytes: u64,
    lix_total_bytes: u64,
    sqlite_db_bytes: u64,
    sqlite_wal_bytes: u64,
    sqlite_shm_bytes: u64,
    sqlite_other_bytes: u64,
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

impl ProfileBackend {
    fn name(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            #[cfg(feature = "rocksdb")]
            Self::RocksDb => "rocksdb",
            #[cfg(feature = "rocksdb")]
            Self::RocksDbBlob { .. } => "rocksdb-blob",
        }
    }

    fn blob_min_size(self) -> Option<u64> {
        match self {
            #[cfg(feature = "rocksdb")]
            Self::RocksDbBlob { min_blob_size } => Some(min_blob_size),
            Self::Sqlite => None,
            #[cfg(feature = "rocksdb")]
            Self::RocksDb => None,
        }
    }

    async fn open(self, path: &Path) -> FsBackend {
        match self {
            Self::Sqlite => FsBackend::open(path).await.unwrap(),
            #[cfg(feature = "rocksdb")]
            Self::RocksDb => FsBackend::open_rocksdb(path).await.unwrap(),
            #[cfg(feature = "rocksdb")]
            Self::RocksDbBlob { min_blob_size } => FsBackend::open_rocksdb_with_blob_options(
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
    let mut backend = ProfileBackend::Sqlite;
    let mut in_place = false;
    let mut json = false;
    let mut keep_workspace = false;
    let mut src = None;

    while let Some(arg) = raw.next() {
        match arg.as_str() {
            "--backend" => {
                let value = raw.next().expect("--backend requires a value");
                backend = match value.as_str() {
                    "sqlite" => ProfileBackend::Sqlite,
                    "rocksdb" => {
                        #[cfg(feature = "rocksdb")]
                        {
                            ProfileBackend::RocksDb
                        }
                        #[cfg(not(feature = "rocksdb"))]
                        {
                            panic!("profile_fs_open was built without the rocksdb feature")
                        }
                    }
                    "rocksdb-blob" => {
                        #[cfg(feature = "rocksdb")]
                        {
                            ProfileBackend::RocksDbBlob {
                                min_blob_size: 64 * 1024,
                            }
                        }
                        #[cfg(not(feature = "rocksdb"))]
                        {
                            panic!("profile_fs_open was built without the rocksdb feature")
                        }
                    }
                    other => panic!("unknown backend '{other}'"),
                };
            }
            "--blob-min" => {
                let value = raw.next().expect("--blob-min requires a value");
                let min_blob_size = parse_size(&value);
                #[cfg(feature = "rocksdb")]
                {
                    backend = ProfileBackend::RocksDbBlob { min_blob_size };
                }
                #[cfg(not(feature = "rocksdb"))]
                {
                    let _ = min_blob_size;
                    panic!("profile_fs_open was built without the rocksdb feature")
                }
            }
            "--in-place" => in_place = true,
            "--json" => json = true,
            "--keep-workspace" => keep_workspace = true,
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
        in_place,
        json,
        keep_workspace,
        src: src.expect(
            "usage: profile_fs_open [--json] [--in-place] [--keep-workspace] [--backend sqlite|rocksdb|rocksdb-blob] [--blob-min bytes] <src_dir>",
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
    let internal_dir = lix_dir.join(".internal");
    let rocksdb_dir = internal_dir.join("rocksdb");
    collect_lix_stats_recursive(&lix_dir, &internal_dir, &rocksdb_dir, stats);
}

fn collect_lix_stats_recursive(
    dir: &Path,
    internal_dir: &Path,
    rocksdb_dir: &Path,
    stats: &mut ProfileStats,
) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let entry = entry.unwrap();
        let path = entry.path();
        let metadata = entry.metadata().unwrap();
        if metadata.is_dir() {
            collect_lix_stats_recursive(&path, internal_dir, rocksdb_dir, stats);
            continue;
        }
        if !metadata.is_file() {
            continue;
        }

        let bytes = metadata.len();
        stats.lix_total_bytes += bytes;
        if path.strip_prefix(rocksdb_dir).is_ok() {
            classify_rocksdb_file(&path, bytes, stats);
        } else if path.strip_prefix(internal_dir).is_ok() {
            classify_sqlite_file(&path, bytes, stats);
        }
    }
}

fn classify_sqlite_file(path: &Path, bytes: u64, stats: &mut ProfileStats) {
    match path.file_name().and_then(|name| name.to_str()) {
        Some("db.sqlite") => stats.sqlite_db_bytes += bytes,
        Some("db.sqlite-wal") => stats.sqlite_wal_bytes += bytes,
        Some("db.sqlite-shm") => stats.sqlite_shm_bytes += bytes,
        _ => stats.sqlite_other_bytes += bytes,
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
    warm_elapsed: Option<Duration>,
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
        match (copy_elapsed, warm_elapsed) {
            (Some(copy_elapsed), Some(warm_elapsed)) => println!(
                concat!(
                    "{{",
                    "\"backend\":\"{}\",",
                    "\"blob_min_size\":{},",
                    "\"copy_ms\":{},",
                    "\"cold_open_ms\":{},",
                    "\"warm_reopen_ms\":{},",
                    "\"workspace_path\":{},",
                    "\"corpus_file_count\":{},",
                    "\"corpus_bytes\":{},",
                    "\"lix_total_bytes\":{},",
                    "\"sqlite_db_bytes\":{},",
                    "\"sqlite_wal_bytes\":{},",
                    "\"sqlite_shm_bytes\":{},",
                    "\"sqlite_other_bytes\":{},",
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
                workspace_json,
                stats.corpus_file_count,
                stats.corpus_bytes,
                stats.lix_total_bytes,
                stats.sqlite_db_bytes,
                stats.sqlite_wal_bytes,
                stats.sqlite_shm_bytes,
                stats.sqlite_other_bytes,
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
                    "\"workspace_path\":{},",
                    "\"corpus_file_count\":{},",
                    "\"corpus_bytes\":{},",
                    "\"lix_total_bytes\":{},",
                    "\"sqlite_db_bytes\":{},",
                    "\"sqlite_wal_bytes\":{},",
                    "\"sqlite_shm_bytes\":{},",
                    "\"sqlite_other_bytes\":{},",
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
                workspace_json,
                stats.corpus_file_count,
                stats.corpus_bytes,
                stats.lix_total_bytes,
                stats.sqlite_db_bytes,
                stats.sqlite_wal_bytes,
                stats.sqlite_shm_bytes,
                stats.sqlite_other_bytes,
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
        print_text_stats(stats, workspace_path);
    } else {
        println!("COLD_OPEN_MS={}", duration_ms(open_elapsed));
        print_text_stats(stats, workspace_path);
    }
}

fn print_text_stats(stats: &ProfileStats, workspace_path: Option<&Path>) {
    if let Some(workspace_path) = workspace_path {
        println!("WORKSPACE_PATH={}", workspace_path.display());
    }
    println!("CORPUS_FILE_COUNT={}", stats.corpus_file_count);
    println!("CORPUS_BYTES={}", stats.corpus_bytes);
    println!("LIX_TOTAL_BYTES={}", stats.lix_total_bytes);
    println!("SQLITE_DB_BYTES={}", stats.sqlite_db_bytes);
    println!("SQLITE_WAL_BYTES={}", stats.sqlite_wal_bytes);
    println!("SQLITE_SHM_BYTES={}", stats.sqlite_shm_bytes);
    println!("SQLITE_OTHER_BYTES={}", stats.sqlite_other_bytes);
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
        let t_open = Instant::now();
        let backend = args.backend.open(src).await;
        let open_elapsed = t_open.elapsed();
        eprintln!("{} in-place open: {open_elapsed:?}", args.backend.name());
        let mut stats = collect_profile_stats(src);
        collect_backend_profile_stats(&backend, &mut stats);
        drop(backend);
        print_result(&args, None, open_elapsed, None, &stats, Some(src));
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

    let t_open = Instant::now();
    let backend = args.backend.open(&work).await;
    let open_elapsed = t_open.elapsed();
    eprintln!("{} cold open: {open_elapsed:?}", args.backend.name());
    drop(backend);

    // Warm reopen (now .lix exists).
    let t_warm = Instant::now();
    let backend = args.backend.open(&work).await;
    let warm_elapsed = t_warm.elapsed();
    eprintln!("{} warm reopen: {warm_elapsed:?}", args.backend.name());
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
        Some(warm_elapsed),
        &stats,
        workspace_path,
    );
    if args.keep_workspace {
        let _kept_root: PathBuf = tmp.keep();
        eprintln!("kept workspace: {}", work.display());
    }
}
