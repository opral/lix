// Cold-open profiling harness for the filesystem backend.
//
// Usage:
//   cargo run --release --example profile_fs_open --features sqlite,rocksdb -- \
//     --backend sqlite <src_dir>
//
// Copies <src_dir> (sans any existing .lix) into a fresh temp dir, then times
// FsBackend::open on the cold workspace.

use std::path::Path;
use std::time::{Duration, Instant};

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
    src: String,
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
        src: src.expect(
            "usage: profile_fs_open [--json] [--in-place] [--backend sqlite|rocksdb|rocksdb-blob] [--blob-min bytes] <src_dir>",
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

fn print_result(
    args: &Args,
    copy_elapsed: Option<Duration>,
    open_elapsed: Duration,
    warm_elapsed: Option<Duration>,
) {
    if args.json {
        let blob_min_json = args
            .backend
            .blob_min_size()
            .map_or("null".to_string(), |size| size.to_string());
        match (copy_elapsed, warm_elapsed) {
            (Some(copy_elapsed), Some(warm_elapsed)) => println!(
                "{{\"backend\":\"{}\",\"blob_min_size\":{},\"copy_ms\":{},\"cold_open_ms\":{},\"warm_reopen_ms\":{}}}",
                args.backend.name(),
                blob_min_json,
                duration_ms(copy_elapsed),
                duration_ms(open_elapsed),
                duration_ms(warm_elapsed)
            ),
            _ => println!(
                "{{\"backend\":\"{}\",\"blob_min_size\":{},\"open_ms\":{}}}",
                args.backend.name(),
                blob_min_json,
                duration_ms(open_elapsed)
            ),
        }
    } else if args.in_place {
        println!("OPEN_MS={}", duration_ms(open_elapsed));
    } else {
        println!("COLD_OPEN_MS={}", duration_ms(open_elapsed));
    }
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
        drop(backend);
        print_result(&args, None, open_elapsed, None);
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

    print_result(&args, Some(copy_elapsed), open_elapsed, Some(warm_elapsed));
}
