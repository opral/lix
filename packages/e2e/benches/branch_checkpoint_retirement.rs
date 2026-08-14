use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::time::Instant;

#[cfg(not(target_family = "wasm"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

use lix::open_lix;
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    BranchCheckpointDeleteBenchResult, delete_and_commit_branch_plugin_checkpoints_for_bench,
    delete_branch_plugin_checkpoints_for_bench, seed_branch_plugin_checkpoints_for_bench,
};
use lix::{CreateBranchOptions, Value};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const BRANCH_ID: &str = "01920000-0000-7000-8000-000000000001";

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDB,
    SlateDB,
}

impl Backend {
    fn parse(value: &str) -> Self {
        match value {
            "rocksdb" => Self::RocksDB,
            "slatedb" => Self::SlateDB,
            _ => panic!("backend must be rocksdb or slatedb, got '{value}'"),
        }
    }
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RocksDB => "rocksdb",
            Self::SlateDB => "slatedb",
        })
    }
}

fn main() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create branch-checkpoint benchmark runtime")
        .block_on(run());
}

async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    let Some(command) = args.get(1).map(String::as_str) else {
        usage();
        return;
    };
    let Some(backend) = args.get(2).map(|value| Backend::parse(value)) else {
        usage();
        return;
    };
    let Some(path) = args.get(3).map(String::as_str) else {
        usage();
        return;
    };
    let file_count = args
        .get(4)
        .map(|value| value.parse::<usize>().expect("file_count must be positive"))
        .unwrap_or(10_000);
    let batch_or_samples = args
        .get(5)
        .map(|value| {
            value
                .parse::<usize>()
                .expect("batch/samples must be positive")
        })
        .unwrap_or(10_000)
        .max(1);
    match command {
        "setup" => {
            assert!(!Path::new(path).exists(), "refusing to overwrite {path}");
            match backend {
                Backend::RocksDB => {
                    let storage = RocksDB::open(path).expect("open RocksDB");
                    open_lix()
                        .with_storage(storage.clone())
                        .await
                        .expect("initialize RocksDB");
                    create_bench_branch(&storage).await;
                    let started = Instant::now();
                    seed_branch_plugin_checkpoints_for_bench(
                        &StorageAdapter::new(storage.clone()),
                        BRANCH_ID,
                        file_count,
                        batch_or_samples,
                    )
                    .await
                    .expect("seed branch checkpoints");
                    storage.flush().expect("flush RocksDB");
                    println!(
                        "branch_checkpoint_retirement,phase=setup,backend={backend},files={file_count},batch_size={batch_or_samples},elapsed_ms={},bytes={}",
                        started.elapsed().as_millis(),
                        directory_bytes(path),
                    );
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    let storage = SlateDB::open_with_io_counters(path, counters.clone())
                        .expect("open SlateDB");
                    open_lix()
                        .with_storage(storage.clone())
                        .await
                        .expect("initialize SlateDB");
                    create_bench_branch(&storage).await;
                    let before = counters.snapshot();
                    let started = Instant::now();
                    seed_branch_plugin_checkpoints_for_bench(
                        &StorageAdapter::new(storage.clone()),
                        BRANCH_ID,
                        file_count,
                        batch_or_samples,
                    )
                    .await
                    .expect("seed branch checkpoints");
                    storage.flush().await.expect("flush SlateDB");
                    let io = counters.snapshot().saturating_sub(before);
                    println!(
                        "branch_checkpoint_retirement,phase=setup,backend={backend},files={file_count},batch_size={batch_or_samples},elapsed_ms={},bytes={},read_objects={},read_bytes={},write_objects={},write_bytes={}",
                        started.elapsed().as_millis(),
                        directory_bytes(path),
                        io.read_objects,
                        io.read_bytes,
                        io.write_objects,
                        io.write_bytes,
                    );
                }
            }
        }
        "measure" => {
            let samples = batch_or_samples;
            let warmups = args
                .get(6)
                .map(|value| {
                    value
                        .parse::<usize>()
                        .expect("warmups must be non-negative")
                })
                .unwrap_or(1);
            match backend {
                Backend::RocksDB => {
                    measure(
                        StorageAdapter::new(RocksDB::open(path).expect("open RocksDB")),
                        backend,
                        path,
                        file_count,
                        samples,
                        warmups,
                        None,
                    )
                    .await
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    measure(
                        StorageAdapter::new(
                            SlateDB::open_with_io_counters(path, counters.clone())
                                .expect("open SlateDB"),
                        ),
                        backend,
                        path,
                        file_count,
                        samples,
                        warmups,
                        Some(counters),
                    )
                    .await;
                }
            }
        }
        "commit" => match backend {
            Backend::RocksDB => {
                let storage = StorageAdapter::new(RocksDB::open(path).expect("open RocksDB"));
                let started = Instant::now();
                let result =
                    delete_and_commit_branch_plugin_checkpoints_for_bench(&storage, BRANCH_ID)
                        .await
                        .expect("commit branch checkpoint delete");
                println!(
                    "branch_checkpoint_retirement,phase=commit,backend={backend},files={file_count},matched_entries={},staged_deletes={},read_us={},commit_us={},total_us={},elapsed_us={},backend_bytes={}",
                    result.matched_entries,
                    result.staged_deletes,
                    result.read_us,
                    result.commit_us,
                    result.total_us,
                    started.elapsed().as_micros(),
                    directory_bytes(path),
                );
            }
            Backend::SlateDB => {
                let counters = SlateDBIoCounters::default();
                let storage = StorageAdapter::new(
                    SlateDB::open_with_io_counters(path, counters.clone()).expect("open SlateDB"),
                );
                let before = counters.snapshot();
                let result =
                    delete_and_commit_branch_plugin_checkpoints_for_bench(&storage, BRANCH_ID)
                        .await
                        .expect("commit branch checkpoint delete");
                let io = counters.snapshot().saturating_sub(before);
                println!(
                    "branch_checkpoint_retirement,phase=commit,backend={backend},files={file_count},matched_entries={},staged_deletes={},read_us={},commit_us={},total_us={},read_objects={},read_bytes={},write_objects={},write_bytes={},backend_bytes={}",
                    result.matched_entries,
                    result.staged_deletes,
                    result.read_us,
                    result.commit_us,
                    result.total_us,
                    io.read_objects,
                    io.read_bytes,
                    io.write_objects,
                    io.write_bytes,
                    directory_bytes(path),
                );
            }
        },
        "branch-delete" => match backend {
            Backend::RocksDB => {
                let storage = RocksDB::open(path).expect("open RocksDB");
                let started = Instant::now();
                delete_bench_branch(&storage).await;
                println!(
                    "branch_checkpoint_retirement,phase=branch_delete,backend={backend},files={file_count},elapsed_us={},foreground_prefix_scan=0,backend_bytes={}",
                    started.elapsed().as_micros(),
                    directory_bytes(path),
                );
            }
            Backend::SlateDB => {
                let counters = SlateDBIoCounters::default();
                let storage =
                    SlateDB::open_with_io_counters(path, counters.clone()).expect("open SlateDB");
                let before = counters.snapshot();
                let started = Instant::now();
                delete_bench_branch(&storage).await;
                let io = counters.snapshot().saturating_sub(before);
                println!(
                    "branch_checkpoint_retirement,phase=branch_delete,backend={backend},files={file_count},elapsed_us={},foreground_prefix_scan=0,read_objects={},read_bytes={},write_objects={},write_bytes={},backend_bytes={}",
                    started.elapsed().as_micros(),
                    io.read_objects,
                    io.read_bytes,
                    io.write_objects,
                    io.write_bytes,
                    directory_bytes(path),
                );
            }
        },
        _ => usage(),
    }
}

async fn create_bench_branch<StorageImpl>(storage: &StorageImpl)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open branch-delete benchmark lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open branch-delete benchmark workspace");
    session
        .create_branch(CreateBranchOptions {
            id: Some(BRANCH_ID.to_owned()),
            name: "branch-checkpoint-retirement".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create branch-delete benchmark branch");
}

async fn delete_bench_branch<StorageImpl>(storage: &StorageImpl)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open branch-delete benchmark lix");
    let session = lix
        .open_another_session()
        .await
        .expect("open branch-delete benchmark workspace");
    session
        .execute(
            "DELETE FROM lix_branch WHERE id = $1",
            &[Value::Text(BRANCH_ID.to_owned())],
        )
        .await
        .expect("delete branch-delete benchmark branch");
}

async fn measure<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    backend: Backend,
    path: &str,
    file_count: usize,
    samples: usize,
    warmups: usize,
    counters: Option<SlateDBIoCounters>,
) where
    StorageImpl: Storage,
{
    for _ in 0..warmups {
        delete_branch_plugin_checkpoints_for_bench(&storage, BRANCH_ID)
            .await
            .expect("warm branch checkpoint delete");
    }
    let before_io = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let mut durations = Vec::with_capacity(samples);
    let mut results = Vec::<BranchCheckpointDeleteBenchResult>::with_capacity(samples);
    for _ in 0..samples {
        let started = Instant::now();
        results.push(
            delete_branch_plugin_checkpoints_for_bench(&storage, BRANCH_ID)
                .await
                .expect("measure branch checkpoint delete"),
        );
        durations.push(started.elapsed());
    }
    durations.sort_unstable();
    results.sort_unstable_by_key(|result| result.total_us);
    let result = results.last().expect("samples are positive");
    let io = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(before_io);
    println!(
        "branch_checkpoint_retirement,phase=measure,backend={backend},files={file_count},samples={samples},warmups={warmups},p50_us={},p95_us={},matched_entries={},staged_deletes={},read_us={},delete_descriptor_capacity={},read_objects={},read_bytes={},write_objects={},write_bytes={},backend_bytes={}",
        durations[durations.len() / 2].as_micros(),
        durations[((durations.len() * 95).saturating_sub(1) / 100).min(durations.len() - 1)]
            .as_micros(),
        result.matched_entries,
        result.staged_deletes,
        result.read_us,
        result.delete_descriptor_capacity,
        io.read_objects,
        io.read_bytes,
        io.write_objects,
        io.write_bytes,
        directory_bytes(path),
    );
}

fn directory_bytes(path: &str) -> u64 {
    std::fs::read_dir(path)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum()
}

fn usage() {
    eprintln!(
        "usage: branch_checkpoint_retirement <setup|measure|commit|branch-delete> <rocksdb|slatedb> <path> <files> <batch-or-samples> [warmups]"
    );
}
