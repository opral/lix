use std::alloc::{GlobalAlloc, Layout, System};
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use lix::Value;
use lix::integration::Engine;
use lix::storage::{CoreProjection, KeyRange, ReadOptions, ScanOptions, Storage, StorageRead};
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::collect_repository_gc_for_bench;
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters};

const PART_BYTES: usize = 16 * 1024 * 1024;
const MANIFEST_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0001);
const MANIFEST_CHUNK_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0002);
const PAYLOAD_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0003);
const PRESENCE_SPACE: lix::storage::SpaceId = lix::storage::SpaceId(0x0005_0004);

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static REALLOC_CALLS: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        DEALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        REALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        DEALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AllocationSnapshot {
    alloc_calls: u64,
    alloc_bytes: u64,
    dealloc_calls: u64,
    dealloc_bytes: u64,
    realloc_calls: u64,
}

impl AllocationSnapshot {
    fn now() -> Self {
        Self {
            alloc_calls: ALLOC_CALLS.load(Ordering::Relaxed),
            alloc_bytes: ALLOC_BYTES.load(Ordering::Relaxed),
            dealloc_calls: DEALLOC_CALLS.load(Ordering::Relaxed),
            dealloc_bytes: DEALLOC_BYTES.load(Ordering::Relaxed),
            realloc_calls: REALLOC_CALLS.load(Ordering::Relaxed),
        }
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            alloc_calls: self.alloc_calls.saturating_sub(earlier.alloc_calls),
            alloc_bytes: self.alloc_bytes.saturating_sub(earlier.alloc_bytes),
            dealloc_calls: self.dealloc_calls.saturating_sub(earlier.dealloc_calls),
            dealloc_bytes: self.dealloc_bytes.saturating_sub(earlier.dealloc_bytes),
            realloc_calls: self.realloc_calls.saturating_sub(earlier.realloc_calls),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct SpaceStats {
    rows: u64,
    value_bytes: u64,
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let path = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("/tmp/lix-cas-gc-qualification");
    let size_mib = args
        .get(3)
        .map(|value| value.parse::<usize>().expect("size must be MiB"))
        .unwrap_or(64);
    let retention_checkpoints = args
        .get(4)
        .map(|value| value.parse::<usize>().expect("retention must be a count"))
        .unwrap_or(128);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create qualification runtime");
    runtime.block_on(async move {
        let allocation_before = AllocationSnapshot::now();
        match backend {
            "rocksdb" => {
                let storage = RocksDB::open(path).expect("open RocksDB");
                let result = run(storage.clone(), path, size_mib, retention_checkpoints).await;
                storage.flush().expect("flush RocksDB");
                print_result("rocksdb", path, size_mib, result);
                print_allocations(backend, size_mib, path, allocation_before);
            }
            "slatedb" => {
                let counters = SlateDBIoCounters::default();
                let io_before = counters.snapshot();
                let storage =
                    SlateDB::open_with_io_counters(path, counters.clone()).expect("open SlateDB");
                let result = run(storage.clone(), path, size_mib, retention_checkpoints).await;
                storage.flush().await.expect("flush SlateDB");
                print_result("slatedb", path, size_mib, result);
                println!(
                    "cas_gc_io,backend=slatedb,size_mib={size_mib},path={path},delta={:?}",
                    counters.snapshot().saturating_sub(io_before)
                );
                print_allocations(backend, size_mib, path, allocation_before);
            }
            other => panic!("backend must be rocksdb or slatedb, got {other}"),
        }
    });
}

fn print_allocations(backend: &str, size_mib: usize, path: &str, before: AllocationSnapshot) {
    println!(
        "cas_gc_alloc,backend={backend},size_mib={size_mib},path={path},delta={:?}",
        AllocationSnapshot::now().saturating_sub(before)
    );
}

async fn run<S>(storage: S, path: &str, size_mib: usize, retention_checkpoints: usize) -> RunResult
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let init = Engine::initialize(storage.clone())
        .await
        .expect("initialize repository");
    let engine = Engine::new(storage.clone()).await.expect("open engine");
    let session = engine
        .open_session(init.main_branch_id.clone())
        .await
        .expect("open session");
    let total_size = (size_mib * 1024 * 1024) as u64;
    let upload_started = Instant::now();
    let mut offset = 0_u64;
    let mut part = 0_u64;
    while offset < total_size {
        let part_len = (total_size - offset).min(PART_BYTES as u64) as usize;
        let bytes = random_part(part, part_len);
        session
            .upsert_file_content_part(
                "cas-gc-qualification".to_owned(),
                "/qualification/blob.bin".to_owned(),
                offset,
                total_size,
                bytes.into(),
            )
            .await
            .expect("upload part");
        offset += part_len as u64;
        part += 1;
    }
    let upload_ms = upload_started.elapsed().as_millis();
    let adapter = StorageAdapter::new(storage.clone());
    let before_delete = cas_stats(&storage).await;
    session
        .execute(
            "DELETE FROM lix_file WHERE path = $1",
            &[Value::Text("/qualification/blob.bin".to_owned())],
        )
        .await
        .expect("delete qualification file");
    let immediate_gc = collect_repository_gc_for_bench(&adapter)
        .await
        .expect("immediate GC");
    let immediate = cas_stats(&storage).await;

    for revision in 0..retention_checkpoints {
        session
            .upsert_file_content(
                "/qualification/retention.json".to_owned(),
                format!("{{\"revision\":{revision}}}").into_bytes().into(),
            )
            .await
            .expect("retention commit");
        session
            .create_checkpoint()
            .await
            .expect("retention checkpoint");
    }
    let retained_gc = collect_repository_gc_for_bench(&adapter)
        .await
        .expect("retained GC");
    let after_retention = cas_stats(&storage).await;
    RunResult {
        upload_ms,
        retention_checkpoints,
        before_delete,
        immediate,
        after_retention,
        immediate_gc,
        retained_gc,
        physical_bytes: directory_bytes(Path::new(path)),
    }
}

#[derive(Debug)]
struct RunResult {
    upload_ms: u128,
    retention_checkpoints: usize,
    before_delete: [SpaceStats; 4],
    immediate: [SpaceStats; 4],
    after_retention: [SpaceStats; 4],
    immediate_gc: lix::storage_bench::RepositoryGcCommitBenchResult,
    retained_gc: lix::storage_bench::RepositoryGcCommitBenchResult,
    physical_bytes: u64,
}

fn print_result(backend: &str, path: &str, size_mib: usize, result: impl std::fmt::Debug) {
    println!(
        "cas_gc_qualification,backend={backend},size_mib={size_mib},path={path},result={result:?}"
    );
}

fn random_part(part: u64, len: usize) -> Vec<u8> {
    let mut output = vec![0_u8; len];
    for (block, bytes) in output.chunks_mut(32).enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"lix cas gc qualification payload v1");
        hasher.update(&part.to_le_bytes());
        hasher.update(&(block as u64).to_le_bytes());
        let digest = hasher.finalize();
        let count = bytes.len();
        bytes.copy_from_slice(&digest.as_bytes()[..count]);
    }
    output
}

async fn cas_stats<S: Storage>(storage: &S) -> [SpaceStats; 4] {
    [
        space_stats(storage, MANIFEST_SPACE).await,
        space_stats(storage, MANIFEST_CHUNK_SPACE).await,
        space_stats(storage, PAYLOAD_SPACE).await,
        space_stats(storage, PRESENCE_SPACE).await,
    ]
}

async fn space_stats<S: Storage>(storage: &S, space_id: lix::storage::SpaceId) -> SpaceStats {
    let space = if space_id == PAYLOAD_SPACE {
        lix::storage::StorageSpace::immutable(space_id, "qualification.cas.payload")
    } else {
        lix::storage::StorageSpace::mutable(space_id, "qualification.cas.metadata")
    };
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open stats read");
    let mut stats = SpaceStats {
        rows: 0,
        value_bytes: 0,
    };
    let mut resume_after = None;
    loop {
        let page = read
            .scan(
                space,
                KeyRange {
                    lower: Bound::Unbounded,
                    upper: Bound::Unbounded,
                },
                ScanOptions {
                    projection: CoreProjection::FullValue,
                    resume_after,
                    ..ScanOptions::default()
                },
            )
            .await
            .expect("scan CAS stats");
        stats.rows += page.entries.len() as u64;
        stats.value_bytes += page
            .entries
            .iter()
            .map(|entry| match &entry.value {
                lix::storage::ProjectedValue::FullValue(bytes) => bytes.len() as u64,
                lix::storage::ProjectedValue::KeyOnly => 0,
            })
            .sum::<u64>();
        if !page.has_more {
            break;
        }
        resume_after = page.entries.last().map(|entry| entry.key.clone());
    }
    stats
}

fn directory_bytes(path: &Path) -> u64 {
    fn visit(path: &Path) -> u64 {
        let Ok(metadata) = std::fs::metadata(path) else {
            return 0;
        };
        if metadata.is_file() {
            return metadata.len();
        }
        std::fs::read_dir(path)
            .into_iter()
            .flat_map(|entries| entries.flatten())
            .map(|entry| visit(&entry.path()))
            .sum()
    }
    visit(path)
}
