use std::alloc::{GlobalAlloc, Layout};
use std::fmt::{self, Display, Formatter};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use lix::CreateBranchOptions;
use lix::integration::Engine;
use lix::storage::Storage;
use lix::storage_adapter::StorageAdapter;
use lix::storage_bench::{
    count_tree_chunks_for_bench, run_tree_sweep_epoch_for_bench,
    run_tree_sweep_epoch_steps_for_bench, seed_tree_sweep_garbage_for_bench,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

struct CountingAllocator;

static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static PEAK_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);

#[global_allocator]
static GLOBAL_ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { mimalloc::MiMalloc.alloc(layout) };
        if !pointer.is_null() {
            record_allocation(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { mimalloc::MiMalloc.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { mimalloc::MiMalloc.realloc(pointer, layout, new_size) };
        if !next.is_null() {
            if new_size >= layout.size() {
                record_allocation(new_size - layout.size());
            } else {
                LIVE_BYTES.fetch_sub((layout.size() - new_size) as u64, Ordering::Relaxed);
            }
        }
        next
    }
}

fn record_allocation(bytes: usize) {
    ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
    ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    let live = LIVE_BYTES.fetch_add(bytes as u64, Ordering::Relaxed) + bytes as u64;
    PEAK_LIVE_BYTES.fetch_max(live, Ordering::Relaxed);
}

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
        match self {
            Self::RocksDB => formatter.write_str("rocksdb"),
            Self::SlateDB => formatter.write_str("slatedb"),
        }
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create epoch tree-GC benchmark runtime");
    runtime.block_on(run());
}

async fn run() {
    let args = std::env::args().collect::<Vec<_>>();
    let Some(command) = args.get(1).map(String::as_str) else {
        print_usage();
        return;
    };
    let Some(backend) = args.get(2).map(|value| Backend::parse(value)) else {
        print_usage();
        return;
    };
    let Some(path) = args.get(3).map(String::as_str) else {
        print_usage();
        return;
    };
    match command {
        "setup" => {
            let garbage = parse_positive(args.get(4), "garbage chunks");
            let roots = parse_positive(args.get(5), "active roots");
            assert!(
                !Path::new(path).exists(),
                "refusing to overwrite epoch tree-GC fixture {path}"
            );
            match backend {
                Backend::RocksDB => {
                    let storage = RocksDB::open(path).expect("open epoch tree-GC RocksDB");
                    setup(storage.clone(), backend, path, garbage, roots).await;
                    storage.flush().expect("flush epoch tree-GC RocksDB");
                    drop(storage);
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    let storage = SlateDB::open_with_io_counters(path, counters.clone())
                        .expect("open epoch tree-GC SlateDB");
                    let before = counters.snapshot();
                    setup(storage.clone(), backend, path, garbage, roots).await;
                    storage
                        .flush_memtable_for_diagnostics()
                        .await
                        .expect("flush epoch tree-GC SlateDB memtable");
                    println!(
                        "epoch_tree_gc_setup_io,backend={backend},{}",
                        io_fields(counters.snapshot().saturating_sub(before))
                    );
                    drop(storage);
                }
            }
            println!(
                "epoch_tree_gc_setup_close,backend={backend},post_close_bytes={}",
                directory_bytes(Path::new(path))
            );
        }
        "run" => {
            let page_rows = parse_positive(args.get(4), "page rows");
            let settle_ms = args
                .get(5)
                .map_or(0, |value| value.parse::<u64>().expect("settle ms"));
            match backend {
                Backend::RocksDB => {
                    let storage = RocksDB::open(path).expect("open epoch tree-GC RocksDB");
                    measure(storage.clone(), backend, path, page_rows, None).await;
                    storage.flush().expect("flush epoch tree-GC RocksDB");
                    drop(storage);
                }
                Backend::SlateDB => {
                    let counters = SlateDBIoCounters::default();
                    let storage = SlateDB::open_with_io_counters(path, counters.clone())
                        .expect("open epoch tree-GC SlateDB");
                    let before = counters.snapshot();
                    measure(
                        storage.clone(),
                        backend,
                        path,
                        page_rows,
                        Some(counters.clone()),
                    )
                    .await;
                    storage
                        .flush_memtable_for_diagnostics()
                        .await
                        .expect("flush epoch tree-GC SlateDB memtable");
                    if settle_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(settle_ms)).await;
                    }
                    println!(
                        "epoch_tree_gc_flush_io,backend={backend},page_rows={page_rows},{}",
                        io_fields(counters.snapshot().saturating_sub(before))
                    );
                    drop(storage);
                }
            }
            println!(
                "epoch_tree_gc_close,backend={backend},page_rows={page_rows},settle_ms={settle_ms},post_close_bytes={}",
                directory_bytes(Path::new(path))
            );
        }
        "interrupt" => {
            let page_rows = parse_positive(args.get(4), "page rows");
            let steps = parse_positive(args.get(5), "steps") as u64;
            match backend {
                Backend::RocksDB => {
                    let storage = RocksDB::open(path).expect("open interrupted epoch RocksDB");
                    interrupt(storage.clone(), backend, path, page_rows, steps).await;
                    storage.flush().expect("flush interrupted epoch RocksDB");
                    drop(storage);
                }
                Backend::SlateDB => {
                    let storage = SlateDB::open(path).expect("open interrupted epoch SlateDB");
                    interrupt(storage.clone(), backend, path, page_rows, steps).await;
                    storage
                        .flush_memtable_for_diagnostics()
                        .await
                        .expect("flush interrupted epoch SlateDB memtable");
                    drop(storage);
                }
            }
            println!(
                "epoch_tree_gc_interrupt_close,backend={backend},page_rows={page_rows},steps={steps},post_close_bytes={}",
                directory_bytes(Path::new(path))
            );
        }
        _ => print_usage(),
    }
}

async fn setup<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &str,
    garbage: usize,
    roots: usize,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let started = Instant::now();
    Engine::initialize(storage.clone())
        .await
        .expect("initialize epoch tree-GC fixture");
    let engine = Engine::new(storage.clone())
        .await
        .expect("open epoch tree-GC engine");
    let main = engine
        .open_workspace_session()
        .await
        .expect("open epoch tree-GC main session");
    for index in 1..roots {
        main.create_branch(CreateBranchOptions {
            id: Some(format!("01990000-0000-7000-8000-{index:012x}")),
            name: format!("epoch-tree-gc-root-{index}"),
            from_commit_id: None,
        })
        .await
        .expect("create epoch tree-GC active root");
    }
    drop(main);
    drop(engine);
    let adapter = StorageAdapter::new(storage);
    let seed = seed_tree_sweep_garbage_for_bench(&adapter, garbage, 4_096)
        .await
        .expect("seed epoch tree-GC garbage chunks");
    let chunks = count_tree_chunks_for_bench(&adapter)
        .await
        .expect("count seeded epoch tree chunks");
    println!(
        "epoch_tree_gc_setup,backend={backend},roots={roots},garbage={garbage},tree_chunks={chunks},logical_chunk_bytes={},staged_written_bytes={},seed_us={},total_us={},backend_bytes={}",
        seed.logical_chunk_bytes,
        seed.staged_written_bytes,
        seed.elapsed_us,
        started.elapsed().as_micros(),
        directory_bytes(Path::new(path)),
    );
}

async fn measure<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &str,
    page_rows: usize,
    counters: Option<SlateDBIoCounters>,
) where
    StorageImpl: Storage,
{
    let adapter = StorageAdapter::new(storage);
    let before_chunks = count_tree_chunks_for_bench(&adapter)
        .await
        .expect("count pre-epoch tree chunks");
    let before_bytes = directory_bytes(Path::new(path));
    let io_before = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
    let allocations_before = ALLOCATION_CALLS.load(Ordering::Relaxed);
    let allocated_bytes_before = ALLOCATED_BYTES.load(Ordering::Relaxed);
    let live_before = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_LIVE_BYTES.store(live_before, Ordering::Relaxed);
    let rss_before = process_memory_kib("VmRSS");
    let result = run_tree_sweep_epoch_for_bench(&adapter, page_rows)
        .await
        .expect("run epoch tree-GC");
    let rss_after = process_memory_kib("VmRSS");
    let peak_rss = process_memory_kib("VmHWM");
    let after_chunks = count_tree_chunks_for_bench(&adapter)
        .await
        .expect("count post-epoch tree chunks");
    let after_bytes = directory_bytes(Path::new(path));
    let io = counters
        .as_ref()
        .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
        .saturating_sub(io_before);
    println!(
        "epoch_tree_gc_run,backend={backend},page_rows={page_rows},before_chunks={before_chunks},after_chunks={after_chunks},reclaimed_chunks={},reclaimed_percent={:.4},epoch_id={},steps={},phase_steps={:?},roots={},live_chunks={},candidates={},scanned_rows={},deleted_rows={},total_us={},max_step_us={},commit_us={},staged_puts={},staged_deletes={},staged_written_bytes={},max_step_puts={},max_step_deletes={},max_step_written_bytes={},allocation_calls={},allocated_bytes={},peak_live_delta_bytes={},rss_before_kib={rss_before},rss_after_kib={rss_after},peak_rss_kib={peak_rss},backend_bytes_before={before_bytes},backend_bytes_after={after_bytes},{}",
        before_chunks.saturating_sub(after_chunks),
        100.0 * before_chunks.saturating_sub(after_chunks) as f64 / before_chunks.max(1) as f64,
        result.epoch_id,
        result.steps,
        result.phase_steps,
        result.root_count,
        result.live_chunk_count,
        result.candidate_count,
        result.scanned_rows,
        result.deleted_rows,
        result.total_us,
        result.max_step_us,
        result.commit_us,
        result.staged_puts,
        result.staged_deletes,
        result.staged_written_bytes,
        result.max_step_puts,
        result.max_step_deletes,
        result.max_step_written_bytes,
        ALLOCATION_CALLS
            .load(Ordering::Relaxed)
            .saturating_sub(allocations_before),
        ALLOCATED_BYTES
            .load(Ordering::Relaxed)
            .saturating_sub(allocated_bytes_before),
        PEAK_LIVE_BYTES
            .load(Ordering::Relaxed)
            .saturating_sub(live_before),
        io_fields(io),
    );
}

async fn interrupt<StorageImpl>(
    storage: StorageImpl,
    backend: Backend,
    path: &str,
    page_rows: usize,
    steps: u64,
) where
    StorageImpl: Storage,
{
    let adapter = StorageAdapter::new(storage);
    let before_chunks = count_tree_chunks_for_bench(&adapter)
        .await
        .expect("count pre-interrupt tree chunks");
    let result = run_tree_sweep_epoch_steps_for_bench(&adapter, page_rows, Some(steps))
        .await
        .expect("run interrupted epoch tree-GC");
    let after_chunks = count_tree_chunks_for_bench(&adapter)
        .await
        .expect("count interrupted tree chunks");
    assert!(!result.complete, "interrupt point must precede completion");
    println!(
        "epoch_tree_gc_interrupt,backend={backend},page_rows={page_rows},requested_steps={steps},committed_steps={},epoch_id={},before_chunks={before_chunks},after_chunks={after_chunks},phase_steps={:?},roots={},live_chunks={},candidates={},deleted_rows={},total_us={},backend_bytes={}",
        result.steps,
        result.epoch_id,
        result.phase_steps,
        result.root_count,
        result.live_chunk_count,
        result.candidate_count,
        result.deleted_rows,
        result.total_us,
        directory_bytes(Path::new(path)),
    );
}

fn io_fields(io: SlateDBIoSnapshot) -> String {
    format!(
        "read_objects={},read_bytes={},write_objects={},write_bytes={},list_operations={},listed_objects={},main_read_requests={},main_write_requests={},reader_read_requests={},reader_write_requests={},compactor_read_requests={},compactor_write_requests={},gc_read_requests={},gc_write_requests={}",
        io.read_objects,
        io.read_bytes,
        io.write_objects,
        io.write_bytes,
        io.list_operations,
        io.listed_objects,
        io.main.read_requests,
        io.main.write_requests,
        io.reader.read_requests,
        io.reader.write_requests,
        io.compactor.read_requests,
        io.compactor.write_requests,
        io.gc.read_requests,
        io.gc.write_requests,
    )
}

fn process_memory_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix(field)
                    .and_then(|rest| rest.strip_prefix(':'))
                    .and_then(|rest| rest.split_ascii_whitespace().next())
                    .and_then(|value| value.parse().ok())
            })
        })
        .unwrap_or_default()
}

fn directory_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path).map_or(0, |entries| {
        entries
            .filter_map(Result::ok)
            .map(|entry| {
                entry.metadata().map_or(0, |metadata| {
                    if metadata.is_dir() {
                        directory_bytes(&entry.path())
                    } else {
                        metadata.len()
                    }
                })
            })
            .sum()
    })
}

fn parse_positive(value: Option<&String>, label: &str) -> usize {
    value
        .unwrap_or_else(|| panic!("missing {label}"))
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{label} must be positive"))
        .max(1)
}

fn print_usage() {
    eprintln!(
        "usage:\n  epoch_tree_gc_scale setup <rocksdb|slatedb> <path> <garbage-chunks> <active-roots>\n  epoch_tree_gc_scale run <rocksdb|slatedb> <path> <page-rows> [settle-ms]\n  epoch_tree_gc_scale interrupt <rocksdb|slatedb> <path> <page-rows> <steps>"
    );
}
