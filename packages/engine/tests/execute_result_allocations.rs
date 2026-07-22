use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use lix_engine::{ExecuteResult, Value};

struct CountingAllocator;

static TRACK_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATION_COUNT: AtomicUsize = AtomicUsize::new(0);
static ALLOCATED_BYTES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegates the allocation unchanged to the system allocator.
        let pointer = unsafe { System.alloc(layout) };
        record_allocation(pointer, layout.size());
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: Delegates the allocation unchanged to the system allocator.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        record_allocation(pointer, layout.size());
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: The pointer and layout came from this allocator, which
        // delegates every allocation to the system allocator.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: Delegates the reallocation unchanged to the system allocator.
        let pointer = unsafe { System.realloc(pointer, layout, new_size) };
        record_allocation(pointer, new_size);
        pointer
    }
}

#[derive(Clone, Copy, Debug)]
struct AllocationStats {
    count: usize,
    bytes: usize,
}

fn record_allocation(pointer: *mut u8, bytes: usize) {
    if !pointer.is_null() && TRACK_ALLOCATIONS.load(Ordering::Relaxed) {
        ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
        ALLOCATED_BYTES.fetch_add(bytes, Ordering::Relaxed);
    }
}

fn measure_allocations<T>(operation: impl FnOnce() -> T) -> (T, AllocationStats) {
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    assert!(!TRACK_ALLOCATIONS.swap(true, Ordering::SeqCst));
    let result = operation();
    TRACK_ALLOCATIONS.store(false, Ordering::SeqCst);
    let stats = AllocationStats {
        count: ALLOCATION_COUNT.load(Ordering::Relaxed),
        bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
    };
    (result, stats)
}

fn blob_pointer(result: &ExecuteResult) -> *const u8 {
    let [Value::Blob(bytes)] = result.rows()[0].values() else {
        panic!("diagnostic result must contain one blob")
    };
    bytes.as_ptr()
}

#[test]
#[ignore = "manual deterministic ExecuteResult allocation diagnostic"]
fn execute_result_construction_and_clone_allocations() {
    black_box(ExecuteResult::from_rows_affected(0));
    let (_, rows_affected) =
        measure_allocations(|| black_box(ExecuteResult::from_rows_affected(1)));
    eprintln!(
        "execute_result_rows_affected allocations={} allocated_bytes={}",
        rows_affected.count, rows_affected.bytes
    );
    assert_eq!(
        rows_affected.count, 0,
        "rows-affected-only results must remain allocation-free"
    );

    for size_mib in [1_usize, 10] {
        let size_bytes = size_mib * 1024 * 1024;
        let columns = vec!["data".to_string()];
        let rows = vec![vec![Value::Blob(vec![b'x'; size_bytes])]];
        let (result, construction) =
            measure_allocations(|| ExecuteResult::from_rows(columns, rows));
        let probe = result.clone();
        eprintln!(
            "execute_result_construct size_mib={size_mib} layout_bytes={} allocations={} allocated_bytes={} clone_shares_blob={}",
            size_of::<ExecuteResult>(),
            construction.count,
            construction.bytes,
            blob_pointer(&result) == blob_pointer(&probe),
        );

        for fanout in [1_usize, 4, 16] {
            let ((), cloning) = measure_allocations(|| {
                for _ in 0..fanout {
                    let cloned = result.clone();
                    black_box(blob_pointer(&cloned));
                    black_box(cloned);
                }
            });
            eprintln!(
                "execute_result_clone size_mib={size_mib} subscribers={fanout} allocations={} allocated_bytes={}",
                cloning.count, cloning.bytes
            );
            assert_eq!(cloning.count, 0, "cloning must remain allocation-free");
            assert_eq!(cloning.bytes, 0, "cloning must not copy result storage");
        }
    }
}
