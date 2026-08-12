use std::alloc::{GlobalAlloc, Layout, System};
use std::fmt::{self, Display, Formatter};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::storage_bench::{
    binary_cas_write_accounting, reset_binary_cas_write_accounting,
    take_crud_physical_write_accounting,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::{SlateDB, SlateDBIoCounters, SlateDBIoSnapshot};

const KIB: usize = 1_024;
const MIB: usize = 1_024 * KIB;
const DEFAULT_WARMUPS: usize = 1;
const DEFAULT_SAMPLES: usize = 5;

struct CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

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
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        unsafe { System.realloc(pointer, layout, new_size) }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    New1KiB,
    Deduplicated1KiB,
    Deduplicated64MiB,
}

impl Operation {
    const ALL: [Self; 3] = [
        Self::New1KiB,
        Self::Deduplicated1KiB,
        Self::Deduplicated64MiB,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::New1KiB => "new_1kib",
            Self::Deduplicated1KiB => "deduplicated_1kib",
            Self::Deduplicated64MiB => "deduplicated_64mib",
        }
    }
}

impl Display for Operation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AllocationSnapshot {
    calls: u64,
    bytes: u64,
}

impl AllocationSnapshot {
    fn now() -> Self {
        Self {
            calls: ALLOC_CALLS.load(Ordering::Relaxed),
            bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        }
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            calls: self.calls.saturating_sub(earlier.calls),
            bytes: self.bytes.saturating_sub(earlier.bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CpuSnapshot {
    user_us: u64,
    system_us: u64,
}

impl CpuSnapshot {
    #[cfg(unix)]
    fn now() -> Self {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
        assert_eq!(status, 0, "getrusage should succeed");
        let usage = unsafe { usage.assume_init() };
        Self {
            user_us: timeval_micros(usage.ru_utime),
            system_us: timeval_micros(usage.ru_stime),
        }
    }

    #[cfg(not(unix))]
    fn now() -> Self {
        Self::default()
    }

    fn saturating_sub(self, earlier: Self) -> Self {
        Self {
            user_us: self.user_us.saturating_sub(earlier.user_us),
            system_us: self.system_us.saturating_sub(earlier.system_us),
        }
    }
}

#[cfg(unix)]
fn timeval_micros(value: libc::timeval) -> u64 {
    u64::try_from(value.tv_sec)
        .unwrap_or(0)
        .saturating_mul(1_000_000)
        .saturating_add(u64::try_from(value.tv_usec).unwrap_or(0))
}

#[derive(Clone, Copy, Debug, Default)]
struct Sample {
    latency_ns: u64,
    user_cpu_us: u64,
    system_cpu_us: u64,
    alloc_calls: u64,
    alloc_bytes: u64,
    physical_puts: u64,
    physical_deletes: u64,
    physical_written_bytes: u64,
    chunk_lookups: u64,
    chunk_lookup_batches: u64,
    chunk_lookup_hits: u64,
    chunk_lookup_misses: u64,
    slate_read_objects: u64,
    slate_read_bytes: u64,
    slate_write_objects: u64,
    slate_write_bytes: u64,
    slate_writer_gate_acquisitions: u64,
}

fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let backend = args.get(1).map(String::as_str).unwrap_or("rocksdb");
    let path = args
        .get(2)
        .map(String::as_str)
        .unwrap_or("/tmp/lix-cas-publication-epoch");
    let warmups = args
        .get(3)
        .map(|value| value.parse::<usize>().expect("warmups must be a count"))
        .unwrap_or(DEFAULT_WARMUPS);
    let samples = args
        .get(4)
        .map(|value| value.parse::<usize>().expect("samples must be a count"))
        .unwrap_or(DEFAULT_SAMPLES)
        .max(1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("create publication qualification runtime");
    runtime.block_on(async move {
        match backend {
            "rocksdb" => {
                let storage = RocksDB::open(path).expect("open RocksDB");
                run(backend, storage.clone(), None, warmups, samples).await;
                storage.flush().expect("flush RocksDB");
            }
            "slatedb" => {
                let counters = SlateDBIoCounters::default();
                let storage =
                    SlateDB::open_with_io_counters(path, counters.clone()).expect("open SlateDB");
                run(backend, storage.clone(), Some(counters), warmups, samples).await;
                storage.flush().await.expect("flush SlateDB");
            }
            other => panic!("backend must be rocksdb or slatedb, got {other}"),
        }
    });
}

async fn run<S>(
    backend: &str,
    storage: S,
    slate_io: Option<SlateDBIoCounters>,
    warmups: usize,
    sample_count: usize,
) where
    S: Storage + Clone + Send + Sync + 'static,
{
    let initialized = Engine::initialize(storage.clone())
        .await
        .expect("initialize publication repository");
    let engine = Engine::new(storage)
        .await
        .expect("open publication repository");
    let session = engine
        .open_session(initialized.main_branch_id)
        .await
        .expect("open publication session");
    let stable_1k = deterministic_bytes(KIB, 1);
    let stable_64m = deterministic_bytes(64 * MIB, 2);
    publish(&session, "/epoch-seed-1k.bin", stable_1k.clone()).await;
    publish(&session, "/epoch-seed-64m.bin", stable_64m.clone()).await;

    let mut sequence = 0_u64;
    for operation in Operation::ALL {
        for _ in 0..warmups {
            let payload = payload_for(operation, &stable_1k, &stable_64m, sequence);
            let path = format!("/epoch-warmup-{operation}-{sequence}.bin");
            publish(&session, &path, payload).await;
            sequence = sequence.saturating_add(1);
        }

        let mut samples = Vec::with_capacity(sample_count);
        for _ in 0..sample_count {
            let payload = payload_for(operation, &stable_1k, &stable_64m, sequence);
            let path = format!("/epoch-sample-{operation}-{sequence}.bin");
            let params = [Value::Text(path), Value::Blob(payload.into())];
            let _ = take_crud_physical_write_accounting();
            reset_binary_cas_write_accounting();
            let io_before = slate_io
                .as_ref()
                .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot);
            let allocations_before = AllocationSnapshot::now();
            let cpu_before = CpuSnapshot::now();
            let started = Instant::now();
            let result = session
                .execute(
                    "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                    &params,
                )
                .await
                .expect("timed publication should commit");
            let latency_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let cpu = CpuSnapshot::now().saturating_sub(cpu_before);
            let allocations = AllocationSnapshot::now().saturating_sub(allocations_before);
            let physical = take_crud_physical_write_accounting();
            let binary = binary_cas_write_accounting();
            let io = slate_io
                .as_ref()
                .map_or_else(SlateDBIoSnapshot::default, SlateDBIoCounters::snapshot)
                .saturating_sub(io_before);
            black_box(result);
            samples.push(Sample {
                latency_ns,
                user_cpu_us: cpu.user_us,
                system_cpu_us: cpu.system_us,
                alloc_calls: allocations.calls,
                alloc_bytes: allocations.bytes,
                physical_puts: physical.puts,
                physical_deletes: physical.deletes,
                physical_written_bytes: physical.written_bytes,
                chunk_lookups: binary.chunk_lookup_count,
                chunk_lookup_batches: binary.chunk_lookup_batch_count,
                chunk_lookup_hits: binary.chunk_lookup_hit_count,
                chunk_lookup_misses: binary.chunk_lookup_miss_count,
                slate_read_objects: io.read_objects,
                slate_read_bytes: io.read_bytes,
                slate_write_objects: io.write_objects,
                slate_write_bytes: io.write_bytes,
                slate_writer_gate_acquisitions: io.writer_gate_acquisitions,
            });
            sequence = sequence.saturating_add(1);
        }
        print_medians(backend, operation, warmups, &samples);
    }
}

async fn publish<S>(session: &SessionContext<S>, path: &str, payload: Vec<u8>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[Value::Text(path.to_owned()), Value::Blob(payload.into())],
        )
        .await
        .expect("setup publication should commit");
}

fn payload_for(
    operation: Operation,
    stable_1k: &[u8],
    stable_64m: &[u8],
    sequence: u64,
) -> Vec<u8> {
    match operation {
        Operation::New1KiB => deterministic_bytes(KIB, sequence.saturating_add(10_000)),
        Operation::Deduplicated1KiB => stable_1k.to_vec(),
        Operation::Deduplicated64MiB => stable_64m.to_vec(),
    }
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut output = vec![0_u8; len];
    for (index, bytes) in output.chunks_mut(32).enumerate() {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"lix CAS publication epoch qualification v1");
        hasher.update(&seed.to_le_bytes());
        hasher.update(&(index as u64).to_le_bytes());
        let digest = hasher.finalize();
        bytes.copy_from_slice(&digest.as_bytes()[..bytes.len()]);
    }
    output
}

fn print_medians(backend: &str, operation: Operation, warmups: usize, samples: &[Sample]) {
    println!(
        "cas_publication_epoch,backend={backend},operation={operation},warmups={warmups},samples={},latency_ns={},user_cpu_us={},system_cpu_us={},alloc_calls={},alloc_bytes={},physical_puts={},physical_deletes={},physical_written_bytes={},chunk_lookups={},chunk_lookup_batches={},chunk_lookup_hits={},chunk_lookup_misses={},slate_read_objects={},slate_read_bytes={},slate_write_objects={},slate_write_bytes={},slate_writer_gate_acquisitions={}",
        samples.len(),
        median(samples, |sample| sample.latency_ns),
        median(samples, |sample| sample.user_cpu_us),
        median(samples, |sample| sample.system_cpu_us),
        median(samples, |sample| sample.alloc_calls),
        median(samples, |sample| sample.alloc_bytes),
        median(samples, |sample| sample.physical_puts),
        median(samples, |sample| sample.physical_deletes),
        median(samples, |sample| sample.physical_written_bytes),
        median(samples, |sample| sample.chunk_lookups),
        median(samples, |sample| sample.chunk_lookup_batches),
        median(samples, |sample| sample.chunk_lookup_hits),
        median(samples, |sample| sample.chunk_lookup_misses),
        median(samples, |sample| sample.slate_read_objects),
        median(samples, |sample| sample.slate_read_bytes),
        median(samples, |sample| sample.slate_write_objects),
        median(samples, |sample| sample.slate_write_bytes),
        median(samples, |sample| sample.slate_writer_gate_acquisitions),
    );
}

fn median(samples: &[Sample], field: impl Fn(&Sample) -> u64) -> u64 {
    let mut values = samples.iter().map(field).collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2]
}
