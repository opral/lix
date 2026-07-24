use std::fmt::{self, Display, Formatter};
use std::hint::black_box;
use std::time::{Duration, Instant};

use lix_engine::storage_adapter::StorageAdapter;
use lix_engine::storage_bench::{
    binary_cas_write_accounting, layout_accounting, read_binary_cas_for_bench,
    reset_binary_cas_write_accounting, write_binary_cas_for_bench,
};
use lix_engine::{ReadOptions, Storage};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;
use tempfile::TempDir;

const BACKENDS: &[Backend] = &[Backend::Rocks, Backend::Slate];
const SIZES: &[usize] = &[4 << 10, 32 << 10];
const OPERATIONS: &[Operation] = &[
    Operation::UniqueWrite,
    Operation::DedupeWrite,
    Operation::HotRead,
];
const DEFAULT_WARMUPS: usize = 20;
const DEFAULT_SAMPLES: usize = 200;

#[derive(Clone, Copy, Debug)]
enum Backend {
    Rocks,
    Slate,
}

impl Display for Backend {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rocks => formatter.write_str("rocksdb"),
            Self::Slate => formatter.write_str("slatedb"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    UniqueWrite,
    DedupeWrite,
    HotRead,
}

impl Display for Operation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UniqueWrite => formatter.write_str("unique_write"),
            Self::DedupeWrite => formatter.write_str("dedupe_write"),
            Self::HotRead => formatter.write_str("hot_read"),
        }
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create small blob benchmark runtime");
    runtime.block_on(run());
}

async fn run() {
    let warmups = env_usize("LIX_SMALL_BLOB_WARMUPS", DEFAULT_WARMUPS);
    let samples = env_usize("LIX_SMALL_BLOB_SAMPLES", DEFAULT_SAMPLES).max(1);

    for &backend in BACKENDS {
        if !selected("LIX_SMALL_BLOB_BACKENDS", &backend.to_string()) {
            continue;
        }
        for &size in SIZES {
            if !selected("LIX_SMALL_BLOB_SIZES_KIB", &(size >> 10).to_string()) {
                continue;
            }
            for &operation in OPERATIONS {
                if !selected("LIX_SMALL_BLOB_OPERATIONS", &operation.to_string()) {
                    continue;
                }
                run_case(backend, size, operation, warmups, samples).await;
            }
        }
    }
}

async fn run_case(
    backend: Backend,
    size: usize,
    operation: Operation,
    warmups: usize,
    samples: usize,
) {
    let mut fixture = Fixture::new(backend, size, operation).await;
    for _ in 0..warmups {
        fixture.run_once().await;
    }

    reset_binary_cas_write_accounting();
    let mut timings = Vec::with_capacity(samples);
    for _ in 0..samples {
        let prepared = fixture.prepare();
        let started = Instant::now();
        black_box(fixture.execute(prepared).await);
        timings.push(started.elapsed());
    }
    let accounting = binary_cas_write_accounting();
    let layout = fixture.layout().await;
    timings.sort_unstable();

    println!(
        "small_blob_cas,backend={backend},operation={operation},size_bytes={size},\
         warmups={warmups},samples={samples},p50_ns={},p95_ns={},mean_ns={},\
         p50_us={},p95_us={},mean_us={},\
         chunk_lookups={},chunk_lookup_batches={},chunk_lookup_hits={},\
         chunk_lookup_misses={},chunk_lookup_us={},manifest_rows={},\
         manifest_value_bytes={},manifest_chunk_rows={},payload_rows={},\
         payload_value_bytes={},presence_rows={}",
        percentile(&timings, 50, 100).as_nanos(),
        percentile(&timings, 95, 100).as_nanos(),
        (timings.iter().sum::<Duration>() / samples as u32).as_nanos(),
        duration_us(percentile(&timings, 50, 100)),
        duration_us(percentile(&timings, 95, 100)),
        duration_us(timings.iter().sum::<Duration>() / samples as u32),
        accounting.chunk_lookup_count,
        accounting.chunk_lookup_batch_count,
        accounting.chunk_lookup_hit_count,
        accounting.chunk_lookup_miss_count,
        accounting.chunk_lookup_elapsed_ns / 1_000,
        layout.manifest_rows,
        layout.manifest_value_bytes,
        layout.manifest_chunk_rows,
        layout.payload_rows,
        layout.payload_value_bytes,
        layout.presence_rows,
    );
}

fn duration_us(duration: Duration) -> u128 {
    duration.as_micros()
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn selected(variable: &str, candidate: &str) -> bool {
    std::env::var(variable).map_or(true, |selection| {
        selection
            .split(',')
            .map(str::trim)
            .any(|value| value == candidate)
    })
}

struct PreparedOperation {
    bytes: Option<Vec<u8>>,
    expected_hash: Option<String>,
}

struct BackendFixture<S: Storage> {
    storage: StorageAdapter<S>,
    _temp_dir: TempDir,
    size: usize,
    operation: Operation,
    version: u64,
    stable_bytes: Vec<u8>,
    stable_hash: String,
}

impl<S> BackendFixture<S>
where
    S: Storage,
{
    async fn create(storage: S, temp_dir: TempDir, size: usize, operation: Operation) -> Self {
        let storage = StorageAdapter::new(storage);
        let stable_bytes = deterministic_bytes(size, 0x5a17);
        let stable_hash = write_binary_cas_for_bench(&storage, &stable_bytes)
            .await
            .expect("seed small blob benchmark");
        let stored_bytes = read_binary_cas_for_bench(&storage, &stable_hash)
            .await
            .expect("validate benchmark blob")
            .expect("seeded benchmark blob should exist");
        assert_eq!(stored_bytes, stable_bytes);
        Self {
            storage,
            _temp_dir: temp_dir,
            size,
            operation,
            version: 1,
            stable_bytes,
            stable_hash,
        }
    }

    fn prepare(&mut self) -> PreparedOperation {
        let version = self.version;
        self.version += 1;
        match self.operation {
            Operation::UniqueWrite => PreparedOperation {
                bytes: Some(deterministic_bytes(self.size, version)),
                expected_hash: None,
            },
            Operation::DedupeWrite => PreparedOperation {
                bytes: Some(self.stable_bytes.clone()),
                expected_hash: Some(self.stable_hash.clone()),
            },
            Operation::HotRead => PreparedOperation {
                bytes: None,
                expected_hash: Some(self.stable_hash.clone()),
            },
        }
    }

    async fn execute(&self, prepared: PreparedOperation) -> usize {
        match prepared.bytes {
            Some(bytes) => {
                let hash = write_binary_cas_for_bench(&self.storage, &bytes)
                    .await
                    .expect("write benchmark blob");
                if let Some(expected_hash) = prepared.expected_hash {
                    assert_eq!(hash, expected_hash);
                }
                bytes.len()
            }
            None => {
                let hash = prepared
                    .expected_hash
                    .expect("read benchmark operation should have a hash");
                let bytes = read_binary_cas_for_bench(&self.storage, &hash)
                    .await
                    .expect("read benchmark blob")
                    .expect("benchmark blob should exist");
                bytes.len()
            }
        }
    }

    async fn layout(&self) -> Layout {
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .expect("open layout accounting read");
        let spaces = layout_accounting(&read).await;
        Layout {
            manifest_rows: rows(&spaces, "binary_cas.manifest"),
            manifest_value_bytes: value_bytes(&spaces, "binary_cas.manifest"),
            manifest_chunk_rows: rows(&spaces, "binary_cas.manifest_chunk"),
            payload_rows: rows(&spaces, "binary_cas.chunk"),
            payload_value_bytes: value_bytes(&spaces, "binary_cas.chunk"),
            presence_rows: rows(&spaces, "binary_cas.chunk_presence"),
        }
    }
}

enum Fixture {
    Rocks(BackendFixture<RocksDB>),
    Slate(BackendFixture<SlateDB>),
}

impl Fixture {
    async fn new(backend: Backend, size: usize, operation: Operation) -> Self {
        let temp_dir = tempfile::tempdir().expect("create small blob benchmark directory");
        let database_path = temp_dir.path().join("database");
        match backend {
            Backend::Rocks => {
                let storage = RocksDB::open(&database_path).expect("open benchmark RocksDB");
                Self::Rocks(BackendFixture::create(storage, temp_dir, size, operation).await)
            }
            Backend::Slate => {
                let storage = SlateDB::open(&database_path).expect("open benchmark SlateDB");
                Self::Slate(BackendFixture::create(storage, temp_dir, size, operation).await)
            }
        }
    }

    fn prepare(&mut self) -> PreparedOperation {
        match self {
            Self::Rocks(fixture) => fixture.prepare(),
            Self::Slate(fixture) => fixture.prepare(),
        }
    }

    async fn execute(&self, prepared: PreparedOperation) -> usize {
        match self {
            Self::Rocks(fixture) => fixture.execute(prepared).await,
            Self::Slate(fixture) => fixture.execute(prepared).await,
        }
    }

    async fn run_once(&mut self) {
        let prepared = self.prepare();
        black_box(self.execute(prepared).await);
    }

    async fn layout(&self) -> Layout {
        match self {
            Self::Rocks(fixture) => fixture.layout().await,
            Self::Slate(fixture) => fixture.layout().await,
        }
    }
}

#[derive(Default)]
struct Layout {
    manifest_rows: u64,
    manifest_value_bytes: u64,
    manifest_chunk_rows: u64,
    payload_rows: u64,
    payload_value_bytes: u64,
    presence_rows: u64,
}

fn rows(spaces: &[lix_engine::storage_bench::StorageLayoutAccounting], name: &str) -> u64 {
    spaces
        .iter()
        .find(|space| space.space == name)
        .map_or(0, |space| space.rows)
}

fn value_bytes(spaces: &[lix_engine::storage_bench::StorageLayoutAccounting], name: &str) -> u64 {
    spaces
        .iter()
        .find(|space| space.space == name)
        .map_or(0, |space| space.value_bytes)
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0; len];
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let generated = state.to_le_bytes();
        chunk.copy_from_slice(&generated[..chunk.len()]);
    }
    bytes
}
