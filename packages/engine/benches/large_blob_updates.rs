use std::fmt::{self, Display, Formatter};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use lix_engine::storage_bench::{binary_cas_write_accounting, reset_binary_cas_write_accounting};
use lix_engine::{
    CoreProjection, Engine, KeyRange, MAX_SCAN_PAGE_ROWS, ProjectedValue, ReadOptions, ScanOptions,
    SessionContext, SpaceId, Storage, StorageRead, Value,
};
use lix_rocksdb_storage::RocksDB;
use lix_slatedb_storage::SlateDB;
use tempfile::TempDir;

const SIZES: &[usize] = &[1 << 20, 4 << 20, 10 << 20];
const BACKENDS: &[Backend] = &[Backend::Rocks, Backend::Slate];
const OPERATIONS: &[Operation] = &[
    Operation::LocalizedUpdate,
    Operation::FullRewrite,
    Operation::InitialWrite,
];
const LOCAL_EDIT_BYTES: usize = 4 << 10;
const MANIFEST_SPACE: SpaceId = SpaceId(0x0005_0001);
const MANIFEST_CHUNK_SPACE: SpaceId = SpaceId(0x0005_0002);
const PAYLOAD_SPACE: SpaceId = SpaceId(0x0005_0003);
const PRESENCE_SPACE: SpaceId = SpaceId(0x0005_0004);
const UPSERT_SQL: &str = "INSERT INTO lix_file (path, data) VALUES ($1, $2) \
                          ON CONFLICT (path) DO UPDATE SET data = excluded.data";

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
    LocalizedUpdate,
    FullRewrite,
    InitialWrite,
}

impl Display for Operation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalizedUpdate => formatter.write_str("localized_4k_update"),
            Self::FullRewrite => formatter.write_str("full_rewrite"),
            Self::InitialWrite => formatter.write_str("initial_write"),
        }
    }
}

fn large_blob_updates(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create large blob benchmark runtime");

    if std::env::var_os("LIX_LARGE_BLOB_ACCOUNTING").is_some() {
        runtime.block_on(print_accounting());
        return;
    }

    let mut group = c.benchmark_group("large_blob_updates");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(4));

    for &backend in BACKENDS {
        for &size in SIZES {
            group.throughput(Throughput::Bytes(size as u64));
            for &operation in OPERATIONS {
                let parameter = format!("{backend}/{operation}/{}mib", size >> 20);
                group.bench_with_input(
                    BenchmarkId::new("sql_blob_write", parameter),
                    &(backend, size, operation),
                    |b, &(backend, size, operation)| {
                        b.iter_custom(|iterations| {
                            let mut fixture =
                                runtime.block_on(Fixture::new(backend, size, operation));
                            let mut elapsed = Duration::ZERO;
                            for _ in 0..iterations {
                                let prepared = fixture.prepare();
                                let started = Instant::now();
                                let rows_affected = runtime.block_on(fixture.write(prepared));
                                elapsed += started.elapsed();
                                black_box(rows_affected);
                            }
                            elapsed
                        });
                    },
                );
            }
        }
    }

    group.finish();
}

async fn print_accounting() {
    let samples = std::env::var("LIX_LARGE_BLOB_SAMPLES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(12)
        .max(1);

    for &backend in BACKENDS {
        if !selected("LIX_LARGE_BLOB_BACKENDS", &backend.to_string()) {
            continue;
        }
        for &size in SIZES {
            if !selected("LIX_LARGE_BLOB_SIZES_MIB", &(size >> 20).to_string()) {
                continue;
            }
            for &operation in OPERATIONS {
                if !selected("LIX_LARGE_BLOB_OPERATIONS", &operation.to_string()) {
                    continue;
                }
                let mut fixture = Fixture::new(backend, size, operation).await;
                fixture.flush().await;
                let storage_bytes_before = directory_bytes(fixture.root());
                reset_binary_cas_write_accounting();

                let mut timings = Vec::with_capacity(samples);
                for _ in 0..samples {
                    let prepared = fixture.prepare();
                    let started = Instant::now();
                    black_box(fixture.write(prepared).await);
                    timings.push(started.elapsed());
                }
                let metrics = binary_cas_write_accounting();

                fixture.flush().await;
                let storage_bytes_after = directory_bytes(fixture.root());
                let manifest = fixture.space_accounting(MANIFEST_SPACE).await;
                let manifest_chunk = fixture.space_accounting(MANIFEST_CHUNK_SPACE).await;
                let payload = fixture.space_accounting(PAYLOAD_SPACE).await;
                let presence = fixture.space_accounting(PRESENCE_SPACE).await;
                timings.sort_unstable();

                println!(
                    "large_blob_accounting,backend={backend},operation={operation},\
                     size_bytes={size},samples={samples},p50_ms={:.3},p95_ms={:.3},\
                     total_ms={:.3},chunk_lookups={},chunk_lookup_batches={},\
                     chunk_lookup_hits={},chunk_lookup_misses={},chunk_lookup_ms={:.3},\
                     storage_bytes_before={storage_bytes_before},\
                     storage_bytes_after={storage_bytes_after},storage_bytes_delta={},\
                     manifest_rows={},manifest_value_bytes={},manifest_chunk_rows={},\
                     manifest_chunk_value_bytes={},payload_rows={},payload_value_bytes={},\
                     presence_rows={},presence_value_bytes={}",
                    percentile(&timings, 50, 100).as_secs_f64() * 1_000.0,
                    percentile(&timings, 95, 100).as_secs_f64() * 1_000.0,
                    timings.iter().sum::<Duration>().as_secs_f64() * 1_000.0,
                    metrics.chunk_lookup_count,
                    metrics.chunk_lookup_batch_count,
                    metrics.chunk_lookup_hit_count,
                    metrics.chunk_lookup_miss_count,
                    Duration::from_nanos(metrics.chunk_lookup_elapsed_ns).as_secs_f64() * 1_000.0,
                    i128::from(storage_bytes_after) - i128::from(storage_bytes_before),
                    manifest.rows,
                    manifest.value_bytes,
                    manifest_chunk.rows,
                    manifest_chunk.value_bytes,
                    payload.rows,
                    payload.value_bytes,
                    presence.rows,
                    presence.value_bytes,
                );
            }
        }
    }
}

fn selected(variable: &str, candidate: &str) -> bool {
    std::env::var(variable).map_or(true, |selection| {
        selection
            .split(',')
            .map(str::trim)
            .any(|value| value == candidate)
    })
}

fn percentile(sorted: &[Duration], numerator: usize, denominator: usize) -> Duration {
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

struct PreparedWrite {
    path: String,
    data: Vec<u8>,
}

struct WorkloadState {
    size: usize,
    operation: Operation,
    base: Vec<u8>,
    version: u64,
}

impl WorkloadState {
    fn new(size: usize, operation: Operation) -> Self {
        Self {
            size,
            operation,
            base: deterministic_bytes(size, 0),
            version: 1,
        }
    }

    fn seed(&self) -> Option<PreparedWrite> {
        match self.operation {
            Operation::InitialWrite => None,
            Operation::LocalizedUpdate | Operation::FullRewrite => Some(PreparedWrite {
                path: "/large.bin".to_owned(),
                data: self.base.clone(),
            }),
        }
    }

    fn prepare(&mut self) -> PreparedWrite {
        let version = self.version;
        self.version += 1;
        match self.operation {
            Operation::LocalizedUpdate => {
                let mut data = self.base.clone();
                let edit_len = LOCAL_EDIT_BYTES.min(data.len());
                let edit_start = (data.len() - edit_len) / 2;
                fill_deterministic(
                    &mut data[edit_start..edit_start + edit_len],
                    version ^ 0x5a17_5a17_5a17_5a17,
                );
                PreparedWrite {
                    path: "/large.bin".to_owned(),
                    data,
                }
            }
            Operation::FullRewrite => PreparedWrite {
                path: "/large.bin".to_owned(),
                data: deterministic_bytes(self.size, version),
            },
            Operation::InitialWrite => PreparedWrite {
                path: format!("/large-{version}.bin"),
                data: deterministic_bytes(self.size, version),
            },
        }
    }
}

struct BackendFixture<S: Storage> {
    session: SessionContext<S>,
    storage: S,
    _temp_dir: TempDir,
    root: PathBuf,
    workload: WorkloadState,
}

impl<S> BackendFixture<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn create(storage: S, temp_dir: TempDir, size: usize, operation: Operation) -> Self {
        let root = temp_dir.path().to_owned();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("initialize large blob benchmark engine");
        let engine = Engine::new(storage.clone())
            .await
            .expect("open large blob benchmark engine");
        let session = engine
            .open_session(receipt.main_branch_id)
            .await
            .expect("open large blob benchmark session");
        let workload = WorkloadState::new(size, operation);
        let fixture = Self {
            session,
            storage,
            _temp_dir: temp_dir,
            root,
            workload,
        };
        if let Some(seed) = fixture.workload.seed() {
            fixture.write(seed).await;
        }
        fixture
    }

    fn prepare(&mut self) -> PreparedWrite {
        self.workload.prepare()
    }

    async fn write(&self, prepared: PreparedWrite) -> u64 {
        let result = self
            .session
            .execute(
                UPSERT_SQL,
                &[Value::Text(prepared.path), Value::Blob(prepared.data)],
            )
            .await
            .expect("write large benchmark blob");
        assert_eq!(result.rows_affected(), 1);
        result.rows_affected()
    }

    async fn space_accounting(&self, space: SpaceId) -> SpaceAccounting {
        space_accounting(&self.storage, space).await
    }
}

enum Fixture {
    Rocks(BackendFixture<RocksDB>),
    Slate(BackendFixture<SlateDB>),
}

impl Fixture {
    async fn new(backend: Backend, size: usize, operation: Operation) -> Self {
        let temp_dir = tempfile::tempdir().expect("create large blob benchmark directory");
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

    fn prepare(&mut self) -> PreparedWrite {
        match self {
            Self::Rocks(fixture) => fixture.prepare(),
            Self::Slate(fixture) => fixture.prepare(),
        }
    }

    async fn write(&self, prepared: PreparedWrite) -> u64 {
        match self {
            Self::Rocks(fixture) => fixture.write(prepared).await,
            Self::Slate(fixture) => fixture.write(prepared).await,
        }
    }

    fn root(&self) -> &Path {
        match self {
            Self::Rocks(fixture) => &fixture.root,
            Self::Slate(fixture) => &fixture.root,
        }
    }

    async fn flush(&self) {
        match self {
            Self::Rocks(fixture) => fixture.storage.flush().expect("flush benchmark RocksDB"),
            Self::Slate(fixture) => fixture
                .storage
                .flush()
                .await
                .expect("flush benchmark SlateDB"),
        }
    }

    async fn space_accounting(&self, space: SpaceId) -> SpaceAccounting {
        match self {
            Self::Rocks(fixture) => fixture.space_accounting(space).await,
            Self::Slate(fixture) => fixture.space_accounting(space).await,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct SpaceAccounting {
    rows: u64,
    value_bytes: u64,
}

async fn space_accounting<S>(storage: &S, space: SpaceId) -> SpaceAccounting
where
    S: Storage,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open accounting read");
    let mut accounting = SpaceAccounting::default();
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
                    limit_rows: MAX_SCAN_PAGE_ROWS,
                    resume_after,
                },
            )
            .await
            .expect("scan accounting space");
        accounting.rows += page.entries.len() as u64;
        accounting.value_bytes += page
            .entries
            .iter()
            .map(|entry| match &entry.value {
                ProjectedValue::KeyOnly => 0,
                ProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum::<u64>();
        if !page.has_more {
            break;
        }
        resume_after = Some(
            page.entries
                .last()
                .expect("non-final accounting page has a row")
                .key
                .clone(),
        );
    }
    accounting
}

fn deterministic_bytes(len: usize, seed: u64) -> Vec<u8> {
    let mut bytes = vec![0; len];
    fill_deterministic(&mut bytes, seed);
    bytes
}

fn fill_deterministic(bytes: &mut [u8], seed: u64) {
    let mut state = seed ^ 0xd1b5_4a32_d192_ed03;
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let generated = state.to_le_bytes();
        chunk.copy_from_slice(&generated[..chunk.len()]);
    }
}

fn directory_bytes(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    fs::read_dir(path)
        .expect("read benchmark storage directory")
        .map(|entry| directory_bytes(&entry.expect("read benchmark directory entry").path()))
        .sum()
}

criterion_group!(benches, large_blob_updates);
criterion_main!(benches);
