use std::fmt::{self, Display, Formatter};
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use lix::Value;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CoreProjection, Key, KeyRange, MAX_SCAN_PAGE_ROWS, ProjectedValue,
    ReadOptions, SpaceId, Storage, StorageRead, StoredValue, WriteOptions,
};
use lix::storage_adapter::{StorageAdapter, StorageSpace};
use lix::storage_bench::{binary_cas_write_accounting, reset_binary_cas_write_accounting};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use tempfile::TempDir;

const SIZES: &[usize] = &[1 << 20, 4 << 20, 10 << 20];
const BACKENDS: &[Backend] = &[Backend::Rocks, Backend::Slate];
const OPERATIONS: &[Operation] = &[
    Operation::LocalizedUpdate,
    Operation::FullRewrite,
    Operation::InitialWrite,
    Operation::ResumableInitialWrite,
    Operation::RawBackendInitialWrite,
];
const LOCAL_EDIT_BYTES: usize = 4 << 10;
const MANIFEST_SPACE: SpaceId = SpaceId(0x0005_0001);
const MANIFEST_CHUNK_SPACE: SpaceId = SpaceId(0x0005_0002);
const PAYLOAD_SPACE: SpaceId = SpaceId(0x0005_0003);
const PRESENCE_SPACE: SpaceId = SpaceId(0x0005_0004);
/// The null-control lane's own space, outside the engine registry.
///
/// `raw_backend_initial_write` deliberately bypasses the binary CAS and stores
/// plain payload bytes. It used to write those bytes to `PAYLOAD_SPACE`, which
/// the engine registers as `binary_cas.chunk` with immutable semantics, so the
/// same id reached the backend as mutable on this lane and immutable
/// everywhere else — one physical location written, another read. Bench-owned
/// ids live in `0x00ff_....`, which the registry never allocates.
const RAW_PAYLOAD_SPACE: SpaceId = SpaceId(0x00ff_0006);
const UPSERT_SQL: &str = "INSERT INTO lix_file (path, content) VALUES ($1, $2) \
                          ON CONFLICT (path) DO UPDATE SET content = excluded.content";

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
    ResumableInitialWrite,
    RawBackendInitialWrite,
}

impl Display for Operation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalizedUpdate => formatter.write_str("localized_4k_update"),
            Self::FullRewrite => formatter.write_str("full_rewrite"),
            Self::InitialWrite => formatter.write_str("initial_write"),
            Self::ResumableInitialWrite => formatter.write_str("resumable_initial_write"),
            Self::RawBackendInitialWrite => formatter.write_str("raw_backend_initial_write"),
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
        for size in accounting_sizes() {
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
                let skip_space_accounting =
                    std::env::var_os("LIX_LARGE_BLOB_SKIP_SPACE_ACCOUNTING").is_some();
                let manifest = if skip_space_accounting {
                    SpaceAccounting::default()
                } else {
                    fixture.space_accounting(MANIFEST_SPACE).await
                };
                let manifest_chunk = if skip_space_accounting {
                    SpaceAccounting::default()
                } else {
                    fixture.space_accounting(MANIFEST_CHUNK_SPACE).await
                };
                let payload = if skip_space_accounting {
                    SpaceAccounting::default()
                } else {
                    fixture.space_accounting(PAYLOAD_SPACE).await
                };
                let presence = if skip_space_accounting {
                    SpaceAccounting::default()
                } else {
                    fixture.space_accounting(PRESENCE_SPACE).await
                };
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

fn accounting_sizes() -> Vec<usize> {
    let Ok(selection) = std::env::var("LIX_LARGE_BLOB_SIZES_MIB") else {
        return SIZES.to_vec();
    };
    let sizes = selection
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<usize>()
                .ok()
                .and_then(|mib| mib.checked_mul(1 << 20))
                .filter(|bytes| *bytes > 0)
                .unwrap_or_else(|| panic!("invalid LIX_LARGE_BLOB_SIZES_MIB value '{value}'"))
        })
        .collect::<Vec<_>>();
    assert!(
        !sizes.is_empty(),
        "LIX_LARGE_BLOB_SIZES_MIB must select at least one positive size"
    );
    sizes
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
    version: u64,
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
            base: if matches!(
                operation,
                Operation::LocalizedUpdate | Operation::FullRewrite
            ) {
                deterministic_bytes(size, 0)
            } else {
                Vec::new()
            },
            version: 1,
        }
    }

    fn seed(&self) -> Option<PreparedWrite> {
        match self.operation {
            Operation::InitialWrite
            | Operation::ResumableInitialWrite
            | Operation::RawBackendInitialWrite => None,
            Operation::LocalizedUpdate | Operation::FullRewrite => Some(PreparedWrite {
                path: "/large.bin".to_owned(),
                data: self.base.clone(),
                version: 0,
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
                    version,
                }
            }
            Operation::FullRewrite => PreparedWrite {
                path: "/large.bin".to_owned(),
                data: deterministic_bytes(self.size, version),
                version,
            },
            Operation::InitialWrite => PreparedWrite {
                path: format!("/large-{version}.bin"),
                data: deterministic_bytes(self.size, version),
                version,
            },
            Operation::ResumableInitialWrite => PreparedWrite {
                path: format!("/large-resumable-{version}.bin"),
                data: Vec::new(),
                version,
            },
            Operation::RawBackendInitialWrite => PreparedWrite {
                path: String::new(),
                data: Vec::new(),
                version,
            },
        }
    }
}

struct BackendFixture<S: Storage + 'static> {
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
        if matches!(self.workload.operation, Operation::RawBackendInitialWrite) {
            let adapter = StorageAdapter::new(self.storage.clone());
            let payload_space = StorageSpace::mutable(RAW_PAYLOAD_SPACE, "bench.raw_payload");
            let mut offset = 0usize;
            while offset < self.workload.size {
                let len = (self.workload.size - offset).min(lix::FILE_UPLOAD_PART_BYTES);
                let data = Bytes::from(deterministic_bytes(len, prepared.version ^ offset as u64));
                let mut writes = adapter.new_write_set();
                for (part_index, chunk) in data.chunks(1024 * 1024).enumerate() {
                    let chunk_start = part_index * 1024 * 1024;
                    writes.put(
                        payload_space,
                        Key(Bytes::copy_from_slice(
                            blake3::hash(&(offset + chunk_start).to_be_bytes()).as_bytes(),
                        )),
                        StoredValue {
                            bytes: data.slice(chunk_start..chunk_start + chunk.len()),
                        },
                    );
                }
                let prepared_write = adapter
                    .prepare_write_set(writes, WriteOptions::default())
                    .await
                    .expect("prepare raw backend write");
                prepared_write
                    .commit()
                    .await
                    .expect("commit raw backend payload");
                offset += len;
            }
            return 1;
        }
        if matches!(self.workload.operation, Operation::ResumableInitialWrite) {
            let total_size = self.workload.size as u64;
            let upload_id = format!("large-blob-bench-{}", prepared.version);
            let mut offset = 0usize;
            while offset < self.workload.size {
                let len = (self.workload.size - offset).min(lix::FILE_UPLOAD_PART_BYTES);
                let data = deterministic_bytes(len, prepared.version ^ offset as u64);
                let progress = self
                    .session
                    .upsert_file_content_part(
                        upload_id.clone(),
                        prepared.path.clone(),
                        offset as u64,
                        total_size,
                        data.into(),
                    )
                    .await
                    .expect("write resumable benchmark blob part");
                offset += len;
                assert_eq!(progress.next_offset, offset as u64);
            }
            return 1;
        }
        let result = self
            .session
            .execute(
                UPSERT_SQL,
                &[
                    Value::Text(prepared.path),
                    Value::Blob(prepared.data.into()),
                ],
            )
            .await
            .expect("write large benchmark blob");
        assert_eq!(result.rows_affected(), 1);
        result.rows_affected()
    }

    async fn space_accounting(&self, space: SpaceId) -> SpaceAccounting {
        space_accounting(&self.storage, space, self.workload.operation).await
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

/// A physical space id has exactly one value semantics, and it is declared in
/// the engine registry, not here. Reading it back through
/// `storage_bench::storage_space_by_id` is what keeps this scan pointed at the
/// physical location the engine actually wrote: scanning `binary_cas.chunk` as
/// mutable reads the wrong RocksDB column family, and scanning a mutable space
/// as immutable hands raw bytes to the immutable-locator decoder and fails
/// with `Corruption("immutable segment locator is invalid")`.
///
/// The null-control lane is the one space this bench owns, so it is the one
/// space this bench declares.
async fn space_accounting<S>(
    storage: &S,
    space: SpaceId,
    operation: Operation,
) -> SpaceAccounting
where
    S: Storage,
{
    let space = if space == PAYLOAD_SPACE && matches!(operation, Operation::RawBackendInitialWrite)
    {
        StorageSpace::mutable(RAW_PAYLOAD_SPACE, "bench.raw_payload")
    } else {
        lix::storage_bench::storage_space_by_id(space.0)
    };
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("open accounting read");
    let mut accounting = SpaceAccounting::default();
    let mut cursor = read
        .begin_scan(
            space,
            KeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            BeginScanOptions {
                projection: CoreProjection::FullValue,
                ..BeginScanOptions::default()
            },
        )
        .await
        .expect("begin accounting scan");
    loop {
        let (page, page_has_more) = cursor
            .next_page(MAX_SCAN_PAGE_ROWS)
            .await
            .expect("scan accounting space").into_parts();
        accounting.rows += page.len() as u64;
        accounting.value_bytes += page
            .iter()
            .map(|entry| match &entry.value {
                ProjectedValue::KeyOnly => 0,
                ProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum::<u64>();
        if !page_has_more {
            break;
        }
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
