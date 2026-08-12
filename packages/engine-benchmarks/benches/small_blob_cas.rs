use std::fmt::{self, Display, Formatter};
use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;
use lix::registered_spaces::{
    BINARY_CAS_CHUNK_PRESENCE_SPACE, BINARY_CAS_CHUNK_SPACE, BINARY_CAS_MANIFEST_CHUNK_SPACE,
    BINARY_CAS_MANIFEST_SPACE,
};
use lix::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, Precondition, ProjectedValue, PutBatch,
    PutEntry, ReadDurability, ReadOptions, SpaceId, Storage, StorageSpace, StorageWrite,
    StoredValue, WriteOptions,
};
use lix::storage_adapter::{StorageAdapter, StorageAdapterRead};
use lix::storage_bench::{
    binary_cas_write_accounting, layout_accounting, read_binary_cas_for_bench,
    reset_binary_cas_write_accounting, write_binary_cas_for_bench,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use tempfile::TempDir;

const BACKENDS: &[Backend] = &[Backend::Rocks, Backend::Slate];
const SIZES: &[usize] = &[4 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10];
const OPERATIONS: &[Operation] = &[
    Operation::UniqueWrite,
    Operation::DedupeWrite,
    Operation::HotRead,
    Operation::DurableSingletonRead,
    Operation::VisibleHotBatchRead,
    Operation::VisiblePreconditionBatch,
    Operation::VisibleIdempotentPreconditionBatch,
];
const DEFAULT_WARMUPS: usize = 20;
const DEFAULT_SAMPLES: usize = 200;
const DIRECT_SINGLETON_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00ff_0002), "bench.direct_singleton");
const DIRECT_BATCH_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00ff_0004), "bench.direct_batch");
const DIRECT_BATCH_KEYS: usize = 1024;
const DIRECT_BATCH_KEY_SUFFIX: &str = "0123456789abcdef0123456789abcdef0123456789abcdef";
const DIRECT_PRECONDITION_SPACE: StorageSpace =
    StorageSpace::mutable(SpaceId(0x00ff_0005), "bench.direct_precondition");
const DIRECT_PRECONDITION_VALUE: &[u8] = b"precondition-value";
const DIRECT_IDEMPOTENCY_RECEIPT_KEY: &[u8] = b"idempotency-receipt";

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
    DurableSingletonRead,
    VisibleHotBatchRead,
    VisiblePreconditionBatch,
    VisibleIdempotentPreconditionBatch,
}

impl Display for Operation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UniqueWrite => formatter.write_str("unique_write"),
            Self::DedupeWrite => formatter.write_str("dedupe_write"),
            Self::HotRead => formatter.write_str("hot_read"),
            Self::DurableSingletonRead => formatter.write_str("durable_singleton_read"),
            Self::VisibleHotBatchRead => formatter.write_str("visible_hot_batch_read"),
            Self::VisiblePreconditionBatch => formatter.write_str("visible_precondition_batch"),
            Self::VisibleIdempotentPreconditionBatch => {
                formatter.write_str("visible_idempotent_precondition_batch")
            }
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
                // RocksDB intentionally rejects the explicit durable tier, so
                // do not compare it to SlateDB with mismatched read semantics.
                if matches!(operation, Operation::DurableSingletonRead)
                    && !matches!(backend, Backend::Slate)
                {
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
    let sample_count = u32::try_from(samples).expect("benchmark sample count should fit in u32");
    let mean = timings.iter().sum::<Duration>() / sample_count;

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
        mean.as_nanos(),
        duration_us(percentile(&timings, 50, 100)),
        duration_us(percentile(&timings, 95, 100)),
        duration_us(mean),
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
    direct_key: Key,
    direct_value_len: usize,
    direct_batch_keys: Vec<Key>,
    direct_precondition_keys: Vec<Key>,
}

impl<S> BackendFixture<S>
where
    S: Storage,
{
    async fn create(
        storage: S,
        temp_dir: TempDir,
        size: usize,
        operation: Operation,
        direct_key: Key,
        direct_value_len: usize,
        direct_batch_keys: Vec<Key>,
        direct_precondition_keys: Vec<Key>,
    ) -> Self {
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
            direct_key,
            direct_value_len,
            direct_batch_keys,
            direct_precondition_keys,
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
            Operation::DurableSingletonRead
            | Operation::VisibleHotBatchRead
            | Operation::VisiblePreconditionBatch
            | Operation::VisibleIdempotentPreconditionBatch => PreparedOperation {
                bytes: None,
                expected_hash: None,
            },
        }
    }

    async fn execute(&self, prepared: PreparedOperation) -> usize {
        if matches!(self.operation, Operation::DurableSingletonRead) {
            return self.read_durable_singleton().await;
        }
        if matches!(self.operation, Operation::VisibleHotBatchRead) {
            return self.read_visible_hot_batch().await;
        }
        if matches!(
            self.operation,
            Operation::VisiblePreconditionBatch | Operation::VisibleIdempotentPreconditionBatch
        ) {
            return self.check_visible_preconditions().await;
        }
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

    async fn read_durable_singleton(&self) -> usize {
        let read = self
            .storage
            .begin_read(ReadOptions {
                durability: ReadDurability::Durable,
                ..ReadOptions::default()
            })
            .await
            .expect("open durable singleton read");
        let value = read
            .get_many(&[GetManyRequest {
                space: DIRECT_SINGLETON_SPACE,
                keys: std::slice::from_ref(&self.direct_key),
                opts: GetOptions::default(),
            }])
            .await
            .expect("read durable singleton value")
            .values
            .into_iter()
            .next()
            .flatten()
            .expect("durable singleton value should exist");
        match value {
            ProjectedValue::FullValue(value) => {
                assert_eq!(value.len(), self.direct_value_len);
                value.len()
            }
            ProjectedValue::KeyOnly => panic!("durable singleton read returned key only"),
        }
    }

    async fn read_visible_hot_batch(&self) -> usize {
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .expect("open visible hot batch read");
        let values = read
            .get_many(&[GetManyRequest {
                space: DIRECT_BATCH_SPACE,
                keys: &self.direct_batch_keys,
                opts: GetOptions {
                    projection: CoreProjection::KeyOnly,
                },
            }])
            .await
            .expect("read visible hot batch values")
            .values;
        assert_eq!(values.len(), self.direct_batch_keys.len());
        assert!(
            values
                .iter()
                .all(|value| matches!(value, Some(ProjectedValue::KeyOnly)))
        );
        values.len()
    }

    async fn check_visible_preconditions(&self) -> usize {
        let idempotent = matches!(
            self.operation,
            Operation::VisibleIdempotentPreconditionBatch
        );
        let mut preconditions = self
            .direct_precondition_keys
            .iter()
            .cloned()
            .map(|key| Precondition::KeyValueEquals {
                space: DIRECT_PRECONDITION_SPACE,
                key,
                expected: Bytes::from_static(DIRECT_PRECONDITION_VALUE),
            })
            .collect::<Vec<_>>();
        if idempotent {
            preconditions.push(Precondition::KeyAbsent {
                space: DIRECT_PRECONDITION_SPACE,
                key: Key(Bytes::from_static(DIRECT_IDEMPOTENCY_RECEIPT_KEY)),
            });
        }
        let checked = preconditions.len();
        let write = self
            .storage
            .prepare_write_set(
                self.storage.new_write_set(),
                WriteOptions {
                    idempotency_key: idempotent
                        .then(|| Bytes::from_static(DIRECT_IDEMPOTENCY_RECEIPT_KEY)),
                    preconditions,
                    ..WriteOptions::default()
                },
            )
            .await
            .expect("check direct visible preconditions");
        drop(write);
        checked
    }

    async fn layout(&self) -> Layout {
        let read = self
            .storage
            .begin_read(ReadOptions::default())
            .await
            .expect("open layout accounting read");
        let spaces = layout_accounting(&read).await;
        Layout {
            manifest_rows: rows(&spaces, BINARY_CAS_MANIFEST_SPACE.name),
            manifest_value_bytes: value_bytes(&spaces, BINARY_CAS_MANIFEST_SPACE.name),
            manifest_chunk_rows: rows(&spaces, BINARY_CAS_MANIFEST_CHUNK_SPACE.name),
            payload_rows: rows(&spaces, BINARY_CAS_CHUNK_SPACE.name),
            payload_value_bytes: value_bytes(&spaces, BINARY_CAS_CHUNK_SPACE.name),
            presence_rows: rows(&spaces, BINARY_CAS_CHUNK_PRESENCE_SPACE.name),
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
        let direct_key = Key(Bytes::from_static(b"direct-singleton"));
        let direct_value = Bytes::from(vec![0xa5; size]);
        let direct_value_len = direct_value.len();
        let direct_batch_keys = matches!(operation, Operation::VisibleHotBatchRead)
            .then(direct_batch_keys)
            .unwrap_or_default();
        let direct_precondition_keys = matches!(
            operation,
            Operation::VisiblePreconditionBatch | Operation::VisibleIdempotentPreconditionBatch
        )
        .then(direct_precondition_keys)
        .unwrap_or_default();
        match backend {
            Backend::Rocks => {
                let storage = RocksDB::open(&database_path).expect("open benchmark RocksDB");
                if matches!(operation, Operation::DurableSingletonRead) {
                    seed_direct_durable_value(&storage, &direct_key, &direct_value).await;
                    storage
                        .flush()
                        .expect("flush direct durable RocksDB seed value");
                }
                if matches!(operation, Operation::VisibleHotBatchRead) {
                    seed_direct_batch_values(&storage, &direct_batch_keys).await;
                }
                if matches!(
                    operation,
                    Operation::VisiblePreconditionBatch
                        | Operation::VisibleIdempotentPreconditionBatch
                ) {
                    seed_direct_precondition_values(&storage, &direct_precondition_keys).await;
                }
                Self::Rocks(
                    BackendFixture::create(
                        storage,
                        temp_dir,
                        size,
                        operation,
                        direct_key,
                        direct_value_len,
                        direct_batch_keys,
                        direct_precondition_keys,
                    )
                    .await,
                )
            }
            Backend::Slate => {
                let storage = SlateDB::open(&database_path).expect("open benchmark SlateDB");
                if matches!(operation, Operation::DurableSingletonRead) {
                    seed_direct_durable_value(&storage, &direct_key, &direct_value).await;
                    storage
                        .flush()
                        .await
                        .expect("flush direct durable SlateDB seed value");
                }
                if matches!(operation, Operation::VisibleHotBatchRead) {
                    seed_direct_batch_values(&storage, &direct_batch_keys).await;
                }
                if matches!(
                    operation,
                    Operation::VisiblePreconditionBatch
                        | Operation::VisibleIdempotentPreconditionBatch
                ) {
                    seed_direct_precondition_values(&storage, &direct_precondition_keys).await;
                }
                Self::Slate(
                    BackendFixture::create(
                        storage,
                        temp_dir,
                        size,
                        operation,
                        direct_key,
                        direct_value_len,
                        direct_batch_keys,
                        direct_precondition_keys,
                    )
                    .await,
                )
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

async fn seed_direct_durable_value<S>(storage: &S, key: &Key, value: &Bytes)
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin direct durable seed write");
    write
        .put_many(
            DIRECT_SINGLETON_SPACE,
            PutBatch {
                entries: vec![PutEntry {
                    key: key.clone(),
                    value: StoredValue {
                        bytes: value.clone(),
                    },
                }],
            },
        )
        .await
        .expect("stage direct durable seed value");
    write
        .commit()
        .await
        .expect("commit direct durable seed value");
}

async fn seed_direct_batch_values<S>(storage: &S, keys: &[Key])
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin direct batch seed write");
    write
        .put_many(
            DIRECT_BATCH_SPACE,
            PutBatch {
                entries: keys
                    .iter()
                    .cloned()
                    .map(|key| PutEntry {
                        key,
                        value: StoredValue {
                            bytes: Bytes::from_static(b"batch-value"),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("stage direct batch seed values");
    write
        .commit()
        .await
        .expect("commit direct batch seed values");
}

fn direct_batch_keys() -> Vec<Key> {
    (0..DIRECT_BATCH_KEYS)
        .map(|index| {
            Key(Bytes::from(format!(
                "hot-batch-{index:04}-{DIRECT_BATCH_KEY_SUFFIX}"
            )))
        })
        .collect()
}

fn direct_precondition_keys() -> Vec<Key> {
    ["branch-head", "tracked-mutation-revision"]
        .into_iter()
        .map(|key| Key(Bytes::from_static(key.as_bytes())))
        .collect()
}

async fn seed_direct_precondition_values<S>(storage: &S, keys: &[Key])
where
    S: Storage,
{
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin direct precondition seed write");
    write
        .put_many(
            DIRECT_PRECONDITION_SPACE,
            PutBatch {
                entries: keys
                    .iter()
                    .cloned()
                    .map(|key| PutEntry {
                        key,
                        value: StoredValue {
                            bytes: Bytes::from_static(DIRECT_PRECONDITION_VALUE),
                        },
                    })
                    .collect(),
            },
        )
        .await
        .expect("stage direct precondition seed values");
    write
        .commit()
        .await
        .expect("commit direct precondition seed values");
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

/// The accounting row for one space, or a panic naming what was actually there.
///
/// `layout_accounting` emits a row for **every** registered space, empty ones
/// included, so a miss here never means "this space holds nothing" — it means
/// the name is not a registered space name. These lookups used to answer that
/// with `map_or(0, ..)`, which made a renamed space report `0 rows / 0 bytes`
/// as a measurement: the bench stayed green and published zeroes. Renames are
/// routine, because a space name carries its record encoding version
/// (`branch.head_control.v10` -> `v11`), so this was a live way to publish a
/// silently wrong CAS byte count. Callers now pass a registry handle's `.name`
/// and a miss is loud.
fn space<'a>(
    spaces: &'a [lix::storage_bench::StorageLayoutAccounting],
    name: &str,
) -> &'a lix::storage_bench::StorageLayoutAccounting {
    spaces
        .iter()
        .find(|space| space.space == name)
        .unwrap_or_else(|| {
            let known = spaces
                .iter()
                .map(|space| space.space)
                .collect::<Vec<_>>()
                .join(", ");
            panic!(
                "storage space '{name}' is not in the layout accounting, so this \
                 bench cannot measure it. Registered spaces: {known}"
            )
        })
}

fn rows(spaces: &[lix::storage_bench::StorageLayoutAccounting], name: &str) -> u64 {
    space(spaces, name).rows
}

fn value_bytes(spaces: &[lix::storage_bench::StorageLayoutAccounting], name: &str) -> u64 {
    space(spaces, name).value_bytes
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
