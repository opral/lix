use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use lix_engine::Storage;
use lix_engine::storage::{
    CommitResult, CoreProjection, GetManyResult, GetOptions, Key, KeyRange, Memory, Prefix,
    ProjectedValue, PutBatch, PutEntry, ReadOptions, ScanChunk, ScanOptions, SpaceId, StorageError,
    StorageRead, StorageWrite, StoredValue, WriteOptions, WriteStats,
};
use lix_engine::storage_adapter::{
    PointReadPlan, ScanPlan, StorageAdapter, StorageAdapterReadScope, StorageReadStats,
    StorageSpace, StorageWriteSet, StorageWriteSetStats,
};
use lix_rocksdb_storage::RocksDB;
use lix_sqlite_storage::SQLite;
use rustc_hash::FxBuildHasher;
use tempfile::TempDir;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

fn storage_benchmark_group<'a>(
    c: &'a mut Criterion,
    name: &'static str,
) -> criterion::BenchmarkGroup<'a, criterion::measurement::WallTime> {
    let mut group = c.benchmark_group(name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }
    group
}

#[derive(Clone, Copy)]
struct WriteCase {
    name: &'static str,
    writes: u32,
    spaces: u32,
    value_size: usize,
    mix: WriteMix,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum WriteMix {
    PutsOnly,
    DeletesOnly,
    PutDelete80_20,
}

#[derive(Clone, Copy)]
enum WriteOrder {
    Sorted,
    Reverse,
    Shuffled,
}

impl WriteOrder {
    fn name(self) -> &'static str {
        match self {
            Self::Sorted => "sorted",
            Self::Reverse => "reverse_sorted",
            Self::Shuffled => "shuffled",
        }
    }
}

#[derive(Clone)]
enum WriteMutation {
    Put(StorageSpace, Key, StoredValue),
    Delete(StorageSpace, Key),
}

#[derive(Clone)]
struct DirectWriteBatches {
    puts: Vec<(StorageSpace, PutBatch)>,
    deletes: Vec<(StorageSpace, Vec<Key>)>,
}

fn block_on<F: Future>(future: F) -> F::Output {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("storage benchmark runtime should build")
        })
        .block_on(future)
}

#[derive(Clone, Copy)]
struct PointCase {
    name: &'static str,
    requested_keys: usize,
    unique_keys: usize,
}

#[derive(Clone, Copy)]
struct DeleteRangeCase {
    name: &'static str,
    rows: usize,
    chunk_size: usize,
}

#[derive(Clone, Copy)]
struct ScanChunkingCase {
    name: &'static str,
    rows: usize,
    chunk_size: usize,
    scan: ScanChunkingMode,
}

#[derive(Clone, Copy)]
enum ScanChunkingMode {
    Range,
    Prefix,
}

#[derive(Clone, Debug)]
struct DeleteRangeFallbackStats {
    scanned: usize,
    deleted: usize,
    chunks: usize,
    write_stats: StorageWriteSetStats,
}

#[derive(Clone, Debug, Default)]
struct ScanDrainStats {
    scanned: usize,
    chunks: usize,
    storage_calls: u64,
    read_stats: StorageReadStats,
}

trait StorageBenchStorage {
    type Storage: Storage;

    fn name(&self) -> &'static str;

    fn open_empty(&self) -> Self::Storage;

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Storage;

    fn fork_for_write(&self, storage: &Self::Storage) -> Self::Storage;
}

#[derive(Clone, Copy)]
struct InMemoryBenchStorage;

impl StorageBenchStorage for InMemoryBenchStorage {
    type Storage = Memory;

    fn name(&self) -> &'static str {
        "in_memory"
    }

    fn open_empty(&self) -> Self::Storage {
        Memory::new()
    }

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Storage {
        seeded_memory_with_value_size(space.0, rows, value_size)
    }

    fn fork_for_write(&self, storage: &Self::Storage) -> Self::Storage {
        storage
            .fork_snapshot()
            .expect("fork in-memory bench storage")
    }
}

struct SQLiteTempBenchStorage {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

impl SQLiteTempBenchStorage {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create sqlite bench matrix temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }

    fn next_path(&self) -> PathBuf {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        self.temp_dir
            .path()
            .join(format!("storage-v2-matrix-{database_id}.sqlite"))
    }
}

impl StorageBenchStorage for SQLiteTempBenchStorage {
    type Storage = SQLite;

    fn name(&self) -> &'static str {
        "sqlite_temp"
    }

    fn open_empty(&self) -> Self::Storage {
        SQLite::open(self.next_path()).expect("open empty sqlite bench storage")
    }

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Storage {
        let storage = self.open_empty();
        seed_storage_points(&storage, space, rows, value_size, "sqlite bench storage");
        storage
            .checkpoint()
            .expect("checkpoint seeded sqlite bench storage");
        storage
    }

    fn fork_for_write(&self, storage: &Self::Storage) -> Self::Storage {
        storage
            .checkpoint()
            .expect("checkpoint sqlite bench seed before fork");
        let fork_path = self.next_path();
        fs::copy(storage.path(), &fork_path).expect("copy sqlite bench seed database");
        SQLite::open(fork_path).expect("open sqlite bench fork")
    }
}

struct RocksDBTempBenchStorage {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

impl RocksDBTempBenchStorage {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create rocksdb bench matrix temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }

    fn next_path(&self) -> PathBuf {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        self.temp_dir
            .path()
            .join(format!("storage-v2-matrix-{database_id}.rocksdb"))
    }
}

impl StorageBenchStorage for RocksDBTempBenchStorage {
    type Storage = RocksDB;

    fn name(&self) -> &'static str {
        "rocksdb_temp"
    }

    fn open_empty(&self) -> Self::Storage {
        RocksDB::open(self.next_path()).expect("open empty rocksdb bench storage")
    }

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Storage {
        let storage = self.open_empty();
        seed_storage_points(&storage, space, rows, value_size, "rocksdb bench storage");
        storage.flush().expect("flush seeded rocksdb bench storage");
        storage
    }

    fn fork_for_write(&self, storage: &Self::Storage) -> Self::Storage {
        storage
            .flush()
            .expect("flush rocksdb bench seed before fork");
        let fork_path = self.next_path();
        copy_dir_recursive(storage.path(), &fork_path).expect("copy rocksdb bench seed database");
        RocksDB::open(fork_path).expect("open rocksdb bench fork")
    }
}

const WRITE_CASES: &[WriteCase] = &[
    WriteCase {
        name: "puts_k128_g1_v32",
        writes: 128,
        spaces: 1,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    },
    WriteCase {
        name: "puts_k1024_g1_v32",
        writes: 1_024,
        spaces: 1,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    },
    WriteCase {
        name: "puts_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    },
    WriteCase {
        name: "puts_k8192_g16_v32",
        writes: 8_192,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    },
    WriteCase {
        name: "puts_k1024_g64_v32",
        writes: 1_024,
        spaces: 64,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    },
    WriteCase {
        name: "puts_k4096_g256_v32",
        writes: 4_096,
        spaces: 256,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    },
    WriteCase {
        name: "deletes_k1024_g16",
        writes: 1_024,
        spaces: 16,
        value_size: 0,
        mix: WriteMix::DeletesOnly,
    },
    WriteCase {
        name: "mixed80_20_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutDelete80_20,
    },
    WriteCase {
        name: "puts_k1024_g16_v1024",
        writes: 1_024,
        spaces: 16,
        value_size: 1_024,
        mix: WriteMix::PutsOnly,
    },
    WriteCase {
        name: "puts_k1024_g16_v65536",
        writes: 1_024,
        spaces: 16,
        value_size: 65_536,
        mix: WriteMix::PutsOnly,
    },
];

const POINT_CASES: &[PointCase] = &[
    PointCase {
        name: "m100_u100",
        requested_keys: 100,
        unique_keys: 100,
    },
    PointCase {
        name: "m1000_u1000",
        requested_keys: 1_000,
        unique_keys: 1_000,
    },
    PointCase {
        name: "m1000_u100",
        requested_keys: 1_000,
        unique_keys: 100,
    },
    PointCase {
        name: "m10000_u100",
        requested_keys: 10_000,
        unique_keys: 100,
    },
    PointCase {
        name: "m10000_u10000",
        requested_keys: 10_000,
        unique_keys: 10_000,
    },
];

const DELETE_RANGE_CASES: &[DeleteRangeCase] = &[
    DeleteRangeCase {
        name: "delete_prefix_q100",
        rows: 100,
        chunk_size: 256,
    },
    DeleteRangeCase {
        name: "delete_prefix_q1000",
        rows: 1_000,
        chunk_size: 512,
    },
    DeleteRangeCase {
        name: "delete_prefix_q10000",
        rows: 10_000,
        chunk_size: 1_024,
    },
];

const SCAN_CHUNKING_CASES: &[ScanChunkingCase] = &[
    ScanChunkingCase {
        name: "drain_range_q10000_single",
        rows: 10_000,
        chunk_size: 10_001,
        scan: ScanChunkingMode::Range,
    },
    ScanChunkingCase {
        name: "drain_range_q10000_chunk1",
        rows: 10_000,
        chunk_size: 1,
        scan: ScanChunkingMode::Range,
    },
    ScanChunkingCase {
        name: "drain_range_q10000_chunk10",
        rows: 10_000,
        chunk_size: 10,
        scan: ScanChunkingMode::Range,
    },
    ScanChunkingCase {
        name: "drain_range_q10000_chunk100",
        rows: 10_000,
        chunk_size: 100,
        scan: ScanChunkingMode::Range,
    },
    ScanChunkingCase {
        name: "drain_prefix_q10000_chunk10",
        rows: 10_000,
        chunk_size: 10,
        scan: ScanChunkingMode::Prefix,
    },
    ScanChunkingCase {
        name: "drain_prefix_q10000_single",
        rows: 10_000,
        chunk_size: 10_001,
        scan: ScanChunkingMode::Prefix,
    },
];

fn storage_v2_benches(c: &mut Criterion) {
    if std::env::var_os("STORAGE_V2_BENCH_DIRECT_PROFILE_ONLY").is_some() {
        match std::env::var("STORAGE_V2_BENCH_DIRECT_PROFILE_STORAGE").as_deref() {
            Ok("in_memory") => bench_storage_direct_profile(c, InMemoryBenchStorage),
            Ok("sqlite_temp") => bench_storage_direct_profile(c, SQLiteTempBenchStorage::new()),
            Ok("rocksdb_temp") => bench_storage_direct_profile(c, RocksDBTempBenchStorage::new()),
            Ok(other) => panic!("unknown direct profile storage: {other}"),
            Err(_) => {
                bench_storage_direct_profile(c, InMemoryBenchStorage);
                bench_storage_direct_profile(c, SQLiteTempBenchStorage::new());
                bench_storage_direct_profile(c, RocksDBTempBenchStorage::new());
            }
        }
        return;
    }

    bench_write_set_lowering(c);
    bench_write_set_construction(c);
    bench_write_set_build_and_commit(c, InMemoryBenchStorage);
    bench_write_set_build_and_commit(c, SQLiteTempBenchStorage::new());
    bench_write_set_build_and_commit(c, RocksDBTempBenchStorage::new());
    bench_direct_write_order(c, InMemoryBenchStorage);
    bench_direct_write_order(c, SQLiteTempBenchStorage::new());
    bench_direct_write_order(c, RocksDBTempBenchStorage::new());
    bench_write_batch_seal_sort(c);
    bench_delete_range_fallback(c, InMemoryBenchStorage);
    bench_delete_range_fallback(c, SQLiteTempBenchStorage::new());
    bench_delete_range_fallback(c, RocksDBTempBenchStorage::new());
    bench_delete_range_native(c, InMemoryBenchStorage);
    bench_delete_range_native(c, SQLiteTempBenchStorage::new());
    bench_delete_range_native(c, RocksDBTempBenchStorage::new());
    bench_delete_range_storage_helpers(c, InMemoryBenchStorage);
    bench_delete_range_storage_helpers(c, SQLiteTempBenchStorage::new());
    bench_delete_range_storage_helpers(c, RocksDBTempBenchStorage::new());
    bench_scan_chunking_matrix(c, InMemoryBenchStorage);
    bench_scan_chunking_matrix(c, SQLiteTempBenchStorage::new());
    bench_scan_chunking_matrix(c, RocksDBTempBenchStorage::new());
    bench_durable_commit(c, InMemoryBenchStorage);
    bench_durable_commit(c, SQLiteTempBenchStorage::new());
    bench_durable_commit(c, RocksDBTempBenchStorage::new());
    bench_point_request_plan(c);
    bench_storage_direct_profile(c, InMemoryBenchStorage);
    bench_storage_direct_profile(c, SQLiteTempBenchStorage::new());
    bench_storage_direct_profile(c, RocksDBTempBenchStorage::new());
    bench_hash_algorithms(c);
}

fn bench_point_request_plan(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/point_request_plan");

    for case in POINT_CASES {
        if case.requested_keys != case.unique_keys {
            continue;
        }
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::new("dedupe", case.name), case, |b, _case| {
            b.iter(|| {
                black_box(PointReadPlan::new(space(1), black_box(&keys)));
            });
        });
        group.bench_with_input(
            BenchmarkId::new("known_unique", case.name),
            case,
            |b, _case| {
                b.iter_batched(
                    || keys.clone(),
                    |keys| {
                        black_box(PointReadPlan::from_unique_keys(space(1), black_box(keys)));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_hash_algorithms(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/hash_algorithms");

    let point_keys = point_request_keys(10_000, 100);
    let unique_keys = point_request_keys(1_000, 1_000);
    let write_mutations = write_mutations(&WriteCase {
        name: "puts_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    });

    bench_hash_algorithm(
        &mut group,
        "std_siphash",
        RandomState::new(),
        &point_keys,
        &unique_keys,
        &write_mutations,
    );
    bench_hash_algorithm(
        &mut group,
        "ahash",
        ahash::RandomState::new(),
        &point_keys,
        &unique_keys,
        &write_mutations,
    );
    bench_hash_algorithm(
        &mut group,
        "rustc_fx",
        FxBuildHasher,
        &point_keys,
        &unique_keys,
        &write_mutations,
    );
    bench_hash_algorithm(
        &mut group,
        "xxh3",
        Xxh3DefaultBuilder::new(),
        &point_keys,
        &unique_keys,
        &write_mutations,
    );
    bench_hash_algorithm(
        &mut group,
        "blake3",
        Blake3BuildHasher,
        &point_keys,
        &unique_keys,
        &write_mutations,
    );

    group.finish();
}

fn bench_hash_algorithm<S>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &'static str,
    build_hasher: S,
    point_keys: &[Key],
    unique_keys: &[Key],
    write_mutations: &[WriteMutation],
) where
    S: BuildHasher + Clone + 'static,
{
    group.throughput(Throughput::Elements(point_keys.len() as u64));
    group.bench_function(BenchmarkId::new("point_reconstruction", name), |b| {
        b.iter(|| {
            let mut seen =
                HashSet::with_capacity_and_hasher(point_keys.len(), build_hasher.clone());
            let mut storage_keys = Vec::with_capacity(point_keys.len());
            for key in black_box(point_keys) {
                if seen.insert(key) {
                    storage_keys.push(key.clone());
                }
            }

            let mut found =
                HashMap::with_capacity_and_hasher(storage_keys.len(), build_hasher.clone());
            for key in &storage_keys {
                found.insert(key.clone(), ProjectedValue::FullValue(key.0.clone()));
            }

            let mut values = Vec::with_capacity(point_keys.len());
            for key in point_keys {
                values.push(found.get(key).cloned());
            }

            assert_eq!(storage_keys.len(), 100);
            assert_eq!(values.len(), point_keys.len());
            black_box(values);
        });
    });

    group.throughput(Throughput::Elements(unique_keys.len() as u64));
    group.bench_function(BenchmarkId::new("unique_point_reconstruction", name), |b| {
        b.iter(|| {
            let mut seen =
                HashSet::with_capacity_and_hasher(unique_keys.len(), build_hasher.clone());
            let mut storage_keys = Vec::with_capacity(unique_keys.len());
            for key in black_box(unique_keys) {
                if seen.insert(key) {
                    storage_keys.push(key.clone());
                }
            }

            let mut found =
                HashMap::with_capacity_and_hasher(storage_keys.len(), build_hasher.clone());
            for key in &storage_keys {
                found.insert(key.clone(), ProjectedValue::FullValue(key.0.clone()));
            }

            let mut values = Vec::with_capacity(unique_keys.len());
            for key in unique_keys {
                values.push(found.get(key).cloned());
            }

            assert_eq!(storage_keys.len(), unique_keys.len());
            assert_eq!(values.len(), unique_keys.len());
            black_box(values);
        });
    });

    group.throughput(Throughput::Elements(write_mutations.len() as u64));
    group.bench_function(BenchmarkId::new("write_validation", name), |b| {
        b.iter(|| {
            let mut seen =
                HashSet::with_capacity_and_hasher(write_mutations.len(), build_hasher.clone());
            for mutation in black_box(write_mutations) {
                match mutation {
                    WriteMutation::Put(space, key, _) | WriteMutation::Delete(space, key) => {
                        assert!(seen.insert((space.id, key.clone())));
                    }
                }
            }
            assert_eq!(seen.len(), write_mutations.len());
            black_box(seen);
        });
    });

    group.throughput(Throughput::Elements(point_keys.len() as u64));
    group.bench_function(BenchmarkId::new("raw_hash", name), |b| {
        b.iter(|| {
            let mut hash = 0;
            for key in black_box(point_keys) {
                hash ^= build_hasher.hash_one(key);
            }
            black_box(hash);
        });
    });
}

#[derive(Clone, Copy, Default)]
struct Blake3BuildHasher;

impl BuildHasher for Blake3BuildHasher {
    type Hasher = Blake3StdHasher;

    fn build_hasher(&self) -> Self::Hasher {
        Blake3StdHasher::default()
    }
}

#[derive(Clone, Default)]
struct Blake3StdHasher {
    inner: blake3::Hasher,
}

impl Hasher for Blake3StdHasher {
    fn finish(&self) -> u64 {
        let digest = self.inner.finalize();
        let mut bytes = [0; 8];
        bytes.copy_from_slice(&digest.as_bytes()[..8]);
        u64::from_le_bytes(bytes)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.inner.update(bytes);
    }
}

fn bench_write_set_lowering(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/write_set_lowering");

    for case in WRITE_CASES {
        assert_eq!(
            case.writes % case.spaces,
            0,
            "write cases must divide cleanly across spaces"
        );
        let mutations = write_mutations(case);
        group.throughput(Throughput::Elements(u64::from(case.writes)));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let storage_impl = CountingStorage::default();
                    let storage = StorageAdapter::new(storage_impl.clone());
                    let writes = canonical_write_set_from_mutations(&mutations);
                    (storage, storage_impl, writes)
                },
                |(storage, storage_impl, writes)| {
                    let (_commit, stats) =
                        block_on(storage.commit_write_set(writes, WriteOptions::default()))
                            .expect("commit write set");
                    let expected_deletes = case.expected_deletes();
                    let expected_puts = case.writes - expected_deletes;
                    assert_eq!(stats.staged_puts, u64::from(expected_puts));
                    assert_eq!(stats.staged_deletes, u64::from(expected_deletes));
                    assert_eq!(stats.touched_spaces, u64::from(case.spaces));
                    assert_eq!(stats.put_batches, u64::from(case.expected_put_batches()));
                    assert_eq!(
                        stats.delete_batches,
                        u64::from(case.expected_delete_batches())
                    );
                    let expected_revision_put_batches =
                        u64::from((expected_puts + expected_deletes) > 0);
                    assert_eq!(
                        storage_impl.state.put_many_calls.load(Ordering::Relaxed),
                        u64::from(case.expected_put_batches()) + expected_revision_put_batches
                    );
                    assert_eq!(
                        storage_impl.state.delete_many_calls.load(Ordering::Relaxed),
                        u64::from(case.expected_delete_batches())
                    );
                    assert_eq!(storage_impl.state.commit_calls.load(Ordering::Relaxed), 1);
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_write_set_construction(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/write_set_construction");

    for case in WRITE_CASES {
        assert_eq!(
            case.writes % case.spaces,
            0,
            "write cases must divide cleanly across spaces"
        );
        let mutations = write_mutations(case);
        group.throughput(Throughput::Elements(u64::from(case.writes)));

        group.bench_with_input(BenchmarkId::new("checked", case.name), case, |b, case| {
            b.iter(|| {
                let writes = checked_write_set_from_mutations(black_box(&mutations));
                let stats = writes.stats();
                assert_eq!(
                    stats.staged_puts,
                    u64::from(case.writes - case.expected_deletes())
                );
                assert_eq!(stats.staged_deletes, u64::from(case.expected_deletes()));
                assert_eq!(stats.touched_spaces, u64::from(case.spaces));
                black_box(writes);
            });
        });

        group.bench_with_input(BenchmarkId::new("canonical", case.name), case, |b, case| {
            b.iter(|| {
                let writes = canonical_write_set_from_mutations(black_box(&mutations));
                let stats = writes.stats();
                assert_eq!(
                    stats.staged_puts,
                    u64::from(case.writes - case.expected_deletes())
                );
                assert_eq!(stats.staged_deletes, u64::from(case.expected_deletes()));
                assert_eq!(stats.touched_spaces, u64::from(case.spaces));
                black_box(writes);
            });
        });
    }

    group.finish();
}

fn bench_write_set_build_and_commit<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let group_name = format!(
        "storage_v2/write_set_build_and_commit/{}",
        storage_family.name()
    );
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    for case in WRITE_CASES
        .iter()
        .filter(|case| case.writes == 1_024 || case.writes == 128)
    {
        let mutations = write_mutations(case);
        group.throughput(Throughput::Elements(u64::from(case.writes)));

        group.bench_with_input(BenchmarkId::new("checked", case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let storage = storage_family.open_empty();
                    StorageAdapter::new(storage)
                },
                |storage| {
                    let writes = checked_write_set_from_mutations(black_box(&mutations));
                    let (_commit, stats) =
                        block_on(storage.commit_write_set(writes, WriteOptions::default()))
                            .expect("checked build and commit");
                    assert_eq!(
                        stats.staged_puts,
                        u64::from(case.writes - case.expected_deletes())
                    );
                    assert_eq!(stats.staged_deletes, u64::from(case.expected_deletes()));
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("canonical", case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let storage = storage_family.open_empty();
                    StorageAdapter::new(storage)
                },
                |storage| {
                    let writes = canonical_write_set_from_mutations(black_box(&mutations));
                    let (_commit, stats) =
                        block_on(storage.commit_write_set(writes, WriteOptions::default()))
                            .expect("canonical build and commit");
                    assert_eq!(
                        stats.staged_puts,
                        u64::from(case.writes - case.expected_deletes())
                    );
                    assert_eq!(stats.staged_deletes, u64::from(case.expected_deletes()));
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_direct_write_order<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let group_name = format!("storage_v2/direct_write_order/{}", storage_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    #[expect(clippy::items_after_statements)]
    const WRITES: usize = 10_000;
    #[expect(clippy::items_after_statements)]
    const VALUE_SIZE: usize = 32;
    let storage_space = space(1);
    for order in [
        WriteOrder::Sorted,
        WriteOrder::Reverse,
        WriteOrder::Shuffled,
    ] {
        let batch = direct_ordered_put_batch(storage_space, WRITES, VALUE_SIZE, order);
        group.throughput(Throughput::Elements(WRITES as u64));
        group.bench_function(order.name(), |b| {
            b.iter_batched(
                || (storage_family.open_empty(), batch.clone()),
                |(storage, batch)| {
                    let mut write = block_on(storage.begin_write(WriteOptions::default()))
                        .expect("begin direct write-order write");
                    block_on(write.put_many(SpaceId(1), PutBatch { entries: batch }))
                        .expect("put direct write-order batch");
                    let commit = block_on(write.commit()).expect("commit direct write-order batch");
                    assert_eq!(commit.stats.put_entries, WRITES as u64);
                    assert_eq!(commit.stats.deleted_entries, 0);
                    assert_eq!(commit.stats.storage_calls, 1);
                    black_box(commit);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_write_batch_seal_sort(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/write_batch_seal_sort");

    #[expect(clippy::items_after_statements)]
    const WRITES: usize = 10_000;
    #[expect(clippy::items_after_statements)]
    const VALUE_SIZE: usize = 32;
    let storage_space = space(1);
    for order in [
        WriteOrder::Sorted,
        WriteOrder::Reverse,
        WriteOrder::Shuffled,
    ] {
        let batch = direct_ordered_put_batch(storage_space, WRITES, VALUE_SIZE, order);
        group.throughput(Throughput::Elements(WRITES as u64));
        group.bench_function(order.name(), |b| {
            b.iter_batched(
                || batch.clone(),
                |batch| {
                    let sealed = seal_sorted_unique_put_batch(black_box(batch));
                    assert_eq!(sealed.len(), WRITES);
                    black_box(sealed);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

#[expect(clippy::cast_possible_truncation)]
fn bench_delete_range_fallback<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let group_name = format!("storage_v2/delete_range_fallback/{}", storage_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    for case in DELETE_RANGE_CASES {
        let seed = storage_family.seed_points(SpaceId(1), case.rows as u32, 32);
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let storage = storage_family.fork_for_write(&seed);
                    StorageAdapter::new(storage)
                },
                |storage| {
                    let stats = block_on(fallback_delete_range(
                        &storage,
                        space(1),
                        point_scan_range(),
                        case.chunk_size,
                    ))
                    .expect("delete range fallback");
                    assert_eq!(stats.scanned, case.rows);
                    assert_eq!(stats.deleted, case.rows);
                    assert_eq!(stats.chunks, case.rows.div_ceil(case.chunk_size));
                    assert_eq!(stats.write_stats.staged_deletes, case.rows as u64);
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

#[expect(clippy::cast_possible_truncation)]
fn bench_delete_range_native<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let group_name = format!("storage_v2/delete_range_native/{}", storage_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    for case in DELETE_RANGE_CASES {
        let seed = storage_family.seed_points(SpaceId(1), case.rows as u32, 32);
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter_batched(
                || storage_family.fork_for_write(&seed),
                |storage| {
                    let mut write = block_on(storage.begin_write(WriteOptions::default()))
                        .expect("begin native delete_range write");
                    block_on(write.delete_range(SpaceId(1), point_scan_range()))
                        .expect("native delete_range");
                    let commit = block_on(write.commit()).expect("commit native delete_range");
                    assert_eq!(commit.stats.deleted_ranges, 1);
                    assert_eq!(commit.stats.storage_calls, 1);
                    black_box((case.rows, commit));
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

#[expect(clippy::cast_possible_truncation)]
fn bench_delete_range_storage_helpers<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let group_name = format!(
        "storage_v2/delete_range_storage_helpers/{}",
        storage_family.name()
    );
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    for case in DELETE_RANGE_CASES {
        let seed = storage_family.seed_points(SpaceId(1), case.rows as u32, 32);
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(
            BenchmarkId::new("delete_range", case.name),
            case,
            |b, _case| {
                b.iter_batched(
                    || {
                        let storage = storage_family.fork_for_write(&seed);
                        StorageAdapter::new(storage)
                    },
                    |storage| {
                        let commit = block_on(storage.delete_range(
                            space(1),
                            point_scan_range(),
                            WriteOptions::default(),
                        ))
                        .expect("storage delete_range helper");
                        assert_eq!(commit.stats.deleted_ranges, 1);
                        assert_eq!(commit.stats.storage_calls, 1);
                        black_box(commit);
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("delete_prefix", case.name),
            case,
            |b, _case| {
                b.iter_batched(
                    || {
                        let storage = storage_family.fork_for_write(&seed);
                        StorageAdapter::new(storage)
                    },
                    |storage| {
                        let commit = block_on(storage.delete_prefix(
                            space(1),
                            Prefix {
                                bytes: Bytes::from_static(b"point-"),
                            },
                            WriteOptions::default(),
                        ))
                        .expect("storage delete_prefix helper");
                        assert_eq!(commit.stats.deleted_ranges, 1);
                        assert_eq!(commit.stats.storage_calls, 1);
                        black_box(commit);
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("clear_space", case.name),
            case,
            |b, _case| {
                b.iter_batched(
                    || {
                        let storage = storage_family.fork_for_write(&seed);
                        StorageAdapter::new(storage)
                    },
                    |storage| {
                        let commit =
                            block_on(storage.clear_space(space(1), WriteOptions::default()))
                                .expect("storage clear_space helper");
                        assert_eq!(commit.stats.deleted_ranges, 1);
                        assert_eq!(commit.stats.storage_calls, 1);
                        black_box(commit);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_scan_chunking_matrix<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let group_name = format!("storage_v2/scan_chunking/{}", storage_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    let seed = storage_family.seed_points(SpaceId(1), 10_000, 32);
    let read = block_on(seed.begin_read(ReadOptions::default())).expect("begin chunked scan read");
    let scope = StorageAdapterReadScope::new(read);

    for case in SCAN_CHUNKING_CASES {
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(
            BenchmarkId::new("materialized", case.name),
            case,
            |b, case| {
                b.iter(|| {
                    let stats = block_on(drain_scan_materialized(
                        &scope,
                        space(1),
                        case.scan,
                        case.rows,
                        case.chunk_size,
                    ))
                    .expect("drain chunked materialized scan");
                    assert_eq!(stats.scanned, case.rows);
                    assert_eq!(stats.chunks, case.rows.div_ceil(case.chunk_size));
                    assert_scan_drain_stats(&stats, case);
                    black_box(stats);
                });
            },
        );
    }

    group.finish();
}

fn bench_durable_commit<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let group_name = format!("storage_v2/durable_commit/{}", storage_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    let case = WriteCase {
        name: "puts_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    let mutations = write_mutations(&case);
    group.throughput(Throughput::Elements(u64::from(case.writes)));
    group.bench_function(BenchmarkId::new("durable", case.name), |b| {
        b.iter_batched(
            || {
                let storage = storage_family.open_empty();
                let storage = StorageAdapter::new(storage);
                let writes = canonical_write_set_from_mutations(&mutations);
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) =
                    block_on(storage.commit_write_set(writes, WriteOptions::default()))
                        .expect("durable commit");
                assert_eq!(stats.staged_puts, u64::from(case.writes));
                assert_eq!(stats.put_batches, u64::from(case.spaces));
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_storage_direct_profile<StorageImpl>(c: &mut Criterion, storage_family: StorageImpl)
where
    StorageImpl: StorageBenchStorage,
{
    let selected_case = std::env::var("STORAGE_V2_BENCH_DIRECT_PROFILE_CASE").ok();
    let should_run = |case_name: &str| {
        selected_case
            .as_deref()
            .is_none_or(|selected| selected == case_name)
    };

    let group_name = format!(
        "storage_v2/storage_direct_profile/{}",
        storage_family.name()
    );
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    let direct_put_case = WriteCase {
        name: "direct_commit_puts_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    if should_run(direct_put_case.name) {
        let direct_put_mutations = write_mutations(&direct_put_case);
        let direct_put_batches = direct_write_batches_from_mutations(&direct_put_mutations);
        let warm_storage = storage_family.open_empty();
        block_on(commit_direct_write_batches(
            &warm_storage,
            direct_put_batches.clone(),
        ))
        .expect("warm direct put storage");
        group.throughput(Throughput::Elements(u64::from(direct_put_case.writes)));
        group.bench_function(direct_put_case.name, |b| {
            b.iter_batched(
                || (storage_family.open_empty(), direct_put_batches.clone()),
                |(storage, batches)| {
                    let commit = block_on(commit_direct_write_batches(&storage, batches))
                        .expect("direct storage put commit");
                    assert_eq!(commit.stats.put_entries, 1_024);
                    assert_eq!(commit.stats.deleted_entries, 0);
                    assert_eq!(commit.stats.storage_calls, 16);
                    black_box(commit);
                },
                BatchSize::LargeInput,
            );
        });
    }

    let clean_direct_put_case = WriteCase {
        name: "direct_commit_puts_reused_storage_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    if should_run(clean_direct_put_case.name) {
        let clean_direct_put_mutations = write_mutations(&clean_direct_put_case);
        let clean_direct_put_batches =
            direct_write_batches_from_mutations(&clean_direct_put_mutations);
        let storage = storage_family.open_empty();
        block_on(commit_direct_write_batches(
            &storage,
            clean_direct_put_batches.clone(),
        ))
        .expect("warm reused direct put storage");
        group.throughput(Throughput::Elements(u64::from(
            clean_direct_put_case.writes,
        )));
        group.bench_function(clean_direct_put_case.name, |b| {
            b.iter(|| {
                let commit = block_on(commit_direct_write_batches(
                    &storage,
                    black_box(clean_direct_put_batches.clone()),
                ))
                .expect("direct reused storage put commit");
                assert_eq!(commit.stats.put_entries, 1_024);
                assert_eq!(commit.stats.deleted_entries, 0);
                assert_eq!(commit.stats.storage_calls, 16);
                black_box(commit);
            });
        });
    }

    let mixed_case = WriteCase {
        name: "direct_mixed80_20_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutDelete80_20,
    };
    if should_run(mixed_case.name) {
        let mixed_mutations = write_mutations(&mixed_case);
        let mixed_batches = direct_write_batches_from_mutations(&mixed_mutations);
        let warm_storage = storage_family.open_empty();
        block_on(commit_direct_write_batches(
            &warm_storage,
            mixed_batches.clone(),
        ))
        .expect("warm direct mixed storage");
        group.throughput(Throughput::Elements(u64::from(mixed_case.writes)));
        group.bench_function(mixed_case.name, |b| {
            b.iter_batched(
                || (storage_family.open_empty(), mixed_batches.clone()),
                |(storage, batches)| {
                    let commit = block_on(commit_direct_write_batches(&storage, batches))
                        .expect("direct storage mixed commit");
                    assert_eq!(commit.stats.put_entries, 816);
                    assert_eq!(commit.stats.deleted_entries, 208);
                    assert_eq!(commit.stats.storage_calls, 32);
                    black_box(commit);
                },
                BatchSize::LargeInput,
            );
        });
    }

    let touched_case = WriteCase {
        name: "direct_commit_puts_k128_g16_existing10k_touched_v32",
        writes: 128,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    if should_run(touched_case.name) {
        let touched_mutations = write_mutations(&touched_case);
        let touched_batches = direct_write_batches_from_mutations(&touched_mutations);
        let touched_seed = storage_family.seed_points(SpaceId(1), 10_000, 32);
        let warm_storage = storage_family.fork_for_write(&touched_seed);
        block_on(commit_direct_write_batches(
            &warm_storage,
            touched_batches.clone(),
        ))
        .expect("warm direct touched storage");
        group.throughput(Throughput::Elements(u64::from(touched_case.writes)));
        group.bench_function(touched_case.name, |b| {
            b.iter_batched(
                || {
                    (
                        storage_family.fork_for_write(&touched_seed),
                        touched_batches.clone(),
                    )
                },
                |(storage, batches)| {
                    let commit = block_on(commit_direct_write_batches(&storage, batches))
                        .expect("direct storage touched commit");
                    assert_eq!(commit.stats.put_entries, 128);
                    assert_eq!(commit.stats.deleted_entries, 0);
                    assert_eq!(commit.stats.storage_calls, 16);
                    black_box(commit);
                },
                BatchSize::LargeInput,
            );
        });
    }

    if should_run("direct_get_many_m1000_u100") {
        let point_storage = storage_family.seed_points(SpaceId(1), 100, 32);
        let point_keys = physical_point_request_keys(1, 1_000, 100);
        group.throughput(Throughput::Elements(1_000));
        if should_run("direct_get_many_m1000_u100") {
            group.bench_function("direct_get_many_m1000_u100", |b| {
                b.iter(|| {
                    let read = block_on(point_storage.begin_read(ReadOptions::default()))
                        .expect("begin direct point read");
                    let result = block_on(read.get_many(
                        SpaceId(1),
                        black_box(&point_keys),
                        GetOptions::default(),
                    ))
                    .expect("direct get_many");
                    assert_eq!(result.values.len(), 1_000);
                    assert_eq!(
                        result.values.iter().filter(|value| value.is_some()).count(),
                        1_000
                    );
                    drop(read);
                    black_box(result);
                });
            });
        }
    }

    if should_run("direct_get_many_unique_u100") {
        let point_storage = storage_family.seed_points(SpaceId(1), 100, 32);
        let point_keys = physical_point_request_keys(1, 100, 100);
        group.throughput(Throughput::Elements(100));
        if should_run("direct_get_many_unique_u100") {
            group.bench_function("direct_get_many_unique_u100", |b| {
                b.iter(|| {
                    let read = block_on(point_storage.begin_read(ReadOptions::default()))
                        .expect("begin direct unique point read");
                    let result = block_on(read.get_many(
                        SpaceId(1),
                        black_box(&point_keys),
                        GetOptions::default(),
                    ))
                    .expect("direct unique get_many");
                    assert_eq!(result.values.len(), 100);
                    assert_eq!(
                        result.values.iter().filter(|value| value.is_some()).count(),
                        100
                    );
                    drop(read);
                    black_box(result);
                });
            });
        }
    }

    for (case_name, rows, value_size) in [
        ("direct_get_many_unique_u1_v4096", 1, 4_096),
        ("direct_get_many_unique_u100_v4096", 100, 4_096),
        ("direct_get_many_unique_u1_v65536", 1, 65_536),
        ("direct_get_many_unique_u100_v65536", 100, 65_536),
    ] {
        if should_run(case_name) {
            let point_storage = storage_family.seed_points(SpaceId(1), rows, value_size);
            let point_keys = physical_point_request_keys(1, rows as usize, rows as usize);
            group.throughput(Throughput::Bytes(
                u64::from(rows)
                    * u64::try_from(value_size).expect("point read value size fits u64"),
            ));
            group.bench_function(case_name, |b| {
                b.iter(|| {
                    let read = block_on(point_storage.begin_read(ReadOptions::default()))
                        .expect("begin direct unique large-value point read");
                    let result = block_on(read.get_many(
                        SpaceId(1),
                        black_box(&point_keys),
                        GetOptions::default(),
                    ))
                    .expect("direct unique large-value get_many");
                    assert_eq!(result.values.len(), rows as usize);
                    assert_eq!(
                        result.values.iter().filter(|value| value.is_some()).count(),
                        rows as usize
                    );
                    drop(read);
                    black_box(result);
                });
            });
        }
    }

    if should_run("direct_get_many_unique_u1000") {
        let point_storage = storage_family.seed_points(SpaceId(1), 1_000, 32);
        let point_keys = physical_point_request_keys(1, 1_000, 1_000);
        group.throughput(Throughput::Elements(1_000));
        if should_run("direct_get_many_unique_u1000") {
            group.bench_function("direct_get_many_unique_u1000", |b| {
                b.iter(|| {
                    let read = block_on(point_storage.begin_read(ReadOptions::default()))
                        .expect("begin direct unique point read");
                    let result = block_on(read.get_many(
                        SpaceId(1),
                        black_box(&point_keys),
                        GetOptions::default(),
                    ))
                    .expect("direct unique get_many");
                    assert_eq!(result.values.len(), 1_000);
                    assert_eq!(
                        result.values.iter().filter(|value| value.is_some()).count(),
                        1_000
                    );
                    drop(read);
                    black_box(result);
                });
            });
        }
    }

    if should_run("direct_scan_materialized_q1000") {
        let scan_storage = storage_family.seed_points(SpaceId(1), 1_000, 32);
        let scan_range = physical_point_scan_range(1);
        group.throughput(Throughput::Elements(1_000));
        if should_run("direct_scan_materialized_q1000") {
            group.bench_function("direct_scan_materialized_q1000", |b| {
                b.iter(|| {
                    let read = block_on(scan_storage.begin_read(ReadOptions::default()))
                        .expect("begin direct materialized scan read");
                    let chunk = block_on(materialize_storage_scan(
                        &read,
                        scan_range.clone(),
                        ScanOptions {
                            limit_rows: 1_001,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                    ))
                    .expect("direct materialized scan");
                    assert_eq!(chunk.entries.len(), 1_000);
                    assert!(!chunk.has_more);
                    drop(read);
                    black_box(chunk);
                });
            });
        }
    }

    for (case_name, rows) in [
        ("direct_scan_full_q1_v65536", 1),
        ("direct_scan_full_q100_v65536", 100),
    ] {
        if should_run(case_name) {
            let scan_storage = storage_family.seed_points(SpaceId(1), rows, 65_536);
            let scan_range = physical_point_scan_range(1);
            group.throughput(Throughput::Bytes(u64::from(rows) * 65_536));
            group.bench_function(case_name, |b| {
                b.iter(|| {
                    let read = block_on(scan_storage.begin_read(ReadOptions::default()))
                        .expect("begin direct full-value scan read");
                    let chunk = block_on(materialize_storage_scan(
                        &read,
                        scan_range.clone(),
                        ScanOptions {
                            limit_rows: rows as usize + 1,
                            projection: CoreProjection::FullValue,
                            ..ScanOptions::default()
                        },
                    ))
                    .expect("direct full-value scan");
                    assert_eq!(chunk.entries.len(), rows as usize);
                    assert!(!chunk.has_more);
                    drop(read);
                    black_box(chunk);
                });
            });
        }
    }

    group.finish();
}

#[derive(Clone, Default)]
struct CountingStorage {
    state: Arc<CountingState>,
}

#[derive(Default)]
struct CountingState {
    commit_calls: AtomicU64,
    put_many_calls: AtomicU64,
    delete_many_calls: AtomicU64,
}

struct CountingWrite {
    state: Arc<CountingState>,
}

impl Storage for CountingStorage {
    type Read<'a>
        = EmptyRead
    where
        Self: 'a;

    type Write<'a>
        = CountingWrite
    where
        Self: 'a;
    async fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        Ok(EmptyRead)
    }

    async fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(CountingWrite {
            state: Arc::clone(&self.state),
        })
    }
}

impl StorageWrite for CountingWrite {
    async fn put_many(&mut self, _space: SpaceId, _entries: PutBatch) -> Result<(), StorageError> {
        self.state.put_many_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn delete_many(&mut self, _space: SpaceId, _keys: &[Key]) -> Result<(), StorageError> {
        self.state.delete_many_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn delete_range(
        &mut self,
        _space: SpaceId,
        _range: KeyRange,
    ) -> Result<(), StorageError> {
        self.state.delete_many_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.state.commit_calls.fetch_add(1, Ordering::Relaxed);
        Ok(CommitResult {
            commit_id: None,
            stats: WriteStats::default(),
        })
    }

    async fn rollback(self) -> Result<(), StorageError> {
        Ok(())
    }
}

struct EmptyRead;

impl StorageRead for EmptyRead {
    async fn get_many(
        &self,
        _space: SpaceId,
        _keys: &[Key],
        _opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        unreachable!("write-set benchmark does not point-read")
    }

    async fn scan(
        &self,
        _space: SpaceId,
        _range: KeyRange,
        _opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        unreachable!("write-set benchmark does not scan")
    }
}

impl WriteCase {
    #[expect(clippy::cast_possible_truncation)]
    fn expected_deletes(&self) -> u32 {
        let writes_per_space = self.writes / self.spaces;
        match self.mix {
            WriteMix::PutsOnly => 0,
            WriteMix::DeletesOnly => self.writes,
            WriteMix::PutDelete80_20 => {
                self.spaces * (0..writes_per_space).filter(|index| index % 5 == 0).count() as u32
            }
        }
    }

    fn expected_put_batches(&self) -> u32 {
        match self.mix {
            WriteMix::DeletesOnly => 0,
            WriteMix::PutsOnly | WriteMix::PutDelete80_20 => self.spaces,
        }
    }

    fn expected_delete_batches(&self) -> u32 {
        match self.mix {
            WriteMix::PutsOnly => 0,
            WriteMix::DeletesOnly | WriteMix::PutDelete80_20 => self.spaces,
        }
    }
}

fn seeded_memory_with_value_size(space_id: u32, rows: u32, value_size: usize) -> Memory {
    let storage = Memory::new();
    seed_storage_points(
        &storage,
        SpaceId(space_id),
        rows,
        value_size,
        "in-memory storage",
    );
    storage
}

fn seed_storage_points<StorageImpl>(
    storage: &StorageImpl,
    space_id: SpaceId,
    rows: u32,
    value_size: usize,
    storage_name: &str,
) where
    StorageImpl: Storage + Clone,
{
    let storage = StorageAdapter::new(storage.clone());
    let mut writes = StorageWriteSet::with_capacity(rows as usize, 1);
    for index in 0..rows {
        writes.put(
            space(space_id.0),
            key(format!("point-{index:04}")),
            value(index, value_size),
        );
    }
    let (_commit, stats) = block_on(storage.commit_write_set(writes, WriteOptions::default()))
        .unwrap_or_else(|error| panic!("seed {storage_name}: {error}"));
    assert_eq!(stats.staged_puts, u64::from(rows));
}

fn direct_write_batches_from_mutations(mutations: &[WriteMutation]) -> DirectWriteBatches {
    let mut puts = BTreeMap::<StorageSpace, Vec<PutEntry>>::new();
    let mut deletes = BTreeMap::<StorageSpace, Vec<Key>>::new();
    for mutation in mutations {
        match mutation {
            WriteMutation::Put(space, key, value) => {
                puts.entry(*space).or_default().push(PutEntry {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
            WriteMutation::Delete(space, key) => {
                deletes.entry(*space).or_default().push(key.clone());
            }
        }
    }
    DirectWriteBatches {
        puts: puts
            .into_iter()
            .map(|(space, entries)| (space, PutBatch { entries }))
            .collect(),
        deletes: deletes.into_iter().collect(),
    }
}

async fn commit_direct_write_batches<StorageImpl>(
    storage: &StorageImpl,
    batches: DirectWriteBatches,
) -> Result<CommitResult, StorageError>
where
    StorageImpl: Storage,
{
    let mut write = storage.begin_write(WriteOptions::default()).await?;
    for (space, batch) in batches.puts {
        write.put_many(space.id, batch).await?;
    }
    for (space, keys) in batches.deletes {
        write.delete_many(space.id, &keys).await?;
    }
    write.commit().await
}

#[expect(clippy::cast_possible_truncation)]
async fn fallback_delete_range<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    storage_space: StorageSpace,
    range: KeyRange,
    chunk_size: usize,
) -> Result<DeleteRangeFallbackStats, String>
where
    StorageImpl: Storage,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    let mut keys = Vec::new();
    let mut resume_after = None::<Key>;
    let mut scanned = 0usize;
    let mut chunks = 0usize;

    loop {
        let result = ScanPlan::range(storage_space, range.clone())
            .collect(
                &read,
                ScanOptions {
                    limit_rows: chunk_size,
                    projection: CoreProjection::KeyOnly,
                    resume_after,
                },
            )
            .await
            .map_err(|error| error.to_string())?;

        scanned += result.value.entries.len();
        chunks += usize::from(!result.value.entries.is_empty() || result.value.has_more);
        resume_after = result.value.entries.last().map(|entry| entry.key.clone());
        keys.extend(result.value.entries.into_iter().map(|entry| entry.key));

        if !result.value.has_more {
            break;
        }
    }

    let mut writes = StorageWriteSet::with_capacity(keys.len(), 1);
    writes.reserve_space(storage_space, 0, keys.len());
    for key in keys {
        writes.delete(storage_space, key);
    }

    let (_commit, write_stats) = storage
        .commit_write_set(writes, WriteOptions::default())
        .await
        .map_err(|error| error.to_string())?;
    Ok(DeleteRangeFallbackStats {
        scanned,
        deleted: write_stats.staged_deletes as usize,
        chunks,
        write_stats,
    })
}

async fn drain_scan_materialized<R>(
    read: &StorageAdapterReadScope<R>,
    storage_space: StorageSpace,
    scan: ScanChunkingMode,
    expected_rows: usize,
    chunk_size: usize,
) -> Result<ScanDrainStats, StorageError>
where
    R: StorageRead,
{
    let mut resume_after = None::<Key>;
    let mut stats = ScanDrainStats::default();

    loop {
        let opts = ScanOptions {
            limit_rows: chunk_size,
            projection: CoreProjection::KeyOnly,
            resume_after,
        };
        let plan = match scan {
            ScanChunkingMode::Range => ScanPlan::range(storage_space, point_scan_range()),
            ScanChunkingMode::Prefix => ScanPlan::prefix(
                storage_space,
                Prefix {
                    bytes: Bytes::from_static(b"point-"),
                },
            ),
        };
        let chunk = plan.collect(read, opts).await?;

        let entries = &chunk.value.entries;
        stats.scanned += entries.len();
        stats.storage_calls += chunk.stats.storage_calls;
        stats.chunks += usize::from(!entries.is_empty() || chunk.value.has_more);
        stats.read_stats.add(chunk.stats);
        resume_after = entries.last().map(|entry| entry.key.clone());

        if !chunk.value.has_more {
            break;
        }
    }

    assert_eq!(
        stats.storage_calls,
        expected_rows.div_ceil(chunk_size) as u64
    );
    Ok(stats)
}

fn assert_scan_drain_stats(stats: &ScanDrainStats, case: &ScanChunkingCase) {
    let expected_chunks = case.rows.div_ceil(case.chunk_size);
    let expected_resume_after = expected_chunks.saturating_sub(1) as u64;
    let expected_has_more = expected_chunks.saturating_sub(1) as u64;

    assert_eq!(stats.read_stats.storage_calls, expected_chunks as u64);
    assert_eq!(stats.read_stats.scan_rows, case.rows as u64);
    assert_eq!(stats.read_stats.scan_resume_after, expected_resume_after);
    assert_eq!(stats.read_stats.scan_has_more, expected_has_more);
    assert_eq!(
        stats.read_stats.scan_limit_rows_total,
        (expected_chunks * case.chunk_size) as u64
    );
    assert_eq!(stats.read_stats.scan_limit_rows_max, case.chunk_size as u64);
    assert_eq!(
        stats.read_stats.scan_key_only_chunks,
        expected_chunks as u64
    );
    assert_eq!(stats.read_stats.scan_full_value_chunks, 0);

    match case.scan {
        ScanChunkingMode::Range => {
            assert_eq!(stats.read_stats.range_scan_chunks, expected_chunks as u64);
            assert_eq!(stats.read_stats.prefix_scan_chunks, 0);
            assert_eq!(stats.read_stats.prefix_lowered, 0);
        }
        ScanChunkingMode::Prefix => {
            assert_eq!(stats.read_stats.range_scan_chunks, 0);
            assert_eq!(stats.read_stats.prefix_scan_chunks, expected_chunks as u64);
            assert_eq!(stats.read_stats.prefix_lowered, expected_chunks as u64);
        }
    }
}

fn copy_dir_recursive(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    fs::create_dir_all(to)?;
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let source = entry.path();
        let destination = to.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_dir_recursive(&source, &destination)?;
        } else if file_type.is_file() {
            fs::copy(source, destination)?;
        }
    }
    Ok(())
}

fn point_scan_range() -> KeyRange {
    KeyRange {
        lower: Bound::Included(key("point-0000")),
        upper: Bound::Excluded(key("point:")),
    }
}

async fn materialize_storage_scan<R>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions,
) -> Result<ScanChunk, StorageError>
where
    R: StorageRead,
{
    read.scan(space(1).id, range, opts).await
}

fn checked_write_set_from_mutations(mutations: &[WriteMutation]) -> StorageWriteSet {
    let mut writes = StorageWriteSet::with_capacity(mutations.len(), unique_space_count(mutations));
    for mutation in mutations {
        match mutation {
            WriteMutation::Put(space, key, value) => {
                writes.put(*space, key.clone(), value.clone());
            }
            WriteMutation::Delete(space, key) => {
                writes.delete(*space, key.clone());
            }
        }
    }
    writes
}

fn canonical_write_set_from_mutations(mutations: &[WriteMutation]) -> StorageWriteSet {
    let mut counts = HashMap::<SpaceId, (StorageSpace, usize, usize)>::new();
    let mut space_order = Vec::<StorageSpace>::new();
    for mutation in mutations {
        match mutation {
            WriteMutation::Put(space, _, _) => {
                counts
                    .entry(space.id)
                    .and_modify(|(_, puts, _)| *puts += 1)
                    .or_insert_with(|| {
                        space_order.push(*space);
                        (*space, 1, 0)
                    });
            }
            WriteMutation::Delete(space, _) => {
                counts
                    .entry(space.id)
                    .and_modify(|(_, _, deletes)| *deletes += 1)
                    .or_insert_with(|| {
                        space_order.push(*space);
                        (*space, 0, 1)
                    });
            }
        }
    }

    let mut writes = StorageWriteSet::with_capacity(mutations.len(), counts.len());
    for space in space_order {
        if let Some((_, puts, deletes)) = counts.get(&space.id).copied() {
            writes.reserve_space(space, puts, deletes);
        }
    }

    for mutation in mutations {
        match mutation {
            WriteMutation::Put(space, key, value) => {
                writes.put(*space, key.clone(), value.clone());
            }
            WriteMutation::Delete(space, key) => {
                writes.delete(*space, key.clone());
            }
        }
    }
    writes
}

fn unique_space_count(mutations: &[WriteMutation]) -> usize {
    let mut spaces = HashSet::new();
    for mutation in mutations {
        match mutation {
            WriteMutation::Put(space, _, _) | WriteMutation::Delete(space, _) => {
                spaces.insert(space.id);
            }
        }
    }
    spaces.len()
}

fn write_mutations(case: &WriteCase) -> Vec<WriteMutation> {
    let mut mutations = Vec::with_capacity(case.writes as usize);
    let writes_per_space = case.writes / case.spaces;
    for space_id in 0..case.spaces {
        for index in 0..writes_per_space {
            let global_index = space_id * writes_per_space + index;
            match case.mix {
                WriteMix::PutsOnly => mutations.push(WriteMutation::Put(
                    space(space_id),
                    key(format!("put-{space_id:03}-{index:05}")),
                    value(global_index, case.value_size),
                )),
                WriteMix::DeletesOnly => mutations.push(WriteMutation::Delete(
                    space(space_id),
                    key(format!("delete-{space_id:03}-{index:05}")),
                )),
                WriteMix::PutDelete80_20 => {
                    if index % 5 == 0 {
                        mutations.push(WriteMutation::Delete(
                            space(space_id),
                            key(format!("delete-{space_id:03}-{index:05}")),
                        ));
                    } else {
                        mutations.push(WriteMutation::Put(
                            space(space_id),
                            key(format!("put-{space_id:03}-{index:05}")),
                            value(global_index, case.value_size),
                        ));
                    }
                }
            }
        }
    }
    mutations
}

#[expect(clippy::cast_possible_truncation)]
fn direct_ordered_put_batch(
    _storage_space: StorageSpace,
    writes: usize,
    value_size: usize,
    order: WriteOrder,
) -> Vec<PutEntry> {
    let indexes = match order {
        WriteOrder::Sorted => (0..writes).collect::<Vec<_>>(),
        WriteOrder::Reverse => (0..writes).rev().collect::<Vec<_>>(),
        WriteOrder::Shuffled => (0..writes)
            .map(|index| (index * 7_919) % writes)
            .collect::<Vec<_>>(),
    };

    indexes
        .into_iter()
        .map(|index| PutEntry {
            key: key(format!("ordered-put-{index:05}")),
            value: value(index as u32, value_size),
        })
        .collect()
}

fn seal_sorted_unique_put_batch(mut entries: Vec<PutEntry>) -> Vec<PutEntry> {
    entries.sort_by(|left, right| left.key.cmp(&right.key));
    for window in entries.windows(2) {
        assert_ne!(
            window[0].key, window[1].key,
            "sealed benchmark input must be unique"
        );
    }
    entries
}

fn point_request_keys(requested_keys: usize, unique_keys: usize) -> Vec<Key> {
    (0..requested_keys)
        .map(|index| key(format!("point-{:04}", index % unique_keys)))
        .collect()
}

fn physical_point_request_keys(
    _space_id: u32,
    requested_keys: usize,
    unique_keys: usize,
) -> Vec<Key> {
    // Keys are logical under the space-aware interface; the space travels
    // as a parameter on the storage calls.
    point_request_keys(requested_keys, unique_keys)
}

fn physical_point_scan_range(_space_id: u32) -> KeyRange {
    // Ranges are logical under the space-aware interface.
    point_scan_range()
}

fn space(id: u32) -> StorageSpace {
    StorageSpace::new(SpaceId(id), "bench.storage_v2")
}

fn key(bytes: impl Into<String>) -> Key {
    Key(Bytes::from(bytes.into()))
}

fn value(seed: u32, size: usize) -> StoredValue {
    let mut bytes = vec![0; size];
    let seed_bytes = seed.to_le_bytes();
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = seed_bytes[index % seed_bytes.len()];
    }
    StoredValue {
        bytes: Bytes::from(bytes),
    }
}

criterion_group!(benches, storage_v2_benches);
criterion_main!(benches);
