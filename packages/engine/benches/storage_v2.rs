use std::cell::{Cell, RefCell};
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::hash::{BuildHasher, Hasher};
use std::ops::Bound;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use lix_engine::backend_v2::{
    get_many as backend_get_many, visit_range as backend_visit_range, Backend, BackendCapabilities,
    BackendError, BackendRangeScan, BackendRead, BackendWrite, BufferedRangeScan, CommitResult,
    CoreProjection, GetOptions, InMemoryBackend, Key, KeyRange, KeyRef, PointVisitor, Prefix,
    ProjectedValue, ProjectedValueRef, PutBatch, PutEntry, ReadEntry, ReadOptions, ScanChunk,
    ScanOptions, SpaceId, StoredValue, WriteConcurrency, WriteOptions, WriteStats,
};
use lix_engine::storage_v2::{
    PointReadBuffer, PointReadPlan, ScanBuffer, ScanPlan, StorageContext, StorageReadScope,
    StorageReadStats, StorageSpace, StorageWriteSet, StorageWriteSetStats,
};
use redb_backend_v2::RedbBackend;
use rocksdb_backend_v2::RocksDbBackend;
use rustc_hash::FxBuildHasher;
use sqlite_backend_v2::SqliteBackend;
use tempfile::TempDir;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

#[allow(dead_code)]
#[path = "../tests/backend/support/redb_backend.rs"]
mod redb_backend_v2;

#[allow(dead_code)]
#[path = "../tests/backend/support/rocksdb_backend.rs"]
mod rocksdb_backend_v2;

#[allow(dead_code)]
#[path = "../tests/backend/support/sqlite_backend.rs"]
mod sqlite_backend_v2;

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

#[derive(Default)]
struct CountingPointVisitor {
    visited: usize,
}

impl PointVisitor for CountingPointVisitor {
    fn visit(
        &mut self,
        index: usize,
        key: &Key,
        value: Option<ProjectedValueRef<'_>>,
    ) -> Result<(), BackendError> {
        self.visited += 1;
        black_box((index, key, value));
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct PointCase {
    name: &'static str,
    requested_keys: usize,
    unique_keys: usize,
    existing_unique_keys: usize,
}

#[derive(Clone, Copy)]
struct PrefixCase {
    name: &'static str,
    rows: usize,
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
    backend_calls: u64,
    read_stats: StorageReadStats,
}

trait StorageBenchBackend {
    type Backend: Backend;

    fn name(&self) -> &'static str;

    fn open_empty(&self) -> Self::Backend;

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Backend;

    fn fork_for_write(&self, backend: &Self::Backend) -> Self::Backend;
}

#[derive(Clone, Copy)]
struct InMemoryBenchBackend;

impl StorageBenchBackend for InMemoryBenchBackend {
    type Backend = InMemoryBackend;

    fn name(&self) -> &'static str {
        "in_memory"
    }

    fn open_empty(&self) -> Self::Backend {
        InMemoryBackend::new()
    }

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Backend {
        seeded_in_memory_backend_with_value_size(space.0, rows, value_size)
    }

    fn fork_for_write(&self, backend: &Self::Backend) -> Self::Backend {
        backend
            .fork_snapshot()
            .expect("fork in-memory bench backend")
    }
}

struct SqliteTempBenchBackend {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

impl SqliteTempBenchBackend {
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

impl StorageBenchBackend for SqliteTempBenchBackend {
    type Backend = SqliteBackend;

    fn name(&self) -> &'static str {
        "sqlite_temp"
    }

    fn open_empty(&self) -> Self::Backend {
        SqliteBackend::open(self.next_path()).expect("open empty sqlite bench backend")
    }

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Backend {
        let backend = self.open_empty();
        seed_backend_points(&backend, space, rows, value_size, "sqlite bench backend");
        backend
            .checkpoint()
            .expect("checkpoint seeded sqlite bench backend");
        backend
    }

    fn fork_for_write(&self, backend: &Self::Backend) -> Self::Backend {
        backend
            .checkpoint()
            .expect("checkpoint sqlite bench seed before fork");
        let fork_path = self.next_path();
        fs::copy(backend.path(), &fork_path).expect("copy sqlite bench seed database");
        SqliteBackend::open(fork_path).expect("open sqlite bench fork")
    }
}

struct RedbTempBenchBackend {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

impl RedbTempBenchBackend {
    fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create redb bench matrix temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }

    fn next_path(&self) -> PathBuf {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        self.temp_dir
            .path()
            .join(format!("storage-v2-matrix-{database_id}.redb"))
    }
}

impl StorageBenchBackend for RedbTempBenchBackend {
    type Backend = RedbBackend;

    fn name(&self) -> &'static str {
        "redb_temp"
    }

    fn open_empty(&self) -> Self::Backend {
        RedbBackend::open(self.next_path()).expect("open empty redb bench backend")
    }

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Backend {
        let backend = self.open_empty();
        seed_backend_points(&backend, space, rows, value_size, "redb bench backend");
        backend
    }

    fn fork_for_write(&self, backend: &Self::Backend) -> Self::Backend {
        let fork_path = self.next_path();
        fs::copy(backend.path(), &fork_path).expect("copy redb bench seed database");
        RedbBackend::open(fork_path).expect("open redb bench fork")
    }
}

struct RocksDbTempBenchBackend {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

impl RocksDbTempBenchBackend {
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

impl StorageBenchBackend for RocksDbTempBenchBackend {
    type Backend = RocksDbBackend;

    fn name(&self) -> &'static str {
        "rocksdb_temp"
    }

    fn open_empty(&self) -> Self::Backend {
        RocksDbBackend::open(self.next_path()).expect("open empty rocksdb bench backend")
    }

    fn seed_points(&self, space: SpaceId, rows: u32, value_size: usize) -> Self::Backend {
        let backend = self.open_empty();
        seed_backend_points(&backend, space, rows, value_size, "rocksdb bench backend");
        backend.flush().expect("flush seeded rocksdb bench backend");
        backend
    }

    fn fork_for_write(&self, backend: &Self::Backend) -> Self::Backend {
        backend
            .flush()
            .expect("flush rocksdb bench seed before fork");
        let fork_path = self.next_path();
        copy_dir_recursive(backend.path(), &fork_path).expect("copy rocksdb bench seed database");
        RocksDbBackend::open(fork_path).expect("open rocksdb bench fork")
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
        existing_unique_keys: 100,
    },
    PointCase {
        name: "m1000_u1000",
        requested_keys: 1_000,
        unique_keys: 1_000,
        existing_unique_keys: 1_000,
    },
    PointCase {
        name: "m1000_u100",
        requested_keys: 1_000,
        unique_keys: 100,
        existing_unique_keys: 100,
    },
    PointCase {
        name: "m10000_u100",
        requested_keys: 10_000,
        unique_keys: 100,
        existing_unique_keys: 100,
    },
    PointCase {
        name: "m10000_u10000",
        requested_keys: 10_000,
        unique_keys: 10_000,
        existing_unique_keys: 10_000,
    },
    PointCase {
        name: "m1000_u100_missing10",
        requested_keys: 1_000,
        unique_keys: 100,
        existing_unique_keys: 90,
    },
    PointCase {
        name: "m1000_u100_missing90",
        requested_keys: 1_000,
        unique_keys: 100,
        existing_unique_keys: 10,
    },
];

const PREFIX_CASES: &[PrefixCase] = &[
    PrefixCase {
        name: "q0",
        rows: 0,
    },
    PrefixCase {
        name: "q100",
        rows: 100,
    },
    PrefixCase {
        name: "q1000",
        rows: 1_000,
    },
    PrefixCase {
        name: "q10000",
        rows: 10_000,
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
        match std::env::var("STORAGE_V2_BENCH_DIRECT_PROFILE_BACKEND").as_deref() {
            Ok("in_memory") => bench_backend_direct_profile(c, InMemoryBenchBackend),
            Ok("sqlite_temp") => bench_backend_direct_profile(c, SqliteTempBenchBackend::new()),
            Ok("redb_temp") => bench_backend_direct_profile(c, RedbTempBenchBackend::new()),
            Ok("rocksdb_temp") => bench_backend_direct_profile(c, RocksDbTempBenchBackend::new()),
            Ok(other) => panic!("unknown direct profile backend: {other}"),
            Err(_) => {
                bench_backend_direct_profile(c, InMemoryBenchBackend);
                bench_backend_direct_profile(c, SqliteTempBenchBackend::new());
                bench_backend_direct_profile(c, RedbTempBenchBackend::new());
                bench_backend_direct_profile(c, RocksDbTempBenchBackend::new());
            }
        }
        return;
    }

    bench_write_set_lowering(c);
    bench_write_set_construction(c);
    bench_write_set_build_and_commit(c, InMemoryBenchBackend);
    bench_write_set_build_and_commit(c, SqliteTempBenchBackend::new());
    bench_write_set_build_and_commit(c, RedbTempBenchBackend::new());
    bench_write_set_build_and_commit(c, RocksDbTempBenchBackend::new());
    bench_delete_range_fallback(c, InMemoryBenchBackend);
    bench_delete_range_fallback(c, SqliteTempBenchBackend::new());
    bench_delete_range_fallback(c, RedbTempBenchBackend::new());
    bench_delete_range_fallback(c, RocksDbTempBenchBackend::new());
    bench_delete_range_native(c, InMemoryBenchBackend);
    bench_delete_range_native(c, SqliteTempBenchBackend::new());
    bench_delete_range_native(c, RedbTempBenchBackend::new());
    bench_delete_range_native(c, RocksDbTempBenchBackend::new());
    bench_delete_range_storage_helpers(c, InMemoryBenchBackend);
    bench_delete_range_storage_helpers(c, SqliteTempBenchBackend::new());
    bench_delete_range_storage_helpers(c, RedbTempBenchBackend::new());
    bench_delete_range_storage_helpers(c, RocksDbTempBenchBackend::new());
    bench_scan_chunking_matrix(c, InMemoryBenchBackend);
    bench_scan_chunking_matrix(c, SqliteTempBenchBackend::new());
    bench_scan_chunking_matrix(c, RedbTempBenchBackend::new());
    bench_scan_chunking_matrix(c, RocksDbTempBenchBackend::new());
    bench_durable_commit(c, InMemoryBenchBackend);
    bench_durable_commit(c, SqliteTempBenchBackend::new());
    bench_durable_commit(c, RedbTempBenchBackend::new());
    bench_durable_commit(c, RocksDbTempBenchBackend::new());
    bench_point_request_plan(c);
    bench_point_read_adapter(c);
    bench_point_read_indexed_adapter(c);
    bench_point_read_indexed_lean_backend(c);
    bench_point_read_planned_lean_backend(c);
    bench_prefix_scan_adapter(c);
    bench_storage_backend_matrix(c, InMemoryBenchBackend);
    bench_storage_backend_matrix(c, SqliteTempBenchBackend::new());
    bench_storage_backend_matrix(c, RedbTempBenchBackend::new());
    bench_storage_backend_matrix(c, RocksDbTempBenchBackend::new());
    bench_backend_direct_profile(c, InMemoryBenchBackend);
    bench_backend_direct_profile(c, SqliteTempBenchBackend::new());
    bench_backend_direct_profile(c, RedbTempBenchBackend::new());
    bench_backend_direct_profile(c, RocksDbTempBenchBackend::new());
    bench_in_memory_backend(c);
    bench_scan_visitor_baseline(c);
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
            let mut backend_keys = Vec::with_capacity(point_keys.len());
            for key in black_box(point_keys) {
                if seen.insert(key) {
                    backend_keys.push(key.clone());
                }
            }

            let mut found =
                HashMap::with_capacity_and_hasher(backend_keys.len(), build_hasher.clone());
            for key in &backend_keys {
                found.insert(key.clone(), ProjectedValue::FullValue(key.0.clone()));
            }

            let mut values = Vec::with_capacity(point_keys.len());
            for key in point_keys {
                values.push(found.get(key).cloned());
            }

            assert_eq!(backend_keys.len(), 100);
            assert_eq!(values.len(), point_keys.len());
            black_box(values);
        });
    });

    group.throughput(Throughput::Elements(unique_keys.len() as u64));
    group.bench_function(BenchmarkId::new("unique_point_reconstruction", name), |b| {
        b.iter(|| {
            let mut seen =
                HashSet::with_capacity_and_hasher(unique_keys.len(), build_hasher.clone());
            let mut backend_keys = Vec::with_capacity(unique_keys.len());
            for key in black_box(unique_keys) {
                if seen.insert(key) {
                    backend_keys.push(key.clone());
                }
            }

            let mut found =
                HashMap::with_capacity_and_hasher(backend_keys.len(), build_hasher.clone());
            for key in &backend_keys {
                found.insert(key.clone(), ProjectedValue::FullValue(key.0.clone()));
            }

            let mut values = Vec::with_capacity(unique_keys.len());
            for key in unique_keys {
                values.push(found.get(key).cloned());
            }

            assert_eq!(backend_keys.len(), unique_keys.len());
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
        group.throughput(Throughput::Elements(case.writes as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let backend = CountingBackend::default();
                    let storage = StorageContext::new(backend.clone());
                    let writes = canonical_write_set_from_mutations(&mutations);
                    (storage, backend, writes)
                },
                |(storage, backend, writes)| {
                    let (_commit, stats) = storage
                        .commit_write_set(writes, WriteOptions::default())
                        .expect("commit write set");
                    let expected_deletes = case.expected_deletes();
                    let expected_puts = case.writes - expected_deletes;
                    assert_eq!(stats.staged_puts, expected_puts as u64);
                    assert_eq!(stats.staged_deletes, expected_deletes as u64);
                    assert_eq!(stats.touched_spaces, case.spaces as u64);
                    assert_eq!(stats.put_batches, case.expected_put_batches() as u64);
                    assert_eq!(stats.delete_batches, case.expected_delete_batches() as u64);
                    assert_eq!(
                        backend.state.put_many_calls.get(),
                        case.expected_put_batches() as u64
                    );
                    assert_eq!(
                        backend.state.delete_many_calls.get(),
                        case.expected_delete_batches() as u64
                    );
                    assert_eq!(backend.state.commit_calls.get(), 1);
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
        group.throughput(Throughput::Elements(case.writes as u64));

        group.bench_with_input(BenchmarkId::new("checked", case.name), case, |b, case| {
            b.iter(|| {
                let writes = checked_write_set_from_mutations(black_box(&mutations));
                let stats = writes.stats();
                assert_eq!(
                    stats.staged_puts,
                    (case.writes - case.expected_deletes()) as u64
                );
                assert_eq!(stats.staged_deletes, case.expected_deletes() as u64);
                assert_eq!(stats.touched_spaces, case.spaces as u64);
                black_box(writes);
            });
        });

        group.bench_with_input(BenchmarkId::new("canonical", case.name), case, |b, case| {
            b.iter(|| {
                let writes = canonical_write_set_from_mutations(black_box(&mutations));
                let stats = writes.stats();
                assert_eq!(
                    stats.staged_puts,
                    (case.writes - case.expected_deletes()) as u64
                );
                assert_eq!(stats.staged_deletes, case.expected_deletes() as u64);
                assert_eq!(stats.touched_spaces, case.spaces as u64);
                black_box(writes);
            });
        });
    }

    group.finish();
}

fn bench_write_set_build_and_commit<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let group_name = format!(
        "storage_v2/write_set_build_and_commit/{}",
        backend_family.name()
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
        group.throughput(Throughput::Elements(case.writes as u64));

        group.bench_with_input(BenchmarkId::new("checked", case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let backend = backend_family.open_empty();
                    StorageContext::new(backend)
                },
                |storage| {
                    let writes = checked_write_set_from_mutations(black_box(&mutations));
                    let (_commit, stats) = storage
                        .commit_write_set(writes, WriteOptions::default())
                        .expect("checked build and commit");
                    assert_eq!(
                        stats.staged_puts,
                        (case.writes - case.expected_deletes()) as u64
                    );
                    assert_eq!(stats.staged_deletes, case.expected_deletes() as u64);
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("canonical", case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let backend = backend_family.open_empty();
                    StorageContext::new(backend)
                },
                |storage| {
                    let writes = canonical_write_set_from_mutations(black_box(&mutations));
                    let (_commit, stats) = storage
                        .commit_write_set(writes, WriteOptions::default())
                        .expect("canonical build and commit");
                    assert_eq!(
                        stats.staged_puts,
                        (case.writes - case.expected_deletes()) as u64
                    );
                    assert_eq!(stats.staged_deletes, case.expected_deletes() as u64);
                    black_box(stats);
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_delete_range_fallback<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let group_name = format!("storage_v2/delete_range_fallback/{}", backend_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    for case in DELETE_RANGE_CASES {
        let seed = backend_family.seed_points(SpaceId(1), case.rows as u32, 32);
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter_batched(
                || {
                    let backend = backend_family.fork_for_write(&seed);
                    StorageContext::new(backend)
                },
                |storage| {
                    let stats = fallback_delete_range(
                        &storage,
                        space(1),
                        point_scan_range(),
                        case.chunk_size,
                    )
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

fn bench_delete_range_native<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let group_name = format!("storage_v2/delete_range_native/{}", backend_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    for case in DELETE_RANGE_CASES {
        let seed = backend_family.seed_points(SpaceId(1), case.rows as u32, 32);
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter_batched(
                || backend_family.fork_for_write(&seed),
                |backend| {
                    let mut write = backend
                        .begin_write(WriteOptions::default())
                        .expect("begin native delete_range write");
                    write
                        .delete_range(physical_point_scan_range(1))
                        .expect("native delete_range");
                    let commit = write.commit().expect("commit native delete_range");
                    assert_eq!(commit.stats.deleted_ranges, 1);
                    assert_eq!(commit.stats.backend_calls, 1);
                    black_box((case.rows, commit));
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

fn bench_delete_range_storage_helpers<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let group_name = format!(
        "storage_v2/delete_range_storage_helpers/{}",
        backend_family.name()
    );
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    for case in DELETE_RANGE_CASES {
        let seed = backend_family.seed_points(SpaceId(1), case.rows as u32, 32);
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(
            BenchmarkId::new("delete_range", case.name),
            case,
            |b, _case| {
                b.iter_batched(
                    || {
                        let backend = backend_family.fork_for_write(&seed);
                        StorageContext::new(backend)
                    },
                    |storage| {
                        let commit = storage
                            .delete_range(space(1), point_scan_range(), WriteOptions::default())
                            .expect("storage delete_range helper");
                        assert_eq!(commit.stats.deleted_ranges, 1);
                        assert_eq!(commit.stats.backend_calls, 1);
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
                        let backend = backend_family.fork_for_write(&seed);
                        StorageContext::new(backend)
                    },
                    |storage| {
                        let commit = storage
                            .delete_prefix(
                                space(1),
                                Prefix {
                                    bytes: Bytes::from_static(b"point-"),
                                },
                                WriteOptions::default(),
                            )
                            .expect("storage delete_prefix helper");
                        assert_eq!(commit.stats.deleted_ranges, 1);
                        assert_eq!(commit.stats.backend_calls, 1);
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
                        let backend = backend_family.fork_for_write(&seed);
                        StorageContext::new(backend)
                    },
                    |storage| {
                        let commit = storage
                            .clear_space(space(1), WriteOptions::default())
                            .expect("storage clear_space helper");
                        assert_eq!(commit.stats.deleted_ranges, 1);
                        assert_eq!(commit.stats.backend_calls, 1);
                        black_box(commit);
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_scan_chunking_matrix<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let group_name = format!("storage_v2/scan_chunking/{}", backend_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    let seed = backend_family.seed_points(SpaceId(1), 10_000, 32);
    let read = seed
        .begin_read(ReadOptions::default())
        .expect("begin chunked scan read");
    let scope = StorageReadScope::new(read);

    for case in SCAN_CHUNKING_CASES {
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(
            BenchmarkId::new("materialized", case.name),
            case,
            |b, case| {
                b.iter(|| {
                    let stats = drain_scan_materialized(
                        &scope,
                        space(1),
                        case.scan,
                        case.rows,
                        case.chunk_size,
                    )
                    .expect("drain chunked materialized scan");
                    assert_eq!(stats.scanned, case.rows);
                    assert_eq!(stats.chunks, case.rows.div_ceil(case.chunk_size));
                    assert_scan_drain_stats(&stats, case);
                    black_box(stats);
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("visit", case.name), case, |b, case| {
            b.iter(|| {
                let stats =
                    drain_scan_visit(&scope, space(1), case.scan, case.rows, case.chunk_size)
                        .expect("drain chunked visitor scan");
                assert_eq!(stats.scanned, case.rows);
                assert_eq!(stats.chunks, case.rows.div_ceil(case.chunk_size));
                assert_scan_drain_stats(&stats, case);
                black_box(stats);
            });
        });

        group.bench_with_input(
            BenchmarkId::new("cursor_visit", case.name),
            case,
            |b, case| {
                b.iter(|| {
                    let stats = drain_scan_cursor_visit(
                        &scope,
                        space(1),
                        case.scan,
                        case.rows,
                        case.chunk_size,
                    )
                    .expect("drain cursor chunked visitor scan");
                    assert_eq!(stats.scanned, case.rows);
                    assert_eq!(stats.chunks, case.rows.div_ceil(case.chunk_size));
                    assert_cursor_scan_drain_stats(&stats, case);
                    black_box(stats);
                });
            },
        );
    }

    group.finish();
}

fn bench_durable_commit<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let group_name = format!("storage_v2/durable_commit/{}", backend_family.name());
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
    group.throughput(Throughput::Elements(case.writes as u64));
    group.bench_function(BenchmarkId::new("durable", case.name), |b| {
        b.iter_batched(
            || {
                let backend = backend_family.open_empty();
                let storage = StorageContext::new(backend);
                let writes = canonical_write_set_from_mutations(&mutations);
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) = storage
                    .commit_write_set(writes, WriteOptions::default())
                    .expect("durable commit");
                assert_eq!(stats.staged_puts, case.writes as u64);
                assert_eq!(stats.put_batches, case.spaces as u64);
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_point_read_adapter(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/point_read_adapter");

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let expected_missing_slots = case.requested_missing_slots();
        let read = StorageReadScope::new(PointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = PointReadPlan::new(space(1), black_box(&keys))
                    .materialize(&read, GetOptions::default())
                    .expect("point read");
                assert_eq!(result.stats.requested_keys, case.requested_keys as u64);
                assert_eq!(result.stats.unique_backend_keys, case.unique_keys as u64);
                assert_eq!(result.stats.backend_calls, 1);
                assert_eq!(result.value.len(), case.requested_keys);
                assert_eq!(
                    result.value.iter().filter(|value| value.is_none()).count(),
                    expected_missing_slots
                );
                black_box(result.value);
            });
        });
    }

    group.finish();
}

fn bench_point_read_indexed_adapter(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/point_read_indexed_adapter");

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let expected_unique_missing = case.unique_keys - case.existing_unique_keys;
        let read = StorageReadScope::new(PointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let plan = PointReadPlan::new(space(1), black_box(&keys));
                let result = plan
                    .collect(&read, GetOptions::default())
                    .expect("indexed point read");
                assert_eq!(result.stats.requested_keys, case.requested_keys as u64);
                assert_eq!(result.stats.unique_backend_keys, case.unique_keys as u64);
                assert_eq!(result.stats.backend_calls, 1);
                assert_eq!(result.value.len(), case.requested_keys);
                assert_eq!(result.value.unique_values.len(), case.unique_keys);
                assert_eq!(
                    result
                        .value
                        .unique_values
                        .iter()
                        .filter(|value| value.is_none())
                        .count(),
                    expected_unique_missing
                );
                black_box(result.value);
            });
        });
    }

    group.finish();
}

fn bench_point_read_indexed_lean_backend(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/point_read_indexed_lean_backend");

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let expected_unique_missing = case.unique_keys - case.existing_unique_keys;
        let read = StorageReadScope::new(LeanPointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let plan = PointReadPlan::new(space(1), black_box(&keys));
                let result = plan
                    .collect(&read, GetOptions::default())
                    .expect("indexed point read");
                assert_eq!(result.stats.requested_keys, case.requested_keys as u64);
                assert_eq!(result.stats.unique_backend_keys, case.unique_keys as u64);
                assert_eq!(result.stats.backend_calls, 1);
                assert_eq!(result.value.len(), case.requested_keys);
                assert_eq!(result.value.unique_values.len(), case.unique_keys);
                assert_eq!(
                    result
                        .value
                        .unique_values
                        .iter()
                        .filter(|value| value.is_none())
                        .count(),
                    expected_unique_missing
                );
                black_box(result.value);
            });
        });
    }

    group.finish();
}

fn bench_point_read_planned_lean_backend(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/point_read_planned_lean_backend");

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let plan = PointReadPlan::new(space(1), &keys);
        let expected_unique_missing = case.unique_keys - case.existing_unique_keys;
        let read = StorageReadScope::new(LeanPointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = black_box(&plan)
                    .collect(&read, GetOptions::default())
                    .expect("planned indexed point read");
                assert_eq!(result.stats.requested_keys, case.requested_keys as u64);
                assert_eq!(result.stats.unique_backend_keys, case.unique_keys as u64);
                assert_eq!(result.stats.backend_calls, 1);
                assert_eq!(result.value.len(), case.requested_keys);
                assert_eq!(result.value.unique_values.len(), case.unique_keys);
                assert_eq!(
                    result
                        .value
                        .unique_values
                        .iter()
                        .filter(|value| value.is_none())
                        .count(),
                    expected_unique_missing
                );
                black_box(result.value);
            });
        });

        let mut buffer = PointReadBuffer::new();
        group.bench_with_input(BenchmarkId::new("buffered", case.name), case, |b, case| {
            b.iter(|| {
                let result = black_box(&plan)
                    .collect_into(&read, GetOptions::default(), &mut buffer)
                    .expect("buffered planned indexed point read");
                assert_eq!(result.stats.requested_keys, case.requested_keys as u64);
                assert_eq!(result.stats.unique_backend_keys, case.unique_keys as u64);
                assert_eq!(result.stats.backend_calls, 1);
                assert_eq!(result.value.len(), case.requested_keys);
                assert_eq!(result.value.unique_values.len(), case.unique_keys);
                assert_eq!(
                    result
                        .value
                        .unique_values
                        .iter()
                        .filter(|value| value.is_none())
                        .count(),
                    expected_unique_missing
                );
                black_box(result.value);
            });
        });

        group.bench_with_input(
            BenchmarkId::new("visit_unique", case.name),
            case,
            |b, case| {
                b.iter(|| {
                    let mut visited = 0usize;
                    let mut missing = 0usize;
                    let stats = black_box(&plan)
                        .visit(
                            &read,
                            GetOptions::default(),
                            &mut |index: usize, key: &Key, value: Option<ProjectedValueRef<'_>>| {
                                visited += 1;
                                if value.is_none() {
                                    missing += 1;
                                }
                                black_box((index, key, value));
                                Ok(())
                            },
                        )
                        .expect("planned point visitor");
                    assert_eq!(stats.requested_keys, case.requested_keys as u64);
                    assert_eq!(stats.unique_backend_keys, case.unique_keys as u64);
                    assert_eq!(stats.backend_calls, 1);
                    assert_eq!(visited, case.unique_keys);
                    assert_eq!(missing, expected_unique_missing);
                    black_box(stats);
                });
            },
        );
    }

    group.finish();
}

fn bench_prefix_scan_adapter(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/prefix_scan_adapter");

    for case in PREFIX_CASES {
        let read = StorageReadScope::new(PrefixReadBackend::new(case.rows));
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = ScanPlan::prefix(
                    space(1),
                    Prefix {
                        bytes: Bytes::from_static(b"row-"),
                    },
                )
                .collect(
                    &read,
                    ScanOptions {
                        limit_rows: case.rows + 1,
                        ..ScanOptions::default()
                    },
                )
                .expect("prefix scan");
                assert_eq!(result.stats.prefix_lowered, 1);
                assert_eq!(result.stats.backend_calls, 1);
                assert_eq!(result.value.entries.len(), case.rows);
                black_box(result.value);
            });
        });
    }

    group.finish();
}

fn bench_storage_backend_matrix<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let group_name = format!("storage_v2/backend_matrix/{}", backend_family.name());
    let mut group = c.benchmark_group(group_name);
    group.sample_size(10);
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }

    let commit_case = WriteCase {
        name: "commit_puts_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    let commit_mutations = write_mutations(&commit_case);
    group.throughput(Throughput::Elements(commit_case.writes as u64));
    group.bench_function(commit_case.name, |b| {
        b.iter_batched(
            || {
                let backend = backend_family.open_empty();
                let storage = StorageContext::new(backend);
                let writes = write_set_from_mutations(&storage, &commit_mutations);
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) = storage
                    .commit_write_set(writes, WriteOptions::default())
                    .expect("commit backend matrix write set");
                assert_eq!(stats.staged_puts, 1_024);
                assert_eq!(stats.put_batches, 16);
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    let mixed_case = WriteCase {
        name: "mixed80_20_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutDelete80_20,
    };
    let mixed_mutations = write_mutations(&mixed_case);
    group.throughput(Throughput::Elements(mixed_case.writes as u64));
    group.bench_function(mixed_case.name, |b| {
        b.iter_batched(
            || {
                let backend = backend_family.open_empty();
                let storage = StorageContext::new(backend);
                let writes = write_set_from_mutations(&storage, &mixed_mutations);
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) = storage
                    .commit_write_set(writes, WriteOptions::default())
                    .expect("commit backend matrix mixed write set");
                assert_eq!(stats.staged_puts, 816);
                assert_eq!(stats.staged_deletes, 208);
                assert_eq!(stats.put_batches, 16);
                assert_eq!(stats.delete_batches, 16);
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    let touched_case = WriteCase {
        name: "commit_puts_k128_g16_existing10k_touched_v32",
        writes: 128,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    let touched_mutations = write_mutations(&touched_case);
    let touched_seed = backend_family.seed_points(SpaceId(1), 10_000, 32);
    group.throughput(Throughput::Elements(touched_case.writes as u64));
    group.bench_function(touched_case.name, |b| {
        b.iter_batched(
            || {
                let backend = backend_family.fork_for_write(&touched_seed);
                let storage = StorageContext::new(backend);
                let writes = write_set_from_mutations(&storage, &touched_mutations);
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) = storage
                    .commit_write_set(writes, WriteOptions::default())
                    .expect("commit backend matrix touched write set");
                assert_eq!(stats.staged_puts, 128);
                assert_eq!(stats.put_batches, 16);
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    let point_backend = backend_family.seed_points(SpaceId(1), 100, 32);
    let point_read = point_backend
        .begin_read(ReadOptions::default())
        .expect("begin backend matrix point read");
    let point_scope = StorageReadScope::new(point_read);
    let point_keys = point_request_keys(1_000, 100);
    let point_plan = PointReadPlan::new(space(1), &point_keys);
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("planned_visit_unique_m1000_u100", |b| {
        b.iter(|| {
            let mut visited = 0usize;
            let mut bytes_seen = 0usize;
            let stats = black_box(&point_plan)
                .visit(
                    &point_scope,
                    GetOptions::default(),
                    &mut |index: usize, key: &Key, value: Option<ProjectedValueRef<'_>>| {
                        visited += 1;
                        if let Some(ProjectedValueRef::FullValue(value)) = value {
                            bytes_seen += value.len();
                        }
                        black_box((index, key, value));
                        Ok(())
                    },
                )
                .expect("backend matrix planned point visitor");
            assert_eq!(stats.requested_keys, 1_000);
            assert_eq!(stats.unique_backend_keys, 100);
            assert_eq!(stats.backend_calls, 1);
            assert_eq!(visited, 100);
            assert_eq!(bytes_seen, 3_200);
            black_box(stats);
        });
    });

    group.bench_function("planned_get_many_m1000_u100", |b| {
        b.iter(|| {
            let result = black_box(&point_plan)
                .collect(&point_scope, GetOptions::default())
                .expect("backend matrix planned point read");
            assert_eq!(result.stats.requested_keys, 1_000);
            assert_eq!(result.stats.unique_backend_keys, 100);
            assert_eq!(result.stats.backend_calls, 1);
            assert_eq!(result.value.unique_values.len(), 100);
            black_box(result.value);
        });
    });

    let mut point_buffer = PointReadBuffer::new();
    group.bench_function("planned_get_many_buffered_m1000_u100", |b| {
        b.iter(|| {
            let result = black_box(&point_plan)
                .collect_into(&point_scope, GetOptions::default(), &mut point_buffer)
                .expect("backend matrix buffered planned point read");
            assert_eq!(result.stats.requested_keys, 1_000);
            assert_eq!(result.stats.unique_backend_keys, 100);
            assert_eq!(result.stats.backend_calls, 1);
            assert_eq!(result.value.unique_values.len(), 100);
            black_box(result.value);
        });
    });

    for rows in [10usize, 100] {
        let scan_backend = backend_family.seed_points(SpaceId(1), rows as u32, 32);
        let scan_read = scan_backend
            .begin_read(ReadOptions::default())
            .expect("begin backend matrix small scan read");
        let scan_scope = StorageReadScope::new(scan_read);
        group.throughput(Throughput::Elements(rows as u64));

        group.bench_function(format!("scan_range_visit_key_only_q{rows}"), |b| {
            b.iter(|| {
                let mut visited = 0usize;
                let result = ScanPlan::range(space(1), point_scan_range())
                    .visit(
                        &scan_scope,
                        ScanOptions {
                            limit_rows: rows + 1,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                            visited += 1;
                            assert!(matches!(value, ProjectedValueRef::KeyOnly));
                            black_box(key);
                            Ok(())
                        },
                    )
                    .expect("backend matrix small scan visitor");
                assert_eq!(visited, rows);
                assert_eq!(result.value.emitted, rows);
                assert!(!result.value.has_more);
                black_box(result);
            });
        });

        group.bench_function(format!("scan_range_q{rows}"), |b| {
            b.iter(|| {
                let chunk = ScanPlan::range(space(1), point_scan_range())
                    .collect(
                        &scan_scope,
                        ScanOptions {
                            limit_rows: rows + 1,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                    )
                    .expect("backend matrix small materialized scan");
                assert_eq!(chunk.value.entries.len(), rows);
                assert_eq!(chunk.stats.backend_calls, 1);
                black_box(chunk.value);
            });
        });

        group.bench_function(format!("prefix_scan_q{rows}"), |b| {
            b.iter(|| {
                let chunk = ScanPlan::prefix(
                    space(1),
                    Prefix {
                        bytes: Bytes::from_static(b"point-"),
                    },
                )
                .collect(
                    &scan_scope,
                    ScanOptions {
                        limit_rows: rows + 1,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                )
                .expect("backend matrix small prefix scan");
                assert_eq!(chunk.value.entries.len(), rows);
                assert_eq!(chunk.stats.backend_calls, 1);
                assert_eq!(chunk.stats.prefix_lowered, 1);
                black_box(chunk.value);
            });
        });
    }

    let scan_backend = backend_family.seed_points(SpaceId(1), 1_000, 32);
    let scan_read = scan_backend
        .begin_read(ReadOptions::default())
        .expect("begin backend matrix scan read");
    let scan_scope = StorageReadScope::new(scan_read);
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("scan_range_visit_key_only_q1000", |b| {
        b.iter(|| {
            let mut visited = 0usize;
            let result = ScanPlan::range(space(1), point_scan_range())
                .visit(
                    &scan_scope,
                    ScanOptions {
                        limit_rows: 1_001,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                    &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                        visited += 1;
                        assert!(matches!(value, ProjectedValueRef::KeyOnly));
                        black_box(key);
                        Ok(())
                    },
                )
                .expect("backend matrix scan visitor");
            assert_eq!(visited, 1_000);
            assert_eq!(result.value.emitted, 1_000);
            assert!(!result.value.has_more);
            black_box(result);
        });
    });

    group.bench_function("scan_range_q1000", |b| {
        b.iter(|| {
            let chunk = ScanPlan::range(space(1), point_scan_range())
                .collect(
                    &scan_scope,
                    ScanOptions {
                        limit_rows: 1_001,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                )
                .expect("backend matrix materialized scan");
            assert_eq!(chunk.value.entries.len(), 1_000);
            assert_eq!(chunk.stats.backend_calls, 1);
            black_box(chunk.value);
        });
    });

    group.bench_function("prefix_scan_q1000", |b| {
        b.iter(|| {
            let chunk = ScanPlan::prefix(
                space(1),
                Prefix {
                    bytes: Bytes::from_static(b"point-"),
                },
            )
            .collect(
                &scan_scope,
                ScanOptions {
                    limit_rows: 1_001,
                    projection: CoreProjection::KeyOnly,
                    ..ScanOptions::default()
                },
            )
            .expect("backend matrix prefix scan");
            assert_eq!(chunk.value.entries.len(), 1_000);
            assert_eq!(chunk.stats.backend_calls, 1);
            assert_eq!(chunk.stats.prefix_lowered, 1);
            black_box(chunk.value);
        });
    });

    group.finish();
}

fn bench_backend_direct_profile<B>(c: &mut Criterion, backend_family: B)
where
    B: StorageBenchBackend,
{
    let selected_case = std::env::var("STORAGE_V2_BENCH_DIRECT_PROFILE_CASE").ok();
    let should_run = |case_name: &str| {
        selected_case
            .as_deref()
            .is_none_or(|selected| selected == case_name)
    };

    let group_name = format!(
        "storage_v2/backend_direct_profile/{}",
        backend_family.name()
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
        let warm_backend = backend_family.open_empty();
        commit_direct_write_batches(&warm_backend, direct_put_batches.clone())
            .expect("warm direct put backend");
        group.throughput(Throughput::Elements(direct_put_case.writes as u64));
        group.bench_function(direct_put_case.name, |b| {
            b.iter_batched(
                || (backend_family.open_empty(), direct_put_batches.clone()),
                |(backend, batches)| {
                    let commit = commit_direct_write_batches(&backend, batches)
                        .expect("direct backend put commit");
                    assert_eq!(commit.stats.put_entries, 1_024);
                    assert_eq!(commit.stats.deleted_entries, 0);
                    assert_eq!(commit.stats.backend_calls, 16);
                    black_box(commit);
                },
                BatchSize::LargeInput,
            );
        });
    }

    let clean_direct_put_case = WriteCase {
        name: "direct_commit_puts_reused_backend_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    if should_run(clean_direct_put_case.name) {
        let clean_direct_put_mutations = write_mutations(&clean_direct_put_case);
        let clean_direct_put_batches =
            direct_write_batches_from_mutations(&clean_direct_put_mutations);
        let backend = backend_family.open_empty();
        commit_direct_write_batches(&backend, clean_direct_put_batches.clone())
            .expect("warm reused direct put backend");
        group.throughput(Throughput::Elements(clean_direct_put_case.writes as u64));
        group.bench_function(clean_direct_put_case.name, |b| {
            b.iter(|| {
                let commit = commit_direct_write_batches(
                    &backend,
                    black_box(clean_direct_put_batches.clone()),
                )
                .expect("direct reused backend put commit");
                assert_eq!(commit.stats.put_entries, 1_024);
                assert_eq!(commit.stats.deleted_entries, 0);
                assert_eq!(commit.stats.backend_calls, 16);
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
        let warm_backend = backend_family.open_empty();
        commit_direct_write_batches(&warm_backend, mixed_batches.clone())
            .expect("warm direct mixed backend");
        group.throughput(Throughput::Elements(mixed_case.writes as u64));
        group.bench_function(mixed_case.name, |b| {
            b.iter_batched(
                || (backend_family.open_empty(), mixed_batches.clone()),
                |(backend, batches)| {
                    let commit = commit_direct_write_batches(&backend, batches)
                        .expect("direct backend mixed commit");
                    assert_eq!(commit.stats.put_entries, 816);
                    assert_eq!(commit.stats.deleted_entries, 208);
                    assert_eq!(commit.stats.backend_calls, 32);
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
        let touched_seed = backend_family.seed_points(SpaceId(1), 10_000, 32);
        let warm_backend = backend_family.fork_for_write(&touched_seed);
        commit_direct_write_batches(&warm_backend, touched_batches.clone())
            .expect("warm direct touched backend");
        group.throughput(Throughput::Elements(touched_case.writes as u64));
        group.bench_function(touched_case.name, |b| {
            b.iter_batched(
                || {
                    (
                        backend_family.fork_for_write(&touched_seed),
                        touched_batches.clone(),
                    )
                },
                |(backend, batches)| {
                    let commit = commit_direct_write_batches(&backend, batches)
                        .expect("direct backend touched commit");
                    assert_eq!(commit.stats.put_entries, 128);
                    assert_eq!(commit.stats.deleted_entries, 0);
                    assert_eq!(commit.stats.backend_calls, 16);
                    black_box(commit);
                },
                BatchSize::LargeInput,
            );
        });
    }

    if should_run("direct_get_many_m1000_u100") || should_run("direct_visit_keys_m1000_u100") {
        let point_backend = backend_family.seed_points(SpaceId(1), 100, 32);
        let point_keys = physical_point_request_keys(1, 1_000, 100);
        group.throughput(Throughput::Elements(1_000));
        if should_run("direct_get_many_m1000_u100") {
            group.bench_function("direct_get_many_m1000_u100", |b| {
                b.iter(|| {
                    let read = point_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct point read");
                    let result =
                        backend_get_many(&read, black_box(&point_keys), GetOptions::default())
                            .expect("direct get_many");
                    assert_eq!(result.values.len(), 1_000);
                    assert_eq!(
                        result.values.iter().filter(|value| value.is_some()).count(),
                        1_000
                    );
                    read.close().expect("close direct point read");
                    black_box(result);
                });
            });
        }

        if should_run("direct_visit_keys_m1000_u100") {
            group.bench_function("direct_visit_keys_m1000_u100", |b| {
                b.iter(|| {
                    let read = point_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct point visitor read");
                    let mut visitor = CountingPointVisitor::default();
                    read.visit_keys(black_box(&point_keys), GetOptions::default(), &mut visitor)
                        .expect("direct visit_keys");
                    assert_eq!(visitor.visited, 1_000);
                    read.close().expect("close direct point visitor read");
                    black_box(visitor.visited);
                });
            });
        }
    }

    if should_run("direct_get_many_unique_u100") || should_run("direct_visit_keys_unique_u100") {
        let point_backend = backend_family.seed_points(SpaceId(1), 100, 32);
        let point_keys = physical_point_request_keys(1, 100, 100);
        group.throughput(Throughput::Elements(100));
        if should_run("direct_get_many_unique_u100") {
            group.bench_function("direct_get_many_unique_u100", |b| {
                b.iter(|| {
                    let read = point_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct unique point read");
                    let result =
                        backend_get_many(&read, black_box(&point_keys), GetOptions::default())
                            .expect("direct unique get_many");
                    assert_eq!(result.values.len(), 100);
                    assert_eq!(
                        result.values.iter().filter(|value| value.is_some()).count(),
                        100
                    );
                    read.close().expect("close direct unique point read");
                    black_box(result);
                });
            });
        }

        if should_run("direct_visit_keys_unique_u100") {
            group.bench_function("direct_visit_keys_unique_u100", |b| {
                b.iter(|| {
                    let read = point_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct unique point visitor read");
                    let mut visitor = CountingPointVisitor::default();
                    read.visit_keys(black_box(&point_keys), GetOptions::default(), &mut visitor)
                        .expect("direct unique visit_keys");
                    assert_eq!(visitor.visited, 100);
                    read.close()
                        .expect("close direct unique point visitor read");
                    black_box(visitor.visited);
                });
            });
        }
    }

    if should_run("direct_get_many_unique_u1000") || should_run("direct_visit_keys_unique_u1000") {
        let point_backend = backend_family.seed_points(SpaceId(1), 1_000, 32);
        let point_keys = physical_point_request_keys(1, 1_000, 1_000);
        group.throughput(Throughput::Elements(1_000));
        if should_run("direct_get_many_unique_u1000") {
            group.bench_function("direct_get_many_unique_u1000", |b| {
                b.iter(|| {
                    let read = point_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct unique point read");
                    let result =
                        backend_get_many(&read, black_box(&point_keys), GetOptions::default())
                            .expect("direct unique get_many");
                    assert_eq!(result.values.len(), 1_000);
                    assert_eq!(
                        result.values.iter().filter(|value| value.is_some()).count(),
                        1_000
                    );
                    read.close().expect("close direct unique point read");
                    black_box(result);
                });
            });
        }

        if should_run("direct_visit_keys_unique_u1000") {
            group.bench_function("direct_visit_keys_unique_u1000", |b| {
                b.iter(|| {
                    let read = point_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct unique point visitor read");
                    let mut visitor = CountingPointVisitor::default();
                    read.visit_keys(black_box(&point_keys), GetOptions::default(), &mut visitor)
                        .expect("direct unique visit_keys");
                    assert_eq!(visitor.visited, 1_000);
                    read.close()
                        .expect("close direct unique point visitor read");
                    black_box(visitor.visited);
                });
            });
        }
    }

    if should_run("direct_scan_visit_key_only_q1000")
        || should_run("direct_scan_materialized_q1000")
    {
        let scan_backend = backend_family.seed_points(SpaceId(1), 1_000, 32);
        let scan_range = physical_point_scan_range(1);
        group.throughput(Throughput::Elements(1_000));
        if should_run("direct_scan_visit_key_only_q1000") {
            group.bench_function("direct_scan_visit_key_only_q1000", |b| {
                b.iter(|| {
                    let read = scan_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct scan visitor read");
                    let mut visited = 0usize;
                    let result = backend_visit_range(
                        &read,
                        scan_range.clone(),
                        ScanOptions {
                            limit_rows: 1_001,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                            visited += 1;
                            assert!(matches!(value, ProjectedValueRef::KeyOnly));
                            black_box(key);
                            Ok(())
                        },
                    )
                    .expect("direct scan visitor");
                    assert_eq!(visited, 1_000);
                    assert_eq!(result.emitted, 1_000);
                    assert!(!result.has_more);
                    read.close().expect("close direct scan visitor read");
                    black_box(result);
                });
            });
        }

        if should_run("direct_scan_materialized_q1000") {
            group.bench_function("direct_scan_materialized_q1000", |b| {
                b.iter(|| {
                    let read = scan_backend
                        .begin_read(ReadOptions::default())
                        .expect("begin direct materialized scan read");
                    let chunk = materialize_backend_scan(
                        &read,
                        scan_range.clone(),
                        ScanOptions {
                            limit_rows: 1_001,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                    )
                    .expect("direct materialized scan");
                    assert_eq!(chunk.entries.len(), 1_000);
                    assert!(!chunk.has_more);
                    read.close().expect("close direct materialized scan read");
                    black_box(chunk);
                });
            });
        }
    }

    group.finish();
}

fn bench_in_memory_backend(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/in_memory_backend");

    group.throughput(Throughput::Elements(1_024));
    let commit_case = WriteCase {
        name: "commit_puts_k1024_g16_v32",
        writes: 1_024,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    let commit_mutations = write_mutations(&commit_case);
    group.bench_function("commit_puts_k1024_g16_v32", |b| {
        b.iter_batched(
            || {
                let backend = InMemoryBackend::new();
                let storage = StorageContext::new(backend);
                let mut writes = storage.new_write_set();
                for mutation in &commit_mutations {
                    match mutation {
                        WriteMutation::Put(space, key, value) => {
                            writes.put(*space, key.clone(), value.clone());
                        }
                        WriteMutation::Delete(space, key) => {
                            writes.delete(*space, key.clone());
                        }
                    }
                }
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) = storage
                    .commit_write_set(writes, WriteOptions::default())
                    .expect("commit write set");
                assert_eq!(stats.staged_puts, 1_024);
                assert_eq!(stats.put_batches, 16);
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    let direct_commit_batches = put_batches_by_space(&commit_mutations);
    group.bench_function("direct_commit_puts_k1024_g16_v32", |b| {
        b.iter_batched(
            || (InMemoryBackend::new(), direct_commit_batches.clone()),
            |(backend, batches)| {
                let mut write = backend
                    .begin_write(WriteOptions::default())
                    .expect("begin direct in-memory write");
                for (_space, batch) in batches {
                    write.put_many(batch).expect("put direct batch");
                }
                let commit = write.commit().expect("commit direct write");
                assert_eq!(commit.stats.put_entries, 1_024);
                assert_eq!(commit.stats.backend_calls, 16);
                black_box(commit);
            },
            BatchSize::LargeInput,
        );
    });

    group.throughput(Throughput::Elements(128));
    let untouched_existing_commit_case = WriteCase {
        name: "commit_puts_k128_g16_existing10k_untouched_v32",
        writes: 128,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    let untouched_existing_commit_mutations = write_mutations(&untouched_existing_commit_case);
    let untouched_existing_commit_backend =
        seeded_in_memory_backend_with_value_size(999, 10_000, 32);
    group.bench_function("commit_puts_k128_g16_existing10k_untouched_v32", |b| {
        b.iter_batched(
            || {
                let backend = untouched_existing_commit_backend
                    .fork_snapshot()
                    .expect("fork untouched existing backend");
                let storage = StorageContext::new(backend);
                let mut writes = storage.new_write_set();
                for mutation in &untouched_existing_commit_mutations {
                    match mutation {
                        WriteMutation::Put(space, key, value) => {
                            writes.put(*space, key.clone(), value.clone());
                        }
                        WriteMutation::Delete(space, key) => {
                            writes.delete(*space, key.clone());
                        }
                    }
                }
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) = storage
                    .commit_write_set(writes, WriteOptions::default())
                    .expect("commit write set");
                assert_eq!(stats.staged_puts, 128);
                assert_eq!(stats.put_batches, 16);
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    let touched_existing_commit_case = WriteCase {
        name: "commit_puts_k128_g16_existing10k_touched_v32",
        writes: 128,
        spaces: 16,
        value_size: 32,
        mix: WriteMix::PutsOnly,
    };
    let touched_existing_commit_mutations = write_mutations(&touched_existing_commit_case);
    let touched_existing_commit_backend = seeded_in_memory_backend_with_value_size(1, 10_000, 32);
    group.bench_function("commit_puts_k128_g16_existing10k_touched_v32", |b| {
        b.iter_batched(
            || {
                let backend = touched_existing_commit_backend
                    .fork_snapshot()
                    .expect("fork touched existing backend");
                let storage = StorageContext::new(backend);
                let mut writes = storage.new_write_set();
                for mutation in &touched_existing_commit_mutations {
                    match mutation {
                        WriteMutation::Put(space, key, value) => {
                            writes.put(*space, key.clone(), value.clone());
                        }
                        WriteMutation::Delete(space, key) => {
                            writes.delete(*space, key.clone());
                        }
                    }
                }
                (storage, writes)
            },
            |(storage, writes)| {
                let (_commit, stats) = storage
                    .commit_write_set(writes, WriteOptions::default())
                    .expect("commit write set");
                assert_eq!(stats.staged_puts, 128);
                assert_eq!(stats.put_batches, 16);
                black_box(stats);
            },
            BatchSize::LargeInput,
        );
    });

    let direct_touched_existing_batches = put_batches_by_space(&touched_existing_commit_mutations);
    group.bench_function("direct_commit_puts_k128_g16_existing10k_touched_v32", |b| {
        b.iter_batched(
            || {
                (
                    touched_existing_commit_backend
                        .fork_snapshot()
                        .expect("fork touched direct existing backend"),
                    direct_touched_existing_batches.clone(),
                )
            },
            |(backend, batches)| {
                let mut write = backend
                    .begin_write(WriteOptions::default())
                    .expect("begin direct touched write");
                for (_space, batch) in batches {
                    write.put_many(batch).expect("put direct touched batch");
                }
                let commit = write.commit().expect("commit direct touched write");
                assert_eq!(commit.stats.put_entries, 128);
                assert_eq!(commit.stats.backend_calls, 16);
                black_box(commit);
            },
            BatchSize::LargeInput,
        );
    });

    for depth in [0_u32, 1, 8, 32] {
        let layered_backend = layered_in_memory_backend(1, 1_000, depth, 8);
        let layered_read = layered_backend
            .begin_read(ReadOptions::default())
            .expect("begin layered read");
        let layered_scope = StorageReadScope::new(layered_read);
        let layered_keys = point_request_keys(1_000, 100);
        let layered_plan = PointReadPlan::new(space(1), &layered_keys);
        group.bench_function(
            format!("overlay_depth_visit_base_d{depth}_m1000_u100"),
            |b| {
                b.iter(|| {
                    let mut visitor = CountingPointVisitor::default();
                    let stats = black_box(&layered_plan)
                        .visit(&layered_scope, GetOptions::default(), &mut visitor)
                        .expect("visit layered point values");
                    assert_eq!(stats.unique_backend_keys, 100);
                    assert_eq!(visitor.visited, 100);
                    black_box(stats);
                });
            },
        );

        group.bench_function(format!("overlay_depth_scan_base_q1000_d{depth}"), |b| {
            b.iter(|| {
                let mut visitor = |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                    assert!(matches!(value, ProjectedValueRef::KeyOnly));
                    black_box(key);
                    Ok(())
                };
                let result = ScanPlan::range(space(1), point_scan_range())
                    .visit(
                        &layered_scope,
                        ScanOptions {
                            limit_rows: 1_001,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                        &mut visitor,
                    )
                    .expect("scan layered base range");
                assert_eq!(result.value.emitted, 1_000);
                black_box(result);
            });
        });
    }

    group.throughput(Throughput::Elements(1_000));
    let get_many_backend = seeded_in_memory_backend(1, 100);
    let get_many_read = get_many_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let get_many_read = StorageReadScope::new(get_many_read);
    let get_many_keys = point_request_keys(1_000, 100);
    group.bench_function("get_many_m1000_u100", |b| {
        b.iter(|| {
            let result = PointReadPlan::new(space(1), black_box(&get_many_keys))
                .materialize(&get_many_read, GetOptions::default())
                .expect("point read");
            assert_eq!(result.stats.requested_keys, 1_000);
            assert_eq!(result.stats.unique_backend_keys, 100);
            assert_eq!(result.stats.backend_calls, 1);
            assert_eq!(result.value.len(), 1_000);
            black_box(result.value);
        });
    });

    group.throughput(Throughput::Elements(1_000));
    let planned_get_many_backend = seeded_in_memory_backend(1, 100);
    let planned_get_many_read = planned_get_many_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let planned_get_many_read = StorageReadScope::new(planned_get_many_read);
    let planned_get_many_keys = point_request_keys(1_000, 100);
    let planned_get_many_plan = PointReadPlan::new(space(1), &planned_get_many_keys);
    group.bench_function("planned_get_many_m1000_u100", |b| {
        b.iter(|| {
            let result = black_box(&planned_get_many_plan)
                .collect(&planned_get_many_read, GetOptions::default())
                .expect("planned point read");
            assert_eq!(result.stats.requested_keys, 1_000);
            assert_eq!(result.stats.unique_backend_keys, 100);
            assert_eq!(result.stats.backend_calls, 1);
            assert_eq!(result.value.len(), 1_000);
            assert_eq!(result.value.unique_values.len(), 100);
            black_box(result.value);
        });
    });

    let mut planned_get_many_buffer = PointReadBuffer::new();
    group.bench_function("planned_get_many_buffered_m1000_u100", |b| {
        b.iter(|| {
            let result = black_box(&planned_get_many_plan)
                .collect_into(
                    &planned_get_many_read,
                    GetOptions::default(),
                    &mut planned_get_many_buffer,
                )
                .expect("buffered planned point read");
            assert_eq!(result.stats.requested_keys, 1_000);
            assert_eq!(result.stats.unique_backend_keys, 100);
            assert_eq!(result.stats.backend_calls, 1);
            assert_eq!(result.value.len(), 1_000);
            assert_eq!(result.value.unique_values.len(), 100);
            black_box(result.value);
        });
    });

    group.bench_function("planned_visit_unique_m1000_u100", |b| {
        b.iter(|| {
            let mut visited = 0usize;
            let mut bytes_seen = 0usize;
            let stats = black_box(&planned_get_many_plan)
                .visit(
                    &planned_get_many_read,
                    GetOptions::default(),
                    &mut |index: usize, key: &Key, value: Option<ProjectedValueRef<'_>>| {
                        visited += 1;
                        if let Some(ProjectedValueRef::FullValue(value)) = value {
                            bytes_seen += value.len();
                        }
                        black_box((index, key, value));
                        Ok(())
                    },
                )
                .expect("planned point visitor");
            assert_eq!(stats.requested_keys, 1_000);
            assert_eq!(stats.unique_backend_keys, 100);
            assert_eq!(stats.backend_calls, 1);
            assert_eq!(visited, 100);
            assert_eq!(bytes_seen, 3_200);
            black_box(stats);
        });
    });

    group.throughput(Throughput::Elements(1_000));
    let scan_backend = seeded_in_memory_backend(1, 1_000);
    let scan_read = scan_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let scan_range = physical_point_scan_range(1);
    group.bench_function("scan_range_q1000", |b| {
        b.iter(|| {
            let chunk = materialize_backend_scan(
                &scan_read,
                scan_range.clone(),
                ScanOptions {
                    limit_rows: 1_001,
                    projection: CoreProjection::KeyOnly,
                    ..ScanOptions::default()
                },
            )
            .expect("scan range");
            assert_eq!(chunk.entries.len(), 1_000);
            black_box(chunk);
        });
    });

    group.throughput(Throughput::Elements(1_000));
    let scan_visit_backend = seeded_in_memory_backend(1, 1_000);
    let scan_visit_read = scan_visit_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let scan_visit_range = physical_point_scan_range(1);
    group.bench_function("scan_range_visit_key_only_q1000", |b| {
        b.iter(|| {
            let mut visited = 0usize;
            let result = scan_visit_read
                .visit_scan_range(
                    scan_visit_range.clone(),
                    ScanOptions {
                        limit_rows: 1_001,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                    |key, value| {
                        visited += 1;
                        assert!(value.is_none());
                        black_box(key);
                    },
                )
                .expect("visit scan range");
            assert_eq!(visited, 1_000);
            assert_eq!(result.emitted, 1_000);
            assert!(!result.has_more);
            black_box(result);
        });
    });

    group.finish();
}

fn bench_scan_visitor_baseline(c: &mut Criterion) {
    let mut group = storage_benchmark_group(c, "storage_v2/scan_visitor_baseline");
    if std::env::var_os("STORAGE_V2_BENCH_SMOKE").is_none() {
        group.warm_up_time(Duration::from_millis(500));
        group.measurement_time(Duration::from_secs(1));
    }

    for rows in [0usize, 1, 10, 100, 1_000, 10_000] {
        let backend = seeded_in_memory_backend_with_value_size(1, rows as u32, 32);
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let scan_range = physical_point_scan_range(1);
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(format!("owned_key_only_q{rows}"), |b| {
            b.iter(|| {
                let chunk = materialize_backend_scan(
                    &read,
                    scan_range.clone(),
                    ScanOptions {
                        limit_rows: rows + 1,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                )
                .expect("scan range");
                assert_eq!(chunk.entries.len(), rows);
                black_box(chunk);
            });
        });

        group.bench_function(format!("visit_key_only_q{rows}"), |b| {
            b.iter(|| {
                let mut visited = 0usize;
                let result = read
                    .visit_scan_range(
                        scan_range.clone(),
                        ScanOptions {
                            limit_rows: rows + 1,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                        |key, value| {
                            visited += 1;
                            assert!(value.is_none());
                            black_box(key);
                        },
                    )
                    .expect("visit scan range");
                assert_eq!(visited, rows);
                assert_eq!(result.emitted, rows);
                assert!(!result.has_more);
                black_box(result);
            });
        });
    }

    for value_size in [32usize, 1_024, 65_536] {
        let backend = seeded_in_memory_backend_with_value_size(1, 1_000, value_size);
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let scan_range = physical_point_scan_range(1);
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("owned_full_value_q1000_v{value_size}"), |b| {
            b.iter(|| {
                let chunk = materialize_backend_scan(
                    &read,
                    scan_range.clone(),
                    ScanOptions {
                        limit_rows: 1_001,
                        projection: CoreProjection::FullValue,
                        ..ScanOptions::default()
                    },
                )
                .expect("scan range");
                assert_eq!(chunk.entries.len(), 1_000);
                black_box(chunk);
            });
        });

        group.bench_function(format!("visit_full_value_q1000_v{value_size}"), |b| {
            b.iter(|| {
                let mut visited = 0usize;
                let mut bytes_seen = 0usize;
                let result = read
                    .visit_scan_range(
                        scan_range.clone(),
                        ScanOptions {
                            limit_rows: 1_001,
                            projection: CoreProjection::FullValue,
                            ..ScanOptions::default()
                        },
                        |key, value| {
                            visited += 1;
                            let value = value.expect("full value");
                            bytes_seen += value.len();
                            black_box((key, value));
                        },
                    )
                    .expect("visit scan range");
                assert_eq!(visited, 1_000);
                assert_eq!(bytes_seen, value_size * 1_000);
                assert_eq!(result.emitted, 1_000);
                assert!(!result.has_more);
                black_box(result);
            });
        });
    }

    let materialize_backend = seeded_in_memory_backend_with_value_size(1, 1_000, 32);
    let materialize_read = materialize_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let storage_materialize_read = StorageReadScope::new(
        materialize_backend
            .begin_read(ReadOptions::default())
            .expect("begin read"),
    );
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("visit_materialize_key_only_q1000", |b| {
        b.iter(|| {
            let chunk =
                materialize_scan_visit(&materialize_read, CoreProjection::KeyOnly, 1_001, None)
                    .expect("materialize visitor scan");
            assert_eq!(chunk.entries.len(), 1_000);
            black_box(chunk);
        });
    });

    group.bench_function("storage_buffer_key_only_q1000", |b| {
        let mut buffer = ScanBuffer::with_capacity(1_001);
        b.iter(|| {
            let chunk = ScanPlan::range(space(1), point_scan_range())
                .collect_into(
                    &storage_materialize_read,
                    ScanOptions {
                        projection: CoreProjection::KeyOnly,
                        limit_rows: 1_001,
                        ..ScanOptions::default()
                    },
                    &mut buffer,
                )
                .expect("storage scan buffer");
            assert_eq!(chunk.value.entries.len(), 1_000);
            black_box(chunk.value.entries);
            black_box(chunk.value.has_more);
        });
    });

    group.bench_function("storage_visit_key_only_q1000", |b| {
        b.iter(|| {
            let mut visited = 0usize;
            let result = ScanPlan::range(space(1), point_scan_range())
                .visit(
                    &storage_materialize_read,
                    ScanOptions {
                        projection: CoreProjection::KeyOnly,
                        limit_rows: 1_001,
                        ..ScanOptions::default()
                    },
                    &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                        visited += 1;
                        assert_eq!(value, ProjectedValueRef::KeyOnly);
                        black_box(key);
                        Ok(())
                    },
                )
                .expect("storage visit scan");
            assert_eq!(visited, 1_000);
            assert_eq!(result.value.emitted, 1_000);
            black_box(result);
        });
    });

    group.bench_function("visit_materialize_full_value_q1000_v32", |b| {
        b.iter(|| {
            let chunk =
                materialize_scan_visit(&materialize_read, CoreProjection::FullValue, 1_001, None)
                    .expect("materialize visitor scan");
            assert_eq!(chunk.entries.len(), 1_000);
            black_box(chunk);
        });
    });

    group.bench_function("storage_visit_full_value_q1000_v32", |b| {
        b.iter(|| {
            let mut visited = 0usize;
            let mut bytes_seen = 0usize;
            let result = ScanPlan::range(space(1), point_scan_range())
                .visit(
                    &storage_materialize_read,
                    ScanOptions {
                        projection: CoreProjection::FullValue,
                        limit_rows: 1_001,
                        ..ScanOptions::default()
                    },
                    &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                        visited += 1;
                        let ProjectedValueRef::FullValue(value) = value else {
                            panic!("expected full value");
                        };
                        bytes_seen += value.len();
                        black_box((key, value));
                        Ok(())
                    },
                )
                .expect("storage visit scan");
            assert_eq!(visited, 1_000);
            assert_eq!(bytes_seen, 32_000);
            assert_eq!(result.value.emitted, 1_000);
            black_box(result);
        });
    });

    group.bench_function("storage_buffer_full_value_q1000_v32", |b| {
        let mut buffer = ScanBuffer::with_capacity(1_001);
        b.iter(|| {
            let chunk = ScanPlan::range(space(1), point_scan_range())
                .collect_into(
                    &storage_materialize_read,
                    ScanOptions {
                        projection: CoreProjection::FullValue,
                        limit_rows: 1_001,
                        ..ScanOptions::default()
                    },
                    &mut buffer,
                )
                .expect("storage scan buffer");
            assert_eq!(chunk.value.entries.len(), 1_000);
            black_box(chunk.value.entries);
            black_box(chunk.value.has_more);
        });
    });

    for limit_rows in [10usize, 100, 1_000] {
        let backend = seeded_in_memory_backend_with_value_size(1, 1_000, 32);
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let scan_range = physical_point_scan_range(1);
        group.throughput(Throughput::Elements(limit_rows as u64));
        group.bench_function(format!("owned_key_only_q1000_limit{limit_rows}"), |b| {
            b.iter(|| {
                let chunk = materialize_backend_scan(
                    &read,
                    scan_range.clone(),
                    ScanOptions {
                        limit_rows,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                )
                .expect("scan range");
                assert_eq!(chunk.entries.len(), limit_rows);
                assert_eq!(chunk.has_more, limit_rows < 1_000);
                black_box(chunk);
            });
        });

        group.bench_function(format!("visit_key_only_q1000_limit{limit_rows}"), |b| {
            b.iter(|| {
                let mut visited = 0usize;
                let result = read
                    .visit_scan_range(
                        scan_range.clone(),
                        ScanOptions {
                            limit_rows,
                            projection: CoreProjection::KeyOnly,
                            ..ScanOptions::default()
                        },
                        |key, value| {
                            visited += 1;
                            assert!(value.is_none());
                            black_box(key);
                        },
                    )
                    .expect("visit scan range");
                assert_eq!(visited, limit_rows);
                assert_eq!(result.emitted, limit_rows);
                assert_eq!(result.has_more, limit_rows < 1_000);
                black_box(result);
            });
        });
    }

    for chunk_size in [10usize, 100] {
        let backend = seeded_in_memory_backend_with_value_size(1, 1_000, 32);
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        let scan_range = physical_point_scan_range(1);
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(
            format!("owned_drain_key_only_q1000_chunk{chunk_size}"),
            |b| {
                b.iter(|| {
                    let mut emitted = 0usize;
                    let mut resume_after = None;
                    loop {
                        let chunk = materialize_backend_scan(
                            &read,
                            scan_range.clone(),
                            ScanOptions {
                                limit_rows: chunk_size,
                                projection: CoreProjection::KeyOnly,
                                resume_after: resume_after.as_ref(),
                            },
                        )
                        .expect("scan range");
                        emitted += chunk.entries.len();
                        resume_after = chunk.entries.last().map(|entry| entry.key.clone());
                        if !chunk.has_more {
                            break;
                        }
                    }
                    assert_eq!(emitted, 1_000);
                    black_box(resume_after);
                });
            },
        );

        group.bench_function(
            format!("visit_drain_key_only_q1000_chunk{chunk_size}"),
            |b| {
                b.iter(|| {
                    let mut emitted = 0usize;
                    let mut resume_after = None;
                    loop {
                        let mut chunk_last_key = None;
                        let result = read
                            .visit_scan_range(
                                scan_range.clone(),
                                ScanOptions {
                                    limit_rows: chunk_size,
                                    projection: CoreProjection::KeyOnly,
                                    resume_after: resume_after.as_ref(),
                                },
                                |key, value| {
                                    assert!(value.is_none());
                                    chunk_last_key = Some(key.to_owned_key());
                                    black_box(key);
                                },
                            )
                            .expect("visit scan range");
                        emitted += result.emitted;
                        resume_after = chunk_last_key;
                        if !result.has_more {
                            break;
                        }
                    }
                    assert_eq!(emitted, 1_000);
                    black_box(resume_after);
                });
            },
        );
    }

    group.finish();
}

#[derive(Clone, Default)]
struct CountingBackend {
    state: Rc<CountingState>,
}

#[derive(Default)]
struct CountingState {
    commit_calls: Cell<u64>,
    put_many_calls: Cell<u64>,
    delete_many_calls: Cell<u64>,
}

struct CountingWrite {
    state: Rc<CountingState>,
}

impl Backend for CountingBackend {
    type Read<'a>
        = EmptyRead
    where
        Self: 'a;

    type Write<'a>
        = CountingWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(EmptyRead)
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(CountingWrite {
            state: Rc::clone(&self.state),
        })
    }
}

impl BackendWrite for CountingWrite {
    fn put_many(&mut self, _entries: PutBatch) -> Result<(), BackendError> {
        self.state
            .put_many_calls
            .set(self.state.put_many_calls.get() + 1);
        Ok(())
    }

    fn delete_many(&mut self, _keys: &[Key]) -> Result<(), BackendError> {
        self.state
            .delete_many_calls
            .set(self.state.delete_many_calls.get() + 1);
        Ok(())
    }

    fn delete_range(&mut self, _range: KeyRange) -> Result<(), BackendError> {
        self.state
            .delete_many_calls
            .set(self.state.delete_many_calls.get() + 1);
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        self.state
            .commit_calls
            .set(self.state.commit_calls.get() + 1);
        Ok(CommitResult {
            commit_id: None,
            stats: WriteStats::default(),
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

#[derive(Clone)]
struct PointReadBackend {
    values: Rc<Vec<ReadEntry>>,
    requested_keys: Rc<RefCell<Vec<Key>>>,
}

impl PointReadBackend {
    fn new(existing_unique_keys: usize) -> Self {
        let values = (0..existing_unique_keys)
            .map(|index| {
                let key = key(format!("point-{index:04}"));
                ReadEntry {
                    key: key.clone(),
                    value: ProjectedValue::FullValue(key.0.clone()),
                }
            })
            .collect();
        Self {
            values: Rc::new(values),
            requested_keys: Rc::new(RefCell::new(Vec::new())),
        }
    }
}

impl BackendRead for PointReadBackend {
    type RangeScan<'a> = BufferedRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        _opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        self.requested_keys.replace(keys.to_vec());
        for (index, key) in keys.iter().enumerate() {
            let value = self
                .values
                .iter()
                .find(|entry| entry.key == *key)
                .map(|entry| entry.value.as_ref());
            visitor.visit(index, key, value)?;
        }
        Ok(())
    }

    fn with_range_scan<T, F>(
        &self,
        _range: KeyRange,
        _opts: ScanOptions<'_>,
        _f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        unreachable!("point-read benchmark does not scan")
    }
}

#[derive(Clone)]
struct LeanPointReadBackend {
    values: Rc<Vec<ReadEntry>>,
}

impl LeanPointReadBackend {
    fn new(existing_unique_keys: usize) -> Self {
        let values = (0..existing_unique_keys)
            .map(|index| {
                let key = key(format!("point-{index:04}"));
                ReadEntry {
                    key: key.clone(),
                    value: ProjectedValue::FullValue(key.0.clone()),
                }
            })
            .collect();
        Self {
            values: Rc::new(values),
        }
    }
}

impl BackendRead for LeanPointReadBackend {
    type RangeScan<'a> = BufferedRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        _opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        let found = keys.len().min(self.values.len());
        for (index, key) in keys.iter().take(found).enumerate() {
            let value = Some(self.values[index].value.as_ref());
            visitor.visit(index, key, value)?;
        }
        for (index, key) in keys.iter().enumerate().skip(found) {
            visitor.visit(index, key, None)?;
        }
        Ok(())
    }

    fn with_range_scan<T, F>(
        &self,
        _range: KeyRange,
        _opts: ScanOptions<'_>,
        _f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        unreachable!("lean point-read benchmark does not scan")
    }
}

#[derive(Clone)]
struct PrefixReadBackend {
    entries: Rc<Vec<ReadEntry>>,
}

impl PrefixReadBackend {
    fn new(rows: usize) -> Self {
        let entries = (0..rows)
            .map(|index| {
                let key = key(format!("row-{index:04}"));
                ReadEntry {
                    key,
                    value: ProjectedValue::KeyOnly,
                }
            })
            .collect();
        Self {
            entries: Rc::new(entries),
        }
    }
}

impl BackendRead for PrefixReadBackend {
    type RangeScan<'a> = BufferedRangeScan;

    fn visit_keys<V>(
        &self,
        _keys: &[Key],
        _opts: GetOptions<'_>,
        _visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        unreachable!("prefix-scan benchmark does not point-read")
    }

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        assert_eq!(range.lower, Bound::Included(key("row-")));
        assert_eq!(range.upper, Bound::Excluded(key("row.")));
        if opts.limit_rows == 0 {
            let mut cursor = BufferedRangeScan::default();
            return f(&mut cursor);
        }
        let mut cursor = BufferedRangeScan::new((*self.entries).clone());
        f(&mut cursor)
    }
}

#[derive(Clone, Copy)]
struct EmptyRead;

impl BackendRead for EmptyRead {
    type RangeScan<'a> = BufferedRangeScan;

    fn visit_keys<V>(
        &self,
        _keys: &[Key],
        _opts: GetOptions<'_>,
        _visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        unreachable!("write-set benchmark does not point-read")
    }

    fn with_range_scan<T, F>(
        &self,
        _range: KeyRange,
        _opts: ScanOptions<'_>,
        _f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        unreachable!("write-set benchmark does not scan")
    }
}

impl WriteCase {
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

impl PointCase {
    fn requested_missing_slots(&self) -> usize {
        point_request_keys(self.requested_keys, self.unique_keys)
            .iter()
            .filter(|key| point_key_index(key) >= self.existing_unique_keys)
            .count()
    }
}

fn seeded_in_memory_backend(space_id: u32, rows: u32) -> InMemoryBackend {
    seeded_in_memory_backend_with_value_size(space_id, rows, 32)
}

fn seeded_in_memory_backend_with_value_size(
    space_id: u32,
    rows: u32,
    value_size: usize,
) -> InMemoryBackend {
    let backend = InMemoryBackend::new();
    seed_backend_points(
        &backend,
        SpaceId(space_id),
        rows,
        value_size,
        "in-memory backend",
    );
    backend
}

fn seed_backend_points<B>(
    backend: &B,
    space_id: SpaceId,
    rows: u32,
    value_size: usize,
    backend_name: &str,
) where
    B: Backend + Clone,
{
    let storage = StorageContext::new(backend.clone());
    let mut writes = StorageWriteSet::with_capacity(rows as usize, 1);
    for index in 0..rows {
        writes.put(
            space(space_id.0),
            key(format!("point-{index:04}")),
            value(index, value_size),
        );
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, WriteOptions::default())
        .unwrap_or_else(|error| panic!("seed {backend_name}: {error}"));
    assert_eq!(stats.staged_puts, rows as u64);
}

fn layered_in_memory_backend(
    space_id: u32,
    base_rows: u32,
    overlay_depth: u32,
    rows_per_layer: u32,
) -> InMemoryBackend {
    let backend = seeded_in_memory_backend_with_value_size(space_id, base_rows, 32);
    for layer in 0..overlay_depth {
        let entries = (0..rows_per_layer)
            .map(|index| PutEntry {
                key: space(space_id).encode_key(&key(format!("zz-layer-{layer:04}-{index:04}"))),
                value: value(layer * rows_per_layer + index, 32),
            })
            .collect();
        let mut write = backend
            .begin_write(WriteOptions::default())
            .expect("begin overlay layer write");
        write
            .put_many(PutBatch { entries })
            .expect("write overlay layer");
        let commit = write.commit().expect("commit overlay layer");
        assert_eq!(commit.stats.put_entries, rows_per_layer as u64);
    }
    backend
}

fn put_batches_by_space(mutations: &[WriteMutation]) -> Vec<(StorageSpace, PutBatch)> {
    let mut batches = BTreeMap::<StorageSpace, Vec<PutEntry>>::new();
    for mutation in mutations {
        if let WriteMutation::Put(space, key, value) = mutation {
            batches.entry(*space).or_default().push(PutEntry {
                key: space.encode_key(key),
                value: value.clone(),
            });
        }
    }
    batches
        .into_iter()
        .map(|(space, entries)| (space, PutBatch { entries }))
        .collect()
}

fn direct_write_batches_from_mutations(mutations: &[WriteMutation]) -> DirectWriteBatches {
    let mut puts = BTreeMap::<StorageSpace, Vec<PutEntry>>::new();
    let mut deletes = BTreeMap::<StorageSpace, Vec<Key>>::new();
    for mutation in mutations {
        match mutation {
            WriteMutation::Put(space, key, value) => {
                puts.entry(*space).or_default().push(PutEntry {
                    key: space.encode_key(key),
                    value: value.clone(),
                });
            }
            WriteMutation::Delete(space, key) => {
                deletes
                    .entry(*space)
                    .or_default()
                    .push(space.encode_key(key));
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

fn commit_direct_write_batches<B>(
    backend: &B,
    batches: DirectWriteBatches,
) -> Result<CommitResult, BackendError>
where
    B: Backend,
{
    let mut write = backend.begin_write(WriteOptions::default())?;
    for (_space, batch) in batches.puts {
        write.put_many(batch)?;
    }
    for (_space, keys) in batches.deletes {
        write.delete_many(&keys)?;
    }
    write.commit()
}

fn fallback_delete_range<B>(
    storage: &StorageContext<B>,
    storage_space: StorageSpace,
    range: KeyRange,
    chunk_size: usize,
) -> Result<DeleteRangeFallbackStats, String>
where
    B: Backend,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .map_err(|error| error.to_string())?;
    let mut keys = Vec::new();
    let mut resume_after = None::<Key>;
    let mut scanned = 0usize;
    let mut chunks = 0usize;

    loop {
        let mut chunk_last_key = None::<Key>;
        let result = ScanPlan::range(storage_space, range.clone())
            .visit(
                &read,
                ScanOptions {
                    limit_rows: chunk_size,
                    projection: CoreProjection::KeyOnly,
                    resume_after: resume_after.as_ref(),
                },
                &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                    assert!(matches!(value, ProjectedValueRef::KeyOnly));
                    let key = key.to_owned_key();
                    chunk_last_key = Some(key.clone());
                    keys.push(key);
                    Ok(())
                },
            )
            .map_err(|error| error.to_string())?;

        scanned += result.value.emitted;
        chunks += usize::from(result.value.emitted > 0 || result.value.has_more);
        resume_after = chunk_last_key;

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
        .map_err(|error| error.to_string())?;
    Ok(DeleteRangeFallbackStats {
        scanned,
        deleted: write_stats.staged_deletes as usize,
        chunks,
        write_stats,
    })
}

fn drain_scan_materialized<R>(
    read: &StorageReadScope<R>,
    storage_space: StorageSpace,
    scan: ScanChunkingMode,
    expected_rows: usize,
    chunk_size: usize,
) -> Result<ScanDrainStats, BackendError>
where
    R: BackendRead,
{
    let mut resume_after = None::<Key>;
    let mut stats = ScanDrainStats::default();

    loop {
        let opts = ScanOptions {
            limit_rows: chunk_size,
            projection: CoreProjection::KeyOnly,
            resume_after: resume_after.as_ref(),
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
        let chunk = plan.collect(read, opts)?;

        let entries = &chunk.value.entries;
        stats.scanned += entries.len();
        stats.backend_calls += chunk.stats.backend_calls;
        stats.chunks += usize::from(!entries.is_empty() || chunk.value.has_more);
        stats.read_stats.add(chunk.stats);
        resume_after = entries.last().map(|entry| entry.key.clone());

        if !chunk.value.has_more {
            break;
        }
    }

    assert_eq!(
        stats.backend_calls,
        expected_rows.div_ceil(chunk_size) as u64
    );
    Ok(stats)
}

fn drain_scan_visit<R>(
    read: &StorageReadScope<R>,
    storage_space: StorageSpace,
    scan: ScanChunkingMode,
    expected_rows: usize,
    chunk_size: usize,
) -> Result<ScanDrainStats, BackendError>
where
    R: BackendRead,
{
    let mut resume_after = None::<Key>;
    let mut stats = ScanDrainStats::default();

    loop {
        let mut chunk_last_key = None::<Key>;
        let opts = ScanOptions {
            limit_rows: chunk_size,
            projection: CoreProjection::KeyOnly,
            resume_after: resume_after.as_ref(),
        };
        let mut visitor = |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            assert!(matches!(value, ProjectedValueRef::KeyOnly));
            chunk_last_key = Some(key.to_owned_key());
            Ok(())
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
        let result = plan.visit(read, opts, &mut visitor)?;

        stats.scanned += result.value.emitted;
        stats.backend_calls += result.stats.backend_calls;
        stats.chunks += usize::from(result.value.emitted > 0 || result.value.has_more);
        stats.read_stats.add(result.stats);
        resume_after = chunk_last_key;

        if !result.value.has_more {
            break;
        }
    }

    assert_eq!(
        stats.backend_calls,
        expected_rows.div_ceil(chunk_size) as u64
    );
    Ok(stats)
}

fn drain_scan_cursor_visit<R>(
    read: &StorageReadScope<R>,
    storage_space: StorageSpace,
    scan: ScanChunkingMode,
    expected_rows: usize,
    chunk_size: usize,
) -> Result<ScanDrainStats, BackendError>
where
    R: BackendRead,
{
    let opts = ScanOptions {
        limit_rows: chunk_size,
        projection: CoreProjection::KeyOnly,
        resume_after: None,
    };
    let mut stats = ScanDrainStats::default();

    match scan {
        ScanChunkingMode::Range => {
            ScanPlan::range(storage_space, point_scan_range()).cursor(read, opts, |cursor| {
                drain_storage_cursor(cursor, chunk_size, &mut stats)
            })?
        }
        ScanChunkingMode::Prefix => ScanPlan::prefix(
            storage_space,
            Prefix {
                bytes: Bytes::from_static(b"point-"),
            },
        )
        .cursor(read, opts, |cursor| {
            drain_storage_cursor(cursor, chunk_size, &mut stats)
        })?,
    }

    assert_eq!(
        stats.backend_calls,
        expected_rows.div_ceil(chunk_size) as u64
    );
    Ok(stats)
}

fn drain_storage_cursor<C>(
    cursor: &mut lix_engine::storage_v2::ScanCursor<'_, C>,
    chunk_size: usize,
    stats: &mut ScanDrainStats,
) -> Result<(), BackendError>
where
    C: BackendRangeScan,
{
    loop {
        let result = cursor.visit_next_with_stats(
            chunk_size,
            &mut |_key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
                assert!(matches!(value, ProjectedValueRef::KeyOnly));
                Ok(())
            },
        )?;

        stats.scanned += result.value.emitted;
        stats.backend_calls += result.stats.backend_calls;
        stats.chunks += usize::from(result.value.emitted > 0 || result.value.has_more);
        stats.read_stats.add(result.stats);

        if !result.value.has_more {
            break;
        }
    }
    Ok(())
}

fn assert_scan_drain_stats(stats: &ScanDrainStats, case: &ScanChunkingCase) {
    let expected_chunks = case.rows.div_ceil(case.chunk_size);
    let expected_resume_after = expected_chunks.saturating_sub(1) as u64;
    let expected_has_more = expected_chunks.saturating_sub(1) as u64;

    assert_eq!(stats.read_stats.backend_calls, expected_chunks as u64);
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

fn assert_cursor_scan_drain_stats(stats: &ScanDrainStats, case: &ScanChunkingCase) {
    let expected_chunks = case.rows.div_ceil(case.chunk_size);
    let expected_resume_after = expected_chunks.saturating_sub(1) as u64;
    let expected_has_more = expected_chunks.saturating_sub(1) as u64;

    assert_eq!(stats.read_stats.backend_calls, expected_chunks as u64);
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
            assert_eq!(stats.read_stats.prefix_lowered, 1);
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

fn materialize_backend_scan<R>(
    read: &R,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<ScanChunk, BackendError>
where
    R: BackendRead,
{
    let mut entries = Vec::with_capacity(opts.limit_rows);
    let result = backend_visit_range(
        read,
        range,
        opts,
        &mut |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            entries.push(ReadEntry {
                key: key.to_owned_key(),
                value: value.to_owned(),
            });
            Ok(())
        },
    )?;
    Ok(ScanChunk {
        entries,
        has_more: result.has_more,
    })
}

fn materialize_scan_visit(
    read: &lix_engine::backend_v2::InMemoryRead,
    projection: CoreProjection,
    limit_rows: usize,
    resume_after: Option<&Key>,
) -> Result<ScanChunk, BackendError> {
    let mut entries = Vec::with_capacity(limit_rows);
    let result = read.visit_scan_range(
        physical_point_scan_range(1),
        ScanOptions {
            projection,
            limit_rows,
            resume_after,
        },
        |key, value| {
            let value = match value {
                None => ProjectedValue::KeyOnly,
                Some(value) => ProjectedValue::FullValue(Bytes::copy_from_slice(value)),
            };
            entries.push(ReadEntry {
                key: key.to_owned_key(),
                value,
            });
        },
    )?;
    Ok(ScanChunk {
        entries,
        has_more: result.has_more,
    })
}

fn write_set_from_mutations<B>(
    storage: &StorageContext<B>,
    mutations: &[WriteMutation],
) -> StorageWriteSet
where
    B: Backend,
{
    let _ = storage;
    canonical_write_set_from_mutations(mutations)
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

fn point_request_keys(requested_keys: usize, unique_keys: usize) -> Vec<Key> {
    (0..requested_keys)
        .map(|index| key(format!("point-{:04}", index % unique_keys)))
        .collect()
}

fn physical_point_request_keys(
    space_id: u32,
    requested_keys: usize,
    unique_keys: usize,
) -> Vec<Key> {
    let storage_space = space(space_id);
    point_request_keys(requested_keys, unique_keys)
        .into_iter()
        .map(|key| storage_space.encode_key(&key))
        .collect()
}

fn physical_point_scan_range(space_id: u32) -> KeyRange {
    let storage_space = space(space_id);
    storage_space.encode_range(point_scan_range(), None)
}

fn point_key_index(key: &Key) -> usize {
    std::str::from_utf8(&key.0)
        .expect("bench point keys are utf8")
        .strip_prefix("point-")
        .expect("bench point key prefix")
        .parse()
        .expect("bench point key index")
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
