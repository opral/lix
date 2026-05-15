use std::cell::{Cell, RefCell};
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::ops::Bound;
use std::rc::Rc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use lix_engine::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    ConformanceBackend, CoreProjection, GetManyResult, GetOptions, InMemoryBackend, Key, KeyRange,
    Prefix, ProjectedValue, ProjectedValueRef, PutBatch, ReadBatch, ReadEntry, ReadOptions,
    ScanOptions, ScanPage, ScanResult, ScanVisitor, SpaceId, StoredValue, WriteConcurrency,
    WriteOptions, WriteStats,
};
use lix_engine::storage_v2::{
    PointRequestPlan, StorageContext, StorageReadScope, StorageReader, StorageSpace,
};
use rustc_hash::FxBuildHasher;
use xxhash_rust::xxh3::Xxh3DefaultBuilder;

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

fn storage_v2_benches(c: &mut Criterion) {
    bench_write_set_lowering(c);
    bench_point_request_plan(c);
    bench_point_read_adapter(c);
    bench_point_read_indexed_adapter(c);
    bench_point_read_indexed_lean_backend(c);
    bench_point_read_planned_lean_backend(c);
    bench_prefix_scan_adapter(c);
    bench_conformance_backend(c);
    bench_in_memory_backend(c);
    bench_scan_visitor_baseline(c);
    bench_hash_algorithms(c);
}

fn bench_point_request_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_v2/point_request_plan");
    group.sample_size(10);

    for case in POINT_CASES {
        if case.requested_keys != case.unique_keys {
            continue;
        }
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::new("dedupe", case.name), case, |b, _case| {
            b.iter(|| {
                black_box(PointRequestPlan::new(black_box(&keys)));
            });
        });
        group.bench_with_input(
            BenchmarkId::new("known_unique", case.name),
            case,
            |b, _case| {
                b.iter_batched(
                    || keys.clone(),
                    |keys| {
                        black_box(PointRequestPlan::from_unique_keys(black_box(keys)));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_hash_algorithms(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_v2/hash_algorithms");
    group.sample_size(10);

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
    let mut group = c.benchmark_group("storage_v2/write_set_lowering");
    group.sample_size(10);

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
                    let mut writes = storage.new_write_set();
                    for mutation in &mutations {
                        match mutation {
                            WriteMutation::Put(space, key, value) => {
                                writes.stage_put(*space, key.clone(), value.clone());
                            }
                            WriteMutation::Delete(space, key) => {
                                writes.stage_delete(*space, key.clone());
                            }
                        }
                    }
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

fn bench_point_read_adapter(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_v2/point_read_adapter");
    group.sample_size(10);

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let expected_missing_slots = case.requested_missing_slots();
        let read = StorageReadScope::new(PointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = read
                    .get_many_values_caller_order_with_stats(
                        space(1),
                        black_box(&keys),
                        GetOptions::default(),
                    )
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
    let mut group = c.benchmark_group("storage_v2/point_read_indexed_adapter");
    group.sample_size(10);

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let expected_unique_missing = case.unique_keys - case.existing_unique_keys;
        let read = StorageReadScope::new(PointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = read
                    .get_many_indexed_values_caller_order_with_stats(
                        space(1),
                        black_box(&keys),
                        GetOptions::default(),
                    )
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
    let mut group = c.benchmark_group("storage_v2/point_read_indexed_lean_backend");
    group.sample_size(10);

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let expected_unique_missing = case.unique_keys - case.existing_unique_keys;
        let read = StorageReadScope::new(LeanPointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = read
                    .get_many_indexed_values_caller_order_with_stats(
                        space(1),
                        black_box(&keys),
                        GetOptions::default(),
                    )
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
    let mut group = c.benchmark_group("storage_v2/point_read_planned_lean_backend");
    group.sample_size(10);

    for case in POINT_CASES {
        let keys = point_request_keys(case.requested_keys, case.unique_keys);
        let plan = PointRequestPlan::new(&keys);
        let expected_unique_missing = case.unique_keys - case.existing_unique_keys;
        let read = StorageReadScope::new(LeanPointReadBackend::new(case.existing_unique_keys));
        group.throughput(Throughput::Elements(case.requested_keys as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = read
                    .get_many_borrowed_indexed_values_for_plan_with_stats(
                        space(1),
                        black_box(&plan),
                        GetOptions::default(),
                    )
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
    }

    group.finish();
}

fn bench_prefix_scan_adapter(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_v2/prefix_scan_adapter");
    group.sample_size(10);

    for case in PREFIX_CASES {
        let read = StorageReadScope::new(PrefixReadBackend::new(case.rows));
        group.throughput(Throughput::Elements(case.rows as u64));
        group.bench_with_input(BenchmarkId::from_parameter(case.name), case, |b, case| {
            b.iter(|| {
                let result = read
                    .scan_prefix_with_stats(
                        space(1),
                        Prefix {
                            bytes: Bytes::from_static(b"row-"),
                        },
                        ScanOptions {
                            limit_rows: case.rows + 1,
                            ..ScanOptions::default()
                        },
                    )
                    .expect("prefix scan");
                assert_eq!(result.stats.prefix_lowered, 1);
                assert_eq!(result.stats.backend_calls, 1);
                assert_eq!(result.value.entries.entries.len(), case.rows);
                black_box(result.value);
            });
        });
    }

    group.finish();
}

fn bench_conformance_backend(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_v2/conformance_backend");
    group.sample_size(10);

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
                let backend = ConformanceBackend::new();
                let storage = StorageContext::new(backend);
                let mut writes = storage.new_write_set();
                for mutation in &commit_mutations {
                    match mutation {
                        WriteMutation::Put(space, key, value) => {
                            writes.stage_put(*space, key.clone(), value.clone());
                        }
                        WriteMutation::Delete(space, key) => {
                            writes.stage_delete(*space, key.clone());
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

    group.throughput(Throughput::Elements(1_000));
    let get_many_backend = seeded_conformance_backend(1, 100);
    let get_many_read = get_many_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let get_many_read = StorageReadScope::new(get_many_read);
    let get_many_keys = point_request_keys(1_000, 100);
    group.bench_function("get_many_m1000_u100", |b| {
        b.iter(|| {
            let result = get_many_read
                .get_many_values_caller_order_with_stats(
                    space(1),
                    black_box(&get_many_keys),
                    GetOptions::default(),
                )
                .expect("point read");
            assert_eq!(result.stats.requested_keys, 1_000);
            assert_eq!(result.stats.unique_backend_keys, 100);
            assert_eq!(result.stats.backend_calls, 1);
            assert_eq!(result.value.len(), 1_000);
            black_box(result.value);
        });
    });

    group.throughput(Throughput::Elements(1_000));
    let scan_backend = seeded_conformance_backend(1, 1_000);
    let scan_read = scan_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    group.bench_function("scan_range_q1000", |b| {
        b.iter(|| {
            let page = materialize_backend_scan(
                &scan_read,
                SpaceId(1),
                KeyRange {
                    lower: Bound::Included(key("point-0000")),
                    upper: Bound::Excluded(key("point-9999")),
                },
                ScanOptions {
                    limit_rows: 1_001,
                    projection: CoreProjection::KeyOnly,
                    ..ScanOptions::default()
                },
            )
            .expect("scan range");
            assert_eq!(page.entries.entries.len(), 1_000);
            black_box(page);
        });
    });

    group.finish();
}

fn bench_in_memory_backend(c: &mut Criterion) {
    let mut group = c.benchmark_group("storage_v2/in_memory_backend");
    group.sample_size(10);

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
                            writes.stage_put(*space, key.clone(), value.clone());
                        }
                        WriteMutation::Delete(space, key) => {
                            writes.stage_delete(*space, key.clone());
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

    group.throughput(Throughput::Elements(1_000));
    let get_many_backend = seeded_in_memory_backend(1, 100);
    let get_many_read = get_many_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    let get_many_read = StorageReadScope::new(get_many_read);
    let get_many_keys = point_request_keys(1_000, 100);
    group.bench_function("get_many_m1000_u100", |b| {
        b.iter(|| {
            let result = get_many_read
                .get_many_values_caller_order_with_stats(
                    space(1),
                    black_box(&get_many_keys),
                    GetOptions::default(),
                )
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
    let planned_get_many_plan = PointRequestPlan::new(&planned_get_many_keys);
    group.bench_function("planned_get_many_m1000_u100", |b| {
        b.iter(|| {
            let result = planned_get_many_read
                .get_many_borrowed_indexed_values_for_plan_with_stats(
                    space(1),
                    black_box(&planned_get_many_plan),
                    GetOptions::default(),
                )
                .expect("planned point read");
            assert_eq!(result.stats.requested_keys, 1_000);
            assert_eq!(result.stats.unique_backend_keys, 100);
            assert_eq!(result.stats.backend_calls, 1);
            assert_eq!(result.value.len(), 1_000);
            assert_eq!(result.value.unique_values.len(), 100);
            black_box(result.value);
        });
    });

    group.throughput(Throughput::Elements(1_000));
    let scan_backend = seeded_in_memory_backend(1, 1_000);
    let scan_read = scan_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    group.bench_function("scan_range_q1000", |b| {
        b.iter(|| {
            let page = materialize_backend_scan(
                &scan_read,
                SpaceId(1),
                KeyRange {
                    lower: Bound::Included(key("point-0000")),
                    upper: Bound::Excluded(key("point-9999")),
                },
                ScanOptions {
                    limit_rows: 1_001,
                    projection: CoreProjection::KeyOnly,
                    ..ScanOptions::default()
                },
            )
            .expect("scan range");
            assert_eq!(page.entries.entries.len(), 1_000);
            black_box(page);
        });
    });

    group.throughput(Throughput::Elements(1_000));
    let scan_visit_backend = seeded_in_memory_backend(1, 1_000);
    let scan_visit_read = scan_visit_backend
        .begin_read(ReadOptions::default())
        .expect("begin read");
    group.bench_function("scan_range_visit_key_only_q1000", |b| {
        b.iter(|| {
            let mut visited = 0usize;
            let result = scan_visit_read
                .visit_scan_range(
                    SpaceId(1),
                    KeyRange {
                        lower: Bound::Included(key("point-0000")),
                        upper: Bound::Excluded(key("point-9999")),
                    },
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
    let mut group = c.benchmark_group("storage_v2/scan_visitor_baseline");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(1));

    for rows in [0usize, 1, 10, 100, 1_000, 10_000] {
        let backend = seeded_in_memory_backend_with_value_size(1, rows as u32, 32);
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(format!("owned_key_only_q{rows}"), |b| {
            b.iter(|| {
                let page = materialize_backend_scan(
                    &read,
                    SpaceId(1),
                    point_scan_range(),
                    ScanOptions {
                        limit_rows: rows + 1,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                )
                .expect("scan range");
                assert_eq!(page.entries.entries.len(), rows);
                black_box(page);
            });
        });

        group.bench_function(format!("visit_key_only_q{rows}"), |b| {
            b.iter(|| {
                let mut visited = 0usize;
                let result = read
                    .visit_scan_range(
                        SpaceId(1),
                        point_scan_range(),
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
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("owned_full_value_q1000_v{value_size}"), |b| {
            b.iter(|| {
                let page = materialize_backend_scan(
                    &read,
                    SpaceId(1),
                    point_scan_range(),
                    ScanOptions {
                        limit_rows: 1_001,
                        projection: CoreProjection::FullValue,
                        ..ScanOptions::default()
                    },
                )
                .expect("scan range");
                assert_eq!(page.entries.entries.len(), 1_000);
                black_box(page);
            });
        });

        group.bench_function(format!("visit_full_value_q1000_v{value_size}"), |b| {
            b.iter(|| {
                let mut visited = 0usize;
                let mut bytes_seen = 0usize;
                let result = read
                    .visit_scan_range(
                        SpaceId(1),
                        point_scan_range(),
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
    group.throughput(Throughput::Elements(1_000));
    group.bench_function("visit_materialize_key_only_q1000", |b| {
        b.iter(|| {
            let page =
                materialize_scan_visit(&materialize_read, CoreProjection::KeyOnly, 1_001, None)
                    .expect("materialize visitor scan");
            assert_eq!(page.entries.entries.len(), 1_000);
            black_box(page);
        });
    });

    group.bench_function("visit_materialize_full_value_q1000_v32", |b| {
        b.iter(|| {
            let page =
                materialize_scan_visit(&materialize_read, CoreProjection::FullValue, 1_001, None)
                    .expect("materialize visitor scan");
            assert_eq!(page.entries.entries.len(), 1_000);
            black_box(page);
        });
    });

    for limit_rows in [10usize, 100, 1_000] {
        let backend = seeded_in_memory_backend_with_value_size(1, 1_000, 32);
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        group.throughput(Throughput::Elements(limit_rows as u64));
        group.bench_function(format!("owned_key_only_q1000_limit{limit_rows}"), |b| {
            b.iter(|| {
                let page = materialize_backend_scan(
                    &read,
                    SpaceId(1),
                    point_scan_range(),
                    ScanOptions {
                        limit_rows,
                        projection: CoreProjection::KeyOnly,
                        ..ScanOptions::default()
                    },
                )
                .expect("scan range");
                assert_eq!(page.entries.entries.len(), limit_rows);
                assert_eq!(page.has_more, limit_rows < 1_000);
                black_box(page);
            });
        });

        group.bench_function(format!("visit_key_only_q1000_limit{limit_rows}"), |b| {
            b.iter(|| {
                let mut visited = 0usize;
                let result = read
                    .visit_scan_range(
                        SpaceId(1),
                        point_scan_range(),
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

    for page_size in [10usize, 100] {
        let backend = seeded_in_memory_backend_with_value_size(1, 1_000, 32);
        let read = backend
            .begin_read(ReadOptions::default())
            .expect("begin read");
        group.throughput(Throughput::Elements(1_000));
        group.bench_function(format!("owned_drain_key_only_q1000_page{page_size}"), |b| {
            b.iter(|| {
                let mut emitted = 0usize;
                let mut resume_after = None;
                loop {
                    let page = materialize_backend_scan(
                        &read,
                        SpaceId(1),
                        point_scan_range(),
                        ScanOptions {
                            limit_rows: page_size,
                            projection: CoreProjection::KeyOnly,
                            resume_after: resume_after.as_ref(),
                        },
                    )
                    .expect("scan range");
                    emitted += page.entries.entries.len();
                    resume_after = page.entries.entries.last().map(|entry| entry.key.clone());
                    if !page.has_more {
                        break;
                    }
                }
                assert_eq!(emitted, 1_000);
                black_box(resume_after);
            });
        });

        group.bench_function(format!("visit_drain_key_only_q1000_page{page_size}"), |b| {
            b.iter(|| {
                let mut emitted = 0usize;
                let mut resume_after = None;
                loop {
                    let mut page_last_key = None;
                    let result = read
                        .visit_scan_range(
                            SpaceId(1),
                            point_scan_range(),
                            ScanOptions {
                                limit_rows: page_size,
                                projection: CoreProjection::KeyOnly,
                                resume_after: resume_after.as_ref(),
                            },
                            |key, value| {
                                assert!(value.is_none());
                                page_last_key = Some(key.clone());
                                black_box(key);
                            },
                        )
                        .expect("visit scan range");
                    emitted += result.emitted;
                    resume_after = page_last_key;
                    if !result.has_more {
                        break;
                    }
                }
                assert_eq!(emitted, 1_000);
                black_box(resume_after);
            });
        });
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
    fn put_many(&mut self, _space: SpaceId, _entries: PutBatch) -> Result<(), BackendError> {
        self.state
            .put_many_calls
            .set(self.state.put_many_calls.get() + 1);
        Ok(())
    }

    fn delete_many(&mut self, _space: SpaceId, _keys: &[Key]) -> Result<(), BackendError> {
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
    fn get_many(
        &self,
        _space: SpaceId,
        keys: &[Key],
        _opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        self.requested_keys.replace(keys.to_vec());
        Ok(GetManyResult::new(
            keys.iter()
                .map(|key| {
                    self.values
                        .iter()
                        .find(|entry| entry.key == *key)
                        .map(|entry| entry.value.clone())
                })
                .collect(),
        ))
    }

    fn visit_range<V>(
        &self,
        _space: SpaceId,
        _range: KeyRange,
        _opts: ScanOptions<'_>,
        _visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
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
    fn get_many(
        &self,
        _space: SpaceId,
        keys: &[Key],
        _opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        let found = keys.len().min(self.values.len());
        let values = self
            .values
            .iter()
            .take(found)
            .map(|entry| Some(entry.value.clone()))
            .chain(std::iter::repeat_with(|| None).take(keys.len().saturating_sub(found)))
            .collect();
        Ok(GetManyResult::new(values))
    }

    fn visit_range<V>(
        &self,
        _space: SpaceId,
        _range: KeyRange,
        _opts: ScanOptions<'_>,
        _visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
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
    fn get_many(
        &self,
        _space: SpaceId,
        _keys: &[Key],
        _opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        unreachable!("prefix-scan benchmark does not point-read")
    }

    fn visit_range<V>(
        &self,
        _space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        assert_eq!(range.lower, Bound::Included(key("row-")));
        assert_eq!(range.upper, Bound::Excluded(key("row.")));
        let mut emitted = 0;
        for entry in self.entries.iter().take(opts.limit_rows) {
            visitor.visit(&entry.key, ProjectedValueRef::KeyOnly)?;
            emitted += 1;
        }
        Ok(ScanResult {
            emitted,
            has_more: opts.limit_rows < self.entries.len(),
        })
    }
}

#[derive(Clone, Copy)]
struct EmptyRead;

impl BackendRead for EmptyRead {
    fn get_many(
        &self,
        _space: SpaceId,
        _keys: &[Key],
        _opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        unreachable!("write-set benchmark does not point-read")
    }

    fn visit_range<V>(
        &self,
        _space: SpaceId,
        _range: KeyRange,
        _opts: ScanOptions<'_>,
        _visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
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

fn seeded_conformance_backend(space_id: u32, rows: u32) -> ConformanceBackend {
    let backend = ConformanceBackend::new();
    let storage = StorageContext::new(backend.clone());
    let mut writes = storage.new_write_set();
    for index in 0..rows {
        writes.stage_put(
            space(space_id),
            key(format!("point-{index:04}")),
            value(index, 32),
        );
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, WriteOptions::default())
        .expect("seed conformance backend");
    assert_eq!(stats.staged_puts, rows as u64);
    backend
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
    let storage = StorageContext::new(backend.clone());
    let mut writes = storage.new_write_set();
    for index in 0..rows {
        writes.stage_put(
            space(space_id),
            key(format!("point-{index:04}")),
            value(index, value_size),
        );
    }
    let (_commit, stats) = storage
        .commit_write_set(writes, WriteOptions::default())
        .expect("seed in-memory backend");
    assert_eq!(stats.staged_puts, rows as u64);
    backend
}

fn point_scan_range() -> KeyRange {
    KeyRange {
        lower: Bound::Included(key("point-0000")),
        upper: Bound::Excluded(key("point:")),
    }
}

fn materialize_backend_scan<R>(
    read: &R,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<ScanPage, BackendError>
where
    R: BackendRead,
{
    let mut entries = Vec::with_capacity(opts.limit_rows);
    let result = read.visit_range(
        space,
        range,
        opts,
        &mut |key: &Key, value: ProjectedValueRef<'_>| {
            entries.push(ReadEntry {
                key: key.clone(),
                value: value.to_owned(),
            });
            Ok(())
        },
    )?;
    Ok(ScanPage {
        entries: ReadBatch { entries },
        has_more: result.has_more,
    })
}

fn materialize_scan_visit(
    read: &lix_engine::backend_v2::InMemoryRead,
    projection: CoreProjection,
    limit_rows: usize,
    resume_after: Option<&Key>,
) -> Result<ScanPage, BackendError> {
    let mut entries = Vec::with_capacity(limit_rows);
    let result = read.visit_scan_range(
        SpaceId(1),
        point_scan_range(),
        ScanOptions {
            projection,
            limit_rows,
            resume_after,
        },
        |key, value| {
            let value = match value {
                None => ProjectedValue::KeyOnly,
                Some(value) => ProjectedValue::FullValue(value.clone()),
            };
            entries.push(ReadEntry {
                key: key.clone(),
                value,
            });
        },
    )?;
    Ok(ScanPage {
        entries: ReadBatch { entries },
        has_more: result.has_more,
    })
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
