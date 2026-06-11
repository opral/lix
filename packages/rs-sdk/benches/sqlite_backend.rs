use std::hint::black_box;
use std::ops::Bound;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use lix_engine::backend::{KeyRef, PutEntry};
use lix_sdk::{
    Backend, BackendError, BackendRead, BackendWrite, CoreProjection, GetOptions, Key, KeyRange,
    PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor,
    SpaceId, SqliteBackend, StoredValue, WriteOptions,
};
use tempfile::TempDir;

const ROWS: usize = 50_000;
/// Space used by the single-space cells.
const BENCH_SPACE: SpaceId = SpaceId(0x0001_0001);
const POINT_KEYS: usize = 1_000;
const VALUE_SIZE: usize = 256;
const SCAN_CHUNK_ROWS: usize = 1_024;
/// Pre-existing rows for the random-key write benches, so inserts land in a
/// grown B-tree instead of a trivially cached fresh one.
const RANDOM_WRITE_SEED_ROWS: usize = 25_000;

struct SqliteFixture {
    backend: SqliteBackend,
    _temp_dir: TempDir,
}

#[derive(Default)]
struct CountingPointVisitor {
    visited: usize,
    found: usize,
    bytes: usize,
}

impl PointVisitor for CountingPointVisitor {
    fn visit(
        &mut self,
        index: usize,
        key: &Key,
        value: Option<ProjectedValueRef<'_>>,
    ) -> Result<(), BackendError> {
        self.visited += 1;
        if let Some(value) = value {
            self.found += 1;
            if let ProjectedValueRef::FullValue(bytes) = value {
                self.bytes += bytes.len();
            }
        }
        black_box((index, key));
        Ok(())
    }
}

#[derive(Default)]
struct CountingScanVisitor {
    rows: usize,
    bytes: usize,
}

impl ScanVisitor for CountingScanVisitor {
    fn visit(&mut self, key: KeyRef<'_>, value: ProjectedValueRef<'_>) -> Result<(), BackendError> {
        self.rows += 1;
        self.bytes += key.0.len();
        if let ProjectedValueRef::FullValue(bytes) = value {
            self.bytes += bytes.len();
        }
        Ok(())
    }
}

fn bench_sqlite_backend(c: &mut Criterion) {
    let fixture = sqlite_fixture(ROWS, VALUE_SIZE);
    bench_open(c);
    bench_txn_begin(c, &fixture);
    bench_point_reads(c, &fixture);
    bench_range_scans(c, &fixture);
    bench_write_batches(c);
    let spaces = multi_space_fixture();
    bench_space_prefix_scan(c, &spaces);
    bench_space_truncate(c);
    report_file_stats();
}

/// Spaces mirroring the engine's physical layout: a 4-byte big-endian space
/// id prefixes every key. Row counts approximate the 1k-commit accounting
/// mix (json store and change records dominate; tree chunks are few but
/// large).
const SPACE_MIX: &[(u32, usize, usize)] = &[
    (0x0001_0001, 20_000, 300), // json_store.json: blake3 keys, json values
    (0x0006_0002, 20_000, 110), // changelog.change
    (0x0004_0001, 500, 4_096),  // tracked_state.tree_chunk
    (0x0006_0001, 1_000, 200),  // changelog.commit
    (0x0004_0002, 1_000, 20),   // tracked_state.commit_root
    (0x0002_0001, 200, 150),    // untracked_state.row.v1
];

fn space_key_for(index: usize) -> Key {
    // splitmix64 spread within the space, mirroring blake3/uuid key entropy.
    let mut x = (index as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    Key(Bytes::from(format!("{x:016x}/{index:08x}")))
}

fn full_range() -> KeyRange {
    KeyRange {
        lower: Bound::Unbounded,
        upper: Bound::Unbounded,
    }
}

fn seed_spaces(backend: &SqliteBackend) {
    let mut write = backend
        .begin_write(WriteOptions::default())
        .expect("begin write");
    for &(space_id, rows, value_size) in SPACE_MIX {
        let space = SpaceId(space_id);
        let mut entries = (0..rows)
            .map(|index| PutEntry {
                key: space_key_for(index),
                value: value_for(index, value_size),
            })
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.key.0.cmp(&right.key.0));
        write
            .put_many(space, PutBatch { entries })
            .expect("seed space");
    }
    write.commit().expect("seed commit");
}

fn multi_space_fixture() -> SqliteFixture {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("bench.lix");
    let backend = SqliteBackend::open(path).expect("open backend");
    seed_spaces(&backend);
    SqliteFixture {
        backend,
        _temp_dir: temp_dir,
    }
}

/// Scans one space by its physical prefix: the cell the per-space-table
/// format targets (a table scan in v2 vs an interleaved range scan in v1).
fn bench_space_prefix_scan(c: &mut Criterion, fixture: &SqliteFixture) {
    let mut group = c.benchmark_group("sqlite_backend/space_prefix_scan");
    configure_group(&mut group);
    // The engine drives a scan with ONE visit_next call at the caller's
    // limit; the chunked cells below exercise the synthetic pagination
    // pattern, this one the engine pattern.
    group.throughput(Throughput::Elements(20_000u64));
    group.bench_function(
        BenchmarkId::new("json_store_single_call", 20_000usize),
        |b| {
            b.iter(|| {
                let read = fixture
                    .backend
                    .begin_read(ReadOptions::default())
                    .expect("begin read");
                let mut visitor = CountingScanVisitor::default();
                let result = read
                    .scan(
                        SpaceId(0x0001_0001),
                        full_range(),
                        ScanOptions {
                            projection: CoreProjection::FullValue,
                            limit_rows: usize::MAX,
                            resume_after: None,
                        },
                        &mut visitor,
                    )
                    .expect("space scan");
                read.close().expect("close read");
                assert_eq!(visitor.rows, 20_000);
                black_box((result, visitor));
            });
        },
    );
    for (label, space_id, rows) in [
        ("json_store", 0x0001_0001u32, 20_000usize),
        ("tree_chunk", 0x0004_0001, 500),
    ] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::new(label, rows), |b| {
            b.iter(|| {
                let read = fixture
                    .backend
                    .begin_read(ReadOptions::default())
                    .expect("begin read");
                let mut visitor = CountingScanVisitor::default();
                let result =
                    paged_scan(&read, SpaceId(space_id), &mut visitor).expect("space scan");
                read.close().expect("close read");
                assert_eq!(visitor.rows, rows);
                black_box((result, visitor));
            });
        });
    }
    group.finish();
}

/// Deletes one space's full key range: range delete in v1, a candidate for
/// table truncation in v2.
fn bench_space_truncate(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_backend/space_truncate");
    configure_group(&mut group);
    group.sample_size(10);
    group.bench_function(BenchmarkId::new("json_store", 20_000usize), |b| {
        b.iter_batched(
            multi_space_fixture,
            |fixture| {
                let mut write = fixture
                    .backend
                    .begin_write(WriteOptions::default())
                    .expect("begin write");
                write
                    .delete_range(SpaceId(0x0001_0001), full_range())
                    .expect("truncate space");
                write.commit().expect("commit");
                fixture
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

/// One-shot physical-layout report, gated by LIX_SQLITE_FILE_STATS=1:
/// checkpointed file size plus page accounting for the multi-space
/// workload. The truest storage metric for a file-format change.
fn report_file_stats() {
    if std::env::var("LIX_SQLITE_FILE_STATS").map_or(true, |v| v != "1") {
        return;
    }
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("stats.lix");
    let backend = SqliteBackend::open(&path).expect("open backend");
    seed_spaces(&backend);
    drop(backend);
    let conn = rusqlite::Connection::open(&path).expect("open raw");
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .expect("checkpoint");
    let page_size: u64 = conn
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .expect("page_size");
    let page_count: u64 = conn
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .expect("page_count");
    let freelist: u64 = conn
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .expect("freelist");
    let tables: u64 = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table'",
            [],
            |row| row.get(0),
        )
        .expect("tables");
    let file_bytes = std::fs::metadata(&path).expect("metadata").len();
    println!("| sqlite_file_stats | tables | page_size | pages | freelist | file_bytes |");
    println!(
        "| sqlite_file_stats | {tables} | {page_size} | {page_count} | {freelist} | {file_bytes} |"
    );
    // Per-table page accounting when the build has dbstat available.
    if let Ok(mut stmt) =
        conn.prepare("SELECT name, count(*), sum(pgsize) FROM dbstat GROUP BY name ORDER BY name")
    {
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u64>(1)?,
                row.get::<_, u64>(2)?,
            ))
        });
        if let Ok(rows) = rows {
            for row in rows.flatten() {
                println!("| dbstat | {} | {} pages | {} bytes |", row.0, row.1, row.2);
            }
        }
    }
}

fn bench_open(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_backend/open");
    configure_group(&mut group);
    group.bench_function("existing_database", |b| {
        b.iter_batched(
            || {
                let fixture = sqlite_fixture(1_000, VALUE_SIZE);
                let path = fixture.backend.path().to_path_buf();
                (fixture, path)
            },
            |(fixture, path)| {
                let backend = SqliteBackend::open(&path).expect("reopen backend");
                black_box(&backend);
                (fixture, backend)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn bench_txn_begin(c: &mut Criterion, fixture: &SqliteFixture) {
    let mut group = c.benchmark_group("sqlite_backend/txn_begin");
    configure_group(&mut group);
    group.bench_function("read", |b| {
        b.iter(|| {
            let read = fixture
                .backend
                .begin_read(ReadOptions::default())
                .expect("begin read");
            read.close().expect("close read");
        });
    });
    group.bench_function("write_rollback", |b| {
        b.iter(|| {
            let write = fixture
                .backend
                .begin_write(WriteOptions::default())
                .expect("begin write");
            write.rollback().expect("rollback write");
        });
    });
    group.finish();
}

fn bench_point_reads(c: &mut Criterion, fixture: &SqliteFixture) {
    let mut group = c.benchmark_group("sqlite_backend/point_reads");
    configure_group(&mut group);
    group.throughput(Throughput::Elements(POINT_KEYS as u64));

    let existing_keys = point_keys(0, POINT_KEYS);
    group.bench_function(BenchmarkId::new("existing/full_value", POINT_KEYS), |b| {
        b.iter(|| {
            let read = fixture
                .backend
                .begin_read(ReadOptions::default())
                .expect("begin read");
            let mut visitor = CountingPointVisitor::default();
            read.visit_keys(
                BENCH_SPACE,
                black_box(existing_keys.as_slice()),
                GetOptions {
                    projection: CoreProjection::FullValue,
                    _reserved: std::marker::PhantomData,
                },
                &mut visitor,
            )
            .expect("visit keys");
            read.close().expect("close read");
            black_box(visitor);
        });
    });

    let missing_keys = point_keys(ROWS * 2, POINT_KEYS);
    group.bench_function(BenchmarkId::new("missing/key_only", POINT_KEYS), |b| {
        b.iter(|| {
            let read = fixture
                .backend
                .begin_read(ReadOptions::default())
                .expect("begin read");
            let mut visitor = CountingPointVisitor::default();
            read.visit_keys(
                BENCH_SPACE,
                black_box(missing_keys.as_slice()),
                GetOptions {
                    projection: CoreProjection::KeyOnly,
                    _reserved: std::marker::PhantomData,
                },
                &mut visitor,
            )
            .expect("visit keys");
            read.close().expect("close read");
            black_box(visitor);
        });
    });

    group.finish();
}

fn bench_range_scans(c: &mut Criterion, fixture: &SqliteFixture) {
    let mut group = c.benchmark_group("sqlite_backend/range_scan");
    configure_group(&mut group);
    group.throughput(Throughput::Elements(ROWS as u64));

    for projection in [CoreProjection::KeyOnly, CoreProjection::FullValue] {
        let name = match projection {
            CoreProjection::KeyOnly => "key_only",
            CoreProjection::FullValue => "full_value",
        };
        group.bench_function(BenchmarkId::new(name, ROWS), |b| {
            b.iter(|| {
                let read = fixture
                    .backend
                    .begin_read(ReadOptions::default())
                    .expect("begin read");
                let mut visitor = CountingScanVisitor::default();
                let result = read
                    .scan(
                        BENCH_SPACE,
                        full_range(),
                        ScanOptions {
                            projection,
                            limit_rows: usize::MAX,
                            resume_after: None,
                        },
                        &mut visitor,
                    )
                    .expect("range scan");
                read.close().expect("close read");
                black_box((result, visitor));
            });
        });
    }

    group.finish();
}

fn bench_write_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqlite_backend/write_batch");
    configure_group(&mut group);

    for rows in [1_000usize, 10_000usize] {
        group.throughput(Throughput::Elements(rows as u64));
        group.bench_function(BenchmarkId::new("put_many_commit", rows), |b| {
            b.iter_batched(
                || {
                    let temp_dir = tempfile::tempdir().expect("tempdir");
                    let path = temp_dir.path().join("bench.lix");
                    let backend = SqliteBackend::open(path).expect("open backend");
                    (backend, temp_dir, put_batch(0, rows, VALUE_SIZE))
                },
                |(backend, temp_dir, batch)| {
                    let mut write = backend
                        .begin_write(WriteOptions::default())
                        .expect("begin write");
                    write
                        .put_many(BENCH_SPACE, black_box(batch))
                        .expect("put many");
                    let result = write.commit().expect("commit");
                    black_box(result);
                    // Returned so backend teardown (connection close + WAL
                    // checkpoint) drops outside the timed window. The backend
                    // precedes the tempdir so connections close before the
                    // database files are removed.
                    (backend, temp_dir)
                },
                BatchSize::PerIteration,
            );
        });
        // Content-hash keyed spaces (json_store, tree chunks) arrive in
        // effectively random key order and land in an already-grown tree;
        // this variant models that shape.
        group.bench_function(BenchmarkId::new("put_many_commit_random", rows), |b| {
            b.iter_batched(
                || {
                    let temp_dir = tempfile::tempdir().expect("tempdir");
                    let path = temp_dir.path().join("bench.lix");
                    let backend = SqliteBackend::open(path).expect("open backend");
                    let mut write = backend
                        .begin_write(WriteOptions::default())
                        .expect("begin seed write");
                    write
                        .put_many(
                            BENCH_SPACE,
                            random_put_batch(rows, RANDOM_WRITE_SEED_ROWS, VALUE_SIZE),
                        )
                        .expect("seed rows");
                    write.commit().expect("seed commit");
                    (backend, temp_dir, random_put_batch(0, rows, VALUE_SIZE))
                },
                |(backend, temp_dir, batch)| {
                    let mut write = backend
                        .begin_write(WriteOptions::default())
                        .expect("begin write");
                    write
                        .put_many(BENCH_SPACE, black_box(batch))
                        .expect("put many");
                    let result = write.commit().expect("commit");
                    black_box(result);
                    (backend, temp_dir)
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

/// Paged scan via resume_after: the pagination pattern the engine uses for
/// limited scans, one query per page.
fn paged_scan<R: BackendRead>(
    read: &R,
    space: SpaceId,
    visitor: &mut CountingScanVisitor,
) -> Result<ScanResult, BackendError> {
    let mut total = ScanResult::default();
    let mut resume: Option<Key> = None;
    loop {
        let mut last_key: Option<Key> = None;
        let mut page = |key: KeyRef<'_>, value: ProjectedValueRef<'_>| {
            last_key = Some(key.to_owned_key());
            visitor.visit(key, value)
        };
        let chunk = read.scan(
            space,
            full_range(),
            ScanOptions {
                projection: CoreProjection::FullValue,
                limit_rows: SCAN_CHUNK_ROWS,
                resume_after: resume.as_ref(),
            },
            &mut page,
        )?;
        total.emitted += chunk.emitted;
        total.has_more = chunk.has_more;
        if !chunk.has_more {
            return Ok(total);
        }
        resume = last_key;
    }
}

fn sqlite_fixture(rows: usize, value_size: usize) -> SqliteFixture {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("bench.lix");
    let backend = SqliteBackend::open(path).expect("open backend");
    let mut write = backend
        .begin_write(WriteOptions::default())
        .expect("begin write");
    write
        .put_many(BENCH_SPACE, put_batch(0, rows, value_size))
        .expect("seed rows");
    write.commit().expect("seed commit");
    SqliteFixture {
        backend,
        _temp_dir: temp_dir,
    }
}

fn point_keys(start: usize, count: usize) -> Vec<Key> {
    (start..start + count).map(key_for).collect()
}

fn put_batch(start: usize, count: usize, value_size: usize) -> PutBatch {
    PutBatch {
        entries: (start..start + count)
            .map(|index| PutEntry {
                key: key_for(index),
                value: value_for(index, value_size),
            })
            .collect(),
    }
}

fn key_for(index: usize) -> Key {
    Key(Bytes::from(format!("bench/{index:016x}")))
}

fn random_put_batch(start: usize, count: usize, value_size: usize) -> PutBatch {
    PutBatch {
        entries: (start..start + count)
            .map(|index| PutEntry {
                key: random_key_for(index),
                value: value_for(index, value_size),
            })
            .collect(),
    }
}

fn random_key_for(index: usize) -> Key {
    // splitmix64: deterministic pseudo-random spread, no dependency. The
    // {index:08x} suffix guarantees uniqueness independent of the hash.
    let mut x = (index as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    Key(Bytes::from(format!("bench/{x:016x}/{index:08x}")))
}

fn value_for(index: usize, size: usize) -> StoredValue {
    let mut value = vec![0u8; size];
    value[..8].copy_from_slice(&(index as u64).to_be_bytes());
    StoredValue {
        bytes: Bytes::from(value),
    }
}

fn configure_group<M: criterion::measurement::Measurement>(
    group: &mut criterion::BenchmarkGroup<'_, M>,
) {
    group.sample_size(10);
    if std::env::var_os("SQLITE_BACKEND_BENCH_SMOKE").is_some() {
        group.warm_up_time(Duration::from_millis(100));
        group.measurement_time(Duration::from_millis(250));
    }
}

criterion_group!(benches, bench_sqlite_backend);
criterion_main!(benches);
