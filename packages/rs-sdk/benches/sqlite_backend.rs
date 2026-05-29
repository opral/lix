use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use lix_engine::backend::{KeyRef, PutEntry};
use lix_sdk::{
    Backend, BackendError, BackendRangeScan, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetOptions, Key, KeyRange, PointVisitor, ProjectedValueRef, PutBatch,
    ReadOptions, ScanOptions, ScanResult, ScanVisitor, SqliteBackend, StoredValue, WriteOptions,
    WriteStats,
};
use rusqlite::types::{Value as SqlValue, ValueRef as SqlValueRef};
use rusqlite::{Connection, Rows, params};
use tempfile::TempDir;

const ROWS: usize = 50_000;
const POINT_KEYS: usize = 1_000;
const VALUE_SIZE: usize = 256;
const SCAN_CHUNK_ROWS: usize = 1_024;

struct SqliteFixture {
    _temp_dir: TempDir,
    backend: SqliteBackend,
}

struct DirectSqliteFixture {
    _temp_dir: TempDir,
    backend: DirectSqliteBackend,
}

#[derive(Clone)]
struct DirectSqliteBackend {
    path: PathBuf,
    read_pool: Arc<Mutex<Vec<Connection>>>,
    write_pool: Arc<Mutex<Vec<Connection>>>,
}

struct DirectSqliteRead {
    conn: Option<Connection>,
    read_pool: Arc<Mutex<Vec<Connection>>>,
}

struct DirectSqliteRangeScan<'stmt> {
    rows: Rows<'stmt>,
    projection: CoreProjection,
    pending: Option<DirectSqlitePendingRow>,
    done: bool,
}

struct DirectSqlitePendingRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

struct DirectSqliteWrite {
    conn: Option<Connection>,
    write_pool: Arc<Mutex<Vec<Connection>>>,
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
    let direct_fixture = direct_sqlite_fixture(ROWS, VALUE_SIZE);
    bench_point_reads(c, &fixture);
    bench_direct_point_reads(c, &direct_fixture);
    bench_range_scans(c, &fixture, &direct_fixture);
    bench_write_batches(c);
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

fn bench_direct_point_reads(c: &mut Criterion, fixture: &DirectSqliteFixture) {
    let mut group = c.benchmark_group("sqlite_backend/direct_point_reads");
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

fn bench_range_scans(c: &mut Criterion, fixture: &SqliteFixture, direct: &DirectSqliteFixture) {
    let mut group = c.benchmark_group("sqlite_backend/range_scan");
    configure_group(&mut group);
    group.throughput(Throughput::Elements(ROWS as u64));

    for projection in [CoreProjection::KeyOnly, CoreProjection::FullValue] {
        let name = match projection {
            CoreProjection::KeyOnly => "key_only",
            CoreProjection::FullValue => "full_value",
        };
        group.bench_function(BenchmarkId::new(format!("current/{name}"), ROWS), |b| {
            b.iter(|| {
                let read = fixture
                    .backend
                    .begin_read(ReadOptions::default())
                    .expect("begin read");
                let mut visitor = CountingScanVisitor::default();
                let result = read
                    .with_range_scan(
                        KeyRange {
                            lower: Bound::Unbounded,
                            upper: Bound::Unbounded,
                        },
                        ScanOptions {
                            projection,
                            limit_rows: usize::MAX,
                            resume_after: None,
                        },
                        |cursor| visit_all(cursor, &mut visitor),
                    )
                    .expect("range scan");
                read.close().expect("close read");
                black_box((result, visitor));
            });
        });
        group.bench_function(BenchmarkId::new(format!("direct/{name}"), ROWS), |b| {
            b.iter(|| {
                let read = direct
                    .backend
                    .begin_read(ReadOptions::default())
                    .expect("begin read");
                let mut visitor = CountingScanVisitor::default();
                let result = read
                    .with_range_scan(
                        KeyRange {
                            lower: Bound::Unbounded,
                            upper: Bound::Unbounded,
                        },
                        ScanOptions {
                            projection,
                            limit_rows: usize::MAX,
                            resume_after: None,
                        },
                        |cursor| visit_all(cursor, &mut visitor),
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
                    (temp_dir, backend, put_batch(0, rows, VALUE_SIZE))
                },
                |(_temp_dir, backend, batch)| {
                    let mut write = backend
                        .begin_write(WriteOptions::default())
                        .expect("begin write");
                    write.put_many(black_box(batch)).expect("put many");
                    let result = write.commit().expect("commit");
                    black_box(result);
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn visit_all(
    cursor: &mut impl BackendRangeScan,
    visitor: &mut CountingScanVisitor,
) -> Result<ScanResult, BackendError> {
    let mut total = ScanResult::default();
    loop {
        let chunk = cursor.visit_next(SCAN_CHUNK_ROWS, visitor)?;
        total.emitted += chunk.emitted;
        total.has_more = chunk.has_more;
        if !chunk.has_more {
            return Ok(total);
        }
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
        .put_many(put_batch(0, rows, value_size))
        .expect("seed rows");
    write.commit().expect("seed commit");
    SqliteFixture {
        _temp_dir: temp_dir,
        backend,
    }
}

fn direct_sqlite_fixture(rows: usize, value_size: usize) -> DirectSqliteFixture {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let path = temp_dir.path().join("bench-direct.lix");
    let backend = DirectSqliteBackend::open(path).expect("open direct backend");
    let mut write = backend
        .begin_write(WriteOptions::default())
        .expect("begin direct write");
    write
        .put_many(put_batch(0, rows, value_size))
        .expect("seed direct rows");
    write.commit().expect("seed direct commit");
    DirectSqliteFixture {
        _temp_dir: temp_dir,
        backend,
    }
}

impl DirectSqliteBackend {
    fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        direct_initialize_database(&path)?;
        Ok(Self {
            path,
            read_pool: Arc::new(Mutex::new(Vec::new())),
            write_pool: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn connect(&self) -> Result<Connection, BackendError> {
        direct_open_connection(&self.path)
    }
}

impl Backend for DirectSqliteBackend {
    type Read<'a>
        = DirectSqliteRead
    where
        Self: 'a;

    type Write<'a>
        = DirectSqliteWrite
    where
        Self: 'a;

    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        let conn = self
            .read_pool
            .lock()
            .map_err(|error| {
                BackendError::Io(format!("direct sqlite read pool poisoned: {error}"))
            })?
            .pop()
            .map(Ok)
            .unwrap_or_else(|| self.connect())?;
        direct_execute_cached(&conn, "BEGIN DEFERRED TRANSACTION")?;
        direct_pin_read_snapshot(&conn)?;
        Ok(DirectSqliteRead {
            conn: Some(conn),
            read_pool: Arc::clone(&self.read_pool),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        let conn = self
            .write_pool
            .lock()
            .map_err(|error| {
                BackendError::Io(format!("direct sqlite write pool poisoned: {error}"))
            })?
            .pop()
            .map(Ok)
            .unwrap_or_else(|| self.connect())?;
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")
            .map_err(sqlite_error)?;
        Ok(DirectSqliteWrite {
            conn: Some(conn),
            write_pool: Arc::clone(&self.write_pool),
        })
    }
}

impl BackendRead for DirectSqliteRead {
    type RangeScan<'a> = DirectSqliteRangeScan<'a>;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        direct_visit_keys(self.conn()?, keys, opts, visitor)
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
        let (sql, values) = direct_scan_sql(range, opts)?;
        let mut stmt = self.conn()?.prepare_cached(&sql).map_err(sqlite_error)?;
        let rows = stmt
            .query(rusqlite::params_from_iter(values))
            .map_err(sqlite_error)?;
        let mut cursor = DirectSqliteRangeScan {
            rows,
            projection: opts.projection,
            pending: None,
            done: opts.limit_rows == 0,
        };
        f(&mut cursor)
    }

    fn close(mut self) -> Result<(), BackendError> {
        self.finish()
    }
}

impl BackendRangeScan for DirectSqliteRangeScan<'_> {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if limit_rows == 0 || self.done {
            return Ok(ScanResult {
                emitted: 0,
                has_more: !self.done,
            });
        }

        let mut emitted = 0;
        while emitted < limit_rows {
            if let Some(pending) = self.pending.take() {
                direct_visit_pending_row(pending, self.projection, visitor)?;
                emitted += 1;
                continue;
            }

            let Some(row) = self.rows.next().map_err(sqlite_error)? else {
                self.done = true;
                return Ok(ScanResult {
                    emitted,
                    has_more: false,
                });
            };
            let key = direct_blob_ref(row.get_ref(0).map_err(sqlite_error)?, "key")?;
            match self.projection {
                CoreProjection::KeyOnly => {
                    visitor.visit(KeyRef(key), ProjectedValueRef::KeyOnly)?
                }
                CoreProjection::FullValue => {
                    let value = direct_blob_ref(row.get_ref(1).map_err(sqlite_error)?, "value")?;
                    visitor.visit(KeyRef(key), ProjectedValueRef::FullValue(value))?;
                }
            }
            emitted += 1;
        }

        let has_more = self.ensure_pending()?;
        Ok(ScanResult { emitted, has_more })
    }
}

impl DirectSqliteRangeScan<'_> {
    fn ensure_pending(&mut self) -> Result<bool, BackendError> {
        if self.pending.is_some() {
            return Ok(true);
        }
        let Some(row) = self.rows.next().map_err(sqlite_error)? else {
            self.done = true;
            return Ok(false);
        };
        let key = direct_blob_ref(row.get_ref(0).map_err(sqlite_error)?, "key")?.to_vec();
        let value = if matches!(self.projection, CoreProjection::FullValue) {
            Some(direct_blob_ref(row.get_ref(1).map_err(sqlite_error)?, "value")?.to_vec())
        } else {
            None
        };
        self.pending = Some(DirectSqlitePendingRow { key, value });
        Ok(true)
    }
}

impl DirectSqliteRead {
    fn conn(&self) -> Result<&Connection, BackendError> {
        self.conn
            .as_ref()
            .ok_or_else(|| BackendError::Io("direct sqlite read is closed".to_string()))
    }

    fn finish(&mut self) -> Result<(), BackendError> {
        let Some(conn) = self.conn.take() else {
            return Ok(());
        };
        let result = direct_execute_cached(&conn, "ROLLBACK").or_else(ignore_no_transaction);
        if result.is_ok() {
            if let Ok(mut pool) = self.read_pool.lock() {
                pool.push(conn);
            }
        }
        result
    }
}

impl Drop for DirectSqliteRead {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

impl BackendWrite for DirectSqliteWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare_cached(
                "INSERT INTO lix_internal_entries(key, value)
                 VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .map_err(sqlite_error)?;

        for entry in entries.entries {
            stmt.execute(params![entry.key.0.as_ref(), entry.value.bytes.as_ref()])
                .map_err(sqlite_error)?;
        }
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare_cached("DELETE FROM lix_internal_entries WHERE key = ?1")
            .map_err(sqlite_error)?;
        for key in keys {
            stmt.execute(params![key.0.as_ref()])
                .map_err(sqlite_error)?;
        }
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        let mut sql = String::from("DELETE FROM lix_internal_entries WHERE 1 = 1");
        let mut values = Vec::new();
        direct_append_bound_sql(&mut sql, &mut values, "key", ">=", ">", &range.lower);
        direct_append_bound_sql(&mut sql, &mut values, "key", "<=", "<", &range.upper);
        self.conn()?
            .execute(&sql, rusqlite::params_from_iter(values))
            .map_err(sqlite_error)?;
        Ok(())
    }

    fn commit(mut self) -> Result<CommitResult, BackendError> {
        self.finish("COMMIT")?;
        Ok(CommitResult {
            commit_id: None,
            stats: WriteStats::default(),
        })
    }

    fn rollback(mut self) -> Result<(), BackendError> {
        self.finish("ROLLBACK")
    }
}

impl DirectSqliteWrite {
    fn conn(&self) -> Result<&Connection, BackendError> {
        self.conn
            .as_ref()
            .ok_or_else(|| BackendError::Io("direct sqlite write is closed".to_string()))
    }

    fn finish(&mut self, sql: &str) -> Result<(), BackendError> {
        let Some(conn) = self.conn.take() else {
            return Ok(());
        };
        let result = direct_execute_cached(&conn, sql);
        if result.is_ok() {
            if let Ok(mut pool) = self.write_pool.lock() {
                pool.push(conn);
            }
        }
        result
    }
}

impl Drop for DirectSqliteWrite {
    fn drop(&mut self) {
        let _ = self.finish("ROLLBACK");
    }
}

fn direct_initialize_database(path: &Path) -> Result<(), BackendError> {
    let conn = direct_open_connection(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS lix_internal_entries (
            key BLOB NOT NULL,
            value BLOB NOT NULL,
            PRIMARY KEY (key)
        ) WITHOUT ROWID;",
    )
    .map_err(sqlite_error)
}

fn direct_open_connection(path: &Path) -> Result<Connection, BackendError> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(sqlite_error)?;
    conn.execute_batch(
        "PRAGMA synchronous = NORMAL;
         PRAGMA temp_store = MEMORY;
         PRAGMA cache_size = -20000;
         PRAGMA mmap_size = 268435456;
         PRAGMA wal_autocheckpoint = 10000;",
    )
    .map_err(sqlite_error)?;
    Ok(conn)
}

fn direct_pin_read_snapshot(conn: &Connection) -> Result<(), BackendError> {
    let mut stmt = conn
        .prepare_cached("SELECT COUNT(*) FROM lix_internal_entries")
        .map_err(sqlite_error)?;
    let _: i64 = stmt.query_row([], |row| row.get(0)).map_err(sqlite_error)?;
    Ok(())
}

fn direct_execute_cached(conn: &Connection, sql: &str) -> Result<(), BackendError> {
    let mut stmt = conn.prepare_cached(sql).map_err(sqlite_error)?;
    stmt.execute([]).map_err(sqlite_error)?;
    Ok(())
}

fn direct_visit_keys<V>(
    conn: &Connection,
    keys: &[Key],
    opts: GetOptions<'_>,
    visitor: &mut V,
) -> Result<(), BackendError>
where
    V: PointVisitor + ?Sized,
{
    if keys.is_empty() {
        return Ok(());
    }

    let mut placeholders = String::with_capacity(keys.len() * 8);
    let mut values = Vec::with_capacity(keys.len() * 2);
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            placeholders.push_str(", ");
        }
        placeholders.push_str("(?, ?)");
        values.push(SqlValue::Integer(index as i64));
        values.push(SqlValue::Blob(key.0.to_vec()));
    }
    let sql = format!(
        "WITH requested(ord, key) AS (VALUES {placeholders})
         SELECT r.ord, e.value
         FROM requested r
         LEFT JOIN lix_internal_entries e ON e.key = r.key
         ORDER BY r.ord ASC"
    );

    let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(values))
        .map_err(sqlite_error)?;
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let index: i64 = row.get(0).map_err(sqlite_error)?;
        let index = usize::try_from(index).map_err(|_| {
            BackendError::Corruption(format!(
                "direct sqlite requested ordinal was negative: {index}"
            ))
        })?;
        let Some(key) = keys.get(index) else {
            return Err(BackendError::Corruption(format!(
                "direct sqlite requested ordinal out of bounds: {index}"
            )));
        };
        let value_ref = row.get_ref(1).map_err(sqlite_error)?;
        let value = match value_ref {
            SqlValueRef::Null => None,
            SqlValueRef::Blob(value) => Some(match opts.projection {
                CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
                CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
            }),
            other => {
                return Err(BackendError::Corruption(format!(
                    "direct sqlite value column was not a blob: {other:?}"
                )));
            }
        };
        visitor.visit(index, key, value)?;
    }
    Ok(())
}

fn direct_scan_sql(
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<(String, Vec<SqlValue>), BackendError> {
    let mut sql = match opts.projection {
        CoreProjection::KeyOnly => String::from("SELECT key FROM lix_internal_entries WHERE 1 = 1"),
        CoreProjection::FullValue => {
            String::from("SELECT key, value FROM lix_internal_entries WHERE 1 = 1")
        }
    };
    let mut values = Vec::new();

    direct_append_bound_sql(&mut sql, &mut values, "key", ">=", ">", &range.lower);
    direct_append_bound_sql(&mut sql, &mut values, "key", "<=", "<", &range.upper);
    if let Some(resume_after) = opts.resume_after {
        sql.push_str(" AND key > ?");
        values.push(SqlValue::Blob(resume_after.0.to_vec()));
    }
    sql.push_str(" ORDER BY key ASC");
    Ok((sql, values))
}

fn direct_append_bound_sql(
    sql: &mut String,
    values: &mut Vec<SqlValue>,
    column: &str,
    included_op: &str,
    excluded_op: &str,
    bound: &Bound<Key>,
) {
    match bound {
        Bound::Included(key) => {
            sql.push_str(" AND ");
            sql.push_str(column);
            sql.push(' ');
            sql.push_str(included_op);
            sql.push_str(" ?");
            values.push(SqlValue::Blob(key.0.to_vec()));
        }
        Bound::Excluded(key) => {
            sql.push_str(" AND ");
            sql.push_str(column);
            sql.push(' ');
            sql.push_str(excluded_op);
            sql.push_str(" ?");
            values.push(SqlValue::Blob(key.0.to_vec()));
        }
        Bound::Unbounded => {}
    }
}

fn direct_blob_ref<'a>(value: SqlValueRef<'a>, column: &str) -> Result<&'a [u8], BackendError> {
    match value {
        SqlValueRef::Blob(bytes) => Ok(bytes),
        other => Err(BackendError::Corruption(format!(
            "direct sqlite {column} column was not a blob: {other:?}"
        ))),
    }
}

fn direct_visit_pending_row<V>(
    row: DirectSqlitePendingRow,
    projection: CoreProjection,
    visitor: &mut V,
) -> Result<(), BackendError>
where
    V: ScanVisitor + ?Sized,
{
    match projection {
        CoreProjection::KeyOnly => {
            visitor.visit(KeyRef(row.key.as_slice()), ProjectedValueRef::KeyOnly)
        }
        CoreProjection::FullValue => {
            let value = row.value.as_deref().ok_or_else(|| {
                BackendError::Io("direct sqlite pending row missing value".to_string())
            })?;
            visitor.visit(
                KeyRef(row.key.as_slice()),
                ProjectedValueRef::FullValue(value),
            )
        }
    }
}

fn sqlite_error(error: rusqlite::Error) -> BackendError {
    BackendError::Io(error.to_string())
}

fn ignore_no_transaction(error: BackendError) -> Result<(), BackendError> {
    match error {
        BackendError::Io(message) if message.contains("no transaction") => Ok(()),
        other => Err(other),
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
