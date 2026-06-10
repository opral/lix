use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lix_engine::backend::{
    Backend, BackendError, BackendRangeScan, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetOptions, Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch,
    ReadOptions, ScanOptions, ScanResult, ScanVisitor, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use rusqlite::types::{Value as SqlValue, ValueRef as SqlValueRef};
use rusqlite::{Connection, Rows, params};
use tempfile::TempDir;

pub const SQLITE_FORMAT_VERSION: u32 = 1;
const ENTRIES_TABLE: &str = "lix_internal_entries";
/// Keys per point-read chunk; each key binds 2 parameters (ordinal + key),
/// so a full chunk uses 800 of SQLite's historical 999-parameter floor.
/// The specific value is bench-chosen.
const POINT_READ_CHUNK_KEYS: usize = 400;
/// Rows per multi-row upsert statement; each row binds 2 parameters
/// (key + value), so a full chunk uses 256 parameters. Bench-chosen.
const PUT_CHUNK_ROWS: usize = 128;
const _: () = assert!(POINT_READ_CHUNK_KEYS * 2 < 999);
const _: () = assert!(PUT_CHUNK_ROWS * 2 < 999);

#[derive(Debug)]
pub struct SqliteBackendFactory {
    temp_dir: TempDir,
    next_database_id: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct SqliteBackendFixture {
    path: PathBuf,
}

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct SqliteBackend {
    path: PathBuf,
    read_pool: Arc<Mutex<Vec<Connection>>>,
    write_pool: Arc<Mutex<Vec<Connection>>>,
}

#[derive(Clone, Debug)]
pub struct SqliteBackendOptions {
    pub path: PathBuf,
}

#[expect(missing_debug_implementations)]
pub struct SqliteRead {
    conn: Option<Connection>,
    read_pool: Arc<Mutex<Vec<Connection>>>,
}

#[expect(missing_debug_implementations)]
pub struct SqliteRangeScan<'stmt> {
    rows: Rows<'stmt>,
    projection: CoreProjection,
    pending: Option<SqlitePendingRow>,
    done: bool,
}

struct SqlitePendingRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

#[expect(missing_debug_implementations)]
pub struct SqliteWrite {
    conn: Option<Connection>,
    write_pool: Arc<Mutex<Vec<Connection>>>,
    stats: WriteStats,
}

impl Default for SqliteBackendFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteBackendFactory {
    pub fn new() -> Self {
        Self {
            temp_dir: tempfile::tempdir().expect("create sqlite backend temp dir"),
            next_database_id: AtomicU64::new(0),
        }
    }
}

impl BackendFactory for SqliteBackendFactory {
    type Backend = SqliteBackend;
    type Fixture = SqliteBackendFixture;

    fn create_fixture(&self) -> Self::Fixture {
        let database_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        let path = self
            .temp_dir
            .path()
            .join(format!("backend-{database_id}.sqlite"));
        SqliteBackendFixture { path }
    }

    fn config(&self) -> BackendTestConfig {
        BackendTestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendTestConfig::default()
        }
    }
}

impl BackendFixture for SqliteBackendFixture {
    type Backend = SqliteBackend;

    fn open(&self) -> Self::Backend {
        SqliteBackend::open(&self.path).expect("open sqlite backend")
    }
}

impl SqliteBackend {
    pub fn new(options: SqliteBackendOptions) -> Result<Self, BackendError> {
        Self::open(options.path)
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        // Warm one connection per pool at open so the first read and write
        // do not pay connection setup (open + busy_timeout + pragmas) inside
        // their own latency window. The init connection becomes the warm
        // write connection.
        let write_conn = initialize_database(&path)?;
        let read_conn = open_connection(&path)?;
        Ok(Self {
            path,
            read_pool: Arc::new(Mutex::new(vec![read_conn])),
            write_pool: Arc::new(Mutex::new(vec![write_conn])),
        })
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn format_version(&self) -> Result<u32, BackendError> {
        let conn = self.connect()?;
        sqlite_user_version(&conn)
    }

    pub fn busy_timeout_ms(&self) -> Result<i64, BackendError> {
        let conn = self.connect()?;
        conn.pragma_query_value(None, "busy_timeout", |row| row.get::<_, i64>(0))
            .map_err(sqlite_error)
    }

    #[allow(dead_code)]
    pub fn checkpoint(&self) -> Result<(), BackendError> {
        let conn = self.connect()?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .map_err(sqlite_error)
    }

    fn connect(&self) -> Result<Connection, BackendError> {
        open_connection(&self.path)
    }
}

impl Backend for SqliteBackend {
    type Read<'a>
        = SqliteRead
    where
        Self: 'a;

    type Write<'a>
        = SqliteWrite
    where
        Self: 'a;
    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        let conn = self
            .read_pool
            .lock()
            .map_err(|error| BackendError::Io(format!("sqlite read pool poisoned: {error}")))?
            .pop()
            .map(Ok)
            .unwrap_or_else(|| self.connect())?;
        execute_cached(&conn, "BEGIN DEFERRED TRANSACTION")?;
        pin_read_snapshot(&conn)?;
        Ok(SqliteRead {
            conn: Some(conn),
            read_pool: Arc::clone(&self.read_pool),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        let conn = self
            .write_pool
            .lock()
            .map_err(|error| BackendError::Io(format!("sqlite write pool poisoned: {error}")))?
            .pop()
            .map(Ok)
            .unwrap_or_else(|| self.connect())?;
        execute_cached(&conn, "BEGIN IMMEDIATE TRANSACTION")?;
        Ok(SqliteWrite {
            conn: Some(conn),
            write_pool: Arc::clone(&self.write_pool),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for SqliteRead {
    type RangeScan<'a> = SqliteRangeScan<'a>;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        visit_keys(self.conn()?, keys, opts, visitor)
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
        let (sql, values) = scan_sql(range, opts);
        let mut stmt = self.conn()?.prepare_cached(&sql).map_err(sqlite_error)?;
        let rows = stmt
            .query(rusqlite::params_from_iter(values))
            .map_err(sqlite_error)?;
        let mut cursor = SqliteRangeScan {
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

impl BackendRangeScan for SqliteRangeScan<'_> {
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
                visit_sqlite_pending_row(pending, self.projection, visitor)?;
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
            let key = blob_ref(row.get_ref(0).map_err(sqlite_error)?, "key")?;
            match self.projection {
                CoreProjection::KeyOnly => {
                    visitor.visit(KeyRef(key), ProjectedValueRef::KeyOnly)?;
                }
                CoreProjection::FullValue => {
                    let value = blob_ref(row.get_ref(1).map_err(sqlite_error)?, "value")?;
                    visitor.visit(KeyRef(key), ProjectedValueRef::FullValue(value))?;
                }
            }
            emitted += 1;
        }

        let has_more = self.ensure_pending()?;
        Ok(ScanResult { emitted, has_more })
    }
}

impl SqliteRangeScan<'_> {
    fn ensure_pending(&mut self) -> Result<bool, BackendError> {
        if self.pending.is_some() {
            return Ok(true);
        }
        let Some(row) = self.rows.next().map_err(sqlite_error)? else {
            self.done = true;
            return Ok(false);
        };
        let key = blob_ref(row.get_ref(0).map_err(sqlite_error)?, "key")?.to_vec();
        let value = if matches!(self.projection, CoreProjection::FullValue) {
            Some(blob_ref(row.get_ref(1).map_err(sqlite_error)?, "value")?.to_vec())
        } else {
            None
        };
        self.pending = Some(SqlitePendingRow { key, value });
        Ok(true)
    }
}

fn visit_sqlite_pending_row<V>(
    row: SqlitePendingRow,
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
            let value = row
                .value
                .as_deref()
                .ok_or_else(|| BackendError::Io("sqlite pending row missing value".to_string()))?;
            visitor.visit(
                KeyRef(row.key.as_slice()),
                ProjectedValueRef::FullValue(value),
            )
        }
    }
}

impl SqliteRead {
    fn conn(&self) -> Result<&Connection, BackendError> {
        self.conn
            .as_ref()
            .ok_or_else(|| BackendError::Io("sqlite read is closed".to_string()))
    }

    fn finish(&mut self) -> Result<(), BackendError> {
        let Some(conn) = self.conn.take() else {
            return Ok(());
        };
        let result = execute_cached(&conn, "ROLLBACK").or_else(ignore_no_transaction);
        if result.is_ok() {
            if let Ok(mut pool) = self.read_pool.lock() {
                pool.push(conn);
            }
        }
        result
    }
}

impl Drop for SqliteRead {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

impl BackendWrite for SqliteWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        let mut entries = entries.entries;
        // Sorting keeps the upsert's conflict probe on a near-neighbor B-tree
        // descent instead of a fresh root-to-leaf seek per row. Batches hold
        // at most one mutation per key, so apply order does not matter.
        entries.sort_unstable_by(|left, right| left.key.0.cmp(&right.key.0));
        debug_assert!(
            entries.windows(2).all(|pair| pair[0].key != pair[1].key),
            "put batches must hold at most one mutation per key"
        );
        let put_entries = entries.len() as u64;
        let written_bytes = entries
            .iter()
            .map(|entry| entry.value.bytes.len() as u64)
            .sum::<u64>();
        {
            let conn = self.conn();
            let mut chunks = entries.chunks_exact(PUT_CHUNK_ROWS);
            if chunks.len() > 0 {
                let sql = multi_upsert_sql(PUT_CHUNK_ROWS);
                let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
                for chunk in chunks.by_ref() {
                    for (index, entry) in chunk.iter().enumerate() {
                        stmt.raw_bind_parameter(2 * index + 1, &entry.key.0[..])
                            .map_err(sqlite_error)?;
                        stmt.raw_bind_parameter(2 * index + 2, &entry.value.bytes[..])
                            .map_err(sqlite_error)?;
                    }
                    stmt.raw_execute().map_err(sqlite_error)?;
                }
            }
            // The remainder reuses the single-row statement instead of a
            // sized multi-row one so the prepared-statement cache holds two
            // upsert shapes total rather than one per remainder length.
            let remainder = chunks.remainder();
            if !remainder.is_empty() {
                let mut stmt = conn
                    .prepare_cached(
                        "INSERT INTO lix_internal_entries(key, value)
                         VALUES (?1, ?2)
                         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    )
                    .map_err(sqlite_error)?;
                for entry in remainder {
                    stmt.execute(params![entry.key.0.as_ref(), entry.value.bytes.as_ref()])
                        .map_err(sqlite_error)?;
                }
            }
        }
        self.stats.put_entries += put_entries;
        self.stats.written_bytes += written_bytes;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        {
            let conn = self.conn();
            let mut stmt = conn
                .prepare_cached("DELETE FROM lix_internal_entries WHERE key = ?1")
                .map_err(sqlite_error)?;

            for key in keys {
                stmt.execute(params![key.0.as_ref()])
                    .map_err(sqlite_error)?;
            }
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        let mut sql = String::from("DELETE FROM lix_internal_entries WHERE 1 = 1");
        let mut values = Vec::new();
        append_bound_sql(&mut sql, &mut values, "key", ">=", ">", &range.lower);
        append_bound_sql(&mut sql, &mut values, "key", "<=", "<", &range.upper);
        let deleted = self
            .conn()
            .execute(&sql, rusqlite::params_from_iter(values))
            .map_err(sqlite_error)?;
        self.stats.deleted_entries += deleted as u64;
        self.stats.deleted_ranges += 1;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(mut self) -> Result<CommitResult, BackendError> {
        self.finish("COMMIT")?;
        Ok(CommitResult {
            commit_id: None,
            stats: std::mem::take(&mut self.stats),
        })
    }

    fn rollback(mut self) -> Result<(), BackendError> {
        self.finish("ROLLBACK")
    }
}

impl SqliteWrite {
    fn conn(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("sqlite write connection should be available")
    }

    fn finish(&mut self, sql: &str) -> Result<(), BackendError> {
        let Some(conn) = self.conn.take() else {
            return Ok(());
        };
        let result = execute_cached(&conn, sql);
        if result.is_ok() {
            if let Ok(mut pool) = self.write_pool.lock() {
                pool.push(conn);
            }
        }
        result
    }
}

impl Drop for SqliteWrite {
    fn drop(&mut self) {
        let _ = self.finish("ROLLBACK");
    }
}

fn initialize_database(path: &Path) -> Result<Connection, BackendError> {
    let conn = open_connection(path)?;
    let user_version = sqlite_user_version(&conn)?;
    if user_version > SQLITE_FORMAT_VERSION {
        return Err(BackendError::Io(format!(
            "sqlite backend format version {user_version} is newer than supported version {SQLITE_FORMAT_VERSION}"
        )));
    }

    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS lix_internal_entries (
            key BLOB NOT NULL,
            value BLOB NOT NULL,
            PRIMARY KEY (key)
        ) WITHOUT ROWID;",
    )
    .map_err(sqlite_error)?;

    if user_version == 0 {
        conn.pragma_update(None, "user_version", SQLITE_FORMAT_VERSION)
            .map_err(sqlite_error)?;
    }

    Ok(conn)
}

fn open_connection(path: &Path) -> Result<Connection, BackendError> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
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

fn pin_read_snapshot(conn: &Connection) -> Result<(), BackendError> {
    let sql = format!("SELECT key FROM {ENTRIES_TABLE} LIMIT 1");
    let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
    let mut rows = stmt.query([]).map_err(sqlite_error)?;
    let _ = rows.next().map_err(sqlite_error)?;
    Ok(())
}

fn sqlite_user_version(conn: &Connection) -> Result<u32, BackendError> {
    let value = conn
        .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
        .map_err(sqlite_error)?;
    u32::try_from(value)
        .map_err(|_| BackendError::Corruption(format!("sqlite user_version was negative: {value}")))
}

fn execute_cached(conn: &Connection, sql: &str) -> Result<(), BackendError> {
    let mut stmt = conn.prepare_cached(sql).map_err(sqlite_error)?;
    stmt.execute([]).map_err(sqlite_error)?;
    Ok(())
}

fn visit_keys<V>(
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

    visit_points(conn, keys, opts.projection, visitor)
}

fn visit_points<V>(
    conn: &Connection,
    keys: &[Key],
    projection: CoreProjection,
    visitor: &mut V,
) -> Result<(), BackendError>
where
    V: PointVisitor + ?Sized,
{
    for (chunk_index, chunk) in keys.chunks(POINT_READ_CHUNK_KEYS).enumerate() {
        visit_points_chunk(
            conn,
            keys,
            chunk_index * POINT_READ_CHUNK_KEYS,
            chunk,
            projection,
            visitor,
        )?;
    }
    Ok(())
}

#[expect(clippy::cast_possible_wrap)]
fn visit_points_chunk<V>(
    conn: &Connection,
    keys: &[Key],
    offset: usize,
    chunk: &[Key],
    projection: CoreProjection,
    visitor: &mut V,
) -> Result<(), BackendError>
where
    V: PointVisitor + ?Sized,
{
    // No ORDER BY: callers address results by the returned ordinal, never by
    // visit order, and binding by reference avoids an owned copy of every key.
    let projected_column = match projection {
        CoreProjection::KeyOnly => "e.key",
        CoreProjection::FullValue => "e.value",
    };
    let sql = format!(
        "WITH requested(ord, key) AS (VALUES {placeholders})
         SELECT r.ord, {projected_column}
         FROM requested r
         LEFT JOIN lix_internal_entries e ON e.key = r.key",
        placeholders = point_chunk_placeholders(chunk.len()),
    );

    let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
    for (index, key) in chunk.iter().enumerate() {
        stmt.raw_bind_parameter(2 * index + 1, (offset + index) as i64)
            .map_err(sqlite_error)?;
        stmt.raw_bind_parameter(2 * index + 2, &key.0[..])
            .map_err(sqlite_error)?;
    }
    let mut rows = stmt.raw_query();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let index: i64 = row.get(0).map_err(sqlite_error)?;
        let index = usize::try_from(index).map_err(|_| {
            BackendError::Corruption(format!("sqlite requested ordinal was negative: {index}"))
        })?;
        let Some(key) = keys.get(index) else {
            return Err(BackendError::Corruption(format!(
                "sqlite requested ordinal out of bounds: {index}"
            )));
        };
        let projected = row.get_ref(1).map_err(sqlite_error)?;
        let value = match (projection, projected) {
            (_, SqlValueRef::Null) => None,
            (CoreProjection::KeyOnly, SqlValueRef::Blob(_)) => Some(ProjectedValueRef::KeyOnly),
            (CoreProjection::FullValue, SqlValueRef::Blob(value)) => {
                Some(ProjectedValueRef::FullValue(value))
            }
            (_, other) => {
                return Err(BackendError::Corruption(format!(
                    "sqlite projected column was not a blob: {other:?}"
                )));
            }
        };
        visitor.visit(index, key, value)?;
    }
    Ok(())
}

fn multi_upsert_sql(rows: usize) -> String {
    let mut sql = String::with_capacity(64 + rows * 8);
    sql.push_str("INSERT INTO lix_internal_entries(key, value) VALUES ");
    for index in 0..rows {
        if index > 0 {
            sql.push_str(", ");
        }
        sql.push_str("(?, ?)");
    }
    sql.push_str(" ON CONFLICT(key) DO UPDATE SET value = excluded.value");
    sql
}

fn point_chunk_placeholders(len: usize) -> String {
    let mut placeholders = String::with_capacity(len * 8);
    for index in 0..len {
        if index > 0 {
            placeholders.push_str(", ");
        }
        placeholders.push_str("(?, ?)");
    }
    placeholders
}

fn scan_sql(range: KeyRange, opts: ScanOptions<'_>) -> (String, Vec<SqlValue>) {
    let mut sql = match opts.projection {
        CoreProjection::KeyOnly => String::from("SELECT key FROM lix_internal_entries WHERE 1 = 1"),
        CoreProjection::FullValue => {
            String::from("SELECT key, value FROM lix_internal_entries WHERE 1 = 1")
        }
    };
    let mut values = Vec::new();

    append_bound_sql(&mut sql, &mut values, "key", ">=", ">", &range.lower);
    append_bound_sql(&mut sql, &mut values, "key", "<=", "<", &range.upper);
    if let Some(resume_after) = opts.resume_after {
        sql.push_str(" AND key > ?");
        values.push(SqlValue::Blob(resume_after.0.to_vec()));
    }
    sql.push_str(" ORDER BY key ASC");
    (sql, values)
}

fn append_bound_sql(
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

fn blob_ref<'a>(value: SqlValueRef<'a>, column: &str) -> Result<&'a [u8], BackendError> {
    match value {
        SqlValueRef::Blob(bytes) => Ok(bytes),
        other => Err(BackendError::Corruption(format!(
            "sqlite {column} column was not a blob: {other:?}"
        ))),
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
