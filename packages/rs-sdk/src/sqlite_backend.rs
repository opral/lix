use std::collections::VecDeque;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendError, BackendRangeScan, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetOptions, Key, KeyRange, KeyRef, PointVisitor, ProjectedValueRef, PutBatch,
    ReadOptions, ScanOptions, ScanResult, ScanVisitor, StoredValue, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use rusqlite::types::{Value as SqlValue, ValueRef as SqlValueRef};
use rusqlite::{params, Connection};
use tempfile::TempDir;

pub const SQLITE_FORMAT_VERSION: u32 = 1;

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
pub struct SqliteBackend {
    path: PathBuf,
    write_pool: Arc<Mutex<Vec<Connection>>>,
}

pub struct SqliteBackendOptions {
    pub path: PathBuf,
}

#[derive(Clone)]
pub struct SqliteRead {
    state: Arc<Mutex<SqliteReadState>>,
}

struct SqliteReadState {
    conn: Option<Connection>,
}

pub struct SqliteRangeScan {
    rows: VecDeque<SqlitePendingRow>,
    projection: CoreProjection,
    pending: Option<SqlitePendingRow>,
    done: bool,
}

struct SqlitePendingRow {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

pub struct SqliteWrite {
    conn: Option<Connection>,
    write_pool: Arc<Mutex<Vec<Connection>>>,
    stats: WriteStats,
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
        initialize_database(&path)?;
        Ok(Self {
            path,
            write_pool: Arc::new(Mutex::new(Vec::new())),
        })
    }

    #[allow(dead_code)]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[allow(dead_code)]
    pub fn checkpoint(&self) -> Result<(), BackendError> {
        let conn = self.connect()?;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .map_err(sqlite_error)
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
        let conn = self.connect()?;
        execute_cached(&conn, "BEGIN DEFERRED TRANSACTION")?;
        pin_read_snapshot(&conn)?;
        Ok(SqliteRead {
            state: Arc::new(Mutex::new(SqliteReadState { conn: Some(conn) })),
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
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")
            .map_err(sqlite_error)?;
        Ok(SqliteWrite {
            conn: Some(conn),
            write_pool: Arc::clone(&self.write_pool),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for SqliteRead {
    type RangeScan<'a> = SqliteRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        let state = self
            .state
            .lock()
            .map_err(|error| BackendError::Io(format!("sqlite read state poisoned: {error}")))?;
        let conn = state
            .conn
            .as_ref()
            .ok_or_else(|| BackendError::Io("sqlite read is closed".to_string()))?;
        visit_keys(conn, keys, opts, visitor)
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
        let rows = {
            let state = self.state.lock().map_err(|error| {
                BackendError::Io(format!("sqlite read state poisoned: {error}"))
            })?;
            let conn = state
                .conn
                .as_ref()
                .ok_or_else(|| BackendError::Io("sqlite read is closed".to_string()))?;
            collect_range_rows(conn, range, opts)?
        };
        let mut cursor = SqliteRangeScan {
            rows,
            projection: opts.projection,
            pending: None,
            done: opts.limit_rows == 0,
        };
        f(&mut cursor)
    }

    fn close(self) -> Result<(), BackendError> {
        self.finish()
    }
}

impl BackendRangeScan for SqliteRangeScan {
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

            let Some(row) = self.rows.pop_front() else {
                self.done = true;
                return Ok(ScanResult {
                    emitted,
                    has_more: false,
                });
            };
            visit_sqlite_pending_row(row, self.projection, visitor)?;
            emitted += 1;
        }

        let has_more = self.ensure_pending()?;
        Ok(ScanResult { emitted, has_more })
    }
}

impl SqliteRangeScan {
    fn ensure_pending(&mut self) -> Result<bool, BackendError> {
        if self.pending.is_some() {
            return Ok(true);
        }
        let Some(row) = self.rows.pop_front() else {
            self.done = true;
            return Ok(false);
        };
        self.pending = Some(row);
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
    fn finish(&self) -> Result<(), BackendError> {
        if Arc::strong_count(&self.state) > 1 {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|error| BackendError::Io(format!("sqlite read state poisoned: {error}")))?;
        let Some(conn) = state.conn.take() else {
            return Ok(());
        };
        execute_cached(&conn, "ROLLBACK").or_else(ignore_no_transaction)
    }
}

impl Drop for SqliteRead {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

impl BackendWrite for SqliteWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        let mut put_entries = 0;
        let mut written_bytes = 0;
        {
            let conn = self.conn();
            let mut stmt = conn
                .prepare(
                    "INSERT INTO lix_entries(key, value)
                     VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                )
                .map_err(sqlite_error)?;

            for entry in entries.entries {
                let value = stored_value_bytes(entry.value);
                put_entries += 1;
                written_bytes += value.len() as u64;
                stmt.execute(params![entry.key.0.as_ref(), value.as_ref()])
                    .map_err(sqlite_error)?;
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
                .prepare("DELETE FROM lix_entries WHERE key = ?1")
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
        let mut sql = String::from("DELETE FROM lix_entries WHERE 1 = 1");
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

fn initialize_database(path: &Path) -> Result<(), BackendError> {
    let conn = open_connection(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    let user_version = sqlite_user_version(&conn)?;
    if user_version > SQLITE_FORMAT_VERSION {
        return Err(BackendError::Io(format!(
            "sqlite file format version {user_version} is newer than supported version {SQLITE_FORMAT_VERSION}"
        )));
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS lix_entries (
            key BLOB NOT NULL,
            value BLOB NOT NULL,
            PRIMARY KEY (key)
        ) WITHOUT ROWID;",
    )
    .map_err(sqlite_error)?;
    conn.pragma_update(None, "user_version", SQLITE_FORMAT_VERSION)
        .map_err(sqlite_error)?;
    Ok(())
}

fn open_connection(path: &Path) -> Result<Connection, BackendError> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(sqlite_error)?;
    Ok(conn)
}

fn sqlite_user_version(conn: &Connection) -> Result<u32, BackendError> {
    conn.pragma_query_value(None, "user_version", |row| row.get::<_, u32>(0))
        .map_err(sqlite_error)
}

fn pin_read_snapshot(conn: &Connection) -> Result<(), BackendError> {
    let mut stmt = conn
        .prepare_cached("SELECT COUNT(*) FROM lix_entries")
        .map_err(sqlite_error)?;
    let _: i64 = stmt.query_row([], |row| row.get(0)).map_err(sqlite_error)?;
    Ok(())
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
         LEFT JOIN lix_entries e ON e.key = r.key
         ORDER BY r.ord ASC"
    );

    let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(values))
        .map_err(sqlite_error)?;
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
        let value_ref = row.get_ref(1).map_err(sqlite_error)?;
        let value = match value_ref {
            SqlValueRef::Null => None,
            SqlValueRef::Blob(value) => Some(project_value_ref(value, opts.projection)),
            other => {
                return Err(BackendError::Corruption(format!(
                    "sqlite value column was not a blob: {other:?}"
                )));
            }
        };
        visitor.visit(index, key, value)?;
    }
    Ok(())
}

fn collect_range_rows(
    conn: &Connection,
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<VecDeque<SqlitePendingRow>, BackendError> {
    let (sql, values) = scan_sql(range, opts)?;
    let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(values))
        .map_err(sqlite_error)?;
    let mut collected = VecDeque::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let key = blob_ref(row.get_ref(0).map_err(sqlite_error)?, "key")?.to_vec();
        let value = if matches!(opts.projection, CoreProjection::FullValue) {
            Some(blob_ref(row.get_ref(1).map_err(sqlite_error)?, "value")?.to_vec())
        } else {
            None
        };
        collected.push_back(SqlitePendingRow { key, value });
    }
    Ok(collected)
}

fn scan_sql(
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<(String, Vec<SqlValue>), BackendError> {
    let mut sql = match opts.projection {
        CoreProjection::KeyOnly => String::from("SELECT key FROM lix_entries WHERE 1 = 1"),
        CoreProjection::FullValue => String::from("SELECT key, value FROM lix_entries WHERE 1 = 1"),
    };
    let mut values = Vec::new();

    append_bound_sql(&mut sql, &mut values, "key", ">=", ">", &range.lower);
    append_bound_sql(&mut sql, &mut values, "key", "<=", "<", &range.upper);
    if let Some(resume_after) = opts.resume_after {
        sql.push_str(" AND key > ?");
        values.push(SqlValue::Blob(resume_after.0.to_vec()));
    }
    sql.push_str(" ORDER BY key ASC");
    Ok((sql, values))
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

fn project_value_ref(value: &[u8], projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn stored_value_bytes(value: StoredValue) -> Bytes {
    value.bytes
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
