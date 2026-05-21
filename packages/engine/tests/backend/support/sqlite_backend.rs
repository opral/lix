use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendCapabilities, BackendError, BackendRangeScan, BackendRead, BackendWrite,
    CommitResult, CoreProjection, DurableWriteLock, GetOptions, Key, KeyRange, KeyRef,
    PointVisitor, ProjectedValueRef, PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor,
    StoredValue, WriteConcurrency, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use rusqlite::types::{Value as SqlValue, ValueRef as SqlValueRef};
use rusqlite::{params, Connection, Rows};
use tempfile::TempDir;

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
    read_pool: Arc<Mutex<Vec<Connection>>>,
    write_pool: Arc<Mutex<Vec<Connection>>>,
    durable_write_lock: DurableWriteLock,
}

pub struct SqliteRead {
    conn: Option<Connection>,
    read_pool: Arc<Mutex<Vec<Connection>>>,
}

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
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        let durable_write_lock = durable_write_lock_for_path(&path);
        initialize_database(&path)?;
        Ok(Self {
            path,
            read_pool: Arc::new(Mutex::new(Vec::new())),
            write_pool: Arc::new(Mutex::new(Vec::new())),
            durable_write_lock,
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

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::v0(WriteConcurrency::SingleWriter)
    }

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
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")
            .map_err(sqlite_error)?;
        Ok(SqliteWrite {
            conn: Some(conn),
            write_pool: Arc::clone(&self.write_pool),
            stats: WriteStats::default(),
        })
    }

    fn durable_write_lock(&self) -> DurableWriteLock {
        self.durable_write_lock.clone()
    }
}

fn durable_write_lock_for_path(path: &Path) -> DurableWriteLock {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, DurableWriteLock>>> = OnceLock::new();
    let key = canonical_lock_key(path);
    let locks = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut locks = locks
        .lock()
        .expect("sqlite durable write lock registry should not poison");
    if let Some(lock) = locks.get(&key) {
        return lock.clone();
    }
    let lock = DurableWriteLock::new();
    locks.insert(key, lock.clone());
    lock
}

fn canonical_lock_key(path: &Path) -> PathBuf {
    if let Ok(path) = path.canonicalize() {
        return path;
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .expect("current directory should be available")
            .join(path)
    };
    let Some(parent) = absolute.parent() else {
        return absolute;
    };
    let Ok(parent) = parent.canonicalize() else {
        return absolute;
    };
    match absolute.file_name() {
        Some(file_name) => parent.join(file_name),
        None => parent,
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
        visit_keys(self.conn(), keys, opts, visitor)
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
        let (sql, values) = scan_sql(range, opts)?;
        let mut stmt = self.conn().prepare_cached(&sql).map_err(sqlite_error)?;
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
                    visitor.visit(KeyRef(key), ProjectedValueRef::KeyOnly)?
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
    fn conn(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("sqlite read connection is present")
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
        let mut put_entries = 0;
        let mut written_bytes = 0;
        {
            let conn = self.conn();
            let mut stmt = conn
                .prepare(
                    "INSERT INTO entries(key, value)
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
                .prepare("DELETE FROM entries WHERE key = ?1")
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
        let mut sql = String::from("DELETE FROM entries WHERE 1 = 1");
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
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS entries (
            key BLOB NOT NULL,
            value BLOB NOT NULL,
            PRIMARY KEY (key)
        ) WITHOUT ROWID;",
    )
    .map_err(sqlite_error)
}

fn open_connection(path: &Path) -> Result<Connection, BackendError> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(sqlite_error)?;
    Ok(conn)
}

fn pin_read_snapshot(conn: &Connection) -> Result<(), BackendError> {
    let mut stmt = conn
        .prepare_cached("SELECT COUNT(*) FROM entries")
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
         LEFT JOIN entries e ON e.key = r.key
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

fn scan_sql(
    range: KeyRange,
    opts: ScanOptions<'_>,
) -> Result<(String, Vec<SqlValue>), BackendError> {
    let mut sql = match opts.projection {
        CoreProjection::KeyOnly => String::from("SELECT key FROM entries WHERE 1 = 1"),
        CoreProjection::FullValue => String::from("SELECT key, value FROM entries WHERE 1 = 1"),
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
