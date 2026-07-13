#![allow(
    clippy::manual_async_fn,
    reason = "explicit future signatures mirror Backend traits and keep Send guarantees visible"
)]

use std::future::Future;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use lix_engine::backend::{
    Backend, BackendError, BackendRead, BackendWrite, CommitResult, CoreProjection, GetManyResult,
    GetOptions, Key, KeyRange, ProjectedValue, PutBatch, PutEntry, ReadEntry, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, WriteOptions, WriteStats,
};
use lix_engine::{BackendFactory, BackendFixture, BackendTestConfig};
use rusqlite::types::ValueRef as SqlValueRef;
use rusqlite::{Connection, params};
use tempfile::TempDir;

/// Format v2: one table per storage space instead of a single interleaved
/// entries table. Hard cut; v1 files are rejected without migration.
pub const SQLITE_FORMAT_VERSION: u32 = 3;
const LEGACY_ENTRIES_TABLE: &str = "lix_internal_entries";
/// Keys per point-read chunk; each key binds 2 parameters (ordinal + key),
/// so a full chunk uses 800 of SQLite's historical 999-parameter floor.
/// The specific value is bench-chosen.
const POINT_READ_CHUNK_KEYS: usize = 400;
/// Rows per multi-row upsert statement; each row binds 2 parameters
/// (key + value), so a full chunk uses 256 parameters. Bench-chosen.
const PUT_CHUNK_ROWS: usize = 128;
const _: () = assert!(POINT_READ_CHUNK_KEYS * 2 < 999);
const _: () = assert!(PUT_CHUNK_ROWS * 2 < 999);

fn space_table(space: SpaceId) -> String {
    format!("s{:08x}", space.0)
}

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
#[allow(missing_debug_implementations)]
pub struct SqliteBackend {
    path: PathBuf,
    read_pool: Arc<Mutex<Vec<Connection>>>,
    write_pool: Arc<Mutex<Vec<Connection>>>,
}

#[derive(Clone, Debug)]
pub struct SqliteBackendOptions {
    pub path: PathBuf,
}

#[allow(missing_debug_implementations)]
pub struct SqliteRead {
    conn: Mutex<Option<Connection>>,
    read_pool: Arc<Mutex<Vec<Connection>>>,
}

#[allow(missing_debug_implementations)]
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

    fn open(&self) -> impl Future<Output = Self::Backend> + Send {
        async move { SqliteBackend::open(&self.path).expect("open sqlite backend") }
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
    fn begin_read(
        &self,
        _opts: ReadOptions,
    ) -> impl Future<Output = Result<Self::Read<'_>, BackendError>> + Send {
        async move {
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
                conn: Mutex::new(Some(conn)),
                read_pool: Arc::clone(&self.read_pool),
            })
        }
    }

    fn begin_write(
        &self,
        _opts: WriteOptions,
    ) -> impl Future<Output = Result<Self::Write<'_>, BackendError>> + Send {
        async move {
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
}

impl BackendRead for SqliteRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> impl Future<Output = Result<GetManyResult, BackendError>> + Send {
        async move {
            if keys.is_empty() {
                return Ok(GetManyResult::new(Vec::new()));
            }
            let conn = self.conn.lock().map_err(|error| {
                BackendError::Io(format!("sqlite read connection poisoned: {error}"))
            })?;
            let conn = conn
                .as_ref()
                .ok_or_else(|| BackendError::Io("sqlite read is closed".to_string()))?;
            let mut values = vec![None; keys.len()];
            if !space_table_exists(conn, space)? {
                return Ok(GetManyResult::new(values));
            }
            for (chunk_index, chunk) in keys.chunks(POINT_READ_CHUNK_KEYS).enumerate() {
                read_points_chunk(
                    conn,
                    space,
                    chunk_index * POINT_READ_CHUNK_KEYS,
                    chunk,
                    opts.projection,
                    &mut values,
                )?;
            }
            Ok(GetManyResult::new(values))
        }
    }

    fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> impl Future<Output = Result<ScanChunk, BackendError>> + Send {
        async move {
            if opts.page_size() == 0 {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }
            let conn = self.conn.lock().map_err(|error| {
                BackendError::Io(format!("sqlite read connection poisoned: {error}"))
            })?;
            let conn = conn
                .as_ref()
                .ok_or_else(|| BackendError::Io("sqlite read is closed".to_string()))?;
            if !space_table_exists(conn, space)? {
                return Ok(ScanChunk {
                    entries: Vec::new(),
                    has_more: false,
                });
            }

            let columns = match opts.projection {
                CoreProjection::KeyOnly => "key",
                CoreProjection::FullValue => "key, value",
            };
            let table = space_table(space);
            let mut sql = format!("SELECT {columns} FROM {table} WHERE 1 = 1");
            let mut binds: Vec<&[u8]> = Vec::with_capacity(3);
            push_bound(
                &mut sql,
                &mut binds,
                "key >",
                opts.resume_after.as_ref().map(|key| key.0.as_ref()),
            );
            push_range_bounds(&mut sql, &mut binds, &range.lower, &range.upper);
            let limit_index = binds.len() + 1;
            sql.push_str(" ORDER BY key ASC LIMIT ?");
            sql.push_str(&limit_index.to_string());
            let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
            for (index, bytes) in binds.iter().enumerate() {
                stmt.raw_bind_parameter(index + 1, *bytes)
                    .map_err(sqlite_error)?;
            }
            let fetch_limit = i64::try_from(opts.page_size().saturating_add(1)).unwrap_or(i64::MAX);
            stmt.raw_bind_parameter(limit_index, fetch_limit)
                .map_err(sqlite_error)?;

            let mut rows = stmt.raw_query();
            let mut entries = Vec::with_capacity(opts.page_size());
            while let Some(row) = rows.next().map_err(sqlite_error)? {
                if entries.len() == opts.page_size() {
                    return Ok(ScanChunk {
                        entries,
                        has_more: true,
                    });
                }
                let key = blob_ref(row.get_ref(0).map_err(sqlite_error)?, "key")?;
                let value = match opts.projection {
                    CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
                    CoreProjection::FullValue => ProjectedValue::FullValue(Bytes::copy_from_slice(
                        blob_ref(row.get_ref(1).map_err(sqlite_error)?, "value")?,
                    )),
                };
                entries.push(ReadEntry {
                    key: Key(Bytes::copy_from_slice(key)),
                    value,
                });
            }
            Ok(ScanChunk {
                entries,
                has_more: false,
            })
        }
    }
}

impl SqliteRead {
    fn finish(&mut self) -> Result<(), BackendError> {
        let conn = self.conn.get_mut().map_err(|error| {
            BackendError::Io(format!("sqlite read connection poisoned: {error}"))
        })?;
        let Some(conn) = conn.take() else {
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
    fn put_many(
        &mut self,
        space: SpaceId,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
            let mut entries = entries.entries;
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
            self.ensure_space_table(space)?;
            self.put_rows(space, &entries)?;
            self.stats.put_entries += put_entries;
            self.stats.written_bytes += written_bytes;
            self.stats.backend_calls += 1;
            Ok(())
        }
    }

    fn delete_many(
        &mut self,
        space: SpaceId,
        keys: &[Key],
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
            if space_table_exists(self.conn(), space)? {
                let table = space_table(space);
                let sql = format!("DELETE FROM {table} WHERE key = ?1");
                let conn = self.conn();
                let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
                for key in keys {
                    stmt.execute(params![key.0.as_ref()])
                        .map_err(sqlite_error)?;
                }
            }
            self.stats.deleted_entries += keys.len() as u64;
            self.stats.backend_calls += 1;
            Ok(())
        }
    }

    fn delete_range(
        &mut self,
        space: SpaceId,
        range: KeyRange,
    ) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move {
            let deleted = if !space_table_exists(self.conn(), space)? {
                0
            } else {
                let table = space_table(space);
                let unbounded = matches!(
                    (&range.lower, &range.upper),
                    (Bound::Unbounded, Bound::Unbounded)
                );
                if unbounded {
                    let sql = format!("DELETE FROM {table}");
                    self.conn().execute(&sql, []).map_err(sqlite_error)?
                } else {
                    let mut sql = format!("DELETE FROM {table} WHERE 1 = 1");
                    let mut binds: Vec<&[u8]> = Vec::with_capacity(2);
                    push_range_bounds(&mut sql, &mut binds, &range.lower, &range.upper);
                    let mut stmt = self.conn().prepare_cached(&sql).map_err(sqlite_error)?;
                    for (index, bytes) in binds.iter().enumerate() {
                        stmt.raw_bind_parameter(index + 1, *bytes)
                            .map_err(sqlite_error)?;
                    }
                    stmt.raw_execute().map_err(sqlite_error)?
                }
            };
            self.stats.deleted_entries += deleted as u64;
            self.stats.deleted_ranges += 1;
            self.stats.backend_calls += 1;
            Ok(())
        }
    }

    fn commit(mut self) -> impl Future<Output = Result<CommitResult, BackendError>> + Send {
        async move {
            self.finish("COMMIT")?;
            Ok(CommitResult {
                commit_id: None,
                stats: std::mem::take(&mut self.stats),
            })
        }
    }

    fn rollback(mut self) -> impl Future<Output = Result<(), BackendError>> + Send {
        async move { self.finish("ROLLBACK") }
    }
}

impl SqliteWrite {
    fn conn(&self) -> &Connection {
        self.conn
            .as_ref()
            .expect("sqlite write connection should be available")
    }

    /// Unconditional CREATE IF NOT EXISTS: a few microseconds of DDL parse
    /// per batch buys freedom from existence caching and its rollback
    /// semantics.
    fn ensure_space_table(&self, space: SpaceId) -> Result<(), BackendError> {
        let table = space_table(space);
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {table} (
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (key)
            ) WITHOUT ROWID"
        );
        self.conn().execute_batch(&sql).map_err(sqlite_error)
    }

    fn put_rows(&self, space: SpaceId, entries: &[PutEntry]) -> Result<(), BackendError> {
        let table = space_table(space);
        let conn = self.conn();
        let mut chunks = entries.chunks_exact(PUT_CHUNK_ROWS);
        if chunks.len() > 0 {
            let sql = multi_upsert_sql(&table, PUT_CHUNK_ROWS);
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
        // The remainder reuses the single-row statement instead of a sized
        // multi-row one so the prepared-statement cache holds two upsert
        // shapes per space rather than one per remainder length.
        let remainder = chunks.remainder();
        if !remainder.is_empty() {
            let sql = format!(
                "INSERT INTO {table}(key, value)
                 VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value"
            );
            let mut stmt = conn.prepare_cached(&sql).map_err(sqlite_error)?;
            for entry in remainder {
                stmt.execute(params![entry.key.0.as_ref(), entry.value.bytes.as_ref()])
                    .map_err(sqlite_error)?;
            }
        }
        Ok(())
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
    // v3 changed the engine value layouts (identity-only tree values,
    // JsonSlot change records); v1 and v2 files cannot be decoded.
    if (1..SQLITE_FORMAT_VERSION).contains(&user_version) || legacy_table_exists(&conn)? {
        return Err(BackendError::Io(format!(
            "sqlite backend format version {user_version} is not supported by version \
             {SQLITE_FORMAT_VERSION}; there is no migration, recreate the database"
        )));
    }

    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    if user_version == 0 {
        conn.pragma_update(None, "user_version", SQLITE_FORMAT_VERSION)
            .map_err(sqlite_error)?;
    }

    Ok(conn)
}

fn legacy_table_exists(conn: &Connection) -> Result<bool, BackendError> {
    let mut stmt = conn
        .prepare_cached("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1")
        .map_err(sqlite_error)?;
    let mut rows = stmt
        .query(params![LEGACY_ENTRIES_TABLE])
        .map_err(sqlite_error)?;
    Ok(rows.next().map_err(sqlite_error)?.is_some())
}

fn open_connection(path: &Path) -> Result<Connection, BackendError> {
    let conn = Connection::open(path).map_err(sqlite_error)?;
    // Statement shapes multiply per space table (scan/upsert/delete/point
    // variants); the default 16-slot cache would thrash.
    conn.set_prepared_statement_cache_capacity(256);
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
    let mut stmt = conn
        .prepare_cached("SELECT name FROM sqlite_master LIMIT 1")
        .map_err(sqlite_error)?;
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

/// One sqlite_master probe per call; reads cannot create tables, so a space
/// without a table is simply empty.
fn space_table_exists(conn: &Connection, space: SpaceId) -> Result<bool, BackendError> {
    let mut stmt = conn
        .prepare_cached("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1")
        .map_err(sqlite_error)?;
    let mut rows = stmt
        .query(params![space_table(space)])
        .map_err(sqlite_error)?;
    Ok(rows.next().map_err(sqlite_error)?.is_some())
}

#[expect(clippy::cast_possible_wrap)]
fn read_points_chunk(
    conn: &Connection,
    space: SpaceId,
    offset: usize,
    chunk: &[Key],
    projection: CoreProjection,
    values: &mut [Option<ProjectedValue>],
) -> Result<(), BackendError> {
    // No ORDER BY: callers address results by the returned ordinal, never by
    // visit order, and binding by reference avoids an owned copy of every key.
    let projected_column = match projection {
        CoreProjection::KeyOnly => "e.key",
        CoreProjection::FullValue => "e.value",
    };
    let table = space_table(space);
    let sql = format!(
        "WITH requested(ord, key) AS (VALUES {placeholders})
         SELECT r.ord, {projected_column}
         FROM requested r
         LEFT JOIN {table} e ON e.key = r.key",
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
        let Some(slot) = values.get_mut(index) else {
            return Err(BackendError::Corruption(format!(
                "sqlite requested ordinal out of bounds: {index}"
            )));
        };
        let projected = row.get_ref(1).map_err(sqlite_error)?;
        let value = match (projection, projected) {
            (_, SqlValueRef::Null) => None,
            (CoreProjection::KeyOnly, SqlValueRef::Blob(_)) => Some(ProjectedValue::KeyOnly),
            (CoreProjection::FullValue, SqlValueRef::Blob(value)) => {
                Some(ProjectedValue::FullValue(Bytes::copy_from_slice(value)))
            }
            (_, other) => {
                return Err(BackendError::Corruption(format!(
                    "sqlite projected column was not a blob: {other:?}"
                )));
            }
        };
        *slot = value;
    }
    Ok(())
}

fn multi_upsert_sql(table: &str, rows: usize) -> String {
    let mut sql = String::with_capacity(64 + rows * 8);
    sql.push_str("INSERT INTO ");
    sql.push_str(table);
    sql.push_str("(key, value) VALUES ");
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

/// Appends one predicate with a positional placeholder when the value is
/// present; absent values contribute nothing, so each bound combination is
/// its own cached statement shape and the planner always sees seekable
/// predicates.
fn push_bound<'a>(
    sql: &mut String,
    binds: &mut Vec<&'a [u8]>,
    predicate: &str,
    value: Option<&'a [u8]>,
) {
    let Some(bytes) = value else {
        return;
    };
    binds.push(bytes);
    sql.push_str(" AND ");
    sql.push_str(predicate);
    sql.push_str(" ?");
    sql.push_str(&binds.len().to_string());
}

fn push_range_bounds<'a>(
    sql: &mut String,
    binds: &mut Vec<&'a [u8]>,
    lower: &'a Bound<Key>,
    upper: &'a Bound<Key>,
) {
    match lower {
        Bound::Included(key) => push_bound(sql, binds, "key >=", Some(&key.0)),
        Bound::Excluded(key) => push_bound(sql, binds, "key >", Some(&key.0)),
        Bound::Unbounded => {}
    }
    match upper {
        Bound::Included(key) => push_bound(sql, binds, "key <=", Some(&key.0)),
        Bound::Excluded(key) => push_bound(sql, binds, "key <", Some(&key.0)),
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
