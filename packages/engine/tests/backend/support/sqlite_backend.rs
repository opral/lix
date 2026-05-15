use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use lix_engine::backend_v2::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetManyResult, GetOptions, Key, KeyRange, ProjectedValue, ProjectedValueRef,
    PutBatch, ReadOptions, ScanOptions, ScanResult, ScanVisitor, SpaceId, StoredValue,
    WriteConcurrency, WriteOptions, WriteStats,
};
use lix_engine::{BackendV2Factory, BackendV2Fixture, BackendV2TestConfig};
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, Connection};
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

#[derive(Clone, Debug)]
pub struct SqliteBackend {
    path: PathBuf,
}

pub struct SqliteRead {
    conn: Connection,
}

pub struct SqliteWrite {
    conn: Connection,
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

impl BackendV2Factory for SqliteBackendFactory {
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

    fn config(&self) -> BackendV2TestConfig {
        BackendV2TestConfig {
            ephemeral: false,
            supports_concurrent_writers: false,
            ..BackendV2TestConfig::default()
        }
    }
}

impl BackendV2Fixture for SqliteBackendFixture {
    type Backend = SqliteBackend;

    fn open(&self) -> Self::Backend {
        SqliteBackend::open(&self.path).expect("open sqlite backend")
    }
}

impl SqliteBackend {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, BackendError> {
        let path = path.into();
        initialize_database(&path)?;
        Ok(Self { path })
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
        let conn = self.connect()?;
        conn.execute_batch("BEGIN DEFERRED TRANSACTION")
            .map_err(sqlite_error)?;
        pin_read_snapshot(&conn)?;
        Ok(SqliteRead { conn })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        let conn = self.connect()?;
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")
            .map_err(sqlite_error)?;
        Ok(SqliteWrite {
            conn,
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for SqliteRead {
    fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions<'_>,
    ) -> Result<GetManyResult, BackendError> {
        get_many(&self.conn, space, keys, opts)
    }

    fn visit_range<V>(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions<'_>,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        visit_range(&self.conn, space, range, opts, visitor)
    }

    fn close(self) -> Result<(), BackendError> {
        self.conn
            .execute_batch("ROLLBACK")
            .map_err(sqlite_error)
            .or_else(ignore_no_transaction)
    }
}

impl BackendWrite for SqliteWrite {
    fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), BackendError> {
        let mut stmt = self
            .conn
            .prepare(
                "INSERT INTO entries(space_id, key, value)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT(space_id, key) DO UPDATE SET value = excluded.value",
            )
            .map_err(sqlite_error)?;

        for entry in entries.entries {
            let value = stored_value_bytes(entry.value);
            self.stats.put_entries += 1;
            self.stats.written_bytes += value.len() as u64;
            stmt.execute(params![
                space.0 as i64,
                entry.key.0.as_ref(),
                value.as_ref()
            ])
            .map_err(sqlite_error)?;
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), BackendError> {
        let mut stmt = self
            .conn
            .prepare("DELETE FROM entries WHERE space_id = ?1 AND key = ?2")
            .map_err(sqlite_error)?;

        for key in keys {
            stmt.execute(params![space.0 as i64, key.0.as_ref()])
                .map_err(sqlite_error)?;
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        self.conn.execute_batch("COMMIT").map_err(sqlite_error)?;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        self.conn.execute_batch("ROLLBACK").map_err(sqlite_error)
    }
}

fn initialize_database(path: &Path) -> Result<(), BackendError> {
    let conn = open_connection(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS entries (
            space_id INTEGER NOT NULL,
            key BLOB NOT NULL,
            value BLOB NOT NULL,
            PRIMARY KEY (space_id, key)
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
    let _: i64 = conn
        .query_row("SELECT COUNT(*) FROM entries", [], |row| row.get(0))
        .map_err(sqlite_error)?;
    Ok(())
}

fn get_many(
    conn: &Connection,
    space: SpaceId,
    keys: &[Key],
    opts: GetOptions<'_>,
) -> Result<GetManyResult, BackendError> {
    let unique_keys = keys
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    if unique_keys.is_empty() {
        return Ok(GetManyResult::new(Vec::new()));
    }

    let placeholders = std::iter::repeat("(?)")
        .take(unique_keys.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "WITH requested(key) AS (VALUES {placeholders})
         SELECT e.key, e.value
         FROM requested r
         JOIN entries e ON e.space_id = ? AND e.key = r.key
         ORDER BY e.key ASC"
    );
    let mut values = unique_keys
        .into_iter()
        .map(|key| SqlValue::Blob(key.0.to_vec()))
        .collect::<Vec<_>>();
    values.push(SqlValue::Integer(space.0 as i64));

    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(values))
        .map_err(sqlite_error)?;
    let mut found = BTreeMap::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let key_bytes: Vec<u8> = row.get(0).map_err(sqlite_error)?;
        let value_bytes: Vec<u8> = row.get(1).map_err(sqlite_error)?;
        found.insert(
            Key(Bytes::from(key_bytes)),
            project_value(Bytes::from(value_bytes), opts.projection),
        );
    }
    Ok(GetManyResult::new(
        keys.iter()
            .map(|key| found.get(key).cloned())
            .collect::<Vec<_>>(),
    ))
}

fn visit_range<V>(
    conn: &Connection,
    space: SpaceId,
    range: KeyRange,
    opts: ScanOptions<'_>,
    visitor: &mut V,
) -> Result<ScanResult, BackendError>
where
    V: ScanVisitor + ?Sized,
{
    let limit = opts.limit_rows;
    if limit == 0 {
        return Ok(ScanResult::default());
    }

    let mut sql = String::from("SELECT key, value FROM entries WHERE space_id = ?1");
    let mut values = vec![SqlValue::Integer(space.0 as i64)];

    append_bound_sql(&mut sql, &mut values, "key", ">=", ">", &range.lower);
    append_bound_sql(&mut sql, &mut values, "key", "<=", "<", &range.upper);
    if let Some(resume_after) = opts.resume_after {
        sql.push_str(" AND key > ?");
        values.push(SqlValue::Blob(resume_after.0.to_vec()));
    }
    sql.push_str(" ORDER BY key ASC LIMIT ?");
    values.push(SqlValue::Integer((limit.saturating_add(1)) as i64));

    let mut stmt = conn.prepare(&sql).map_err(sqlite_error)?;
    let mut rows = stmt
        .query(rusqlite::params_from_iter(values))
        .map_err(sqlite_error)?;
    let mut emitted = 0;

    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let key_bytes: Vec<u8> = row.get(0).map_err(sqlite_error)?;
        let value_bytes: Vec<u8> = row.get(1).map_err(sqlite_error)?;
        if emitted == limit {
            return Ok(ScanResult {
                emitted,
                has_more: true,
            });
        }
        let value = Bytes::from(value_bytes);
        visitor.visit(
            &Key(Bytes::from(key_bytes)),
            project_value_ref(&value, opts.projection),
        )?;
        emitted += 1;
    }

    Ok(ScanResult {
        emitted,
        has_more: false,
    })
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

fn project_value(value: Bytes, projection: CoreProjection) -> ProjectedValue {
    match projection {
        CoreProjection::KeyOnly => ProjectedValue::KeyOnly,
        CoreProjection::FullValue => ProjectedValue::FullValue(value),
    }
}

fn project_value_ref(value: &Bytes, projection: CoreProjection) -> ProjectedValueRef<'_> {
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
