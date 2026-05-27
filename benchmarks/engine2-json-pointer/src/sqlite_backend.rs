use bytes::Bytes;
use lix_rs_sdk::{
    Backend, BackendError, BackendRangeScan, BackendRead, BackendWrite, CommitResult,
    CoreProjection, GetOptions, Key, KeyRange, LixError, PointVisitor, ProjectedValueRef, PutBatch,
    ReadOptions, ScanOptions, ScanResult, ScanVisitor, StoredValue, WriteOptions, WriteStats,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const KV_TABLE: &str = "lix_engine2_kv";

#[derive(Clone)]
pub struct Engine2SqliteBackend {
    path: Arc<PathBuf>,
    conn: Arc<Mutex<Connection>>,
    read_pool: Arc<Mutex<Vec<Connection>>>,
}

#[derive(Clone)]
pub struct Engine2SqliteRead {
    inner: Arc<Engine2SqliteReadInner>,
}

struct Engine2SqliteReadInner {
    conn: Mutex<Option<Connection>>,
    read_pool: Arc<Mutex<Vec<Connection>>>,
}

pub struct Engine2SqliteRangeScan {
    rows: Vec<(Key, Vec<u8>)>,
    position: usize,
    projection: CoreProjection,
}

pub struct Engine2SqliteWrite {
    conn: Arc<Mutex<Connection>>,
    overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    stats: WriteStats,
}

impl Engine2SqliteBackend {
    pub fn file_backed(path: &Path) -> Result<Self, LixError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "failed to create sqlite benchmark directory {}: {error}",
                        parent.display()
                    ),
                )
            })?;
        }

        let conn = Connection::open(path).map_err(sqlite_lix_error)?;
        configure_connection(&conn)?;
        ensure_kv_table(&conn)?;
        Ok(Self {
            path: Arc::new(path.to_path_buf()),
            conn: Arc::new(Mutex::new(conn)),
            read_pool: Arc::new(Mutex::new(Vec::new())),
        })
    }
}

impl Backend for Engine2SqliteBackend {
    type Read<'a>
        = Engine2SqliteRead
    where
        Self: 'a;

    type Write<'a>
        = Engine2SqliteWrite
    where
        Self: 'a;
    fn begin_read(&self, _opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        let conn = self
            .read_pool
            .lock()
            .map_err(|_| lock_error())?
            .pop()
            .map(Ok)
            .unwrap_or_else(|| {
                Connection::open(self.path.as_ref()).map_err(sqlite_backend_error)
            })?;
        configure_connection_for_read(&conn)?;
        conn.execute_batch("BEGIN DEFERRED TRANSACTION")
            .map_err(sqlite_backend_error)?;
        pin_read_snapshot(&conn)?;
        Ok(Engine2SqliteRead {
            inner: Arc::new(Engine2SqliteReadInner {
                conn: Mutex::new(Some(conn)),
                read_pool: Arc::clone(&self.read_pool),
            }),
        })
    }

    fn begin_write(&self, _opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        Ok(Engine2SqliteWrite {
            conn: Arc::clone(&self.conn),
            overlay: BTreeMap::new(),
            stats: WriteStats::default(),
        })
    }
}

impl BackendRead for Engine2SqliteRead {
    type RangeScan<'cursor> = Engine2SqliteRangeScan;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        self.with_conn(|conn| {
            for (index, key) in keys.iter().enumerate() {
                let value = kv_get_with_connection(conn, key.0.as_ref())?
                    .map(|value| project_value_ref(&value, opts.projection).to_owned());
                visitor.visit(index, key, value.as_ref().map(|value| value.as_ref()))?;
            }
            Ok(())
        })
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
        let rows = self.with_conn(|conn| scan_with_connection(conn, &range, opts.resume_after))?;
        let mut scan = Engine2SqliteRangeScan {
            rows,
            position: 0,
            projection: opts.projection,
        };
        f(&mut scan)
    }

    fn close(self) -> Result<(), BackendError> {
        match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner.finish(),
            Err(_) => Ok(()),
        }
    }
}

impl Engine2SqliteRead {
    fn with_conn<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T, BackendError>,
    ) -> Result<T, BackendError> {
        let conn = self.inner.conn.lock().map_err(|_| lock_error())?;
        let Some(conn) = conn.as_ref() else {
            return Err(BackendError::Io(
                "sqlite benchmark read connection already closed".to_string(),
            ));
        };
        f(conn)
    }
}

impl Engine2SqliteReadInner {
    fn finish(&self) -> Result<(), BackendError> {
        let Some(conn) = self.conn.lock().map_err(|_| lock_error())?.take() else {
            return Ok(());
        };
        let result = conn.execute_batch("ROLLBACK").map_err(sqlite_backend_error);
        if result.is_ok() {
            if let Ok(mut pool) = self.read_pool.lock() {
                pool.push(conn);
            }
        }
        result
    }
}

impl Drop for Engine2SqliteReadInner {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

impl BackendRangeScan for Engine2SqliteRangeScan {
    fn visit_next<V>(
        &mut self,
        limit_rows: usize,
        visitor: &mut V,
    ) -> Result<ScanResult, BackendError>
    where
        V: ScanVisitor + ?Sized,
    {
        if limit_rows == 0 {
            return Ok(ScanResult {
                emitted: 0,
                has_more: self.position < self.rows.len(),
            });
        }

        let mut emitted = 0usize;
        while emitted < limit_rows {
            let Some((key, value)) = self.rows.get(self.position) else {
                break;
            };
            visitor.visit(key.as_ref(), project_value_ref(value, self.projection))?;
            self.position += 1;
            emitted += 1;
        }

        Ok(ScanResult {
            emitted,
            has_more: self.position < self.rows.len(),
        })
    }
}

impl BackendWrite for Engine2SqliteWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        for entry in entries.entries {
            self.stats.put_entries += 1;
            self.stats.written_bytes += entry.value.bytes.len() as u64;
            self.overlay
                .insert(entry.key.0.to_vec(), Some(stored_value_bytes(entry.value)));
        }
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        for key in keys {
            self.overlay.insert(key.0.to_vec(), None);
        }
        self.stats.deleted_entries += keys.len() as u64;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        let conn = self.conn.lock().map_err(|_| lock_error())?;
        let keys = scan_with_connection(&conn, &range, None)?
            .into_iter()
            .map(|(key, _)| key.0.to_vec())
            .collect::<Vec<_>>();
        drop(conn);

        for key in keys {
            self.overlay.insert(key, None);
        }
        let staged_keys = self
            .overlay
            .keys()
            .filter(|key| key_matches_range(key, &range, None))
            .cloned()
            .collect::<Vec<_>>();
        for key in staged_keys {
            self.overlay.insert(key, None);
        }
        self.stats.deleted_ranges += 1;
        self.stats.backend_calls += 1;
        Ok(())
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        let mut conn = self.conn.lock().map_err(|_| lock_error())?;
        let tx = conn.transaction().map_err(sqlite_backend_error)?;
        for (key, value) in self.overlay {
            match value {
                Some(value) => {
                    tx.execute(
                        &format!(
                            "INSERT INTO {KV_TABLE} (key, value) VALUES (?1, ?2) \
                             ON CONFLICT(key) DO UPDATE SET value = excluded.value"
                        ),
                        params![key, value],
                    )
                    .map_err(sqlite_backend_error)?;
                }
                None => {
                    tx.execute(
                        &format!("DELETE FROM {KV_TABLE} WHERE key = ?1"),
                        params![key],
                    )
                    .map_err(sqlite_backend_error)?;
                }
            }
        }
        tx.commit().map_err(sqlite_backend_error)?;
        Ok(CommitResult {
            commit_id: None,
            stats: self.stats,
        })
    }

    fn rollback(self) -> Result<(), BackendError> {
        Ok(())
    }
}

fn configure_connection(conn: &Connection) -> Result<(), LixError> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA temp_store = MEMORY;",
    )
    .map_err(sqlite_lix_error)?;
    Ok(())
}

fn configure_connection_for_read(conn: &Connection) -> Result<(), BackendError> {
    conn.execute_batch(
        "PRAGMA synchronous = NORMAL;\
         PRAGMA temp_store = MEMORY;",
    )
    .map_err(sqlite_backend_error)?;
    Ok(())
}

fn ensure_kv_table(conn: &Connection) -> Result<(), LixError> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {KV_TABLE} (\
         key BLOB NOT NULL PRIMARY KEY,\
         value BLOB NOT NULL\
         ) WITHOUT ROWID"
    ))
    .map_err(sqlite_lix_error)?;
    Ok(())
}

fn pin_read_snapshot(conn: &Connection) -> Result<(), BackendError> {
    let _: Option<()> = conn
        .query_row(&format!("SELECT 1 FROM {KV_TABLE} LIMIT 1"), [], |_| Ok(()))
        .optional()
        .map_err(sqlite_backend_error)?;
    Ok(())
}

fn kv_get_with_connection(conn: &Connection, key: &[u8]) -> Result<Option<Vec<u8>>, BackendError> {
    conn.query_row(
        &format!("SELECT value FROM {KV_TABLE} WHERE key = ?1"),
        params![key],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()
    .map_err(sqlite_backend_error)
}

fn scan_with_connection(
    conn: &Connection,
    range: &KeyRange,
    resume_after: Option<&Key>,
) -> Result<Vec<(Key, Vec<u8>)>, BackendError> {
    let mut stmt = conn
        .prepare(&format!("SELECT key, value FROM {KV_TABLE} ORDER BY key"))
        .map_err(sqlite_backend_error)?;
    let rows = stmt
        .query_map([], |row| {
            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
        })
        .map_err(sqlite_backend_error)?;

    let mut out = Vec::new();
    for row in rows {
        let (key, value) = row.map_err(sqlite_backend_error)?;
        if key_matches_range(&key, range, resume_after) {
            out.push((Key(Bytes::from(key)), value));
        }
    }
    Ok(out)
}

fn key_matches_range(key: &[u8], range: &KeyRange, resume_after: Option<&Key>) -> bool {
    if let Some(resume_after) = resume_after {
        if key <= resume_after.0.as_ref() {
            return false;
        }
    }

    let lower_matches = match &range.lower {
        Bound::Included(lower) => key >= lower.0.as_ref(),
        Bound::Excluded(lower) => key > lower.0.as_ref(),
        Bound::Unbounded => true,
    };
    let upper_matches = match &range.upper {
        Bound::Included(upper) => key <= upper.0.as_ref(),
        Bound::Excluded(upper) => key < upper.0.as_ref(),
        Bound::Unbounded => true,
    };
    lower_matches && upper_matches
}

fn project_value_ref(value: &[u8], projection: CoreProjection) -> ProjectedValueRef<'_> {
    match projection {
        CoreProjection::KeyOnly => ProjectedValueRef::KeyOnly,
        CoreProjection::FullValue => ProjectedValueRef::FullValue(value),
    }
}

fn stored_value_bytes(value: StoredValue) -> Vec<u8> {
    value.bytes.to_vec()
}

fn sqlite_lix_error(error: rusqlite::Error) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sqlite benchmark error: {error}"),
    )
}

fn sqlite_backend_error(error: rusqlite::Error) -> BackendError {
    BackendError::Io(format!("sqlite benchmark error: {error}"))
}

fn lock_error() -> BackendError {
    BackendError::Io("sqlite benchmark mutex poisoned".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TempSqlitePath {
        path: std::path::PathBuf,
    }

    impl TempSqlitePath {
        fn new(label: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system time should be after unix epoch")
                .as_nanos();
            Self {
                path: std::env::temp_dir().join(format!(
                    "lix-engine2-sqlite-backend-{label}-{}-{nanos}.sqlite",
                    std::process::id()
                )),
            }
        }
    }

    impl Drop for TempSqlitePath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(format!("{}-journal", self.path.display()));
            let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
            let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        }
    }

    #[derive(Default)]
    struct SingleValueVisitor {
        value: Option<Vec<u8>>,
    }

    impl PointVisitor for SingleValueVisitor {
        fn visit(
            &mut self,
            index: usize,
            _key: &Key,
            value: Option<ProjectedValueRef<'_>>,
        ) -> Result<(), BackendError> {
            assert_eq!(index, 0);
            self.value = value.map(|value| match value {
                ProjectedValueRef::KeyOnly => Vec::new(),
                ProjectedValueRef::FullValue(bytes) => bytes.to_vec(),
            });
            Ok(())
        }
    }

    #[test]
    fn delete_range_removes_staged_puts_before_commit() {
        let temp = TempSqlitePath::new("delete-range-staged-put");
        let backend = Engine2SqliteBackend::file_backed(&temp.path).expect("backend should open");
        let key = Key(Bytes::from_static(b"staged"));

        let mut write = backend
            .begin_write(WriteOptions::default())
            .expect("write should begin");
        write
            .overlay
            .insert(key.0.to_vec(), Some(b"value".to_vec()));
        write
            .delete_range(KeyRange {
                lower: Bound::Included(key.clone()),
                upper: Bound::Included(key.clone()),
            })
            .expect("delete range should stage");
        write.commit().expect("commit should succeed");

        let read = backend
            .begin_read(ReadOptions::default())
            .expect("read should begin");
        let mut visitor = SingleValueVisitor::default();
        read.visit_keys(&[key], GetOptions::default(), &mut visitor)
            .expect("read should succeed");

        assert_eq!(visitor.value, None);
    }

    #[test]
    fn begin_read_pins_snapshot_until_read_is_dropped() {
        let temp = TempSqlitePath::new("read-pins-snapshot");
        let backend = Engine2SqliteBackend::file_backed(&temp.path).expect("backend should open");
        let key = Key(Bytes::from_static(b"snapshot-key"));

        let mut seed_write = backend
            .begin_write(WriteOptions::default())
            .expect("seed write should begin");
        seed_write
            .overlay
            .insert(key.0.to_vec(), Some(b"A".to_vec()));
        seed_write.commit().expect("seed commit should succeed");

        let old_read = backend
            .begin_read(ReadOptions::default())
            .expect("old read should begin");

        let mut update_write = backend
            .begin_write(WriteOptions::default())
            .expect("update write should begin");
        update_write
            .overlay
            .insert(key.0.to_vec(), Some(b"B".to_vec()));
        update_write.commit().expect("update commit should succeed");

        let mut old_visitor = SingleValueVisitor::default();
        old_read
            .visit_keys(
                std::slice::from_ref(&key),
                GetOptions::default(),
                &mut old_visitor,
            )
            .expect("old read should still work");
        assert_eq!(
            old_visitor.value,
            Some(b"A".to_vec()),
            "read handles must keep the snapshot from begin_read"
        );

        let fresh_read = backend
            .begin_read(ReadOptions::default())
            .expect("fresh read should begin");
        let mut fresh_visitor = SingleValueVisitor::default();
        fresh_read
            .visit_keys(&[key], GetOptions::default(), &mut fresh_visitor)
            .expect("fresh read should work");
        assert_eq!(fresh_visitor.value, Some(b"B".to_vec()));
    }
}
