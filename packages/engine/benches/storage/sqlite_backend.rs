use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteOp,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, BytePageBuilder,
    LixError,
};
use rusqlite::{params, params_from_iter, types::Value as SqlValue, Connection, OptionalExtension};
use std::path::{Path, PathBuf};
use tempfile::TempDir;

#[derive(Clone)]
pub(crate) struct SqliteBenchBackend {
    connection: Arc<Mutex<Connection>>,
    #[allow(dead_code)]
    path: Option<Arc<PathBuf>>,
    _temp_dir: Option<Arc<TempDir>>,
}

pub(crate) struct SqliteBenchTransaction {
    connection: Arc<Mutex<Connection>>,
    finalized: bool,
}

impl SqliteBenchBackend {
    pub(crate) fn tempfile() -> Result<Self, LixError> {
        let temp_dir = Arc::new(TempDir::new().map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("sqlite bench tempdir: {error}"),
            )
        })?);
        let path = Arc::new(temp_dir.path().join("bench.sqlite"));
        let connection = Connection::open(path.as_path()).map_err(sqlite_error)?;
        configure_connection(&connection)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            path: Some(path),
            _temp_dir: Some(temp_dir),
        })
    }

    #[allow(dead_code)]
    pub(crate) fn path(&self) -> Option<&Path> {
        self.path.as_deref().map(PathBuf::as_path)
    }

    fn lock_connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LixError> {
        self.connection
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "sqlite bench connection poisoned"))
    }
}

fn configure_connection(connection: &Connection) -> Result<(), LixError> {
    connection
        .execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            PRAGMA foreign_keys = ON;
            CREATE TABLE kv (
                namespace TEXT NOT NULL,
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (namespace, key)
            ) WITHOUT ROWID;
            ",
        )
        .map_err(sqlite_error)?;
    Ok(())
}

#[async_trait]
impl Backend for SqliteBenchBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        let connection = self.lock_connection()?;
        connection
            .execute_batch("BEGIN DEFERRED")
            .map_err(sqlite_error)?;
        drop(connection);
        Ok(Box::new(SqliteBenchTransaction {
            connection: Arc::clone(&self.connection),
            finalized: false,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        let connection = self.lock_connection()?;
        connection
            .execute_batch("BEGIN IMMEDIATE")
            .map_err(sqlite_error)?;
        drop(connection);
        Ok(Box::new(SqliteBenchTransaction {
            connection: Arc::clone(&self.connection),
            finalized: false,
        }))
    }
}

#[async_trait]
impl BackendReadTransaction for SqliteBenchTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        let connection = self.lock_connection()?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let key_count = group.keys.len();
            let keys = group.keys;
            let mut values = BytePageBuilder::with_capacity(key_count, 0);
            let mut present = Vec::with_capacity(key_count);
            if keys.is_empty() {
                groups.push(BackendKvValueGroup::new(
                    namespace,
                    values.finish(),
                    present,
                ));
                continue;
            }

            let key_placeholders = std::iter::repeat_n("?", key_count)
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "
                SELECT key, value
                FROM kv
                WHERE namespace = ? AND key IN ({key_placeholders})
                "
            );
            let mut parameters = Vec::with_capacity(1 + key_count);
            parameters.push(SqlValue::Text(namespace.clone()));
            parameters.extend(keys.iter().cloned().map(SqlValue::Blob));

            let mut statement = connection.prepare(&sql).map_err(sqlite_error)?;
            let mut rows = statement
                .query(params_from_iter(parameters))
                .map_err(sqlite_error)?;
            let mut values_by_key = HashMap::with_capacity(key_count);
            while let Some(row) = rows.next().map_err(sqlite_error)? {
                values_by_key.insert(
                    row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?,
                    row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?,
                );
            }

            for key in keys {
                if let Some(value) = values_by_key.get(&key) {
                    values.push(value);
                    present.push(true);
                } else {
                    values.push([]);
                    present.push(false);
                }
            }
            if present.len() != key_count {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sqlite get_values returned {} rows for {key_count} requested keys",
                        present.len()
                    ),
                ));
            }
            groups.push(BackendKvValueGroup::new(
                namespace,
                values.finish(),
                present,
            ));
        }
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare_cached("SELECT 1 FROM kv WHERE namespace = ?1 AND key = ?2")
            .map_err(sqlite_error)?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut exists = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                exists.push(
                    statement
                        .query_row(params![namespace.as_str(), key.as_slice()], |_| Ok(()))
                        .optional()
                        .map_err(sqlite_error)?
                        .is_some(),
                );
            }
            groups.push(BackendKvExistsGroup { namespace, exists });
        }
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let connection = self.lock_connection()?;
        sqlite_scan_keys(&connection, request)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let connection = self.lock_connection()?;
        sqlite_scan_values(&connection, request)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let connection = self.lock_connection()?;
        sqlite_scan_entries(&connection, request)
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.lock_connection()?
            .execute_batch("ROLLBACK")
            .map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for SqliteBenchTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let connection = self.lock_connection()?;
        let mut put_statement = connection
            .prepare_cached(
                "
                INSERT INTO kv (namespace, key, value)
                VALUES (?1, ?2, ?3)
                ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value
                ",
            )
            .map_err(sqlite_error)?;
        let mut delete_statement = connection
            .prepare_cached("DELETE FROM kv WHERE namespace = ?1 AND key = ?2")
            .map_err(sqlite_error)?;
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            let namespace = group.namespace().to_string();
            for op in group.ops() {
                match op {
                    BackendKvWriteOp::Put { key, value } => {
                        put_statement
                            .execute(params![namespace.as_str(), key, value])
                            .map_err(sqlite_error)?;
                        stats.puts += 1;
                        stats.bytes_written += key.len() + value.len();
                    }
                    BackendKvWriteOp::Delete { key } => {
                        delete_statement
                            .execute(params![namespace.as_str(), key])
                            .map_err(sqlite_error)?;
                        stats.deletes += 1;
                        stats.bytes_written += key.len();
                    }
                    BackendKvWriteOp::DeleteRange { range } => {
                        sqlite_delete_range(&connection, namespace.as_str(), range)?;
                        stats.delete_ranges += 1;
                        stats.bytes_written += delete_range_bytes(range);
                    }
                }
            }
        }
        Ok(stats)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.lock_connection()?
            .execute_batch("COMMIT")
            .map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }
}

impl SqliteBenchTransaction {
    fn lock_connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LixError> {
        self.connection
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "sqlite bench connection poisoned"))
    }
}

impl Drop for SqliteBenchTransaction {
    fn drop(&mut self) {
        if !self.finalized {
            if let Ok(connection) = self.connection.lock() {
                let _ = connection.execute_batch("ROLLBACK");
            }
        }
    }
}

fn sqlite_scan_keys(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvKeyPage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key FROM kv
            WHERE namespace = ?1
              AND (?2 IS NULL OR key > ?2)
              AND key >= ?3
              AND (?4 IS NULL OR key < ?4)
            ORDER BY key
            LIMIT ?5
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.namespace.as_str(),
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit,
        ])
        .map_err(sqlite_error)?;
    let mut keys = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < request.limit {
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvKeyPage {
        keys: keys.finish(),
        resume_after,
    })
}

fn sqlite_scan_values(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvValuePage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key, value FROM kv
            WHERE namespace = ?1
              AND (?2 IS NULL OR key > ?2)
              AND key >= ?3
              AND (?4 IS NULL OR key < ?4)
            ORDER BY key
            LIMIT ?5
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.namespace.as_str(),
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit,
        ])
        .map_err(sqlite_error)?;
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        if count < request.limit {
            resume_after_candidate = Some(row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?);
            let value = row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?;
            values.push(&value);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvValuePage {
        values: values.finish(),
        resume_after,
    })
}

fn sqlite_scan_entries(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvEntryPage, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let limit = sqlite_fetch_limit(request.limit)?;
    let mut statement = connection
        .prepare_cached(
            "
            SELECT key, value FROM kv
            WHERE namespace = ?1
              AND (?2 IS NULL OR key > ?2)
              AND key >= ?3
              AND (?4 IS NULL OR key < ?4)
            ORDER BY key
            LIMIT ?5
            ",
        )
        .map_err(sqlite_error)?;
    let mut cursor = statement
        .query(params![
            request.namespace.as_str(),
            request.after.as_deref(),
            start.as_slice(),
            end.as_deref(),
            limit,
        ])
        .map_err(sqlite_error)?;
    let mut keys = BytePageBuilder::new();
    let mut values = BytePageBuilder::new();
    let mut count = 0;
    let mut resume_after_candidate = None;
    while let Some(row) = cursor.next().map_err(sqlite_error)? {
        let key = row.get::<_, Vec<u8>>(0).map_err(sqlite_error)?;
        if count < request.limit {
            let value = row.get::<_, Vec<u8>>(1).map_err(sqlite_error)?;
            resume_after_candidate = Some(key.clone());
            keys.push(&key);
            values.push(&value);
        }
        count += 1;
    }
    let resume_after = (count > request.limit)
        .then_some(resume_after_candidate)
        .flatten();
    Ok(BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    })
}

fn sqlite_fetch_limit(limit: usize) -> Result<i64, LixError> {
    if limit == usize::MAX {
        return Ok(i64::MAX);
    }
    let fetch_limit = limit.checked_add(1).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "storage scan limit overflow while checking for next page",
        )
    })?;
    i64::try_from(fetch_limit).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "storage scan limit does not fit into sqlite i64",
        )
    })
}

fn scan_start_key(request: &BackendKvScanRequest) -> Vec<u8> {
    let range_start = match &request.range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    match request.after.as_deref() {
        Some(after) if after > range_start => after.to_vec(),
        _ => range_start.to_vec(),
    }
}

fn scan_end_key(range: &BackendKvScanRange) -> Option<Vec<u8>> {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix_end(prefix),
        BackendKvScanRange::Range { end, .. } => Some(end.clone()),
    }
}

fn sqlite_delete_range(
    connection: &Connection,
    namespace: &str,
    range: &BackendKvScanRange,
) -> Result<(), LixError> {
    let start = match range {
        BackendKvScanRange::Prefix(prefix) => prefix.as_slice(),
        BackendKvScanRange::Range { start, .. } => start.as_slice(),
    };
    match scan_end_key(range) {
        Some(end) => connection
            .execute(
                "DELETE FROM kv WHERE namespace = ?1 AND key >= ?2 AND key < ?3",
                params![namespace, start, end],
            )
            .map(|_| ())
            .map_err(sqlite_error),
        None => connection
            .execute(
                "DELETE FROM kv WHERE namespace = ?1 AND key >= ?2",
                params![namespace, start],
            )
            .map(|_| ())
            .map_err(sqlite_error),
    }
}

fn delete_range_bytes(range: &BackendKvScanRange) -> usize {
    match range {
        BackendKvScanRange::Prefix(prefix) => prefix.len(),
        BackendKvScanRange::Range { start, end } => start.len() + end.len(),
    }
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != u8::MAX {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

fn sqlite_error(error: rusqlite::Error) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sqlite bench backend: {error}"),
    )
}
