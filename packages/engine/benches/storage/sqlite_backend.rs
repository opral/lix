use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvGetRequest, BackendKvGetResult, BackendKvGetResultGroup, BackendKvPair,
    BackendKvScanRange, BackendKvScanRequest, BackendKvScanResult, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, LixError,
};
use rusqlite::{params, Connection, OptionalExtension};

#[derive(Clone)]
pub(crate) struct SqliteBenchBackend {
    connection: Arc<Mutex<Connection>>,
}

pub(crate) struct SqliteBenchTransaction {
    connection: Arc<Mutex<Connection>>,
    finalized: bool,
}

impl SqliteBenchBackend {
    pub(crate) fn new() -> Result<Self, LixError> {
        let connection = Connection::open_in_memory().map_err(sqlite_error)?;
        connection
            .execute_batch(
                "
                PRAGMA journal_mode = MEMORY;
                PRAGMA synchronous = OFF;
                CREATE TABLE kv (
                    namespace TEXT NOT NULL,
                    key BLOB NOT NULL,
                    value BLOB NOT NULL,
                    PRIMARY KEY (namespace, key)
                ) WITHOUT ROWID;
                ",
            )
            .map_err(sqlite_error)?;
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    fn lock_connection(&self) -> Result<std::sync::MutexGuard<'_, Connection>, LixError> {
        self.connection
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "sqlite bench connection poisoned"))
    }
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
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetResult, LixError> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare_cached("SELECT value FROM kv WHERE namespace = ?1 AND key = ?2")
            .map_err(sqlite_error)?;
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let mut values = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let value = statement
                    .query_row(params![group.namespace.as_str(), key.as_slice()], |row| {
                        row.get::<_, Vec<u8>>(0)
                    })
                    .optional()
                    .map_err(sqlite_error)?;
                values.push(value);
            }
            groups.push(BackendKvGetResultGroup {
                namespace: group.namespace,
                values,
            });
        }
        Ok(BackendKvGetResult { groups })
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanResult, LixError> {
        let connection = self.lock_connection()?;
        sqlite_scan(&connection, request)
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
            for put in group.puts {
                put_statement
                    .execute(params![
                        group.namespace.as_str(),
                        put.key.as_slice(),
                        put.value.as_slice()
                    ])
                    .map_err(sqlite_error)?;
                stats.puts += 1;
                stats.bytes_written += put.key.len() + put.value.len();
            }
            for key in group.deletes {
                delete_statement
                    .execute(params![group.namespace.as_str(), key.as_slice()])
                    .map_err(sqlite_error)?;
                stats.deletes += 1;
                stats.bytes_written += key.len();
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

fn sqlite_scan(
    connection: &Connection,
    request: BackendKvScanRequest,
) -> Result<BackendKvScanResult, LixError> {
    let start = scan_start_key(&request);
    let end = scan_end_key(&request.range);
    let fetch_limit = request.limit.checked_add(1).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "storage scan limit overflow while checking for next page",
        )
    })?;
    let limit = i64::try_from(fetch_limit).map_err(|_| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "storage scan limit does not fit into sqlite i64",
        )
    })?;
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
    let mut rows = statement
        .query_map(
            params![
                request.namespace.as_str(),
                request.after.as_deref(),
                start.as_slice(),
                end.as_deref(),
                limit,
            ],
            |row| {
                Ok(BackendKvPair::new(
                    row.get::<_, Vec<u8>>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                ))
            },
        )
        .map_err(sqlite_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sqlite_error)?;
    let has_more = rows.len() > request.limit;
    rows.truncate(request.limit);
    let resume_after = has_more
        .then(|| rows.last().map(|row| row.key.clone()))
        .flatten();
    Ok(BackendKvScanResult { rows, resume_after })
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
