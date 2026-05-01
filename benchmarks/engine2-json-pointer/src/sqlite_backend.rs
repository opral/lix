use async_trait::async_trait;
use lix_rs_sdk::{
    KvPair, KvScanRange, LixBackend, LixBackendTransaction, LixError, TransactionBeginMode,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

const KV_TABLE: &str = "lix_engine2_kv";

#[derive(Clone)]
pub struct Engine2SqliteBackend {
    conn: Arc<Mutex<Connection>>,
}

pub struct Engine2SqliteTransaction {
    conn: Arc<Mutex<Connection>>,
    finalized: bool,
    mode: TransactionBeginMode,
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

        let conn = Connection::open(path).map_err(sqlite_error)?;
        configure_connection(&conn)?;
        ensure_kv_table(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>, LixError> {
        self.conn
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "sqlite benchmark mutex poisoned"))
    }
}

#[async_trait]
impl LixBackend for Engine2SqliteBackend {
    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + Send + Sync + 'static>, LixError> {
        {
            let conn = self.lock_conn()?;
            conn.execute_batch(match mode {
                TransactionBeginMode::Read | TransactionBeginMode::Deferred => "BEGIN TRANSACTION",
                TransactionBeginMode::Write => "BEGIN IMMEDIATE",
            })
            .map_err(sqlite_error)?;
        }

        Ok(Box::new(Engine2SqliteTransaction {
            conn: Arc::clone(&self.conn),
            finalized: false,
            mode,
        }))
    }

    async fn kv_get(&self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        let conn = self.lock_conn()?;
        kv_get_with_connection(&conn, namespace, key)
    }

    async fn kv_scan(
        &self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let conn = self.lock_conn()?;
        kv_scan_with_connection(&conn, namespace, &range, limit)
    }
}

#[async_trait]
impl LixBackendTransaction for Engine2SqliteTransaction {
    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn kv_get(&mut self, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>, LixError> {
        let conn = self.lock_conn()?;
        kv_get_with_connection(&conn, namespace, key)
    }

    async fn kv_scan(
        &mut self,
        namespace: &str,
        range: KvScanRange,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LixError> {
        let conn = self.lock_conn()?;
        kv_scan_with_connection(&conn, namespace, &range, limit)
    }

    async fn kv_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) -> Result<(), LixError> {
        let conn = self.lock_conn()?;
        conn.execute(
            &format!(
                "INSERT INTO {KV_TABLE} (namespace, key, value) VALUES (?1, ?2, ?3) \
                 ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value"
            ),
            params![namespace, key, value],
        )
        .map_err(sqlite_error)?;
        Ok(())
    }

    async fn kv_delete(&mut self, namespace: &str, key: &[u8]) -> Result<(), LixError> {
        let conn = self.lock_conn()?;
        conn.execute(
            &format!("DELETE FROM {KV_TABLE} WHERE namespace = ?1 AND key = ?2"),
            params![namespace, key],
        )
        .map_err(sqlite_error)?;
        Ok(())
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.lock_conn()?
            .execute_batch("COMMIT")
            .map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.lock_conn()?
            .execute_batch("ROLLBACK")
            .map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }
}

impl Engine2SqliteTransaction {
    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>, LixError> {
        self.conn
            .lock()
            .map_err(|_| LixError::new("LIX_ERROR_UNKNOWN", "sqlite benchmark mutex poisoned"))
    }
}

impl Drop for Engine2SqliteTransaction {
    fn drop(&mut self) {
        if self.finalized || std::thread::panicking() {
            return;
        }
        if let Ok(conn) = self.conn.lock() {
            let _ = conn.execute_batch("ROLLBACK");
        }
    }
}

fn configure_connection(conn: &Connection) -> Result<(), LixError> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;\
         PRAGMA synchronous = NORMAL;\
         PRAGMA temp_store = MEMORY;",
    )
    .map_err(sqlite_error)?;
    Ok(())
}

fn ensure_kv_table(conn: &Connection) -> Result<(), LixError> {
    conn.execute_batch(&format!(
        "CREATE TABLE IF NOT EXISTS {KV_TABLE} (\
         namespace TEXT NOT NULL,\
         key BLOB NOT NULL,\
         value BLOB NOT NULL,\
         PRIMARY KEY(namespace, key)\
         ) WITHOUT ROWID"
    ))
    .map_err(sqlite_error)?;
    Ok(())
}

fn kv_get_with_connection(
    conn: &Connection,
    namespace: &str,
    key: &[u8],
) -> Result<Option<Vec<u8>>, LixError> {
    conn.query_row(
        &format!("SELECT value FROM {KV_TABLE} WHERE namespace = ?1 AND key = ?2"),
        params![namespace, key],
        |row| row.get::<_, Vec<u8>>(0),
    )
    .optional()
    .map_err(sqlite_error)
}

fn kv_scan_with_connection(
    conn: &Connection,
    namespace: &str,
    range: &KvScanRange,
    limit: Option<usize>,
) -> Result<Vec<KvPair>, LixError> {
    let mut pairs = match range {
        KvScanRange::Prefix(prefix) => {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT key, value FROM {KV_TABLE} WHERE namespace = ?1 ORDER BY key"
                ))
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(params![namespace], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(sqlite_error)?;
            collect_matching_rows(rows, |key| key.starts_with(prefix))?
        }
        KvScanRange::Range { start, end } => {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT key, value FROM {KV_TABLE} \
                     WHERE namespace = ?1 AND key >= ?2 AND key < ?3 \
                     ORDER BY key"
                ))
                .map_err(sqlite_error)?;
            let rows = stmt
                .query_map(params![namespace, start, end], |row| {
                    Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
                })
                .map_err(sqlite_error)?;
            collect_matching_rows(rows, |_| true)?
        }
    };

    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    Ok(pairs)
}

fn collect_matching_rows<F>(
    rows: rusqlite::MappedRows<
        '_,
        impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<(Vec<u8>, Vec<u8>)>,
    >,
    mut matches: F,
) -> Result<Vec<KvPair>, LixError>
where
    F: FnMut(&[u8]) -> bool,
{
    let mut pairs = Vec::new();
    for row in rows {
        let (key, value) = row.map_err(sqlite_error)?;
        if matches(&key) {
            pairs.push(KvPair::new(key, value));
        }
    }
    Ok(pairs)
}

fn sqlite_error(error: rusqlite::Error) -> LixError {
    LixError::new(
        "LIX_ERROR_UNKNOWN",
        format!("sqlite benchmark error: {error}"),
    )
}
