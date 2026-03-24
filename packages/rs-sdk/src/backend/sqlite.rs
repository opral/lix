use async_trait::async_trait;
use lix_engine::{
    ImageChunkReader, ImageChunkWriter, LixBackend, LixError, LixBackendTransaction, PreparedBatch,
    QueryResult, SqlDialect, Value,
};
use rusqlite::{
    backup::{Backup, StepResult},
    params_from_iter, Connection, Row,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct SqliteBackend {
    conn: Mutex<Option<Connection>>,
    target: SqliteStorageTarget,
}

struct SqliteTransaction<'a> {
    conn: MutexGuard<'a, Option<Connection>>,
    finalized: bool,
    savepoint_name: Option<String>,
}

#[derive(Clone, Debug)]
enum SqliteStorageTarget {
    InMemory,
    Path(PathBuf),
}

impl SqliteBackend {
    pub fn in_memory() -> Result<Self, LixError> {
        let conn = Connection::open_in_memory().map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        configure_connection(&conn, &SqliteStorageTarget::InMemory)?;
        Ok(Self {
            conn: Mutex::new(Some(conn)),
            target: SqliteStorageTarget::InMemory,
        })
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, LixError> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open(&path).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        let target = SqliteStorageTarget::Path(path);
        configure_connection(&conn, &target)?;
        Ok(Self {
            conn: Mutex::new(Some(conn)),
            target,
        })
    }

    pub fn destroy_path(path: impl AsRef<Path>) -> Result<(), LixError> {
        destroy_sqlite_artifacts(path.as_ref())
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Option<Connection>>, LixError> {
        self.conn.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sqlite mutex poisoned".to_string(),
        })
    }
}

#[async_trait(?Send)]
impl LixBackend for SqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let conn = self.lock_conn()?;
        let conn = conn.as_ref().ok_or_else(sqlite_backend_destroyed_error)?;
        execute_sql(conn, sql, params)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let mut conn = self.lock_conn()?;
        let inner = conn.as_mut().ok_or_else(sqlite_backend_destroyed_error)?;
        if inner.is_autocommit() {
            inner
                .execute_batch("BEGIN IMMEDIATE TRANSACTION")
                .map_err(|err| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: err.to_string(),
                })?;
            Ok(Box::new(SqliteTransaction {
                conn,
                finalized: false,
                savepoint_name: None,
            }))
        } else {
            // A transaction is already active (e.g. during init). Use a
            // savepoint so callers deep in the call stack still get an
            // atomic unit of work without failing on nested BEGIN.
            static FALLBACK_SP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let id = FALLBACK_SP.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let name = format!("sp_auto_{id}");
            inner
                .execute_batch(&format!("SAVEPOINT {name}"))
                .map_err(|err| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: err.to_string(),
                })?;
            Ok(Box::new(SqliteTransaction {
                conn,
                finalized: false,
                savepoint_name: Some(name),
            }))
        }
    }

    async fn begin_savepoint(&self, name: &str) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let mut conn = self.lock_conn()?;
        let inner = conn.as_mut().ok_or_else(sqlite_backend_destroyed_error)?;
        inner
            .execute_batch(&format!("SAVEPOINT {name}"))
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
        Ok(Box::new(SqliteTransaction {
            conn,
            finalized: false,
            savepoint_name: Some(name.to_string()),
        }))
    }

    async fn export_image(&self, writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        let conn = self.lock_conn()?;
        let conn = conn.as_ref().ok_or_else(sqlite_backend_destroyed_error)?;
        let image_path = temp_image_path("export");

        let export_result = (|| -> Result<(), LixError> {
            let mut snapshot_conn = Connection::open(&image_path).map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
            let backup = Backup::new(&conn, &mut snapshot_conn).map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
            run_backup_to_completion(&backup)?;
            Ok(())
        })();

        let bytes = std::fs::read(&image_path).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        });
        let _ = std::fs::remove_file(&image_path);
        export_result?;
        let bytes = bytes?;

        for chunk in bytes.chunks(64 * 1024) {
            writer.write_chunk(chunk).await?;
        }
        writer.finish().await?;
        Ok(())
    }

    async fn restore_from_image(&self, reader: &mut dyn ImageChunkReader) -> Result<(), LixError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.read_chunk().await? {
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "image stream is empty".to_string(),
            });
        }

        let image_path = temp_image_path("restore");
        std::fs::write(&image_path, &bytes).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;

        let restore_result = (|| -> Result<(), LixError> {
            let source_conn = Connection::open(&image_path).map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
            let mut conn = self.lock_conn()?;
            let inner = conn.as_mut().ok_or_else(sqlite_backend_destroyed_error)?;
            let backup = Backup::new(&source_conn, inner).map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
            run_backup_to_completion(&backup)?;
            Ok(())
        })();
        let _ = std::fs::remove_file(&image_path);
        restore_result
    }

    async fn destroy(&self) -> Result<(), LixError> {
        let maybe_path = match &self.target {
            SqliteStorageTarget::InMemory => None,
            SqliteStorageTarget::Path(path) => Some(path.clone()),
        };

        let taken = {
            let mut conn = self.lock_conn()?;
            conn.take()
        };
        drop(taken);

        if let Some(path) = maybe_path {
            destroy_sqlite_artifacts(&path)?;
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl LixBackendTransaction for SqliteTransaction<'_> {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(sqlite_backend_destroyed_error)?;
        execute_sql(conn, sql, params)
    }

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(sqlite_backend_destroyed_error)?;
        execute_prepared_batch(conn, batch)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        let sql = match &self.savepoint_name {
            Some(name) => format!("RELEASE SAVEPOINT {name}"),
            None => "COMMIT".to_string(),
        };
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(sqlite_backend_destroyed_error)?;
        conn.execute_batch(&sql).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        self.finalized = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        let sql = match &self.savepoint_name {
            Some(name) => format!("ROLLBACK TO SAVEPOINT {name}; RELEASE SAVEPOINT {name}"),
            None => "ROLLBACK".to_string(),
        };
        let conn = self
            .conn
            .as_mut()
            .ok_or_else(sqlite_backend_destroyed_error)?;
        conn.execute_batch(&sql).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for SqliteTransaction<'_> {
    fn drop(&mut self) {
        if !self.finalized && !std::thread::panicking() {
            let sql = match &self.savepoint_name {
                Some(name) => {
                    format!("ROLLBACK TO SAVEPOINT {name}; RELEASE SAVEPOINT {name}")
                }
                None => "ROLLBACK".to_string(),
            };
            if let Some(conn) = self.conn.as_mut() {
                let _ = conn.execute_batch(&sql);
            }
        }
    }
}

fn configure_connection(conn: &Connection, target: &SqliteStorageTarget) -> Result<(), LixError> {
    conn.busy_timeout(Duration::from_secs(30))
        .map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;

    if matches!(target, SqliteStorageTarget::Path(_)) {
        let journal_mode = query_single_text(conn, "PRAGMA journal_mode = WAL")?;
        if !journal_mode.eq_ignore_ascii_case("wal") {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "failed to enable sqlite WAL mode: expected 'wal', got '{journal_mode}'"
                ),
            });
        }
    }

    Ok(())
}

fn query_single_text(conn: &Connection, sql: &str) -> Result<String, LixError> {
    conn.query_row(sql, [], |row| row.get::<_, String>(0))
        .map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })
}

fn destroy_sqlite_artifacts(path: &Path) -> Result<(), LixError> {
    let mut first_error: Option<std::io::Error> = None;

    for artifact_path in sqlite_artifact_paths(path) {
        match fs::remove_file(&artifact_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) if first_error.is_none() => first_error = Some(error),
            Err(_) => {}
        }
    }

    if let Some(error) = first_error {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "failed to destroy sqlite storage artifacts for {}: {error}",
                path.display()
            ),
        });
    }

    Ok(())
}

fn sqlite_artifact_paths(path: &Path) -> Vec<PathBuf> {
    let display = path.display().to_string();
    vec![
        path.to_path_buf(),
        PathBuf::from(format!("{display}-wal")),
        PathBuf::from(format!("{display}-shm")),
        PathBuf::from(format!("{display}-journal")),
    ]
}

fn sqlite_backend_destroyed_error() -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: "sqlite backend storage has been destroyed".to_string(),
    }
}

fn execute_sql(conn: &Connection, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
    let mut stmt = conn.prepare(sql).map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: err.to_string(),
    })?;
    let columns = stmt
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let bound_params = params.iter().cloned().map(to_sql_value);
    let mut rows = stmt
        .query(params_from_iter(bound_params))
        .map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
    let mut result_rows = Vec::new();
    while let Some(row) = rows.next().map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: err.to_string(),
    })? {
        result_rows.push(map_row(row)?);
    }
    Ok(QueryResult {
        rows: result_rows,
        columns,
    })
}

fn execute_prepared_batch(
    conn: &Connection,
    batch: &PreparedBatch,
) -> Result<QueryResult, LixError> {
    let mut last_result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &batch.steps {
        last_result = execute_sql(conn, &statement.sql, &statement.params)?;
    }
    Ok(last_result)
}

fn run_backup_to_completion(backup: &Backup<'_, '_>) -> Result<(), LixError> {
    loop {
        match backup.step(-1).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })? {
            StepResult::Done => return Ok(()),
            StepResult::More => continue,
            StepResult::Busy | StepResult::Locked => std::thread::sleep(Duration::from_millis(5)),
            _ => continue,
        }
    }
}

fn temp_image_path(operation: &str) -> PathBuf {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-rs-sdk-snapshot-{operation}-{}-{ts}.sqlite",
        std::process::id()
    ))
}

fn map_row(row: &Row<'_>) -> Result<Vec<Value>, LixError> {
    let mut values = Vec::new();
    for idx in 0..row.as_ref().column_count() {
        let value = row.get_ref(idx).map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        values.push(match value {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(value) => Value::Integer(value),
            rusqlite::types::ValueRef::Real(value) => Value::Real(value),
            rusqlite::types::ValueRef::Text(value) => {
                Value::Text(String::from_utf8_lossy(value).to_string())
            }
            rusqlite::types::ValueRef::Blob(value) => Value::Blob(value.to_vec()),
        });
    }
    Ok(values)
}

fn to_sql_value(value: Value) -> rusqlite::types::Value {
    match value {
        Value::Null => rusqlite::types::Value::Null,
        Value::Boolean(value) => rusqlite::types::Value::Integer(if value { 1 } else { 0 }),
        Value::Integer(value) => rusqlite::types::Value::Integer(value),
        Value::Real(value) => rusqlite::types::Value::Real(value),
        Value::Text(value) => rusqlite::types::Value::Text(value),
        Value::Json(value) => rusqlite::types::Value::Text(value.to_string()),
        Value::Blob(value) => rusqlite::types::Value::Blob(value),
    }
}
