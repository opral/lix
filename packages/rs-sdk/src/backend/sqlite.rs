use async_trait::async_trait;
use lix_engine::{
    LixBackend, LixError, LixTransaction, QueryResult, SnapshotChunkReader, SnapshotChunkWriter,
    SqlDialect, Value,
};
use rusqlite::{
    backup::{Backup, StepResult},
    params_from_iter, Connection, Row,
};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct SqliteBackend {
    conn: Mutex<Connection>,
}

struct SqliteTransaction<'a> {
    conn: MutexGuard<'a, Connection>,
    finalized: bool,
}

impl SqliteBackend {
    pub fn in_memory() -> Result<Self, LixError> {
        let conn = Connection::open_in_memory().map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, LixError> {
        let conn = Connection::open(path).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        })?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

#[async_trait(?Send)]
impl LixBackend for SqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let conn = self.conn.lock().map_err(|_| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "sqlite mutex poisoned".to_string(),
        })?;
        execute_sql(&conn, sql, params)
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        let conn = self.conn.lock().map_err(|_| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "sqlite mutex poisoned".to_string(),
        })?;
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")
            .map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
            })?;

        Ok(Box::new(SqliteTransaction {
            conn,
            finalized: false,
        }))
    }

    async fn export_snapshot(&self, writer: &mut dyn SnapshotChunkWriter) -> Result<(), LixError> {
        let conn = self.conn.lock().map_err(|_| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "sqlite mutex poisoned".to_string(),
        })?;
        let snapshot_path = temp_snapshot_path("export");

        let export_result = (|| -> Result<(), LixError> {
            let mut snapshot_conn = Connection::open(&snapshot_path).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
            })?;
            let backup = Backup::new(&conn, &mut snapshot_conn).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
            })?;
            run_backup_to_completion(&backup)?;
            Ok(())
        })();

        let bytes = std::fs::read(&snapshot_path).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        });
        let _ = std::fs::remove_file(&snapshot_path);
        export_result?;
        let bytes = bytes?;

        for chunk in bytes.chunks(64 * 1024) {
            writer.write_chunk(chunk).await?;
        }
        writer.finish().await?;
        Ok(())
    }

    async fn restore_from_snapshot(
        &self,
        reader: &mut dyn SnapshotChunkReader,
    ) -> Result<(), LixError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.read_chunk().await? {
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "snapshot stream is empty".to_string(),
            });
        }

        let snapshot_path = temp_snapshot_path("restore");
        std::fs::write(&snapshot_path, &bytes).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        })?;

        let restore_result = (|| -> Result<(), LixError> {
            let source_conn = Connection::open(&snapshot_path).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
            })?;
            let mut conn = self.conn.lock().map_err(|_| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: "sqlite mutex poisoned".to_string(),
            })?;
            let backup = Backup::new(&source_conn, &mut conn).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
            })?;
            run_backup_to_completion(&backup)?;
            Ok(())
        })();
        let _ = std::fs::remove_file(&snapshot_path);
        restore_result
    }
}

#[async_trait(?Send)]
impl LixTransaction for SqliteTransaction<'_> {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        execute_sql(&self.conn, sql, params)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.conn.execute_batch("COMMIT").map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        })?;
        self.finalized = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.conn
            .execute_batch("ROLLBACK")
            .map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
            })?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for SqliteTransaction<'_> {
    fn drop(&mut self) {
        if !self.finalized && !std::thread::panicking() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

fn execute_sql(conn: &Connection, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
    if params.is_empty() && sql.contains(';') {
        conn.execute_batch(sql).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        })?;
        return Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        });
    }

    let mut stmt = conn.prepare(sql).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
    })?;
    let columns = stmt
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let bound_params = params.iter().cloned().map(to_sql_value);
    let mut rows = stmt
        .query(params_from_iter(bound_params))
        .map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        })?;
    let mut result_rows = Vec::new();
    while let Some(row) = rows.next().map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
    })? {
        result_rows.push(map_row(row)?);
    }
    Ok(QueryResult {
        rows: result_rows,
        columns,
    })
}

fn run_backup_to_completion(backup: &Backup<'_, '_>) -> Result<(), LixError> {
    loop {
        match backup.step(-1).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
        })? {
            StepResult::Done => return Ok(()),
            StepResult::More => continue,
            StepResult::Busy | StepResult::Locked => std::thread::sleep(Duration::from_millis(5)),
            _ => continue,
        }
    }
}

fn temp_snapshot_path(operation: &str) -> PathBuf {
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
        let value = row.get_ref(idx).map_err(|err| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: err.to_string(),
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
        Value::Blob(value) => rusqlite::types::Value::Blob(value),
    }
}
