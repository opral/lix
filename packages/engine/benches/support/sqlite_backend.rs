use lix_engine::{
    LixBackend, LixBackendTransaction, LixError, PreparedBatch, QueryResult, SqlDialect,
    TransactionMode, Value,
};
use rusqlite::{params_from_iter, Connection, Row};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

pub struct BenchSqliteBackend {
    conn: Mutex<Connection>,
}

struct BenchSqliteTransaction<'a> {
    conn: MutexGuard<'a, Connection>,
    finalized: bool,
    savepoint_name: Option<String>,
    mode: TransactionMode,
}

impl BenchSqliteBackend {
    #[allow(dead_code)]
    pub fn in_memory() -> Result<Self, LixError> {
        let conn = Connection::open_in_memory().map_err(sqlite_error)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn file_backed(path: &Path) -> Result<Self, LixError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: format!(
                    "failed to create sqlite benchmark directory {}: {error}",
                    parent.display()
                ),
            })?;
        }

        let conn = Connection::open(path).map_err(sqlite_error)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock_conn(&self) -> Result<MutexGuard<'_, Connection>, LixError> {
        self.conn.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sqlite benchmark mutex poisoned".to_string(),
        })
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackend for BenchSqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let conn = self.lock_conn()?;
        execute_sql(&conn, sql, params)
    }

    async fn begin_transaction(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let conn = self.lock_conn()?;
        let savepoint_name = if conn.is_autocommit() {
            conn.execute_batch(match mode {
                TransactionMode::Read | TransactionMode::Deferred => "BEGIN TRANSACTION",
                TransactionMode::Write => "BEGIN IMMEDIATE",
            })
            .map_err(sqlite_error)?;
            None
        } else {
            match mode {
                TransactionMode::Write => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "sqlite benchmark backend cannot open a nested write transaction inside an active transaction; use begin_savepoint(...) for nested write scopes",
                    ));
                }
                TransactionMode::Read | TransactionMode::Deferred => {
                    return Err(LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "sqlite benchmark backend cannot open a nested read/deferred transaction inside an active transaction",
                    ));
                }
            }
        };

        Ok(Box::new(BenchSqliteTransaction {
            conn,
            finalized: false,
            savepoint_name,
            mode,
        }))
    }

    async fn begin_savepoint(
        &self,
        name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let conn = self.lock_conn()?;
        conn.execute_batch(&format!("SAVEPOINT {}", quote_savepoint_name(name)))
            .map_err(sqlite_error)?;

        Ok(Box::new(BenchSqliteTransaction {
            conn,
            finalized: false,
            savepoint_name: Some(name.to_string()),
            mode: TransactionMode::Write,
        }))
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackendTransaction for BenchSqliteTransaction<'_> {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionMode {
        self.mode
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        execute_sql(&self.conn, sql, params)
    }

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        execute_prepared_batch(&self.conn, batch)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        let sql = match &self.savepoint_name {
            Some(name) => format!("RELEASE SAVEPOINT {}", quote_savepoint_name(name)),
            None => "COMMIT".to_string(),
        };
        self.conn.execute_batch(&sql).map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        let sql = match &self.savepoint_name {
            Some(name) => format!(
                "ROLLBACK TO SAVEPOINT {quoted}; RELEASE SAVEPOINT {quoted}",
                quoted = quote_savepoint_name(name)
            ),
            None => "ROLLBACK".to_string(),
        };
        self.conn.execute_batch(&sql).map_err(sqlite_error)?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for BenchSqliteTransaction<'_> {
    fn drop(&mut self) {
        if self.finalized || std::thread::panicking() {
            return;
        }

        let sql = match &self.savepoint_name {
            Some(name) => format!(
                "ROLLBACK TO SAVEPOINT {quoted}; RELEASE SAVEPOINT {quoted}",
                quoted = quote_savepoint_name(name)
            ),
            None => "ROLLBACK".to_string(),
        };
        let _ = self.conn.execute_batch(&sql);
    }
}

fn execute_sql(conn: &Connection, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
    if params.is_empty() && sql.contains(';') {
        conn.execute_batch(sql).map_err(sqlite_error)?;
        return Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        });
    }

    let mut stmt = conn.prepare(sql).map_err(sqlite_error)?;
    let columns = stmt
        .column_names()
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let bound_params = params.iter().cloned().map(to_sql_value);
    let mut rows = stmt
        .query(params_from_iter(bound_params))
        .map_err(sqlite_error)?;

    let mut result_rows = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
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

fn map_row(row: &Row<'_>) -> Result<Vec<Value>, LixError> {
    let mut values = Vec::with_capacity(row.as_ref().column_count());
    for index in 0..row.as_ref().column_count() {
        let value = row.get_ref(index).map_err(sqlite_error)?;
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

fn quote_savepoint_name(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn sqlite_error(error: impl std::fmt::Display) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    }
}
