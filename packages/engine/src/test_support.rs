use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::types::{Value as SqliteValue, ValueRef};

use crate::{
    boot, BootArgs, Engine, LixBackend, LixBackendTransaction, LixError, NoopWasmRuntime,
    QueryResult, Session, SqlDialect, TransactionMode, Value,
};

#[derive(Clone)]
pub(crate) struct InMemorySqliteBackend {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

struct InMemorySqliteTransaction {
    connection: Arc<Mutex<rusqlite::Connection>>,
    mode: TransactionMode,
}

impl InMemorySqliteBackend {
    pub(crate) fn new() -> Self {
        let connection = rusqlite::Connection::open_in_memory().expect("open sqlite memory db");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("enable foreign keys");
        Self {
            connection: Arc::new(Mutex::new(connection)),
        }
    }
}

#[async_trait(?Send)]
impl LixBackend for InMemorySqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        execute_sql(&connection, sql, params)
    }

    async fn begin_transaction(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        {
            let connection = self.connection.lock().expect("sqlite connection lock");
            connection
                .execute_batch(match mode {
                    TransactionMode::Read | TransactionMode::Deferred => "BEGIN",
                    TransactionMode::Write => "BEGIN IMMEDIATE",
                })
                .map_err(sqlite_error)?;
        }
        Ok(Box::new(InMemorySqliteTransaction {
            connection: Arc::clone(&self.connection),
            mode,
        }))
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.begin_transaction(TransactionMode::Write).await
    }
}

#[async_trait(?Send)]
impl LixBackendTransaction for InMemorySqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionMode {
        self.mode
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        execute_sql(&connection, sql, params)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        connection.execute_batch("COMMIT").map_err(sqlite_error)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        connection.execute_batch("ROLLBACK").map_err(sqlite_error)
    }
}

pub(crate) async fn boot_test_engine(
) -> Result<(InMemorySqliteBackend, Arc<Engine>, Session), LixError> {
    let backend = InMemorySqliteBackend::new();
    let engine = Arc::new(boot(BootArgs::new(
        Box::new(backend.clone()),
        Arc::new(NoopWasmRuntime),
    )));
    engine.initialize().await?;
    let session = engine.open_workspace_session().await?;
    Ok((backend, engine, session))
}

fn execute_sql(
    connection: &rusqlite::Connection,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let bindings = params.iter().map(to_sqlite_value).collect::<Vec<_>>();
    let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
    let column_count = statement.column_count();
    let columns = statement
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();

    if column_count == 0 {
        statement
            .execute(rusqlite::params_from_iter(bindings))
            .map_err(sqlite_error)?;
        return Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        });
    }

    let mut rows = statement
        .query(rusqlite::params_from_iter(bindings))
        .map_err(sqlite_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            values.push(from_sqlite_value(row.get_ref(index).map_err(sqlite_error)?));
        }
        out.push(values);
    }

    Ok(QueryResult { rows: out, columns })
}

fn to_sqlite_value(value: &Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Boolean(value) => SqliteValue::Integer(i64::from(*value)),
        Value::Integer(value) => SqliteValue::Integer(*value),
        Value::Real(value) => SqliteValue::Real(*value),
        Value::Text(value) => SqliteValue::Text(value.clone()),
        Value::Json(value) => SqliteValue::Text(value.to_string()),
        Value::Blob(value) => SqliteValue::Blob(value.clone()),
    }
}

fn from_sqlite_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Integer(value),
        ValueRef::Real(value) => Value::Real(value),
        ValueRef::Text(value) => Value::Text(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::Blob(value.to_vec()),
    }
}

fn sqlite_error(error: rusqlite::Error) -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", error.to_string())
}
