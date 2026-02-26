use async_trait::async_trait;
use lix_engine::{LixBackend, LixError, QueryResult, SqlDialect, Value};
use rusqlite::{params_from_iter, Connection, Row};
use std::sync::Mutex;

pub struct SqliteBackend {
    conn: Mutex<Connection>,
}

impl SqliteBackend {
    pub fn in_memory() -> Result<Self, LixError> {
        let conn = Connection::open_in_memory().map_err(|err| LixError {
            message: err.to_string(),
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
        let conn = self.conn.lock().map_err(|_| LixError {
            message: "sqlite mutex poisoned".to_string(),
        })?;

        if params.is_empty() && sql.contains(';') {
            conn.execute_batch(sql).map_err(|err| LixError {
                message: err.to_string(),
            })?;
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        let mut stmt = conn.prepare(sql).map_err(|err| LixError {
            message: err.to_string(),
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
                message: err.to_string(),
            })?;
        let mut result_rows = Vec::new();
        while let Some(row) = rows.next().map_err(|err| LixError {
            message: err.to_string(),
            })? {
            result_rows.push(map_row(row)?);
        }
        Ok(QueryResult {
            rows: result_rows,
            columns,
        })
    }

    async fn begin_transaction(
        &self,
    ) -> Result<Box<dyn lix_engine::LixTransaction + '_>, LixError> {
        Err(LixError {
            message: "transactions are not implemented for rs-sdk sqlite backend".to_string(),
        })
    }
}

fn map_row(row: &Row<'_>) -> Result<Vec<Value>, LixError> {
    let mut values = Vec::new();
    for idx in 0..row.as_ref().column_count() {
        let value = row.get_ref(idx).map_err(|err| LixError {
            message: err.to_string(),
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
        Value::Integer(value) => rusqlite::types::Value::Integer(value),
        Value::Real(value) => rusqlite::types::Value::Real(value),
        Value::Text(value) => rusqlite::types::Value::Text(value),
        Value::Blob(value) => rusqlite::types::Value::Blob(value),
    }
}
