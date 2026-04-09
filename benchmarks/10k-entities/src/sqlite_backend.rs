use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use lix_engine::{
    collapse_prepared_batch_for_dialect, LixBackend, LixBackendTransaction, LixError,
    PreparedBatch, QueryResult, SqlDialect, TransactionMode, Value,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Column, Executor, Row, TypeInfo, ValueRef};
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct BenchSqliteBackend {
    inner: Arc<BenchSqliteBackendInner>,
}

struct BenchSqliteBackendInner {
    filename: String,
    pool: OnceCell<sqlx::SqlitePool>,
}

struct BenchSqliteTransaction {
    conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
    mode: TransactionMode,
}

impl BenchSqliteBackend {
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

        Ok(Self {
            inner: Arc::new(BenchSqliteBackendInner {
                filename: path.display().to_string(),
                pool: OnceCell::const_new(),
            }),
        })
    }

    async fn pool(&self) -> Result<&sqlx::SqlitePool, LixError> {
        self.inner
            .pool
            .get_or_try_init(|| async {
                let conn = if self.inner.filename == ":memory:" {
                    "sqlite::memory:".to_string()
                } else if self.inner.filename.starts_with("sqlite:")
                    || self.inner.filename.starts_with("file:")
                {
                    self.inner.filename.clone()
                } else {
                    format!("sqlite://{}", self.inner.filename)
                };

                let options = SqliteConnectOptions::from_str(&conn)
                    .map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: error.to_string(),
                    })?
                    .create_if_missing(true)
                    .foreign_keys(true)
                    .busy_timeout(std::time::Duration::from_secs(30));

                SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(options)
                    .await
                    .map_err(|error| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: error.to_string(),
                    })
            })
            .await
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackend for BenchSqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut transaction = self.begin_transaction(TransactionMode::Deferred).await?;
        let result = transaction.execute(sql, params).await;
        match result {
            Ok(result) => {
                transaction.commit().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn begin_transaction(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let pool = self.pool().await?;
        let mut conn = pool.acquire().await.map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?;

        sqlx::query(match mode {
            TransactionMode::Read | TransactionMode::Deferred => "BEGIN",
            TransactionMode::Write => "BEGIN IMMEDIATE",
        })
        .execute(&mut *conn)
        .await
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?;

        Ok(Box::new(BenchSqliteTransaction { conn, mode }))
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.begin_transaction(TransactionMode::Write).await
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackendTransaction for BenchSqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionMode {
        self.mode
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        execute_query_with_connection(&mut self.conn, sql, params).await
    }

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        let collapsed = collapse_prepared_batch_for_dialect(batch, self.dialect())?;
        if collapsed.sql.trim().is_empty() {
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        self.conn
            .execute(collapsed.sql.as_str())
            .await
            .map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: error.to_string(),
            })?;

        Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        })
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("COMMIT")
            .execute(&mut *self.conn)
            .await
            .map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: error.to_string(),
            })?;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("ROLLBACK")
            .execute(&mut *self.conn)
            .await
            .map_err(|error| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: error.to_string(),
            })?;
        Ok(())
    }
}

async fn execute_query_with_connection(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let mut query = sqlx::query(sql);
    for param in params {
        query = bind_param_sqlite(query, param);
    }

    let rows = query
        .fetch_all(&mut **conn)
        .await
        .map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: error.to_string(),
        })?;

    let columns = rows
        .first()
        .map(|row| {
            row.columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let mut result_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut out = Vec::with_capacity(row.columns().len());
        for index in 0..row.columns().len() {
            out.push(map_sqlite_value(&row, index)?);
        }
        result_rows.push(out);
    }

    Ok(QueryResult {
        rows: result_rows,
        columns,
    })
}

fn bind_param_sqlite<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    param: &Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match param {
        Value::Null => query.bind::<Option<i64>>(None),
        Value::Boolean(value) => query.bind(*value),
        Value::Integer(value) => query.bind(*value),
        Value::Real(value) => query.bind(*value),
        Value::Text(value) => query.bind(value.clone()),
        Value::Blob(value) => query.bind(value.clone()),
        Value::Json(value) => query.bind(value.to_string()),
    }
}

fn map_sqlite_value(row: &sqlx::sqlite::SqliteRow, index: usize) -> Result<Value, LixError> {
    let raw = row.try_get_raw(index).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })?;

    if raw.is_null() {
        return Ok(Value::Null);
    }

    match raw.type_info().name() {
        "INTEGER" => row.try_get::<i64, _>(index).map(Value::Integer),
        "REAL" => row.try_get::<f64, _>(index).map(Value::Real),
        "TEXT" => row.try_get::<String, _>(index).map(Value::Text),
        "BLOB" => row.try_get::<Vec<u8>, _>(index).map(Value::Blob),
        _ => row
            .try_get::<String, _>(index)
            .map(Value::Text)
            .or_else(|_| row.try_get::<i64, _>(index).map(Value::Integer))
            .or_else(|_| row.try_get::<f64, _>(index).map(Value::Real))
            .or_else(|_| row.try_get::<Vec<u8>, _>(index).map(Value::Blob)),
    }
    .map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })
}
