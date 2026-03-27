use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Column, Executor, Row, SqlitePool, ValueRef};
use tokio::sync::OnceCell;

use lix_engine::{
    collapse_prepared_batch_for_dialect, LixBackend, LixBackendTransaction, LixError,
    PreparedBatch, QueryResult, SqlDialect, TransactionMode, Value,
};

use crate::support::simulation_test::{Simulation, SimulationBehavior};

static SQLITE_MEMORY_DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn sqlite_simulation() -> Simulation {
    Simulation {
        name: "sqlite",
        setup: None,
        behavior: SimulationBehavior::Base,
        backend_factory: Box::new(|| {
            let db_index = SQLITE_MEMORY_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
            let filename = format!(
                "sqlite:file:lix_sim_{process_id}_{db_index}?mode=memory&cache=shared",
                process_id = std::process::id(),
                db_index = db_index,
            );
            Box::new(SqliteBackend::new(SqliteConfig { filename }))
                as Box<dyn LixBackend + Send + Sync>
        }),
    }
}

#[allow(dead_code)]
pub fn sqlite_backend_with_filename(filename: String) -> Box<dyn LixBackend + Send + Sync> {
    Box::new(SqliteBackend::new(SqliteConfig { filename }))
}

struct SqliteBackend {
    config: SqliteConfig,
    pool: OnceCell<SqlitePool>,
}

struct SqliteLixBackendTransaction {
    conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
    mode: TransactionMode,
}

struct SqliteConfig {
    filename: String,
}

impl SqliteBackend {
    fn new(config: SqliteConfig) -> Self {
        Self {
            config,
            pool: OnceCell::const_new(),
        }
    }

    async fn pool(&self) -> Result<&SqlitePool, LixError> {
        self.pool
            .get_or_try_init(|| async {
                let conn = if self.config.filename == ":memory:" {
                    "sqlite::memory:".to_string()
                } else if self.config.filename.starts_with("sqlite:")
                    || self.config.filename.starts_with("file:")
                {
                    self.config.filename.clone()
                } else {
                    format!("sqlite://{}", self.config.filename)
                };

                let options = SqliteConnectOptions::from_str(&conn)
                    .map_err(|err| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: err.to_string(),
                    })?
                    .foreign_keys(true)
                    .busy_timeout(std::time::Duration::from_secs(30));

                SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect_with(options)
                    .await
                    .map_err(|err| LixError {
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: err.to_string(),
                    })
            })
            .await
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackend for SqliteBackend {
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
        let mut conn = pool.acquire().await.map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        sqlx::query(match mode {
            TransactionMode::Read | TransactionMode::Deferred => "BEGIN",
            TransactionMode::Write => "BEGIN IMMEDIATE",
        })
        .execute(&mut *conn)
        .await
        .map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        Ok(Box::new(SqliteLixBackendTransaction { conn, mode }))
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.begin_transaction(TransactionMode::Write).await
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackendTransaction for SqliteLixBackendTransaction {
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
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
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
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("ROLLBACK")
            .execute(&mut *self.conn)
            .await
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
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

    let rows = query.fetch_all(&mut **conn).await.map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: err.to_string(),
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
        for i in 0..row.columns().len() {
            out.push(map_sqlite_value(&row, i)?);
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
    param: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match param {
        Value::Null => query.bind(Option::<i64>::None),
        Value::Boolean(v) => query.bind(*v),
        Value::Integer(v) => query.bind(*v),
        Value::Real(v) => query.bind(*v),
        Value::Text(v) => query.bind(v.as_str()),
        Value::Json(v) => query.bind(v.to_string()),
        Value::Blob(v) => query.bind(v.as_slice()),
    }
}

fn map_sqlite_value(row: &sqlx::sqlite::SqliteRow, index: usize) -> Result<Value, LixError> {
    if row
        .try_get_raw(index)
        .map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?
        .is_null()
    {
        return Ok(Value::Null);
    }

    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Ok(Value::Integer(value));
    }
    if let Ok(value) = row.try_get::<f64, _>(index) {
        return Ok(Value::Real(value));
    }
    if let Ok(value) = row.try_get::<String, _>(index) {
        return Ok(Value::Text(value));
    }
    if let Ok(value) = row.try_get::<Vec<u8>, _>(index) {
        return Ok(Value::Blob(value));
    }

    Ok(Value::Null)
}
