use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Column, Executor, Row, SqlitePool, ValueRef};
use tokio::sync::OnceCell;

use lix_engine::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};

use crate::support::simulation_test::{Simulation, SimulationBehavior};

pub fn sqlite_simulation() -> Simulation {
    Simulation {
        name: "sqlite",
        setup: None,
        behavior: SimulationBehavior::Base,
        backend_factory: Box::new(|| {
            Box::new(SqliteBackend::new(SqliteConfig {
                filename: ":memory:".to_string(),
            })) as Box<dyn LixBackend + Send + Sync>
        }),
    }
}

struct SqliteBackend {
    config: SqliteConfig,
    pool: OnceCell<SqlitePool>,
}

struct SqliteBackendTransaction {
    conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
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
                } else if self.config.filename.starts_with("sqlite:") {
                    self.config.filename.clone()
                } else {
                    format!("sqlite://{}", self.config.filename)
                };

                let options = SqliteConnectOptions::from_str(&conn)
                    .map_err(|err| LixError {
                        message: err.to_string(),
                    })?
                    .foreign_keys(true);

                SqlitePoolOptions::new()
                    .connect_with(options)
                    .await
                    .map_err(|err| LixError {
                        message: err.to_string(),
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
        let pool = self.pool().await?;

        if params.is_empty() && sql.contains(';') {
            pool.execute(sql).await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            return Ok(QueryResult { rows: Vec::new(), columns: Vec::new() });
        }

        let mut query = sqlx::query(sql);

        for param in params {
            query = bind_param_sqlite(query, param);
        }

        let rows = query.fetch_all(pool).await.map_err(|err| LixError {
            message: err.to_string(),
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

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        let pool = self.pool().await?;
        let mut conn = pool.acquire().await.map_err(|err| LixError {
            message: err.to_string(),
        })?;
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .map_err(|err| LixError {
                message: err.to_string(),
            })?;
        Ok(Box::new(SqliteBackendTransaction { conn }))
    }
}

#[async_trait::async_trait(?Send)]
impl LixTransaction for SqliteBackendTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        if params.is_empty() && sql.contains(';') {
            self.conn.execute(sql).await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            return Ok(QueryResult { rows: Vec::new(), columns: Vec::new() });
        }

        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param_sqlite(query, param);
        }

        let rows = query
            .fetch_all(&mut *self.conn)
            .await
            .map_err(|err| LixError {
                message: err.to_string(),
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

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("COMMIT")
            .execute(&mut *self.conn)
            .await
            .map_err(|err| LixError {
                message: err.to_string(),
            })?;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("ROLLBACK")
            .execute(&mut *self.conn)
            .await
            .map_err(|err| LixError {
                message: err.to_string(),
            })?;
        Ok(())
    }
}

fn bind_param_sqlite<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    param: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match param {
        Value::Null => query.bind(Option::<i64>::None),
        Value::Integer(v) => query.bind(*v),
        Value::Real(v) => query.bind(*v),
        Value::Text(v) => query.bind(v.as_str()),
        Value::Blob(v) => query.bind(v.as_slice()),
    }
}

fn map_sqlite_value(row: &sqlx::sqlite::SqliteRow, index: usize) -> Result<Value, LixError> {
    if row
        .try_get_raw(index)
        .map_err(|err| LixError {
            message: err.to_string(),
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
