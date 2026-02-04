use sqlx::{Row, SqlitePool, ValueRef};
use tokio::sync::OnceCell;

use lix_engine::{LixBackend, LixError, QueryResult, Value};

use crate::support::simulation_test::Simulation;

pub fn sqlite_simulation() -> Simulation {
    Simulation {
        name: "sqlite",
        setup: None,
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

                SqlitePool::connect(&conn).await.map_err(|err| LixError {
                    message: err.to_string(),
                })
            })
            .await
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackend for SqliteBackend {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let pool = self.pool().await?;
        let mut query = sqlx::query(sql);

        for param in params {
            query = bind_param_sqlite(query, param);
        }

        let rows = query.fetch_all(pool).await.map_err(|err| LixError {
            message: err.to_string(),
        })?;

        let mut result_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out = Vec::with_capacity(row.columns().len());
            for i in 0..row.columns().len() {
                out.push(map_sqlite_value(&row, i)?);
            }
            result_rows.push(out);
        }

        Ok(QueryResult { rows: result_rows })
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
