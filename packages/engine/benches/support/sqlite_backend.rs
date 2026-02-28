use lix_engine::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};
use sqlx::{Column, Executor, Row, SqlitePool, ValueRef};
use std::path::Path;
use tokio::sync::OnceCell;

pub struct BenchSqliteBackend {
    conn: String,
    pool: OnceCell<SqlitePool>,
}

struct BenchSqliteTransaction {
    conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
}

impl BenchSqliteBackend {
    #[allow(dead_code)]
    pub fn in_memory() -> Self {
        Self {
            conn: "sqlite::memory:".to_string(),
            pool: OnceCell::const_new(),
        }
    }

    pub fn file_backed(path: &Path) -> Result<Self, LixError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
                    "failed to create sqlite benchmark directory {}: {error}",
                    parent.display()
                ),
            })?;
        }

        if !path.exists() {
            std::fs::File::create(path).map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
                    "failed to create sqlite benchmark file {}: {error}",
                    path.display()
                ),
            })?;
        }

        let conn = format!("sqlite://{}", path.display());
        Ok(Self {
            conn,
            pool: OnceCell::const_new(),
        })
    }

    async fn pool(&self) -> Result<&SqlitePool, LixError> {
        self.pool
            .get_or_try_init(|| async {
                SqlitePool::connect(&self.conn)
                    .await
                    .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
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
        let pool = self.pool().await?;

        if params.is_empty() && sql.contains(';') {
            pool.execute(sql).await.map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
            })?;
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_sqlite(query, param);
        }

        let rows = query.fetch_all(pool).await.map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
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

        let mut out_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out = Vec::with_capacity(row.columns().len());
            for idx in 0..row.columns().len() {
                out.push(map_sqlite_value(&row, idx)?);
            }
            out_rows.push(out);
        }
        Ok(QueryResult {
            rows: out_rows,
            columns,
        })
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        let pool = self.pool().await?;
        let mut conn = pool.acquire().await.map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
        })?;
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
            })?;
        Ok(Box::new(BenchSqliteTransaction { conn }))
    }
}

#[async_trait::async_trait(?Send)]
impl LixTransaction for BenchSqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        if params.is_empty() && sql.contains(';') {
            self.conn.execute(sql).await.map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
            })?;
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_sqlite(query, param);
        }

        let rows = query
            .fetch_all(&mut *self.conn)
            .await
            .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
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

        let mut out_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out = Vec::with_capacity(row.columns().len());
            for idx in 0..row.columns().len() {
                out.push(map_sqlite_value(&row, idx)?);
            }
            out_rows.push(out);
        }
        Ok(QueryResult {
            rows: out_rows,
            columns,
        })
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("COMMIT")
            .execute(&mut *self.conn)
            .await
            .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
            })?;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("ROLLBACK")
            .execute(&mut *self.conn)
            .await
            .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
            })?;
        Ok(())
    }
}

fn bind_sqlite<'q>(
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
        .map_err(|error| LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: error.to_string(),
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
