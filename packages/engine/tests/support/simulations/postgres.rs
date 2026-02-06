use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use postgresql_embedded::{PostgreSQL, Status};
use sqlx::{Executor, PgPool, Row, ValueRef};
use tokio::sync::{Mutex as TokioMutex, OnceCell};

use lix_engine::{LixBackend, LixError, QueryResult, SqlDialect, Value};

use crate::support::simulation_test::Simulation;

static POSTGRES: OnceCell<Arc<PostgresInstance>> = OnceCell::const_new();
static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct PostgresInstance {
    postgresql: TokioMutex<PostgreSQL>,
    settings: postgresql_embedded::Settings,
}

async fn ensure_postgres() -> Result<Arc<PostgresInstance>, LixError> {
    POSTGRES
        .get_or_try_init(|| async {
            let mut settings = postgresql_embedded::Settings::new();
            settings.data_dir = std::env::temp_dir().join("lix-embedded-postgres");
            settings.password_file = std::env::temp_dir().join("lix-embedded-postgres.pgpass");
            settings.password = "lix_test_password".to_string();
            settings.temporary = false;
            settings
                .configuration
                .insert("dynamic_shared_memory_type".to_string(), "mmap".to_string());
            settings
                .configuration
                .insert("shared_buffers".to_string(), "8MB".to_string());
            settings
                .configuration
                .insert("max_connections".to_string(), "10".to_string());
            let mut pg = PostgreSQL::new(settings);
            if pg.settings().data_dir.exists() {
                if pg.settings().data_dir.join("postmaster.pid").exists()
                    && pg.status() == Status::Started
                {
                    let _ = pg.stop().await;
                }
                let _ = std::fs::remove_dir_all(pg.settings().data_dir.clone());
            }
            std::fs::create_dir_all(pg.settings().data_dir.clone()).map_err(|err| LixError {
                message: err.to_string(),
            })?;
            pg.setup().await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            pg.start().await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            let settings = pg.settings().clone();

            Ok(Arc::new(PostgresInstance {
                postgresql: TokioMutex::new(pg),
                settings,
            }))
        })
        .await
        .map(Arc::clone)
}

pub fn postgres_simulation() -> Simulation {
    let connection_string = Arc::new(Mutex::new(None::<String>));
    let setup_handle = connection_string.clone();

    Simulation {
        name: "postgres",
        setup: Some(Arc::new(move || {
            let connection_string = setup_handle.clone();
            Box::pin(async move {
                let instance = ensure_postgres().await?;
                let db_index = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
                let db_name = format!("lix_test_{}", db_index);

                {
                    let pg = instance.postgresql.lock().await;
                    pg.create_database(&db_name).await.map_err(|err| LixError {
                        message: err.to_string(),
                    })?;
                }

                let url = instance.settings.url(&db_name);

                *connection_string
                    .lock()
                    .expect("postgres connection string mutex poisoned") = Some(url);

                Ok(())
            })
        })),
        backend_factory: Box::new(move || {
            let url = connection_string
                .lock()
                .expect("postgres connection string mutex poisoned")
                .clone()
                .expect("postgres setup did not run");
            Box::new(PostgresBackend::new(PostgresConfig {
                connection_string: url,
            })) as Box<dyn LixBackend + Send + Sync>
        }),
    }
}

struct PostgresBackend {
    config: PostgresConfig,
    pool: OnceCell<PgPool>,
}

struct PostgresConfig {
    connection_string: String,
}

impl PostgresBackend {
    fn new(config: PostgresConfig) -> Self {
        Self {
            config,
            pool: OnceCell::const_new(),
        }
    }

    async fn pool(&self) -> Result<&PgPool, LixError> {
        self.pool
            .get_or_try_init(|| async {
                PgPool::connect(&self.config.connection_string)
                    .await
                    .map_err(|err| LixError {
                        message: err.to_string(),
                    })
            })
            .await
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackend for PostgresBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Postgres
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let pool = self.pool().await?;

        if params.is_empty() && sql.contains(';') {
            pool.execute(sql).await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            return Ok(QueryResult { rows: Vec::new() });
        }

        let mut query = sqlx::query(sql);

        for param in params {
            query = bind_param_postgres(query, param);
        }

        let rows = query.fetch_all(pool).await.map_err(|err| LixError {
            message: err.to_string(),
        })?;

        let mut result_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut out = Vec::with_capacity(row.columns().len());
            for i in 0..row.columns().len() {
                out.push(map_postgres_value(&row, i)?);
            }
            result_rows.push(out);
        }

        Ok(QueryResult { rows: result_rows })
    }
}

fn bind_param_postgres<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    param: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match param {
        Value::Null => query.bind(Option::<i64>::None),
        Value::Integer(v) => query.bind(*v),
        Value::Real(v) => query.bind(*v),
        Value::Text(v) => query.bind(v.as_str()),
        Value::Blob(v) => query.bind(v.as_slice()),
    }
}

fn map_postgres_value(row: &sqlx::postgres::PgRow, index: usize) -> Result<Value, LixError> {
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
    if let Ok(value) = row.try_get::<i32, _>(index) {
        return Ok(Value::Integer(value as i64));
    }
    if let Ok(value) = row.try_get::<i16, _>(index) {
        return Ok(Value::Integer(value as i64));
    }
    if let Ok(value) = row.try_get::<f64, _>(index) {
        return Ok(Value::Real(value));
    }
    if let Ok(value) = row.try_get::<f32, _>(index) {
        return Ok(Value::Real(value as f64));
    }
    if let Ok(value) = row.try_get::<bool, _>(index) {
        return Ok(Value::Integer(if value { 1 } else { 0 }));
    }
    if let Ok(value) = row.try_get::<String, _>(index) {
        return Ok(Value::Text(value));
    }
    if let Ok(value) = row.try_get::<Vec<u8>, _>(index) {
        return Ok(Value::Blob(value));
    }

    Ok(Value::Null)
}
