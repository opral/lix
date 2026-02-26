use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use postgresql_embedded::PostgreSQL;
use sqlx::{Column, Executor, PgPool, Row, ValueRef};
use tokio::sync::{Mutex as TokioMutex, OnceCell};

use lix_engine::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};

use crate::support::simulation_test::{Simulation, SimulationBehavior};

static POSTGRES: OnceCell<Arc<PostgresInstance>> = OnceCell::const_new();
static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct PostgresInstance {
    _lock: FileLock,
    postgresql: TokioMutex<PostgreSQL>,
    settings: postgresql_embedded::Settings,
}

async fn ensure_postgres() -> Result<Arc<PostgresInstance>, LixError> {
    POSTGRES
        .get_or_try_init(|| async {
            let lock_path = std::env::temp_dir().join("lix-embedded-postgres.global.lock");
            let lock = acquire_lock(&lock_path).await?;
            cleanup_stale_embedded_postgres_processes()?;

            let process_id = std::process::id();
            let mut settings = postgresql_embedded::Settings::new();
            settings.data_dir =
                std::env::temp_dir().join(format!("lix-embedded-postgres-{process_id}"));
            settings.password_file =
                std::env::temp_dir().join(format!("lix-embedded-postgres-{process_id}.pgpass"));
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
                .insert("max_connections".to_string(), "64".to_string());
            let mut pg = PostgreSQL::new(settings);
            if pg.settings().data_dir.exists() {
                if pg.settings().data_dir.join("postmaster.pid").exists() {
                    let _ = pg.stop().await;
                }
                let _ = std::fs::remove_dir_all(pg.settings().data_dir.clone());
            }
            if pg.settings().password_file.exists() {
                let _ = std::fs::remove_file(pg.settings().password_file.clone());
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
                _lock: lock,
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
                let process_id = std::process::id();
                let now_nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_nanos())
                    .unwrap_or(0);
                let db_name = format!("lix_test_{}_{}_{}", process_id, now_nanos, db_index);

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
        behavior: SimulationBehavior::Base,
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

struct PostgresBackendTransaction {
    conn: sqlx::pool::PoolConnection<sqlx::Postgres>,
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
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        let mut query = sqlx::query(sql);

        for param in params {
            query = bind_param_postgres(query, param);
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
                out.push(map_postgres_value(&row, i)?);
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
        Ok(Box::new(PostgresBackendTransaction { conn }))
    }
}

#[async_trait::async_trait(?Send)]
impl LixTransaction for PostgresBackendTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Postgres
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        if params.is_empty() && sql.contains(';') {
            self.conn.execute(sql).await.map_err(|err| LixError {
                message: err.to_string(),
            })?;
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        let mut query = sqlx::query(sql);
        for param in params {
            query = bind_param_postgres(query, param);
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
                out.push(map_postgres_value(&row, i)?);
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

struct FileLock {
    path: std::path::PathBuf,
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn acquire_lock(path: &std::path::Path) -> Result<FileLock, LixError> {
    let started = std::time::Instant::now();
    let timeout = Duration::from_secs(1800);
    let stale_after = Duration::from_secs(300);

    loop {
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
        {
            Ok(mut file) => {
                use std::io::Write;
                let pid = std::process::id();
                let _ = writeln!(file, "{pid}");
                return Ok(FileLock {
                    path: path.to_path_buf(),
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Ok(content) = std::fs::read_to_string(path) {
                    if let Ok(owner_pid) = content.trim().parse::<u32>() {
                        if !is_pid_alive(owner_pid) {
                            let _ = std::fs::remove_file(path);
                            continue;
                        }
                    }
                }

                if let Ok(metadata) = std::fs::metadata(path) {
                    if let Ok(modified) = metadata.modified() {
                        if modified.elapsed().unwrap_or_default() > stale_after {
                            let _ = std::fs::remove_file(path);
                            continue;
                        }
                    }
                }

                if started.elapsed() > timeout {
                    return Err(LixError {
                        message: format!(
                            "Timed out acquiring postgres simulation lock at {}",
                            path.display()
                        ),
                    });
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => {
                return Err(LixError {
                    message: format!(
                        "Failed to acquire postgres simulation lock at {}: {}",
                        path.display(),
                        error
                    ),
                });
            }
        }
    }
}

fn is_pid_alive(pid: u32) -> bool {
    std::process::Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn cleanup_stale_embedded_postgres_processes() -> Result<(), LixError> {
    let output = std::process::Command::new("ps")
        .args(["-ax", "-o", "pid=,command="])
        .output()
        .map_err(|error| LixError {
            message: format!("failed to list processes for postgres simulation cleanup: {error}"),
        })?;

    let listing = String::from_utf8_lossy(&output.stdout);
    let mut pids = Vec::new();
    for line in listing.lines() {
        if !line.contains("/postgres") || !line.contains("lix-embedded-postgres-") {
            continue;
        }
        let Some(pid_text) = line.split_whitespace().next() else {
            continue;
        };
        if let Ok(pid) = pid_text.parse::<u32>() {
            pids.push(pid);
        }
    }

    pids.sort_unstable();
    pids.dedup();

    for pid in &pids {
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }

    std::thread::sleep(Duration::from_millis(200));

    for pid in &pids {
        if is_pid_alive(*pid) {
            let _ = std::process::Command::new("kill")
                .arg("-KILL")
                .arg(pid.to_string())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
        }
    }

    cleanup_stale_embedded_postgres_artifacts();

    Ok(())
}

fn cleanup_stale_embedded_postgres_artifacts() {
    let temp_dir = std::env::temp_dir();
    let Ok(entries) = std::fs::read_dir(temp_dir) else {
        return;
    };

    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };

        let Some(pid) = embedded_postgres_owner_pid(file_name) else {
            continue;
        };

        if is_pid_alive(pid) {
            continue;
        }

        let path = entry.path();
        if let Ok(file_type) = entry.file_type() {
            if file_type.is_dir() {
                let _ = std::fs::remove_dir_all(path);
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

fn embedded_postgres_owner_pid(file_name: &str) -> Option<u32> {
    let suffix = file_name.strip_prefix("lix-embedded-postgres-")?;
    let suffix = suffix.strip_suffix(".pgpass").unwrap_or(suffix);
    suffix.parse::<u32>().ok()
}
