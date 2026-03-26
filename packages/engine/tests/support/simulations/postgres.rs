use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use postgresql_embedded::PostgreSQL;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, Executor, PgPool, Row, ValueRef};
use tokio::sync::{Mutex as TokioMutex, OnceCell};

use lix_engine::{
    collapse_prepared_batch_for_dialect, LixBackend, LixBackendTransaction, LixError,
    PreparedBatch, QueryResult, SqlDialect, Value,
};

use crate::support::simulation_test::{Simulation, SimulationBehavior};

static POSTGRES: OnceCell<Arc<PostgresInstance>> = OnceCell::const_new();
static DB_COUNTER: AtomicUsize = AtomicUsize::new(0);
const CREATE_DATABASE_RETRY_LIMIT: usize = 8;

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
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
            pg.setup().await.map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
            pg.start().await.map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
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

fn is_retryable_database_creation_error(error: &LixError) -> bool {
    error.description.contains("deadline has elapsed")
}

async fn create_database_url(prefix: &str) -> Result<String, LixError> {
    let process_id = std::process::id();

    for attempt in 0..CREATE_DATABASE_RETRY_LIMIT {
        let instance = ensure_postgres().await?;
        let db_index = DB_COUNTER.fetch_add(1, Ordering::Relaxed);
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let db_name = format!("{prefix}_{process_id}_{now_nanos}_{db_index}");

        let create_result = {
            let pg = instance.postgresql.lock().await;
            pg.create_database(&db_name).await.map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })
        };

        match create_result {
            Ok(()) => return Ok(instance.settings.url(&db_name)),
            Err(error)
                if attempt + 1 < CREATE_DATABASE_RETRY_LIMIT
                    && is_retryable_database_creation_error(&error) =>
            {
                tokio::time::sleep(Duration::from_millis(500 * (attempt as u64 + 1))).await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("database creation retry loop should always return")
}

pub fn postgres_simulation() -> Simulation {
    let connection_string = Arc::new(Mutex::new(None::<String>));
    let setup_handle = connection_string.clone();

    Simulation {
        name: "postgres",
        setup: Some(Arc::new(move || {
            let connection_string = setup_handle.clone();
            Box::pin(async move {
                let url = create_database_url("lix_test").await?;

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

#[allow(dead_code)]
pub fn postgres_backend_with_connection_string(
    connection_string: String,
) -> Box<dyn LixBackend + Send + Sync> {
    Box::new(PostgresBackend::new(PostgresConfig { connection_string }))
}

#[allow(dead_code)]
pub async fn create_postgres_test_database_url(label: &str) -> Result<String, LixError> {
    let normalized_label = label
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    create_database_url(&format!("lix_obs_{normalized_label}")).await
}

struct PostgresBackend {
    config: PostgresConfig,
    pool: OnceCell<PgPool>,
}

struct PostgresLixBackendTransaction {
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
                PgPoolOptions::new()
                    // Simulation tests are mostly single-threaded per engine. Keeping pools
                    // small prevents one highly parallel test binary from exhausting the shared
                    // embedded Postgres instance and stalling later tests at init time.
                    .max_connections(2)
                    .acquire_timeout(Duration::from_secs(60))
                    .connect(&self.config.connection_string)
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
impl LixBackend for PostgresBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Postgres
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        lix_engine::execute_auto_transactional(self, sql, params).await
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let pool = self.pool().await?;
        let mut conn = pool.acquire().await.map_err(|err| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: err.to_string(),
        })?;
        sqlx::query("BEGIN")
            .execute(&mut *conn)
            .await
            .map_err(|err| LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: err.to_string(),
            })?;
        Ok(Box::new(PostgresLixBackendTransaction { conn }))
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.begin_transaction().await
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackendTransaction for PostgresLixBackendTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Postgres
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
    conn: &mut sqlx::pool::PoolConnection<sqlx::Postgres>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let mut query = sqlx::query(sql);
    for param in params {
        query = bind_param_postgres(query, param);
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
            out.push(map_postgres_value(&row, i)?);
        }
        result_rows.push(out);
    }

    Ok(QueryResult {
        rows: result_rows,
        columns,
    })
}

fn bind_param_postgres<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    param: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
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

fn map_postgres_value(row: &sqlx::postgres::PgRow, index: usize) -> Result<Value, LixError> {
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
        return Ok(Value::Boolean(value));
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
                        code: "LIX_ERROR_UNKNOWN".to_string(),
                        description: format!(
                            "Timed out acquiring postgres simulation lock at {}",
                            path.display()
                        ),
                    });
                }

                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "failed to list processes for postgres simulation cleanup: {error}"
            ),
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
    cleanup_stale_sysv_ipc_objects();

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

fn cleanup_stale_sysv_ipc_objects() {
    cleanup_stale_sysv_shared_memory_segments();
    cleanup_stale_sysv_semaphores();
}

fn cleanup_stale_sysv_shared_memory_segments() {
    let Ok(current_user) = std::env::var("USER") else {
        return;
    };

    let Ok(output) = std::process::Command::new("ipcs")
        .args(["-m", "-a"])
        .output()
    else {
        return;
    };

    if !output.status.success() {
        return;
    }

    let mut segment_ids = Vec::new();
    let listing = String::from_utf8_lossy(&output.stdout);
    for line in listing.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 9 || fields[0] != "m" {
            continue;
        }

        let id = fields[1];
        let owner = fields[4];
        let nattch = fields[8];
        if owner == current_user && nattch == "0" {
            segment_ids.push(id.to_string());
        }
    }

    for id in segment_ids {
        let _ = std::process::Command::new("ipcrm")
            .args(["-m", &id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

fn cleanup_stale_sysv_semaphores() {
    let Ok(current_user) = std::env::var("USER") else {
        return;
    };

    let Ok(output) = std::process::Command::new("ipcs")
        .args(["-s", "-p"])
        .output()
    else {
        return;
    };

    if !output.status.success() {
        return;
    }

    let mut semaphore_ids = Vec::new();
    let listing = String::from_utf8_lossy(&output.stdout);
    for line in listing.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 8 || fields[0] != "s" {
            continue;
        }

        let id = fields[1];
        let owner = fields[4];
        if owner != current_user {
            continue;
        }

        let cpid = fields[6].parse::<u32>().ok();
        let lpid = fields[7].parse::<u32>().ok();
        let cpid_alive = cpid.is_some_and(is_pid_alive);
        let lpid_alive = lpid.is_some_and(is_pid_alive);
        if !cpid_alive && !lpid_alive {
            semaphore_ids.push(id.to_string());
        }
    }

    for id in semaphore_ids {
        let _ = std::process::Command::new("ipcrm")
            .args(["-s", &id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}
