use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use lix_engine::{
    boot, collapse_prepared_batch_for_dialect, BootArgs, CreateVersionOptions, Engine,
    ImageChunkReader, ImageChunkWriter, LixBackend, LixError, LixTransaction, NoopWasmRuntime,
    PreparedBatch, QueryResult, SqlDialect, Value, WasmRuntime,
};
use rusqlite::{
    backup::{Backup, StepResult},
    params_from_iter, Connection, Row,
};

fn boot_sqlite_engine_at_path(path: &Path, wasm_runtime: Arc<dyn WasmRuntime>) -> Engine {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("sqlite test parent directory should be creatable");
    }
    let _ = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)
        .expect("sqlite test file should be creatable");
    let backend =
        TestImageSqliteBackend::from_path(path).expect("test sqlite backend should open path");
    let mut args = BootArgs::new(
        Box::new(backend) as Box<dyn LixBackend + Send + Sync>,
        wasm_runtime,
    );
    args.access_to_internal = true;
    boot(args)
}

fn temp_sqlite_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-restore-{label}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

fn cleanup_sqlite_path(path: &Path) {
    let _ = std::fs::remove_file(path);
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let shm = PathBuf::from(format!("{}-shm", path.display()));
    let journal = PathBuf::from(format!("{}-journal", path.display()));
    let _ = std::fs::remove_file(wal);
    let _ = std::fs::remove_file(shm);
    let _ = std::fs::remove_file(journal);
}

struct TestImageSqliteBackend {
    conn: Mutex<Connection>,
}

struct TestImageSqliteTransaction<'a> {
    conn: MutexGuard<'a, Connection>,
    finalized: bool,
}

impl TestImageSqliteBackend {
    fn from_path(path: impl AsRef<Path>) -> Result<Self, LixError> {
        let conn = Connection::open(path).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to open sqlite test database: {error}"),
            )
        })?;
        conn.busy_timeout(Duration::from_secs(30))
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to configure sqlite busy timeout: {error}"),
                )
            })?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }
}

#[async_trait(?Send)]
impl LixBackend for TestImageSqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        lix_engine::execute_auto_transactional(self, sql, params).await
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        let conn = self.conn.lock().map_err(|_| {
            LixError::new("LIX_ERROR_UNKNOWN", "sqlite test backend mutex poisoned")
        })?;
        conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")
            .map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to begin sqlite transaction: {error}"),
                )
            })?;

        Ok(Box::new(TestImageSqliteTransaction {
            conn,
            finalized: false,
        }))
    }

    async fn export_image(&self, writer: &mut dyn ImageChunkWriter) -> Result<(), LixError> {
        let conn = self.conn.lock().map_err(|_| {
            LixError::new("LIX_ERROR_UNKNOWN", "sqlite test backend mutex poisoned")
        })?;
        let image_path = temp_image_path("export");

        let export_result = (|| -> Result<(), LixError> {
            let mut snapshot_conn = Connection::open(&image_path).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to open sqlite export image: {error}"),
                )
            })?;
            let backup = Backup::new(&conn, &mut snapshot_conn).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to start sqlite export backup: {error}"),
                )
            })?;
            run_backup_to_completion(&backup)
        })();

        let bytes = std::fs::read(&image_path).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to read sqlite export image: {error}"),
            )
        });
        let _ = std::fs::remove_file(&image_path);
        export_result?;
        let bytes = bytes?;

        for chunk in bytes.chunks(64 * 1024) {
            writer.write_chunk(chunk).await?;
        }
        writer.finish().await?;
        Ok(())
    }

    async fn restore_from_image(&self, reader: &mut dyn ImageChunkReader) -> Result<(), LixError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.read_chunk().await? {
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "restore image stream is empty",
            ));
        }

        let image_path = temp_image_path("restore");
        std::fs::write(&image_path, &bytes).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to write sqlite restore image: {error}"),
            )
        })?;

        let restore_result = (|| -> Result<(), LixError> {
            let source_conn = Connection::open(&image_path).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to open sqlite restore image: {error}"),
                )
            })?;
            let mut conn = self.conn.lock().map_err(|_| {
                LixError::new("LIX_ERROR_UNKNOWN", "sqlite test backend mutex poisoned")
            })?;
            let backup = Backup::new(&source_conn, &mut conn).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("failed to start sqlite restore backup: {error}"),
                )
            })?;
            run_backup_to_completion(&backup)
        })();
        let _ = std::fs::remove_file(&image_path);
        restore_result
    }

    async fn begin_savepoint(&self, _name: &str) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        self.begin_transaction().await
    }
}

#[async_trait(?Send)]
impl LixTransaction for TestImageSqliteTransaction<'_> {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        execute_sql(&self.conn, sql, params)
    }

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        execute_prepared_batch(&self.conn, batch, self.dialect())
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.conn.execute_batch("COMMIT").map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to commit sqlite transaction: {error}"),
            )
        })?;
        self.finalized = true;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        self.conn.execute_batch("ROLLBACK").map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to roll back sqlite transaction: {error}"),
            )
        })?;
        self.finalized = true;
        Ok(())
    }
}

impl Drop for TestImageSqliteTransaction<'_> {
    fn drop(&mut self) {
        if !self.finalized && !std::thread::panicking() {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

fn execute_sql(conn: &Connection, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
    let mut statement = conn.prepare(sql).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to prepare sqlite query: {error}"),
        )
    })?;
    let columns = statement
        .column_names()
        .into_iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();
    let bound_params = params.iter().cloned().map(to_sql_value);
    let mut rows = statement
        .query(params_from_iter(bound_params))
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to execute sqlite query: {error}"),
            )
        })?;

    let mut result_rows = Vec::new();
    while let Some(row) = rows.next().map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to fetch sqlite row: {error}"),
        )
    })? {
        result_rows.push(map_row(row)?);
    }

    Ok(QueryResult {
        rows: result_rows,
        columns,
    })
}

fn execute_prepared_batch(
    conn: &Connection,
    batch: &PreparedBatch,
    dialect: SqlDialect,
) -> Result<QueryResult, LixError> {
    let collapsed = collapse_prepared_batch_for_dialect(batch, dialect)?;
    if collapsed.sql.trim().is_empty() {
        return Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        });
    }

    conn.execute_batch(&collapsed.sql).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to execute sqlite batch: {error}"),
        )
    })?;
    Ok(QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    })
}

fn run_backup_to_completion(backup: &Backup<'_, '_>) -> Result<(), LixError> {
    loop {
        match backup.step(-1).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("sqlite backup failed: {error}"),
            )
        })? {
            StepResult::Done => return Ok(()),
            StepResult::More => continue,
            StepResult::Busy | StepResult::Locked => std::thread::sleep(Duration::from_millis(5)),
            _ => continue,
        }
    }
}

fn temp_image_path(operation: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "lix-engine-restore-test-{operation}-{}-{nanos}.sqlite",
        std::process::id()
    ))
}

fn map_row(row: &Row<'_>) -> Result<Vec<Value>, LixError> {
    let mut values = Vec::new();
    for index in 0..row.as_ref().column_count() {
        let value = row.get_ref(index).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("failed to read sqlite value: {error}"),
            )
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
        Value::Boolean(value) => rusqlite::types::Value::Integer(if value { 1 } else { 0 }),
        Value::Integer(value) => rusqlite::types::Value::Integer(value),
        Value::Real(value) => rusqlite::types::Value::Real(value),
        Value::Text(value) => rusqlite::types::Value::Text(value),
        Value::Json(value) => rusqlite::types::Value::Text(value.to_string()),
        Value::Blob(value) => rusqlite::types::Value::Blob(value),
    }
}

#[derive(Default)]
struct VecImageWriter {
    bytes: Vec<u8>,
}

#[async_trait(?Send)]
impl ImageChunkWriter for VecImageWriter {
    async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), LixError> {
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }
}

struct OneShotImageReader {
    bytes: Option<Vec<u8>>,
}

impl OneShotImageReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes: Some(bytes) }
    }
}

#[async_trait(?Send)]
impl ImageChunkReader for OneShotImageReader {
    async fn read_chunk(&mut self) -> Result<Option<Vec<u8>>, LixError> {
        Ok(self.bytes.take())
    }
}

async fn export_image_bytes(engine: &Engine) -> Vec<u8> {
    let mut writer = VecImageWriter::default();
    engine
        .export_image(&mut writer)
        .await
        .expect("export_image should succeed");
    writer.bytes
}

fn text_value(value: &Value, field: &str) -> String {
    match value {
        Value::Text(value) => value.clone(),
        other => panic!("expected text value for {field}, got {other:?}"),
    }
}

async fn key_value_version_id(engine: &Engine, key: &str) -> String {
    let result = engine
        .execute(
            "SELECT lixcol_version_id \
             FROM lix_key_value_by_version \
             WHERE key = $1 \
             LIMIT 1",
            &[Value::Text(key.to_string())],
        )
        .await
        .expect("key value version query should succeed");
    assert_eq!(result.statements[0].rows.len(), 1);
    text_value(
        &result.statements[0].rows[0][0],
        "lix_key_value_by_version.lixcol_version_id",
    )
}

async fn run_restore_from_image_refreshes_active_version_cache_sqlite() {
    let source_path = temp_sqlite_path("source");
    let destination_path = temp_sqlite_path("destination");

    let source_engine = boot_sqlite_engine_at_path(&source_path, Arc::new(NoopWasmRuntime));
    source_engine
        .initialize()
        .await
        .expect("source init should succeed");
    let source_version = source_engine
        .create_version(CreateVersionOptions {
            id: Some("after".to_string()),
            name: Some("after".to_string()),
            source_version_id: None,
            hidden: false,
        })
        .await
        .expect("source create_version should succeed");
    source_engine
        .switch_version(source_version.id.clone())
        .await
        .expect("source switch_version should succeed");
    let image_bytes = export_image_bytes(&source_engine).await;

    let destination_engine =
        boot_sqlite_engine_at_path(&destination_path, Arc::new(NoopWasmRuntime));
    destination_engine
        .initialize()
        .await
        .expect("destination init should succeed");
    let destination_version = destination_engine
        .create_version(CreateVersionOptions {
            id: Some("before".to_string()),
            name: Some("before".to_string()),
            source_version_id: None,
            hidden: false,
        })
        .await
        .expect("destination create_version should succeed");
    destination_engine
        .switch_version(destination_version.id.clone())
        .await
        .expect("destination switch_version should succeed");

    let mut reader = OneShotImageReader::new(image_bytes);
    destination_engine
        .restore_from_image(&mut reader)
        .await
        .expect("restore_from_image should succeed");

    destination_engine
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('restore-cache-check', '1')",
            &[],
        )
        .await
        .expect("tracked write after restore should succeed");
    assert_eq!(
        key_value_version_id(&destination_engine, "restore-cache-check").await,
        "after",
        "restore should refresh the active-version cache before subsequent tracked writes"
    );

    drop(destination_engine);
    drop(source_engine);
    cleanup_sqlite_path(&destination_path);
    cleanup_sqlite_path(&source_path);
}

#[test]
fn restore_from_image_refreshes_active_version_cache_sqlite() {
    std::thread::Builder::new()
        .name("restore_from_image_refreshes_active_version_cache_sqlite".to_string())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio runtime should build");
            runtime.block_on(run_restore_from_image_refreshes_active_version_cache_sqlite());
        })
        .expect("restore test thread should spawn")
        .join()
        .expect("restore test thread should not panic");
}
