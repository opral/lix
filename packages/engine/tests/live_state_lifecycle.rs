use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::live_state::tracked::{load_exact_row_with_backend, ExactTrackedRowRequest};
use lix_engine::live_state::untracked::{
    load_exact_row_with_backend as load_exact_untracked_row_with_backend, ExactUntrackedRowRequest,
};
use lix_engine::live_state::{
    apply_rebuild_plan, finalize_commit, init as init_live_state, register_schema, require_ready,
    LiveStateRebuildPlan, LiveStateRebuildScope, SchemaRegistration,
};
use lix_engine::{
    LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect, TransactionMode, Value,
};
use rusqlite::types::{Value as SqliteValue, ValueRef};

#[derive(Clone)]
struct SqliteBackend {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

struct SqliteTransaction {
    connection: Arc<Mutex<rusqlite::Connection>>,
    mode: TransactionMode,
}

impl SqliteBackend {
    fn new() -> Self {
        let connection = rusqlite::Connection::open_in_memory().expect("open sqlite memory db");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("enable foreign keys");
        Self {
            connection: Arc::new(Mutex::new(connection)),
        }
    }
}

#[async_trait(?Send)]
impl LixBackend for SqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        execute_sql(&connection, sql, params)
    }

    async fn begin_transaction(
        &self,
        mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        {
            let connection = self.connection.lock().expect("sqlite connection lock");
            connection
                .execute_batch(match mode {
                    TransactionMode::Read | TransactionMode::Deferred => "BEGIN",
                    TransactionMode::Write => "BEGIN IMMEDIATE",
                })
                .map_err(sqlite_error)?;
        }
        Ok(Box::new(SqliteTransaction {
            connection: Arc::clone(&self.connection),
            mode,
        }))
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.begin_transaction(TransactionMode::Write).await
    }
}

#[async_trait(?Send)]
impl LixBackendTransaction for SqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionMode {
        self.mode
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        execute_sql(&connection, sql, params)
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        connection.execute_batch("COMMIT").map_err(sqlite_error)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        connection.execute_batch("ROLLBACK").map_err(sqlite_error)
    }
}

fn execute_sql(
    connection: &rusqlite::Connection,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let bindings = params.iter().map(to_sqlite_value).collect::<Vec<_>>();
    let mut statement = connection.prepare(sql).map_err(sqlite_error)?;
    let column_count = statement.column_count();
    let columns = statement
        .column_names()
        .iter()
        .map(|name| name.to_string())
        .collect::<Vec<_>>();

    if column_count == 0 {
        statement
            .execute(rusqlite::params_from_iter(bindings))
            .map_err(sqlite_error)?;
        return Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        });
    }

    let mut rows = statement
        .query(rusqlite::params_from_iter(bindings))
        .map_err(sqlite_error)?;
    let mut out = Vec::new();
    while let Some(row) = rows.next().map_err(sqlite_error)? {
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            values.push(from_sqlite_value(row.get_ref(index).map_err(sqlite_error)?));
        }
        out.push(values);
    }

    Ok(QueryResult { rows: out, columns })
}

fn to_sqlite_value(value: &Value) -> SqliteValue {
    match value {
        Value::Null => SqliteValue::Null,
        Value::Boolean(value) => SqliteValue::Integer(i64::from(*value)),
        Value::Integer(value) => SqliteValue::Integer(*value),
        Value::Real(value) => SqliteValue::Real(*value),
        Value::Text(value) => SqliteValue::Text(value.clone()),
        Value::Json(value) => SqliteValue::Text(value.to_string()),
        Value::Blob(value) => SqliteValue::Blob(value.clone()),
    }
}

fn from_sqlite_value(value: ValueRef<'_>) -> Value {
    match value {
        ValueRef::Null => Value::Null,
        ValueRef::Integer(value) => Value::Integer(value),
        ValueRef::Real(value) => Value::Real(value),
        ValueRef::Text(value) => Value::Text(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => Value::Blob(value.to_vec()),
    }
}

fn sqlite_error(error: rusqlite::Error) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    }
}

async fn create_change_table(backend: &SqliteBackend) {
    backend
        .execute(
            "CREATE TABLE IF NOT EXISTS lix_internal_change (\
             id TEXT PRIMARY KEY,\
             created_at TEXT NOT NULL\
             )",
            &[],
        )
        .await
        .expect("create change table");
}

async fn insert_canonical_change(backend: &SqliteBackend, id: &str, created_at: &str) {
    backend
        .execute(
            &format!(
                "INSERT INTO lix_internal_change (id, created_at) VALUES ('{}', '{}')",
                id, created_at
            ),
            &[],
        )
        .await
        .expect("insert canonical change");
}

async fn count_live_tables(backend: &SqliteBackend) -> i64 {
    let result = backend
        .execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'lix_internal_live_v1_%'",
            &[],
        )
        .await
        .expect("count live tables");
    match result.rows[0][0] {
        Value::Integer(value) => value,
        ref other => panic!("expected integer count, got {other:?}"),
    }
}

fn empty_rebuild_plan(scope: LiveStateRebuildScope) -> LiveStateRebuildPlan {
    LiveStateRebuildPlan {
        run_id: "test-rebuild".to_string(),
        scope,
        stats: Vec::new(),
        writes: Vec::new(),
        warnings: Vec::new(),
        debug: None,
    }
}

#[tokio::test]
async fn init_and_register_schema_use_explicit_lifecycle_entrypoints() {
    let backend = SqliteBackend::new();

    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    register_schema(&backend, "lix_commit_edge")
        .await
        .expect("builtin schema registration should succeed");

    let status_rows = backend
        .execute(
            "SELECT mode FROM lix_internal_live_state_status WHERE singleton_id = 1",
            &[],
        )
        .await
        .expect("status query should succeed");
    assert_eq!(status_rows.rows.len(), 1);
    assert_eq!(count_live_tables(&backend).await, 1);
}

#[tokio::test]
async fn register_schema_accepts_runtime_registered_snapshot_without_owning_schema_catalog() {
    let backend = SqliteBackend::new();

    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    register_schema(
        &backend,
        SchemaRegistration::with_registered_snapshot(
            "runtime_profile",
            serde_json::json!({
                "value": {
                    "x-lix-key": "runtime_profile",
                    "x-lix-version": "1",
                    "type": "object",
                    "properties": {
                        "name": { "type": "string" }
                    }
                }
            }),
        ),
    )
    .await
    .expect("runtime schema registration should succeed");

    let result = backend
        .execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'lix_internal_live_v1_runtime_profile'",
            &[],
        )
        .await
        .expect("runtime schema table lookup should succeed");
    assert_eq!(result.rows[0][0], Value::Integer(1));
}

#[tokio::test]
async fn require_ready_stays_not_ready_until_finalize_commit_updates_watermark() {
    let backend = SqliteBackend::new();
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    create_change_table(&backend).await;

    assert!(require_ready(&backend).await.is_err());

    insert_canonical_change(&backend, "change-1", "2026-03-24T00:00:00Z").await;
    let watermark = finalize_commit(&backend)
        .await
        .expect("finalize_commit should mark live_state ready");
    assert_eq!(watermark.change_id, "change-1");
    require_ready(&backend)
        .await
        .expect("live_state should be ready after finalization");
}

#[tokio::test]
async fn rebuild_apply_controls_ready_vs_needs_rebuild_mode() {
    let backend = SqliteBackend::new();
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    create_change_table(&backend).await;
    insert_canonical_change(&backend, "change-1", "2026-03-24T00:00:00Z").await;

    apply_rebuild_plan(
        &backend,
        &empty_rebuild_plan(LiveStateRebuildScope::Versions(
            ["main".to_string()].into_iter().collect(),
        )),
    )
    .await
    .expect("partial rebuild apply should succeed");

    let partial_mode = backend
        .execute(
            "SELECT mode FROM lix_internal_live_state_status WHERE singleton_id = 1",
            &[],
        )
        .await
        .expect("partial mode query should succeed");
    assert_eq!(
        partial_mode.rows[0][0],
        Value::Text("needs_rebuild".to_string())
    );

    apply_rebuild_plan(&backend, &empty_rebuild_plan(LiveStateRebuildScope::Full))
        .await
        .expect("full rebuild apply should succeed");

    let full_mode = backend
        .execute(
            "SELECT mode, latest_change_id FROM lix_internal_live_state_status WHERE singleton_id = 1",
            &[],
        )
        .await
        .expect("full mode query should succeed");
    assert_eq!(full_mode.rows[0][0], Value::Text("ready".to_string()));
    assert_eq!(full_mode.rows[0][1], Value::Text("change-1".to_string()));
}

#[tokio::test]
async fn reads_do_not_auto_create_storage() {
    let backend = SqliteBackend::new();
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");

    let tracked = load_exact_row_with_backend(
        &backend,
        &ExactTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("tracked read should not fail for missing storage");
    assert!(tracked.is_none());

    let untracked = load_exact_untracked_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_id: "main".to_string(),
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("untracked read should not fail for missing storage");
    assert!(untracked.is_none());

    assert_eq!(count_live_tables(&backend).await, 0);
}

#[test]
fn transaction_module_does_not_own_live_state_ddl_or_watermark_sql() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let write_plan = std::fs::read_to_string(root.join("src/transaction/write_plan.rs"))
        .expect("read write_plan.rs");
    let write_runner = std::fs::read_to_string(root.join("src/transaction/write_runner.rs"))
        .expect("read write_runner.rs");
    let execution = std::fs::read_to_string(root.join("src/transaction/execution.rs"))
        .expect("read execution.rs");
    let combined = format!("{write_plan}\n{write_runner}\n{execution}");

    for forbidden in [
        "EnsureUntrackedStorage",
        "ensure_untracked_storage",
        "ensure_schema_live_table_sql_statements",
        "LIVE_STATE_STATUS_CREATE_TABLE_SQL",
        "LIVE_STATE_STATUS_SEED_ROW_SQL",
        "build_mark_live_state_ready_sql",
        "build_set_live_state_mode_sql",
        "quoted_live_table_name(",
    ] {
        assert!(
            !combined.contains(forbidden),
            "transaction module must not own live_state DDL or watermark SQL: found {forbidden}",
        );
    }
}

#[test]
fn live_state_module_does_not_absorb_domain_bootstrap_seeding() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let combined = std::fs::read_to_string(root.join("src/live_state/mod.rs"))
        .expect("read live_state/mod.rs")
        + &std::fs::read_to_string(root.join("src/live_state/lifecycle.rs"))
            .expect("read live_state/lifecycle.rs")
        + &std::fs::read_to_string(root.join("src/live_state/materialize/mod.rs"))
            .expect("read live_state/materialize/mod.rs");

    for forbidden in [
        "seed_default_versions",
        "seed_global_system_directories",
        "seed_default_active_version",
        "seed_default_checkpoint_label",
        "seed_boot_key_values",
        "seed_boot_account",
    ] {
        assert!(
            !combined.contains(forbidden),
            "live_state must not absorb init/domain seeding: found {forbidden}",
        );
    }
}
