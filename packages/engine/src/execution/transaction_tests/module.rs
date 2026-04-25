use std::sync::{Arc, Mutex};

use crate::live_state::tracked::{load_exact_row_with_backend, ExactTrackedRowRequest};
use crate::live_state::untracked::{
    load_exact_row_with_backend as load_exact_untracked_row_with_backend, ExactUntrackedRowRequest,
};
use crate::live_state::{LiveWriteOperation, LiveWriteRow};
use crate::test_support::init_test_backend_core;
use crate::transaction::{LiveStateWriteTransaction, OverlayReadContext, TransactionDelta};
use crate::version::GLOBAL_VERSION_ID;
use crate::{
    LixBackend, LixBackendTransaction, LixError, NullableKeyFilter, QueryResult, SqlDialect,
    TransactionBeginMode, Value,
};
use async_trait::async_trait;
use rusqlite::types::{Value as SqliteValue, ValueRef};

#[derive(Clone)]
struct SqliteBackend {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

struct SqliteTransaction {
    connection: Arc<Mutex<rusqlite::Connection>>,
    mode: TransactionBeginMode,
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

#[async_trait]
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
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        {
            let connection = self.connection.lock().expect("sqlite connection lock");
            connection
                .execute_batch(match mode {
                    TransactionBeginMode::Read | TransactionBeginMode::Deferred => "BEGIN",
                    TransactionBeginMode::Write => "BEGIN IMMEDIATE",
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
        self.begin_transaction(TransactionBeginMode::Write).await
    }
}

#[async_trait]
impl LixBackendTransaction for SqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionBeginMode {
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
        hint: None,
    }
}

async fn init_workspace(backend: &dyn LixBackend) -> Result<(), LixError> {
    init_test_backend_core(backend).await
}

fn tracked_row(entity_id: &str, child_id: &str, change_id: &str, timestamp: &str) -> LiveWriteRow {
    LiveWriteRow {
        entity_id: entity_id.to_string(),
        schema_key: "lix_commit_edge".to_string(),
        schema_version: "1".to_string(),
        file_id: None,
        version_id: "main".to_string(),
        global: false,
        untracked: false,
        plugin_key: None,
        metadata: Some("{\"kind\":\"txn-module\"}".to_string()),
        change_id: change_id.to_string(),
        snapshot_content: Some(format!(
            "{{\"child_id\":\"{child_id}\",\"parent_id\":\"parent-{entity_id}\"}}"
        )),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: LiveWriteOperation::Upsert,
    }
}

fn local_version_head_untracked_write_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> LiveWriteRow {
    LiveWriteRow {
        entity_id: version_id.to_string(),
        schema_key: "lix_version_ref".to_string(),
        schema_version: "1".to_string(),
        file_id: None,
        version_id: GLOBAL_VERSION_ID.to_string(),
        global: true,
        untracked: true,
        plugin_key: None,
        metadata: None,
        change_id: format!("change-version-ref::{version_id}::{commit_id}::{timestamp}"),
        snapshot_content: Some(format!(
            "{{\"id\":\"{version_id}\",\"commit_id\":\"{commit_id}\"}}"
        )),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: LiveWriteOperation::Upsert,
    }
}

#[tokio::test]
async fn isolated_transaction_commits_tracked_and_untracked_batches() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    init_workspace(&backend)
        .await
        .expect("workspace init should succeed");
    let read_context = OverlayReadContext::new(&backend, &backend);
    let backend_txn = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("begin transaction should succeed");

    let mut write_tx = LiveStateWriteTransaction::new(backend_txn, read_context);
    write_tx
        .register_schema("lix_commit_edge")
        .expect("tracked schema registration should stage");
    write_tx
        .register_schema("lix_version_ref")
        .expect("untracked schema registration should stage");
    write_tx
        .stage(TransactionDelta {
            writes: vec![
                tracked_row("edge-1", "child-1", "change-1", timestamp),
                local_version_head_untracked_write_row("main", "commit-1", timestamp),
            ],
        })
        .expect("staging should succeed");

    assert_eq!(write_tx.journal().staged_count(), 1);
    assert!(write_tx.journal().continuation_safe());

    let outcome = write_tx.commit().await.expect("commit should succeed");
    assert_eq!(outcome.tracked_upserts, 1);
    assert_eq!(outcome.untracked_upserts, 1);

    let tracked = load_exact_row_with_backend(
        &backend,
        &ExactTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: NullableKeyFilter::Null,
            untracked: false,
        },
    )
    .await
    .expect("tracked lookup should succeed")
    .expect("tracked row should exist");
    assert_eq!(tracked.change_id.as_deref(), Some("change-1"));
    assert_eq!(
        tracked.property_text("child_id").as_deref(),
        Some("child-1")
    );

    let version_ref = load_exact_untracked_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_id: "main".to_string(),
            file_id: NullableKeyFilter::Null,
            untracked: true,
        },
    )
    .await
    .expect("untracked lookup should succeed")
    .expect("untracked row should exist");
    assert_eq!(
        version_ref.property_text("commit_id").as_deref(),
        Some("commit-1")
    );

    let version_ref_change = backend
        .execute(
            "SELECT change_id \
             FROM lix_internal_live_v1_lix_version_ref \
             WHERE entity_id = 'main' \
               AND version_id = 'global' \
               AND untracked = true \
             LIMIT 1",
            &[],
        )
        .await
        .expect("untracked live row change_id lookup should succeed");
    assert_eq!(version_ref_change.rows.len(), 1);
    match &version_ref_change.rows[0][0] {
        Value::Text(value) => assert!(!value.is_empty()),
        other => panic!("expected text untracked change_id, got {other:?}"),
    }

    let canonical = backend
        .execute(
            "SELECT COUNT(*) \
             FROM lix_internal_untracked_change_visibility v \
             JOIN lix_internal_change ch ON ch.id = v.change_id \
             WHERE ch.schema_key = 'lix_version_ref' \
               AND ch.entity_id = 'main' \
               AND v.version_id = 'global' \
               AND v.visibility_kind = 'global'",
            &[],
        )
        .await
        .expect("canonical untracked visibility lookup should succeed");
    assert_eq!(canonical.rows.len(), 1);
    assert_eq!(canonical.rows[0][0], Value::Integer(1));
}

#[tokio::test]
async fn isolated_transaction_rejects_staging_after_execute() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    init_workspace(&backend)
        .await
        .expect("workspace init should succeed");
    let read_context = OverlayReadContext::new(&backend, &backend);
    let backend_txn = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("begin transaction should succeed");

    let mut write_tx = LiveStateWriteTransaction::new(backend_txn, read_context);
    write_tx
        .register_schema("lix_commit_edge")
        .expect("tracked schema registration should stage");
    write_tx
        .stage(TransactionDelta {
            writes: vec![tracked_row("edge-1", "child-1", "change-1", timestamp)],
        })
        .expect("staging should succeed");

    write_tx.execute().await.expect("execute should succeed");
    let error = write_tx
        .stage(TransactionDelta {
            writes: vec![tracked_row("edge-2", "child-2", "change-2", timestamp)],
        })
        .expect_err("staging after execute must fail");

    assert!(error
        .description
        .contains("cannot stage new transaction work after execute()"));
}

#[tokio::test]
async fn isolated_transaction_rollback_discards_staged_writes() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    init_workspace(&backend)
        .await
        .expect("workspace init should succeed");
    let read_context = OverlayReadContext::new(&backend, &backend);
    let backend_txn = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("begin transaction should succeed");

    let mut write_tx = LiveStateWriteTransaction::new(backend_txn, read_context);
    write_tx
        .register_schema("lix_commit_edge")
        .expect("tracked schema registration should stage");
    write_tx
        .register_schema("lix_version_ref")
        .expect("untracked schema registration should stage");
    write_tx
        .stage(TransactionDelta {
            writes: vec![
                tracked_row("edge-1", "child-1", "change-1", timestamp),
                local_version_head_untracked_write_row("main", "commit-1", timestamp),
            ],
        })
        .expect("staging should succeed");

    write_tx.rollback().await.expect("rollback should succeed");

    let tracked = load_exact_row_with_backend(
        &backend,
        &ExactTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: NullableKeyFilter::Null,
            untracked: false,
        },
    )
    .await
    .expect("tracked lookup should succeed");
    assert!(tracked.is_none());

    let version_ref = load_exact_untracked_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_id: "main".to_string(),
            file_id: NullableKeyFilter::Null,
            untracked: true,
        },
    )
    .await
    .expect("untracked lookup should succeed");
    assert!(version_ref.is_none());
}
