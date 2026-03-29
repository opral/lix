use std::sync::{Arc, Mutex};

use crate::live_state::projection::committed_version_ref_mirror_write_row;
use crate::live_state::tracked::{
    load_exact_row_with_backend, ExactTrackedRowRequest, TrackedWriteOperation, TrackedWriteRow,
};
use crate::live_state::untracked::{
    load_exact_row_with_backend as load_exact_untracked_row_with_backend, ExactUntrackedRowRequest,
    UntrackedWriteRow,
};
use crate::transaction::{ReadContext, TransactionDelta, WriteTransaction};
use crate::{
    LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect, TransactionMode, Value,
};
use async_trait::async_trait;
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

fn tracked_row(
    entity_id: &str,
    child_id: &str,
    change_id: &str,
    timestamp: &str,
) -> TrackedWriteRow {
    TrackedWriteRow {
        entity_id: entity_id.to_string(),
        schema_key: "lix_commit_edge".to_string(),
        schema_version: "1".to_string(),
        file_id: "lix".to_string(),
        version_id: "main".to_string(),
        global: false,
        plugin_key: "lix".to_string(),
        metadata: Some("{\"kind\":\"txn-module\"}".to_string()),
        change_id: change_id.to_string(),
        writer_key: Some("writer-a".to_string()),
        snapshot_content: Some(format!(
            "{{\"child_id\":\"{child_id}\",\"parent_id\":\"parent-{entity_id}\"}}"
        )),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: TrackedWriteOperation::Upsert,
    }
}

#[tokio::test]
async fn isolated_transaction_commits_tracked_and_untracked_batches() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    let read_context = ReadContext::new(&backend, &backend);
    let backend_txn = backend
        .begin_transaction(TransactionMode::Write)
        .await
        .expect("begin transaction should succeed");

    let mut write_tx = WriteTransaction::new(backend_txn, read_context);
    write_tx
        .register_schema("lix_commit_edge")
        .expect("tracked schema registration should stage");
    write_tx
        .register_schema("lix_version_ref")
        .expect("untracked schema registration should stage");
    write_tx
        .stage(TransactionDelta {
            tracked_writes: vec![tracked_row("edge-1", "child-1", "change-1", timestamp)],
            untracked_writes: vec![committed_version_ref_mirror_write_row(
                "main", "commit-1", timestamp,
            )],
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
            file_id: Some("lix".to_string()),
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
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("untracked lookup should succeed")
    .expect("untracked row should exist");
    assert_eq!(
        version_ref.property_text("commit_id").as_deref(),
        Some("commit-1")
    );
}

#[tokio::test]
async fn isolated_transaction_rejects_staging_after_execute() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    let read_context = ReadContext::new(&backend, &backend);
    let backend_txn = backend
        .begin_transaction(TransactionMode::Write)
        .await
        .expect("begin transaction should succeed");

    let mut write_tx = WriteTransaction::new(backend_txn, read_context);
    write_tx
        .register_schema("lix_commit_edge")
        .expect("tracked schema registration should stage");
    write_tx
        .stage(TransactionDelta {
            tracked_writes: vec![tracked_row("edge-1", "child-1", "change-1", timestamp)],
            untracked_writes: Vec::<UntrackedWriteRow>::new(),
        })
        .expect("staging should succeed");

    write_tx.execute().await.expect("execute should succeed");
    let error = write_tx
        .stage(TransactionDelta {
            tracked_writes: vec![tracked_row("edge-2", "child-2", "change-2", timestamp)],
            untracked_writes: Vec::new(),
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
    let read_context = ReadContext::new(&backend, &backend);
    let backend_txn = backend
        .begin_transaction(TransactionMode::Write)
        .await
        .expect("begin transaction should succeed");

    let mut write_tx = WriteTransaction::new(backend_txn, read_context);
    write_tx
        .register_schema("lix_commit_edge")
        .expect("tracked schema registration should stage");
    write_tx
        .register_schema("lix_version_ref")
        .expect("untracked schema registration should stage");
    write_tx
        .stage(TransactionDelta {
            tracked_writes: vec![tracked_row("edge-1", "child-1", "change-1", timestamp)],
            untracked_writes: vec![committed_version_ref_mirror_write_row(
                "main", "commit-1", timestamp,
            )],
        })
        .expect("staging should succeed");

    write_tx.rollback().await.expect("rollback should succeed");

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
    .expect("tracked lookup should succeed");
    assert!(tracked.is_none());

    let version_ref = load_exact_untracked_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_id: "main".to_string(),
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("untracked lookup should succeed");
    assert!(version_ref.is_none());
}
