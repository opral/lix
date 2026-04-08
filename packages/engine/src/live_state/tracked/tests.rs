use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::execution::write::transaction::{ReadContext, TransactionDelta, WriteTransaction};
use crate::live_state::constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
use crate::live_state::init as init_live_state;
use crate::live_state::tracked::{
    load_exact_row_with_backend, load_exact_rows_with_backend, scan_rows_with_backend,
    BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedScanRequest, TrackedWriteOperation,
    TrackedWriteRow,
};
use crate::session::workspace::init as init_workspace;
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
        global: true,
        plugin_key: "lix".to_string(),
        metadata: Some("{\"kind\":\"module-test\"}".to_string()),
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

async fn commit_tracked_rows(
    backend: &SqliteBackend,
    rows: Vec<TrackedWriteRow>,
) -> Result<(), LixError> {
    let read_context = ReadContext::new(backend, backend, backend);
    let backend_txn = backend.begin_transaction(TransactionMode::Write).await?;
    let mut write_tx = WriteTransaction::new(backend_txn, read_context);
    let schema_keys = rows
        .iter()
        .map(|row| row.schema_key.clone())
        .collect::<BTreeSet<_>>();
    for schema_key in schema_keys {
        write_tx.register_schema(schema_key)?;
    }
    write_tx.stage(TransactionDelta {
        tracked_writes: rows,
        untracked_writes: Vec::new(),
    })?;
    write_tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn live_tracked_state_roundtrips_rows() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    init_workspace(&backend)
        .await
        .expect("workspace init should succeed");
    commit_tracked_rows(
        &backend,
        vec![
            tracked_row("edge-1", "child-1", "change-1", timestamp),
            tracked_row("edge-2", "child-2", "change-2", timestamp),
        ],
    )
    .await
    .expect("tracked transaction should succeed");

    let exact = load_exact_row_with_backend(
        &backend,
        &ExactTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("exact tracked lookup should succeed")
    .expect("tracked row should exist");
    assert_eq!(exact.change_id.as_deref(), Some("change-1"));
    assert_eq!(exact.writer_key.as_deref(), Some("writer-a"));
    assert_eq!(exact.property_text("child_id").as_deref(), Some("child-1"));

    let batch = load_exact_rows_with_backend(
        &backend,
        &BatchTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_ids: vec!["edge-1".to_string(), "edge-2".to_string()],
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("batch tracked lookup should succeed");
    assert_eq!(batch.len(), 2);

    let scanned = scan_rows_with_backend(
        &backend,
        &TrackedScanRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            constraints: vec![
                ScanConstraint {
                    field: ScanField::EntityId,
                    operator: ScanOperator::In(vec![
                        Value::Text("edge-1".to_string()),
                        Value::Text("edge-2".to_string()),
                    ]),
                },
                ScanConstraint {
                    field: ScanField::PluginKey,
                    operator: ScanOperator::Eq(Value::Text("lix".to_string())),
                },
                ScanConstraint {
                    field: ScanField::SchemaVersion,
                    operator: ScanOperator::Range {
                        lower: Some(Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                        upper: Some(Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                    },
                },
            ],
            required_columns: vec!["child_id".to_string(), "parent_id".to_string()],
        },
    )
    .await
    .expect("tracked scan should succeed");
    assert_eq!(scanned.len(), 2);
    assert_eq!(
        scanned
            .iter()
            .map(|row| row.property_text("child_id").unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["child-1".to_string(), "child-2".to_string()]
    );
}

#[tokio::test]
async fn live_tracked_state_tombstones_hide_rows() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    let tombstone_time = "2026-03-24T00:05:00Z";
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    init_workspace(&backend)
        .await
        .expect("workspace init should succeed");
    commit_tracked_rows(
        &backend,
        vec![tracked_row("edge-1", "child-1", "change-1", timestamp)],
    )
    .await
    .expect("initial tracked transaction should succeed");

    commit_tracked_rows(
        &backend,
        vec![TrackedWriteRow {
            entity_id: "edge-1".to_string(),
            schema_key: "lix_commit_edge".to_string(),
            schema_version: "1".to_string(),
            file_id: "lix".to_string(),
            version_id: "main".to_string(),
            global: true,
            plugin_key: "lix".to_string(),
            metadata: Some("{\"kind\":\"module-test\"}".to_string()),
            change_id: "change-2".to_string(),
            writer_key: Some("writer-a".to_string()),
            snapshot_content: None,
            created_at: Some(tombstone_time.to_string()),
            updated_at: tombstone_time.to_string(),
            operation: TrackedWriteOperation::Tombstone,
        }],
    )
    .await
    .expect("tracked tombstone transaction should succeed");

    let exact = load_exact_row_with_backend(
        &backend,
        &ExactTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("exact tracked lookup should succeed after tombstone");
    assert!(exact.is_none());

    let scanned = scan_rows_with_backend(
        &backend,
        &TrackedScanRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            constraints: vec![ScanConstraint {
                field: ScanField::EntityId,
                operator: ScanOperator::Eq(Value::Text("edge-1".to_string())),
            }],
            required_columns: vec!["child_id".to_string()],
        },
    )
    .await
    .expect("tracked scan should succeed after tombstone");
    assert!(scanned.is_empty());
}
