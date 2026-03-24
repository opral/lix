use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::live_state::install as install_live_state;
use lix_engine::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use lix_engine::live_state::untracked::{
    active_version_write_row, load_active_version_with_backend, load_exact_row_with_backend,
    load_exact_rows_with_backend, load_version_ref_with_backend, scan_rows_with_backend,
    version_ref_write_row, BatchUntrackedRowRequest,
    ExactUntrackedRowRequest, UntrackedScanRequest, UntrackedWriteOperation, UntrackedWriteRow,
};
use lix_engine::transaction::{ReadContext, TransactionDelta, WriteTransaction};
use lix_engine::{LixBackend, LixError, LixTransaction, QueryResult, SqlDialect, Value};
use rusqlite::types::{Value as SqliteValue, ValueRef};

#[derive(Clone)]
struct SqliteBackend {
    connection: Arc<Mutex<rusqlite::Connection>>,
}

struct SqliteTransaction {
    connection: Arc<Mutex<rusqlite::Connection>>,
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

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        {
            let connection = self.connection.lock().expect("sqlite connection lock");
            connection.execute_batch("BEGIN").map_err(sqlite_error)?;
        }
        Ok(Box::new(SqliteTransaction {
            connection: Arc::clone(&self.connection),
        }))
    }

    async fn begin_savepoint(&self, _name: &str) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        self.begin_transaction().await
    }
}

#[async_trait(?Send)]
impl LixTransaction for SqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
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

async fn commit_untracked_rows(
    backend: &SqliteBackend,
    rows: Vec<UntrackedWriteRow>,
) -> Result<(), LixError> {
    let read_context = ReadContext::new(backend, backend);
    let backend_txn = backend.begin_transaction().await?;
    let mut write_tx = WriteTransaction::new(backend_txn, read_context);
    let schema_keys = rows
        .iter()
        .map(|row| row.schema_key.clone())
        .collect::<BTreeSet<_>>();
    for schema_key in schema_keys {
        write_tx.register_schema(schema_key)?;
    }
    write_tx.stage(TransactionDelta {
        tracked_writes: Vec::new(),
        untracked_writes: rows,
    })?;
    write_tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn live_untracked_state_roundtrips_helper_rows() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    install_live_state(&backend)
        .await
        .expect("live_state install should succeed");
    commit_untracked_rows(
        &backend,
        vec![
            active_version_write_row("active-row", "main", timestamp),
            version_ref_write_row("main", "commit-1", timestamp),
            version_ref_write_row("other", "commit-2", timestamp),
        ],
    )
    .await
    .expect("helper row transaction should succeed");

    let active_version = load_active_version_with_backend(&backend)
        .await
        .expect("active version lookup should succeed")
        .expect("active version row should exist");
    assert_eq!(active_version.entity_id, "active-row");
    assert_eq!(active_version.version_id, "main");

    let version_ref = load_version_ref_with_backend(&backend, "main")
        .await
        .expect("version ref lookup should succeed")
        .expect("version ref row should exist");
    assert_eq!(version_ref.version_id, "main");
    assert_eq!(version_ref.commit_id, "commit-1");

    let exact = load_exact_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_id: "main".to_string(),
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("exact untracked lookup should succeed")
    .expect("exact untracked row should exist");
    assert_eq!(
        exact.property_text("commit_id").as_deref(),
        Some("commit-1")
    );

    let batch = load_exact_rows_with_backend(
        &backend,
        &BatchUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_ids: vec!["main".to_string(), "other".to_string()],
            file_id: Some("lix".to_string()),
        },
    )
    .await
    .expect("batch untracked lookup should succeed");
    assert_eq!(batch.len(), 2);

    let scanned = scan_rows_with_backend(
        &backend,
        &UntrackedScanRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            constraints: vec![
                ScanConstraint {
                    field: ScanField::EntityId,
                    operator: ScanOperator::In(vec![
                        Value::Text("main".to_string()),
                        Value::Text("other".to_string()),
                    ]),
                },
                ScanConstraint {
                    field: ScanField::PluginKey,
                    operator: ScanOperator::Eq(Value::Text("lix".to_string())),
                },
                ScanConstraint {
                    field: ScanField::SchemaVersion,
                    operator: ScanOperator::Range {
                        lower: Some(lix_engine::live_state::constraints::Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                        upper: Some(lix_engine::live_state::constraints::Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                    },
                },
            ],
            required_columns: vec!["commit_id".to_string()],
        },
    )
    .await
    .expect("scan should succeed");
    assert_eq!(scanned.len(), 2);
    assert_eq!(
        scanned
            .iter()
            .map(|row| row.property_text("commit_id").unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["commit-1".to_string(), "commit-2".to_string()]
    );
}

#[tokio::test]
async fn live_untracked_state_delete_removes_rows() {
    let backend = SqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    install_live_state(&backend)
        .await
        .expect("live_state install should succeed");
    commit_untracked_rows(
        &backend,
        vec![version_ref_write_row("main", "commit-1", timestamp)],
    )
    .await
    .expect("initial version ref transaction should succeed");

    commit_untracked_rows(
        &backend,
        vec![UntrackedWriteRow {
            entity_id: "main".to_string(),
            schema_key: "lix_version_ref".to_string(),
            schema_version: "1".to_string(),
            file_id: "lix".to_string(),
            version_id: "global".to_string(),
            global: true,
            plugin_key: "lix".to_string(),
            metadata: None,
            writer_key: None,
            snapshot_content: None,
            created_at: None,
            updated_at: timestamp.to_string(),
            operation: UntrackedWriteOperation::Delete,
        }],
    )
    .await
    .expect("delete transaction should succeed");

    let version_ref = load_version_ref_with_backend(&backend, "main")
        .await
        .expect("version ref lookup should succeed");
    assert!(version_ref.is_none());
}
