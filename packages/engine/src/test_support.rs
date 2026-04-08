use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use rusqlite::types::{Value as SqliteValue, ValueRef};

use crate::contracts::traits::PendingView;
use crate::projections::ProjectionRegistry;
use crate::runtime::functions::{SharedFunctionProvider, SystemFunctionProvider};
use crate::runtime::wasm::NoopWasmRuntime;
use crate::session::SessionWriteSelectorResolver;
use crate::sql::logical_plan::public_ir::{PlannedWrite, ResolvedWritePlan};
use crate::transaction::{ReadContext, TransactionDelta, WriteTransaction};
use crate::write_runtime::{resolve_write_plan_with_functions, WriteResolveError};
use crate::{
    boot, BootArgs, CommittedVersionFrontier, Engine, LixBackend, LixBackendTransaction, LixError,
    QueryResult, ReplayCursor, Session, SqlDialect, TransactionMode, Value,
};

type SqlPredicate = Arc<dyn Fn(&str, &[Value]) -> bool + Send + Sync>;
const TEST_LIVE_STATE_STATUS_TABLE: &str = "lix_internal_live_state_status";

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum TestSqliteBackendEvent {
    Execute {
        sql: String,
        params: Vec<Value>,
        in_transaction: bool,
    },
    BeginTransaction {
        mode: TransactionMode,
    },
    Commit,
    Rollback,
}

#[derive(Clone)]
enum TestSqliteBackendHookAction {
    Fail(LixError),
    Delay(Duration),
}

#[derive(Clone)]
struct TestSqliteBackendHook {
    predicate: SqlPredicate,
    action: TestSqliteBackendHookAction,
}

#[derive(Default)]
struct TestSqliteBackendState {
    events: Vec<TestSqliteBackendEvent>,
    hooks: Vec<TestSqliteBackendHook>,
}

#[derive(Clone)]
pub(crate) struct TestSqliteBackend {
    connection: Arc<Mutex<rusqlite::Connection>>,
    state: Arc<Mutex<TestSqliteBackendState>>,
}

struct TestSqliteTransaction {
    connection: Arc<Mutex<rusqlite::Connection>>,
    state: Arc<Mutex<TestSqliteBackendState>>,
    mode: TransactionMode,
}

impl TestSqliteBackend {
    pub(crate) fn new() -> Self {
        let connection = rusqlite::Connection::open_in_memory().expect("open sqlite memory db");
        connection
            .execute_batch("PRAGMA foreign_keys = ON;")
            .expect("enable foreign keys");
        Self {
            connection: Arc::new(Mutex::new(connection)),
            state: Arc::new(Mutex::new(TestSqliteBackendState::default())),
        }
    }

    pub(crate) fn recorded_events(&self) -> Vec<TestSqliteBackendEvent> {
        self.state.lock().expect("sqlite state lock").events.clone()
    }

    pub(crate) fn executed_sql(&self) -> Vec<String> {
        self.recorded_events()
            .into_iter()
            .filter_map(|event| match event {
                TestSqliteBackendEvent::Execute { sql, .. } => Some(sql),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn count_sql_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&str) -> bool,
    {
        self.executed_sql()
            .into_iter()
            .filter(|sql| predicate(sql))
            .count()
    }

    pub(crate) fn clear_query_log(&self) {
        self.state.lock().expect("sqlite state lock").events.clear();
    }

    pub(crate) fn fail_when<F>(&self, predicate: F, error: LixError)
    where
        F: Fn(&str, &[Value]) -> bool + Send + Sync + 'static,
    {
        self.state
            .lock()
            .expect("sqlite state lock")
            .hooks
            .push(TestSqliteBackendHook {
                predicate: Arc::new(predicate),
                action: TestSqliteBackendHookAction::Fail(error),
            });
    }

    pub(crate) fn delay_when<F>(&self, predicate: F, duration: Duration)
    where
        F: Fn(&str, &[Value]) -> bool + Send + Sync + 'static,
    {
        self.state
            .lock()
            .expect("sqlite state lock")
            .hooks
            .push(TestSqliteBackendHook {
                predicate: Arc::new(predicate),
                action: TestSqliteBackendHookAction::Delay(duration),
            });
    }

    pub(crate) fn block_writes_to(&self, table_name: &str, error: LixError) {
        let table_name = table_name.to_ascii_lowercase();
        self.fail_when(
            move |sql, _params| {
                let sql = sql.trim_start().to_ascii_lowercase();
                let writes = sql.starts_with("insert into ")
                    || sql.starts_with("update ")
                    || sql.starts_with("delete from ");
                writes && sql.contains(&table_name)
            },
            error,
        );
    }

    pub(crate) fn clear_hooks(&self) {
        self.state.lock().expect("sqlite state lock").hooks.clear();
    }
}

#[async_trait(?Send)]
impl LixBackend for TestSqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        execute_with_shared_state(&self.connection, &self.state, sql, params, false).await
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
        self.state
            .lock()
            .expect("sqlite state lock")
            .events
            .push(TestSqliteBackendEvent::BeginTransaction { mode });
        Ok(Box::new(TestSqliteTransaction {
            connection: Arc::clone(&self.connection),
            state: Arc::clone(&self.state),
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
impl LixBackendTransaction for TestSqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionMode {
        self.mode
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        execute_with_shared_state(&self.connection, &self.state, sql, params, true).await
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        connection.execute_batch("COMMIT").map_err(sqlite_error)?;
        self.state
            .lock()
            .expect("sqlite state lock")
            .events
            .push(TestSqliteBackendEvent::Commit);
        Ok(())
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        let connection = self.connection.lock().expect("sqlite connection lock");
        connection.execute_batch("ROLLBACK").map_err(sqlite_error)?;
        self.state
            .lock()
            .expect("sqlite state lock")
            .events
            .push(TestSqliteBackendEvent::Rollback);
        Ok(())
    }
}

pub(crate) async fn boot_test_engine() -> Result<(TestSqliteBackend, Arc<Engine>, Session), LixError>
{
    let backend = TestSqliteBackend::new();
    let engine = Arc::new(boot(BootArgs::new(
        Box::new(backend.clone()),
        Arc::new(NoopWasmRuntime),
    )));
    engine.initialize().await?;
    let session = engine.open_session().await?;
    Ok((backend, engine, session))
}

pub(crate) async fn resolve_write_plan_for_test(
    backend: &dyn LixBackend,
    projection_registry: &ProjectionRegistry,
    planned_write: &PlannedWrite,
    pending_transaction_view: Option<&dyn PendingView>,
) -> Result<ResolvedWritePlan, WriteResolveError> {
    let selector_resolver =
        SessionWriteSelectorResolver::new(backend, projection_registry, pending_transaction_view)
            .await
            .map_err(|error| WriteResolveError {
                message: error.description,
            })?;
    resolve_write_plan_with_functions(
        backend,
        planned_write,
        pending_transaction_view,
        SharedFunctionProvider::new(SystemFunctionProvider),
        &selector_resolver,
    )
    .await
}

pub(crate) async fn init_test_backend_core(backend: &dyn LixBackend) -> Result<(), LixError> {
    crate::live_state::init(backend).await?;
    crate::schema::init(backend).await?;
    crate::canonical::init(backend).await?;
    crate::write_runtime::commit::init(backend).await?;
    crate::session::version_ops::init(backend).await?;
    Ok(())
}

pub(crate) async fn init_test_backend_with_binary_cas(
    backend: &dyn LixBackend,
) -> Result<(), LixError> {
    init_test_backend_core(backend).await?;
    crate::binary_cas::init(backend).await?;
    Ok(())
}

pub(crate) async fn commit_untracked_rows(
    backend: &TestSqliteBackend,
    rows: Vec<crate::live_state::untracked::UntrackedWriteRow>,
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
        tracked_writes: Vec::new(),
        untracked_writes: rows,
    })?;
    write_tx.commit().await?;
    Ok(())
}

pub(crate) async fn seed_local_version_head(
    backend: &TestSqliteBackend,
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> Result<(), LixError> {
    commit_untracked_rows(
        backend,
        vec![crate::live_state::testing::local_version_head_write_row(
            version_id, commit_id, timestamp,
        )],
    )
    .await
}

pub(crate) async fn seed_canonical_snapshot_row(
    backend: &dyn LixBackend,
    snapshot_id: &str,
    content: Option<&str>,
) -> Result<(), LixError> {
    backend
        .execute(
            "INSERT INTO lix_internal_snapshot (id, content) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET content = excluded.content",
            &[
                Value::Text(snapshot_id.to_string()),
                content
                    .map(|value| Value::Text(value.to_string()))
                    .unwrap_or(Value::Null),
            ],
        )
        .await?;
    Ok(())
}

pub(crate) struct CanonicalChangeSeed<'a> {
    pub(crate) id: &'a str,
    pub(crate) entity_id: &'a str,
    pub(crate) schema_key: &'a str,
    pub(crate) schema_version: &'a str,
    pub(crate) file_id: &'a str,
    pub(crate) plugin_key: &'a str,
    pub(crate) snapshot_id: &'a str,
    pub(crate) snapshot_content: Option<&'a str>,
    pub(crate) metadata: Option<&'a str>,
    pub(crate) created_at: &'a str,
}

pub(crate) async fn seed_canonical_change_row(
    backend: &dyn LixBackend,
    seed: CanonicalChangeSeed<'_>,
) -> Result<(), LixError> {
    if let Some(snapshot_content) = seed.snapshot_content {
        seed_canonical_snapshot_row(backend, seed.snapshot_id, Some(snapshot_content)).await?;
    }
    backend
        .execute(
            "INSERT INTO lix_internal_change (\
             id, entity_id, schema_key, schema_version, file_id, plugin_key, snapshot_id, metadata, created_at\
             ) VALUES (\
             $1, $2, $3, $4, $5, $6, $7, $8, $9\
             )",
            &[
                Value::Text(seed.id.to_string()),
                Value::Text(seed.entity_id.to_string()),
                Value::Text(seed.schema_key.to_string()),
                Value::Text(seed.schema_version.to_string()),
                Value::Text(seed.file_id.to_string()),
                Value::Text(seed.plugin_key.to_string()),
                Value::Text(seed.snapshot_id.to_string()),
                seed.metadata
                    .map(|value| Value::Text(value.to_string()))
                    .unwrap_or(Value::Null),
                Value::Text(seed.created_at.to_string()),
            ],
        )
        .await?;
    Ok(())
}

pub(crate) async fn seed_live_state_status_row(
    backend: &dyn LixBackend,
    mode: crate::live_state::LiveStateMode,
    cursor: Option<&ReplayCursor>,
    applied_frontier: Option<&CommittedVersionFrontier>,
    updated_at: &str,
) -> Result<(), LixError> {
    backend
        .execute(
            &format!(
                "INSERT INTO {table} (\
                 singleton_id, mode, latest_change_id, latest_change_created_at, applied_committed_frontier, schema_epoch, updated_at\
                 ) VALUES (\
                 1, $1, $2, $3, $4, $5, $6\
                 ) ON CONFLICT (singleton_id) DO UPDATE SET \
                 mode = excluded.mode, \
                 latest_change_id = excluded.latest_change_id, \
                 latest_change_created_at = excluded.latest_change_created_at, \
                 applied_committed_frontier = excluded.applied_committed_frontier, \
                 schema_epoch = excluded.schema_epoch, \
                 updated_at = excluded.updated_at",
                table = TEST_LIVE_STATE_STATUS_TABLE,
            ),
            &[
                Value::Text(live_state_mode_text(mode).to_string()),
                cursor
                    .map(|value| Value::Text(value.change_id.clone()))
                    .unwrap_or(Value::Null),
                cursor
                    .map(|value| Value::Text(value.created_at.clone()))
                    .unwrap_or(Value::Null),
                applied_frontier
                    .map(|value| Value::Text(value.to_json_string()))
                    .unwrap_or(Value::Null),
                Value::Text(crate::live_state::LIVE_STATE_SCHEMA_EPOCH.to_string()),
                Value::Text(updated_at.to_string()),
            ],
        )
        .await?;
    Ok(())
}

fn live_state_mode_text(mode: crate::live_state::LiveStateMode) -> &'static str {
    match mode {
        crate::live_state::LiveStateMode::Uninitialized => "uninitialized",
        crate::live_state::LiveStateMode::Bootstrapping => "bootstrapping",
        crate::live_state::LiveStateMode::Ready => "ready",
        crate::live_state::LiveStateMode::NeedsRebuild => "needs_rebuild",
        crate::live_state::LiveStateMode::Rebuilding => "rebuilding",
    }
}

async fn execute_with_shared_state(
    connection: &Arc<Mutex<rusqlite::Connection>>,
    state: &Arc<Mutex<TestSqliteBackendState>>,
    sql: &str,
    params: &[Value],
    in_transaction: bool,
) -> Result<QueryResult, LixError> {
    let actions = {
        let mut state = state.lock().expect("sqlite state lock");
        state.events.push(TestSqliteBackendEvent::Execute {
            sql: sql.to_string(),
            params: params.to_vec(),
            in_transaction,
        });
        state
            .hooks
            .iter()
            .filter(|hook| (hook.predicate)(sql, params))
            .map(|hook| hook.action.clone())
            .collect::<Vec<_>>()
    };

    for action in actions {
        match action {
            TestSqliteBackendHookAction::Fail(error) => return Err(error),
            TestSqliteBackendHookAction::Delay(duration) => {
                tokio::time::sleep(duration).await;
            }
        }
    }

    let connection = connection.lock().expect("sqlite connection lock");
    execute_sql(&connection, sql, params)
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
    LixError::new("LIX_ERROR_UNKNOWN", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[tokio::test]
    async fn test_sqlite_backend_records_queries_and_transaction_events() {
        let backend = TestSqliteBackend::new();

        backend
            .execute("SELECT 1 AS one", &[])
            .await
            .expect("direct select should succeed");
        let mut tx = backend
            .begin_transaction(TransactionMode::Write)
            .await
            .expect("transaction should begin");
        tx.execute("SELECT 2 AS two", &[])
            .await
            .expect("transaction select should succeed");
        tx.commit()
            .await
            .expect("transaction commit should succeed");

        let events = backend.recorded_events();
        assert!(matches!(
            events.first(),
            Some(TestSqliteBackendEvent::Execute {
                sql,
                in_transaction: false,
                ..
            }) if sql == "SELECT 1 AS one"
        ));
        assert!(events.iter().any(|event| matches!(
            event,
            TestSqliteBackendEvent::BeginTransaction {
                mode: TransactionMode::Write
            }
        )));
        assert!(events.iter().any(|event| matches!(
            event,
            TestSqliteBackendEvent::Execute {
                sql,
                in_transaction: true,
                ..
            } if sql == "SELECT 2 AS two"
        )));
        assert!(events
            .iter()
            .any(|event| matches!(event, TestSqliteBackendEvent::Commit)));
        assert_eq!(
            backend.count_sql_matching(|sql| sql.starts_with("SELECT")),
            2
        );

        backend.clear_query_log();
        assert!(backend.recorded_events().is_empty());
    }

    #[tokio::test]
    async fn test_sqlite_backend_supports_failure_and_seed_helpers() {
        let backend = TestSqliteBackend::new();
        init_test_backend_core(&backend)
            .await
            .expect("core init should succeed");

        seed_local_version_head(&backend, "main", "commit-1", "2026-03-30T00:00:00Z")
            .await
            .expect("local version head seed should succeed");
        seed_canonical_change_row(
            &backend,
            CanonicalChangeSeed {
                id: "change-1",
                entity_id: "commit-1",
                schema_key: "lix_commit",
                schema_version: "1",
                file_id: "lix",
                plugin_key: "lix",
                snapshot_id: "snapshot-1",
                snapshot_content: Some(
                    "{\"id\":\"commit-1\",\"change_set_id\":\"cs-1\",\"change_ids\":[],\"parent_commit_ids\":[]}",
                ),
                metadata: None,
                created_at: "2026-03-30T00:00:00Z",
            },
        )
        .await
        .expect("canonical change seed should succeed");
        seed_live_state_status_row(
            &backend,
            crate::live_state::LiveStateMode::Ready,
            Some(&ReplayCursor::new("change-1", "2026-03-30T00:00:00Z")),
            Some(&CommittedVersionFrontier {
                version_heads: [("main".to_string(), "commit-1".to_string())]
                    .into_iter()
                    .collect(),
            }),
            "2026-03-30T00:00:00Z",
        )
        .await
        .expect("live-state status seed should succeed");

        let frontier =
            crate::version_state::load_current_committed_version_frontier_with_backend(&backend)
                .await
                .expect("frontier load should succeed");
        assert_eq!(
            frontier.version_heads.get("main").map(String::as_str),
            Some("commit-1")
        );

        backend.delay_when(
            |sql, _params| sql == "SELECT 3 AS delayed",
            Duration::from_millis(5),
        );
        let started = Instant::now();
        backend
            .execute("SELECT 3 AS delayed", &[])
            .await
            .expect("delay hook should not fail the query");
        assert!(
            started.elapsed() >= Duration::from_millis(5),
            "delay hook should defer matching statements"
        );

        backend.block_writes_to(
            "lix_internal_change",
            LixError::new("LIX_ERROR_UNKNOWN", "blocked canonical write"),
        );
        let error = backend
            .execute(
                "INSERT INTO lix_internal_change (id, entity_id, schema_key, schema_version, file_id, plugin_key, snapshot_id, metadata, created_at) \
                 VALUES ('change-2', 'entity', 'schema', '1', 'lix', 'lix', 'no-content', NULL, '2026-03-30T00:00:01Z')",
                &[],
            )
            .await
            .expect_err("write hook should fail matching statements");
        assert_eq!(error.description, "blocked canonical write");

        backend.clear_hooks();
    }
}
