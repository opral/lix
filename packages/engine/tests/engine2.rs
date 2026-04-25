use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use lix_engine::engine2::ExecuteResult;
use lix_engine::wasm::NoopWasmRuntime;
use lix_engine::{
    Engine, Lix, LixBackend, LixBackendTransaction, LixConfig, LixError, PreparedBatch,
    QueryResult, SqlDialect, TransactionBeginMode, Value,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Column, Row, SqlitePool, ValueRef};
use tokio::sync::OnceCell;

#[test]
fn session_execute_inserts_key_value_then_reads_it_back() {
    std::thread::Builder::new()
        .name("sql2_key_value_roundtrip".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            runtime.block_on(async {
                let sqlite_uri = shared_memory_sqlite_uri("key_value_roundtrip");
                let initializer = Lix::boot(LixConfig::new(
                    Box::new(SqliteBackend::new(sqlite_uri.clone())),
                    Arc::new(NoopWasmRuntime),
                ));
                initializer
                    .initialize()
                    .await
                    .expect("backend initialization should succeed");

                let engine = Engine::new(Box::new(SqliteBackend::new(sqlite_uri)))
                    .await
                    .expect("initialized backend should create an engine");
                let session = engine
                    .open_session("global")
                    .await
                    .expect("initialized backend should open a session");

                let insert_result = session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('sql2-key', 'sql2-value')",
                        &[],
                    )
                    .await
                    .expect("session insert should succeed");
                assert_eq!(insert_result, ExecuteResult::AffectedRows(1));

                let result = session
                    .execute(
                        "SELECT key, value FROM lix_key_value WHERE key = 'sql2-key'",
                        &[],
                    )
                    .await
                    .expect("session read should succeed");

                let ExecuteResult::Rows(row_set) = result else {
                    panic!("SELECT should return rows");
                };
                assert_eq!(row_set.len(), 1);
                assert_eq!(
                    row_set.rows()[0].values(),
                    &[
                        Value::Text("sql2-key".to_string()),
                        Value::Text("\"sql2-value\"".to_string()),
                    ]
                );

                drop(session);
                drop(engine);
                drop(initializer);
            });
        })
        .expect("failed to spawn sql2 test thread")
        .join()
        .expect("sql2 test thread panicked");
}

#[test]
fn session_execute_registers_schema_then_writes_lix_state_row() {
    std::thread::Builder::new()
        .name("sql2_registered_schema_lix_state_roundtrip".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            runtime.block_on(async {
                let sqlite_uri = shared_memory_sqlite_uri("registered_schema_lix_state");
                let initializer = Lix::boot(LixConfig::new(
                    Box::new(SqliteBackend::new(sqlite_uri.clone())),
                    Arc::new(NoopWasmRuntime),
                ));
                initializer
                    .initialize()
                    .await
                    .expect("backend initialization should succeed");

                let engine = Engine::new(Box::new(SqliteBackend::new(sqlite_uri)))
                    .await
                    .expect("initialized backend should create an engine");
                let session = engine
                    .open_session("global")
                    .await
                    .expect("initialized backend should open a session");

                let register_schema_result = session
                    .execute(
                        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) \
                         VALUES (\
                         lix_json('{\"x-lix-key\":\"engine2_dummy_schema\",\"x-lix-version\":\"1\",\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"required\":[\"id\",\"name\"],\"additionalProperties\":false}'),\
                         true,\
                         true\
                         )",
                        &[],
                    )
                    .await
                    .expect("session registered schema insert should succeed");
                assert_eq!(register_schema_result, ExecuteResult::AffectedRows(1));

                let insert_state_result = session
                    .execute(
                        "INSERT INTO lix_state (\
                         entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global, untracked\
                         ) VALUES (\
                         'dummy-1', 'engine2_dummy_schema', NULL, NULL, lix_json('{\"id\":\"dummy-1\",\"name\":\"Dummy\"}'), '1', true, true\
                         )",
                        &[],
                    )
                    .await
                    .expect("session lix_state insert for registered schema should succeed");
                assert_eq!(insert_state_result, ExecuteResult::AffectedRows(1));

                let result = session
                    .execute(
                        "SELECT entity_id, schema_key, snapshot_content \
                         FROM lix_state \
                         WHERE schema_key = 'engine2_dummy_schema' AND entity_id = 'dummy-1'",
                        &[],
                    )
                    .await
                    .expect("session lix_state read should succeed");

                let ExecuteResult::Rows(row_set) = result else {
                    panic!("SELECT should return rows");
                };
                assert_eq!(row_set.len(), 1);
                assert_eq!(
                    row_set.rows()[0].values(),
                    &[
                        Value::Text("dummy-1".to_string()),
                        Value::Text("engine2_dummy_schema".to_string()),
                        Value::Text("{\"id\":\"dummy-1\",\"name\":\"Dummy\"}".to_string()),
                    ]
                );

                drop(session);
                drop(engine);
                drop(initializer);
            });
        })
        .expect("failed to spawn sql2 registered schema test thread")
        .join()
        .expect("sql2 registered schema test thread panicked");
}

#[test]
fn session_execute_inserts_directory_then_reads_it_back() {
    std::thread::Builder::new()
        .name("sql2_directory_roundtrip".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            runtime.block_on(async {
                let sqlite_uri = shared_memory_sqlite_uri("directory_roundtrip");
                let initializer = Lix::boot(LixConfig::new(
                    Box::new(SqliteBackend::new(sqlite_uri.clone())),
                    Arc::new(NoopWasmRuntime),
                ));
                initializer
                    .initialize()
                    .await
                    .expect("backend initialization should succeed");

                let engine = Engine::new(Box::new(SqliteBackend::new(sqlite_uri)))
                    .await
                    .expect("initialized backend should create an engine");
                let session = engine
                    .open_session("global")
                    .await
                    .expect("initialized backend should open a session");

                let insert_result = session
                    .execute(
                        "INSERT INTO lix_directory (id, parent_id, name, hidden) \
                         VALUES ('dir-docs', NULL, 'docs', false)",
                        &[],
                    )
                    .await
                    .expect("session directory insert should succeed");
                assert_eq!(insert_result, ExecuteResult::AffectedRows(1));

                let nested_insert_result = session
                    .execute(
                        "INSERT INTO lix_directory (id, path, hidden) \
                         VALUES ('dir-nested', '/docs/nested/', false)",
                        &[],
                    )
                    .await
                    .expect("session nested directory path insert should succeed");
                assert_eq!(nested_insert_result, ExecuteResult::AffectedRows(1));

                let result = session
                    .execute(
                        "SELECT id, path, parent_id, name, hidden \
                         FROM lix_directory \
                         WHERE id IN ('dir-docs', 'dir-nested') \
                         ORDER BY path",
                        &[],
                    )
                    .await
                    .expect("session directory read should succeed");

                let ExecuteResult::Rows(row_set) = result else {
                    panic!("SELECT should return rows");
                };
                assert_eq!(row_set.len(), 2);
                assert_eq!(
                    row_set.rows()[0].values(),
                    &[
                        Value::Text("dir-docs".to_string()),
                        Value::Text("/docs/".to_string()),
                        Value::Null,
                        Value::Text("docs".to_string()),
                        Value::Boolean(false),
                    ]
                );
                assert_eq!(
                    row_set.rows()[1].values(),
                    &[
                        Value::Text("dir-nested".to_string()),
                        Value::Text("/docs/nested/".to_string()),
                        Value::Text("dir-docs".to_string()),
                        Value::Text("nested".to_string()),
                        Value::Boolean(false),
                    ]
                );

                drop(session);
                drop(engine);
                drop(initializer);
            });
        })
        .expect("failed to spawn sql2 directory test thread")
        .join()
        .expect("sql2 directory test thread panicked");
}

#[test]
fn session_execute_inserts_file_then_reads_it_back() {
    std::thread::Builder::new()
        .name("sql2_file_roundtrip".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            runtime.block_on(async {
                let sqlite_uri = shared_memory_sqlite_uri("file_roundtrip");
                let initializer = Lix::boot(LixConfig::new(
                    Box::new(SqliteBackend::new(sqlite_uri.clone())),
                    Arc::new(NoopWasmRuntime),
                ));
                initializer
                    .initialize()
                    .await
                    .expect("backend initialization should succeed");

                let engine = Engine::new(Box::new(SqliteBackend::new(sqlite_uri)))
                    .await
                    .expect("initialized backend should create an engine");
                let session = engine
                    .open_session("global")
                    .await
                    .expect("initialized backend should open a session");

                let file_result = session
                    .execute(
                        "INSERT INTO lix_file (id, path, data, hidden) \
                         VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                        &[],
                    )
                    .await
                    .expect("session file insert should succeed");
                assert_eq!(file_result, ExecuteResult::AffectedRows(1));

                let result = session
                    .execute(
                        "SELECT id, path, data, hidden \
                         FROM lix_file \
                         WHERE id = 'file-readme'",
                        &[],
                    )
                    .await
                    .expect("session file read should succeed");

                let ExecuteResult::Rows(row_set) = result else {
                    panic!("SELECT should return rows");
                };
                assert_eq!(row_set.len(), 1);
                assert_eq!(
                    row_set.rows()[0].values(),
                    &[
                        Value::Text("file-readme".to_string()),
                        Value::Text("/docs/guides/readme.md".to_string()),
                        Value::Blob(b"hello".to_vec()),
                        Value::Boolean(false),
                    ]
                );

                let staged_state_result = session
                    .execute(
                        "SELECT entity_id, schema_key \
                         FROM lix_state \
                         WHERE schema_key IN (\
                           'lix_directory_descriptor', \
                           'lix_file_descriptor', \
                           'lix_binary_blob_ref'\
                         ) \
                         AND entity_id IN (\
                           'lix-auto-dir:global:/docs/', \
                           'lix-auto-dir:global:/docs/guides/', \
                           'file-readme'\
                         ) \
                         ORDER BY schema_key, entity_id",
                        &[],
                    )
                    .await
                    .expect("session staged filesystem state read should succeed");

                let ExecuteResult::Rows(staged_state_rows) = staged_state_result else {
                    panic!("SELECT should return filesystem state rows");
                };
                assert_eq!(
                    staged_state_rows.len(),
                    4,
                    "file path insert should stage exactly two missing dirs, one file descriptor, and one blob ref"
                );

                drop(session);
                drop(engine);
                drop(initializer);
            });
        })
        .expect("failed to spawn sql2 file test thread")
        .join()
        .expect("sql2 file test thread panicked");
}

#[test]
fn session_execute_updates_file_path_and_preserves_data() {
    std::thread::Builder::new()
        .name("sql2_file_path_update".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            runtime.block_on(async {
                let sqlite_uri = shared_memory_sqlite_uri("file_path_update");
                let initializer = Lix::boot(LixConfig::new(
                    Box::new(SqliteBackend::new(sqlite_uri.clone())),
                    Arc::new(NoopWasmRuntime),
                ));
                initializer
                    .initialize()
                    .await
                    .expect("backend initialization should succeed");

                let engine = Engine::new(Box::new(SqliteBackend::new(sqlite_uri)))
                    .await
                    .expect("initialized backend should create an engine");
                let session = engine
                    .open_session("global")
                    .await
                    .expect("initialized backend should open a session");

                let insert_result = session
                    .execute(
                        "INSERT INTO lix_file (id, path, data, hidden) \
                         VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                        &[],
                    )
                    .await
                    .expect("session file insert should succeed");
                assert_eq!(insert_result, ExecuteResult::AffectedRows(1));

                let update_result = session
                    .execute(
                        "UPDATE lix_file \
                         SET path = '/docs/readme-renamed.md' \
                         WHERE id = 'file-readme'",
                        &[],
                    )
                    .await
                    .expect("session file path update should succeed");
                assert_eq!(update_result, ExecuteResult::AffectedRows(1));

                let file_result = session
                    .execute(
                        "SELECT id, path, data \
                         FROM lix_file \
                         WHERE id = 'file-readme'",
                        &[],
                    )
                    .await
                    .expect("session file read after path update should succeed");
                let ExecuteResult::Rows(file_rows) = file_result else {
                    panic!("SELECT should return file rows");
                };
                assert_eq!(file_rows.len(), 1);
                assert_eq!(
                    file_rows.rows()[0].values(),
                    &[
                        Value::Text("file-readme".to_string()),
                        Value::Text("/docs/readme-renamed.md".to_string()),
                        Value::Blob(b"hello".to_vec()),
                    ]
                );

                let state_result = session
                    .execute(
                        "SELECT entity_id, schema_key \
                         FROM lix_state \
                         WHERE schema_key IN (\
                           'lix_directory_descriptor', \
                           'lix_file_descriptor', \
                           'lix_binary_blob_ref'\
                         ) \
                         AND entity_id IN (\
                           'lix-auto-dir:global:/docs/', \
                           'lix-auto-dir:global:/docs/guides/', \
                           'file-readme'\
                         ) \
                         ORDER BY schema_key, entity_id",
                        &[],
                    )
                    .await
                    .expect("session filesystem state read after path update should succeed");
                let ExecuteResult::Rows(state_rows) = state_result else {
                    panic!("SELECT should return filesystem state rows");
                };
                assert_eq!(
                    state_rows.len(),
                    4,
                    "path update should reuse existing /docs/, keep /docs/guides/ visible, update one file descriptor, and preserve one blob ref"
                );

                let directory_result = session
                    .execute(
                        "SELECT path \
                         FROM lix_directory \
                         WHERE path IN ('/docs/', '/docs/guides/') \
                         ORDER BY path",
                        &[],
                    )
                    .await
                    .expect("session directory read after path update should succeed");
                let ExecuteResult::Rows(directory_rows) = directory_result else {
                    panic!("SELECT should return directory rows");
                };
                assert_eq!(
                    directory_rows.len(),
                    2,
                    "path update should not stage an extra directory descriptor"
                );

                drop(session);
                drop(engine);
                drop(initializer);
            });
        })
        .expect("failed to spawn sql2 file path update test thread")
        .join()
        .expect("sql2 file path update test thread panicked");
}

#[test]
fn session_execute_deletes_directory_recursively() {
    std::thread::Builder::new()
        .name("sql2_recursive_directory_delete".to_string())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build tokio runtime");
            runtime.block_on(async {
                let sqlite_uri = shared_memory_sqlite_uri("recursive_directory_delete");
                let initializer = Lix::boot(LixConfig::new(
                    Box::new(SqliteBackend::new(sqlite_uri.clone())),
                    Arc::new(NoopWasmRuntime),
                ));
                initializer
                    .initialize()
                    .await
                    .expect("backend initialization should succeed");

                let engine = Engine::new(Box::new(SqliteBackend::new(sqlite_uri)))
                    .await
                    .expect("initialized backend should create an engine");
                let session = engine
                    .open_session("global")
                    .await
                    .expect("initialized backend should open a session");

                let file_result = session
                    .execute(
                        "INSERT INTO lix_file (id, path, data, hidden) \
                         VALUES ('file-readme', '/docs/guides/readme.md', X'68656C6C6F', false)",
                        &[],
                    )
                    .await
                    .expect("session file insert should succeed");
                assert_eq!(file_result, ExecuteResult::AffectedRows(1));

                let delete_result = session
                    .execute("DELETE FROM lix_directory WHERE path = '/docs/'", &[])
                    .await
                    .expect("session recursive directory delete should succeed");
                assert_eq!(delete_result, ExecuteResult::AffectedRows(1));

                let directories_result = session
                    .execute(
                        "SELECT id, path \
                         FROM lix_directory \
                         WHERE path IN ('/docs/', '/docs/guides/') \
                         ORDER BY path",
                        &[],
                    )
                    .await
                    .expect("session directory read after delete should succeed");
                let ExecuteResult::Rows(directory_rows) = directories_result else {
                    panic!("SELECT should return directory rows");
                };
                assert_eq!(
                    directory_rows.len(),
                    0,
                    "recursive directory delete should hide the root and child directories"
                );

                let file_result = session
                    .execute(
                        "SELECT id, path \
                         FROM lix_file \
                         WHERE path = '/docs/guides/readme.md'",
                        &[],
                    )
                    .await
                    .expect("session file read after delete should succeed");
                let ExecuteResult::Rows(file_rows) = file_result else {
                    panic!("SELECT should return file rows");
                };
                assert_eq!(
                    file_rows.len(),
                    0,
                    "recursive directory delete should hide nested files"
                );

                let state_result = session
                    .execute(
                        "SELECT entity_id, schema_key \
                         FROM lix_state \
                         WHERE schema_key IN (\
                           'lix_directory_descriptor', \
                           'lix_file_descriptor', \
                           'lix_binary_blob_ref'\
                         ) \
                         AND entity_id IN (\
                           'lix-auto-dir:global:/docs/', \
                           'lix-auto-dir:global:/docs/guides/', \
                           'file-readme'\
                         ) \
                         ORDER BY schema_key, entity_id",
                        &[],
                    )
                    .await
                    .expect("session state read after delete should succeed");
                let ExecuteResult::Rows(state_rows) = state_result else {
                    panic!("SELECT should return state rows");
                };
                assert_eq!(
                    state_rows.len(),
                    0,
                    "recursive directory delete should make descriptor/blob-ref state rows not visible"
                );

                drop(session);
                drop(engine);
                drop(initializer);
            });
        })
        .expect("failed to spawn sql2 recursive directory delete test thread")
        .join()
        .expect("sql2 recursive directory delete test thread panicked");
}

fn shared_memory_sqlite_uri(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    format!(
        "sqlite:file:lix_sql2_{label}_{}_{}?mode=memory&cache=shared",
        std::process::id(),
        nanos
    )
}

struct SqliteBackend {
    uri: String,
    pool: OnceCell<SqlitePool>,
}

impl SqliteBackend {
    fn new(uri: String) -> Self {
        Self {
            uri,
            pool: OnceCell::const_new(),
        }
    }

    async fn pool(&self) -> Result<&SqlitePool, LixError> {
        self.pool
            .get_or_try_init(|| async {
                let options = SqliteConnectOptions::from_str(&self.uri)
                    .map_err(to_lix_error)?
                    .foreign_keys(true)
                    .busy_timeout(std::time::Duration::from_secs(30));

                SqlitePoolOptions::new()
                    .max_connections(2)
                    .connect_with(options)
                    .await
                    .map_err(to_lix_error)
            })
            .await
    }
}

struct SqliteTransaction {
    conn: sqlx::pool::PoolConnection<sqlx::Sqlite>,
    mode: TransactionBeginMode,
}

#[async_trait::async_trait]
impl LixBackend for SqliteBackend {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut transaction = self
            .begin_transaction(TransactionBeginMode::Deferred)
            .await?;
        let result = transaction.execute(sql, params).await;
        match result {
            Ok(result) => {
                transaction.commit().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn begin_transaction(
        &self,
        mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        let pool = self.pool().await?;
        let mut conn = pool.acquire().await.map_err(to_lix_error)?;
        sqlx::query(match mode {
            TransactionBeginMode::Read | TransactionBeginMode::Deferred => "BEGIN",
            TransactionBeginMode::Write => "BEGIN IMMEDIATE",
        })
        .execute(&mut *conn)
        .await
        .map_err(to_lix_error)?;
        Ok(Box::new(SqliteTransaction { conn, mode }))
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        self.begin_transaction(TransactionBeginMode::Write).await
    }
}

#[async_trait::async_trait]
impl LixBackendTransaction for SqliteTransaction {
    fn dialect(&self) -> SqlDialect {
        SqlDialect::Sqlite
    }

    fn mode(&self) -> TransactionBeginMode {
        self.mode
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        execute_query_with_connection(&mut self.conn, sql, params).await
    }

    async fn execute_batch(&mut self, batch: &PreparedBatch) -> Result<QueryResult, LixError> {
        for step in &batch.steps {
            if step.sql.trim().is_empty() {
                continue;
            }
            let mut query = sqlx::query(step.sql.as_str()).persistent(false);
            for param in &step.params {
                query = bind_param_sqlite(query, param);
            }
            query
                .execute(&mut *self.conn)
                .await
                .map_err(|err| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!("{} | sql: {}", err, step.sql),
                    hint: None,
                })?;
        }
        Ok(QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        })
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("COMMIT")
            .execute(&mut *self.conn)
            .await
            .map_err(to_lix_error)?;
        Ok(())
    }

    async fn rollback(mut self: Box<Self>) -> Result<(), LixError> {
        sqlx::query("ROLLBACK")
            .execute(&mut *self.conn)
            .await
            .map_err(to_lix_error)?;
        Ok(())
    }
}

async fn execute_query_with_connection(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    let mut query = sqlx::query(sql).persistent(false);
    for param in params {
        query = bind_param_sqlite(query, param);
    }

    let rows = query.fetch_all(&mut **conn).await.map_err(|err| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("{} | sql: {}", err, sql),
        hint: None,
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
            out.push(map_sqlite_value(&row, i)?);
        }
        result_rows.push(out);
    }

    Ok(QueryResult {
        rows: result_rows,
        columns,
    })
}

fn bind_param_sqlite<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    param: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match param {
        Value::Null => query.bind(Option::<i64>::None),
        Value::Boolean(value) => query.bind(*value),
        Value::Integer(value) => query.bind(*value),
        Value::Real(value) => query.bind(*value),
        Value::Text(value) => query.bind(value.as_str()),
        Value::Json(value) => query.bind(value.to_string()),
        Value::Blob(value) => query.bind(value.as_slice()),
    }
}

fn map_sqlite_value(row: &sqlx::sqlite::SqliteRow, index: usize) -> Result<Value, LixError> {
    if row.try_get_raw(index).map_err(to_lix_error)?.is_null() {
        return Ok(Value::Null);
    }
    if let Ok(value) = row.try_get::<i64, _>(index) {
        return Ok(Value::Integer(value));
    }
    if let Ok(value) = row.try_get::<f64, _>(index) {
        return Ok(Value::Real(value));
    }
    if let Ok(value) = row.try_get::<String, _>(index) {
        return Ok(Value::Text(value));
    }
    if let Ok(value) = row.try_get::<Vec<u8>, _>(index) {
        return Ok(Value::Blob(value));
    }
    Ok(Value::Null)
}

fn to_lix_error(error: impl std::fmt::Display) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
        hint: None,
    }
}
