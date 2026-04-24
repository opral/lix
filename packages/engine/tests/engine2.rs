use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
                    .open_session()
                    .await
                    .expect("initialized backend should open a session");

                session
                    .execute(
                        "INSERT INTO lix_key_value (key, value) VALUES ('sql2-key', 'sql2-value')",
                        &[],
                    )
                    .await
                    .expect("session insert should succeed");

                let result = session
                    .execute(
                        "SELECT key, value FROM lix_key_value WHERE key = 'sql2-key'",
                        &[],
                    )
                    .await
                    .expect("session read should succeed");

                assert_eq!(result.statements.len(), 1);
                assert_eq!(
                    result.statements[0].rows,
                    vec![vec![
                        Value::Text("sql2-key".to_string()),
                        Value::Text("\"sql2-value\"".to_string()),
                    ]]
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
                    .max_connections(1)
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
