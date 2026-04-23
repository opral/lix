use crate::backend::{PreparedBatch, PreparedStatement, TransactionBeginMode};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};

pub(crate) trait PersistenceStatementSink {
    fn push_persistence_statement(&mut self, sql: impl Into<String>, params: Vec<Value>);
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum WriteStep {
    Statement { sql: String, params: Vec<Value> },
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct WriteBatch {
    pub(crate) steps: Vec<WriteStep>,
}

impl WriteBatch {
    pub(crate) fn new() -> Self {
        Self { steps: Vec::new() }
    }

    pub(crate) fn push_statement(&mut self, sql: impl Into<String>, params: Vec<Value>) {
        self.steps.push(WriteStep::Statement {
            sql: sql.into(),
            params,
        });
    }

    pub(crate) fn extend(&mut self, other: WriteBatch) {
        self.steps.extend(other.steps);
    }
}

impl PersistenceStatementSink for WriteBatch {
    fn push_persistence_statement(&mut self, sql: impl Into<String>, params: Vec<Value>) {
        self.push_statement(sql, params);
    }
}

pub(crate) async fn execute_write_batch_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    write_batch: WriteBatch,
) -> Result<QueryResult, LixError> {
    execute_write_batch_steps(transaction, write_batch).await
}

#[cfg_attr(not(test), allow(dead_code))]
async fn execute_write_batch_with_backend(
    backend: &dyn LixBackend,
    write_batch: WriteBatch,
) -> Result<QueryResult, LixError> {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await?;
    let result = execute_write_batch_steps(transaction.as_mut(), write_batch).await;
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

async fn execute_write_batch_steps(
    transaction: &mut dyn LixBackendTransaction,
    write_batch: WriteBatch,
) -> Result<QueryResult, LixError> {
    let mut batch = PreparedBatch { steps: Vec::new() };
    for step in write_batch.steps {
        match step {
            WriteStep::Statement { sql, params } => {
                batch.push_statement(PreparedStatement { sql, params });
            }
        }
    }
    crate::execution::execute_prepared_batch_in_transaction(transaction, &batch).await
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::{execute_write_batch_with_backend, WriteBatch};
    use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect, Value};

    #[derive(Default)]
    struct FakeBackend {
        log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        fail_sql: Option<String>,
    }

    struct FakeTransaction {
        log: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
        fail_sql: Option<String>,
    }

    #[async_trait]
    impl LixBackend for FakeBackend {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        async fn execute(&self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.log.lock().unwrap().push(format!("backend:{sql}"));
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn begin_transaction(
            &self,
            _mode: crate::backend::TransactionBeginMode,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.log.lock().unwrap().push("begin".to_string());
            Ok(Box::new(FakeTransaction {
                log: std::sync::Arc::clone(&self.log),
                fail_sql: self.fail_sql.clone(),
            }))
        }

        async fn begin_savepoint(
            &self,
            _name: &str,
        ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
            self.begin_transaction(crate::backend::TransactionBeginMode::Write)
                .await
        }
    }

    #[async_trait]
    impl LixBackendTransaction for FakeTransaction {
        fn dialect(&self) -> SqlDialect {
            SqlDialect::Sqlite
        }

        fn mode(&self) -> crate::backend::TransactionBeginMode {
            crate::backend::TransactionBeginMode::Write
        }

        async fn execute(&mut self, sql: &str, _params: &[Value]) -> Result<QueryResult, LixError> {
            self.log.lock().unwrap().push(format!("tx:{sql}"));
            if self.fail_sql.as_deref() == Some(sql) {
                return Err(LixError::new("LIX_ERROR_UNKNOWN", "boom"));
            }
            Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            })
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            self.log.lock().unwrap().push("commit".to_string());
            Ok(())
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            self.log.lock().unwrap().push("rollback".to_string());
            Ok(())
        }
    }

    #[tokio::test]
    async fn owned_runner_commits_on_success() {
        let backend = FakeBackend::default();
        let mut write_batch = WriteBatch::new();
        write_batch.push_statement("INSERT INTO test VALUES (1)", Vec::new());

        execute_write_batch_with_backend(&backend, write_batch)
            .await
            .expect("write batch should succeed");

        let log = backend.log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "begin".to_string(),
                "tx:INSERT INTO test VALUES (1)".to_string(),
                "commit".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn owned_runner_rolls_back_on_failure() {
        let backend = FakeBackend {
            log: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            fail_sql: Some("INSERT INTO fail VALUES (1)".to_string()),
        };
        let mut write_batch = WriteBatch::new();
        write_batch.push_statement("INSERT INTO fail VALUES (1)", Vec::new());

        let error = execute_write_batch_with_backend(&backend, write_batch)
            .await
            .expect_err("write batch should fail");
        assert!(error.description.contains("boom"));

        let log = backend.log.lock().unwrap().clone();
        assert_eq!(
            log,
            vec![
                "begin".to_string(),
                "tx:INSERT INTO fail VALUES (1)".to_string(),
                "rollback".to_string()
            ]
        );
    }
}
