use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::backend::TransactionBeginMode;
use crate::backend::{LixBackend, LixBackendTransaction, QueryExecutor, SqlDialect};
use crate::{LixError, QueryResult, Value};

pub(crate) struct TransactionBackendAdapter<'a> {
    dialect: SqlDialect,
    transaction: Mutex<&'a mut dyn LixBackendTransaction>,
}

impl<'a> TransactionBackendAdapter<'a> {
    pub(crate) fn new(transaction: &'a mut dyn LixBackendTransaction) -> Self {
        Self {
            dialect: transaction.dialect(),
            transaction: Mutex::new(transaction),
        }
    }
}

#[async_trait]
impl<'a> QueryExecutor for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().await;
        (**guard).execute(sql, params).await
    }
}

#[async_trait]
impl<'a> LixBackend for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().await;
        (**guard).execute(sql, params).await
    }

    async fn begin_transaction(
        &self,
        _mode: TransactionBeginMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "nested transactions are not supported via TransactionBackendAdapter"
                .to_string(),
            hint: None,
        })
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "savepoints are not supported via TransactionBackendAdapter".to_string(),
            hint: None,
        })
    }
}
