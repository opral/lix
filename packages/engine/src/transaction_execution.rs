use std::marker::PhantomData;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::backend::{LixBackend, LixBackendTransaction, QueryExecutor};
use crate::errors::classification::normalize_sql_error_with_backend_and_relation_names;
use crate::{LixError, QueryResult, SqlDialect, TransactionMode, Value};

pub(crate) use crate::backend::program::WriteProgram;

pub(crate) struct TransactionExecutionBackend<'a> {
    dialect: SqlDialect,
    transaction: Mutex<*mut (dyn LixBackendTransaction + 'a)>,
    _lifetime: PhantomData<&'a mut dyn LixBackendTransaction>,
}

unsafe impl<'a> Send for TransactionExecutionBackend<'a> {}
unsafe impl<'a> Sync for TransactionExecutionBackend<'a> {}

impl<'a> TransactionExecutionBackend<'a> {
    pub(crate) fn new(transaction: &'a mut dyn LixBackendTransaction) -> Self {
        Self {
            dialect: transaction.dialect(),
            transaction: Mutex::new(transaction as *mut (dyn LixBackendTransaction + 'a)),
            _lifetime: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<'a> QueryExecutor for TransactionExecutionBackend<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction adapter lock poisoned".to_string(),
        })?;
        unsafe { (&mut **guard).execute(sql, params).await }
    }
}

#[async_trait(?Send)]
impl<'a> LixBackend for TransactionExecutionBackend<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction adapter lock poisoned".to_string(),
        })?;
        unsafe { (&mut **guard).execute(sql, params).await }
    }

    async fn begin_transaction(
        &self,
        _mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "nested transactions are not supported via TransactionExecutionBackend"
                .to_string(),
        })
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "savepoints are not supported via TransactionExecutionBackend".to_string(),
        })
    }
}

pub(crate) async fn execute_write_program_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    program: WriteProgram,
) -> Result<QueryResult, LixError> {
    crate::backend::program_runner::execute_write_program_with_transaction(transaction, program)
        .await
}

pub(crate) async fn normalize_sql_error_with_transaction_and_relation_names(
    transaction: &mut dyn LixBackendTransaction,
    error: LixError,
    relation_names: &[String],
) -> LixError {
    let backend = TransactionExecutionBackend::new(transaction);
    normalize_sql_error_with_backend_and_relation_names(&backend, error, relation_names).await
}

pub(crate) async fn apply_workspace_writer_key_annotations_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    annotations: &std::collections::BTreeMap<
        crate::contracts::artifacts::RowIdentity,
        Option<String>,
    >,
) -> Result<(), LixError> {
    let mut backend = TransactionExecutionBackend::new(transaction);
    crate::annotations::writer_key::apply_workspace_writer_key_annotations_with_executor(
        &mut backend,
        annotations,
    )
    .await
}
