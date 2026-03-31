use std::collections::{BTreeSet, HashMap};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, RwLock};

use crate::backend::QueryExecutor;
use crate::cel::CelEvaluator;
use crate::contracts::artifacts::MutationRow;
use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::functions::SharedFunctionProvider;
use crate::schema::SchemaKey;
use crate::{
    LixBackend, LixBackendTransaction, LixError, QueryResult, SqlDialect, TransactionMode, Value,
};
use async_trait::async_trait;
use jsonschema::JSONSchema;
use sqlparser::ast::Statement;

#[derive(Debug, Default)]
pub struct SchemaCache {
    inner: RwLock<HashMap<SchemaKey, Arc<JSONSchema>>>,
}

impl SchemaCache {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub(crate) fn read(
        &self,
    ) -> std::sync::LockResult<std::sync::RwLockReadGuard<'_, HashMap<SchemaKey, Arc<JSONSchema>>>>
    {
        self.inner.read()
    }

    pub(crate) fn write(
        &self,
    ) -> std::sync::LockResult<std::sync::RwLockWriteGuard<'_, HashMap<SchemaKey, Arc<JSONSchema>>>>
    {
        self.inner.write()
    }
}

#[async_trait(?Send)]
pub(crate) trait RuntimeHost {
    fn cel_evaluator(&self) -> &CelEvaluator;
    fn schema_cache(&self) -> &SchemaCache;

    async fn prepare_runtime_functions_with_backend(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<
        (
            DeterministicSettings,
            SharedFunctionProvider<RuntimeFunctionProvider>,
        ),
        LixError,
    >;

    async fn ensure_runtime_sequence_initialized_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError>;

    async fn persist_runtime_sequence_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        settings: DeterministicSettings,
        functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Result<(), LixError>;
}

pub(crate) struct TransactionBackendAdapter<'a> {
    dialect: SqlDialect,
    transaction: Mutex<*mut (dyn LixBackendTransaction + 'a)>,
    _lifetime: PhantomData<&'a ()>,
}

pub(crate) async fn normalize_sql_execution_error_with_backend(
    backend: &dyn LixBackend,
    error: LixError,
    statements: &[Statement],
) -> LixError {
    crate::errors::classification::normalize_sql_error_with_backend(backend, error, statements)
        .await
}

pub(crate) fn direct_state_file_cache_refresh_targets(
    mutations: &[MutationRow],
) -> BTreeSet<(String, String)> {
    mutations
        .iter()
        .filter(|mutation| !mutation.untracked)
        .filter(|mutation| mutation.file_id != "lix")
        .filter(|mutation| mutation.schema_key != "lix_file_descriptor")
        .filter(|mutation| mutation.schema_key != "lix_directory_descriptor")
        .map(|mutation| (mutation.file_id.clone(), mutation.version_id.clone()))
        .collect()
}

// SAFETY: `TransactionBackendAdapter` is only used inside a single async execution flow.
// Internal access to the raw transaction pointer is serialized with a mutex.
unsafe impl<'a> Send for TransactionBackendAdapter<'a> {}
// SAFETY: see `Send` impl above.
unsafe impl<'a> Sync for TransactionBackendAdapter<'a> {}

impl<'a> TransactionBackendAdapter<'a> {
    pub(crate) fn new(transaction: &'a mut dyn LixBackendTransaction) -> Self {
        Self {
            dialect: transaction.dialect(),
            transaction: Mutex::new(transaction as *mut (dyn LixBackendTransaction + 'a)),
            _lifetime: PhantomData,
        }
    }
}

#[async_trait(?Send)]
impl<'a> QueryExecutor for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction adapter lock poisoned".to_string(),
        })?;
        // SAFETY: the pointer is created from a live `&mut dyn LixBackendTransaction` and
        // this mutex serializes all calls so the mutable borrow is not aliased.
        unsafe { (&mut **guard).execute(sql, params).await }
    }
}

#[async_trait(?Send)]
impl<'a> LixBackend for TransactionBackendAdapter<'a> {
    fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let mut guard = self.transaction.lock().map_err(|_| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction adapter lock poisoned".to_string(),
        })?;
        // SAFETY: the pointer is created from a live `&mut dyn LixBackendTransaction` and
        // this mutex serializes all calls so the mutable borrow is not aliased.
        unsafe { (&mut **guard).execute(sql, params).await }
    }

    async fn begin_transaction(
        &self,
        _mode: TransactionMode,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "nested transactions are not supported via TransactionBackendAdapter"
                .to_string(),
        })
    }

    async fn begin_savepoint(
        &self,
        _name: &str,
    ) -> Result<Box<dyn LixBackendTransaction + '_>, LixError> {
        Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "savepoints are not supported via TransactionBackendAdapter".to_string(),
        })
    }
}
