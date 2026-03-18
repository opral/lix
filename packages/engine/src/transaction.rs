use crate::engine::{reject_internal_table_writes, Engine, EngineTransaction, ExecuteOptions};
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::write_txn_runner::stamp_watermark_before_commit;
use crate::{ExecuteResult, LixError, Value};
use futures_util::FutureExt;
use serde_json::Value as JsonValue;
use std::future::Future;
use std::pin::Pin;

const REGISTER_SCHEMA_HELPER_SQL: &str =
    "INSERT INTO lix_registered_schema (value) VALUES (lix_json($1))";

impl Engine {
    pub async fn register_schema(&self, schema: &JsonValue) -> Result<(), LixError> {
        let mut transaction = self
            .begin_transaction_with_options(ExecuteOptions::default())
            .await?;
        transaction.register_schema(schema).await?;
        transaction.commit().await
    }

    pub async fn begin_transaction_with_options(
        &self,
        options: ExecuteOptions,
    ) -> Result<EngineTransaction<'_>, LixError> {
        self.ensure_no_open_public_sql_transaction("begin_transaction")?;
        let transaction = self.begin_write_unit().await?;
        Ok(EngineTransaction {
            engine: self,
            transaction: Some(transaction),
            core: self.new_shared_transaction_core(options)?,
        })
    }

    pub async fn transaction<T, F>(&self, options: ExecuteOptions, f: F) -> Result<T, LixError>
    where
        F: for<'tx> FnOnce(
            &'tx mut EngineTransaction<'_>,
        ) -> Pin<Box<dyn Future<Output = Result<T, LixError>> + 'tx>>,
    {
        let mut transaction = self.begin_transaction_with_options(options).await?;
        match std::panic::AssertUnwindSafe(f(&mut transaction))
            .catch_unwind()
            .await
        {
            Ok(Ok(value)) => {
                transaction.commit().await?;
                Ok(value)
            }
            Ok(Err(error)) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
            Err(payload) => {
                let _ = transaction.rollback().await;
                std::panic::resume_unwind(payload);
            }
        }
    }
}

impl EngineTransaction<'_> {
    pub async fn register_schema(&mut self, schema: &JsonValue) -> Result<(), LixError> {
        let schema_json = serde_json::to_string(schema).map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("failed to serialize schema definition: {error}"),
        })?;
        self.execute(REGISTER_SCHEMA_HELPER_SQL, &[Value::Text(schema_json)])
            .await?;
        Ok(())
    }

    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        if !self.engine.access_to_internal() {
            let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
            reject_internal_table_writes(&parsed_statements)?;
        }
        self.execute_with_access(sql, params, self.engine.access_to_internal())
            .await
    }

    pub(crate) async fn execute_internal(
        &mut self,
        sql: &str,
        params: &[Value],
    ) -> Result<ExecuteResult, LixError> {
        self.execute_with_access(sql, params, true).await
    }

    async fn execute_with_access(
        &mut self,
        sql: &str,
        params: &[Value],
        allow_internal_tables: bool,
    ) -> Result<ExecuteResult, LixError> {
        let parsed_statements = parse_sql(sql).map_err(LixError::from)?;
        let transaction = self.transaction.as_mut().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        self.engine
            .execute_parsed_statements_in_transaction_core(
                transaction.as_mut(),
                parsed_statements,
                sql,
                params,
                allow_internal_tables,
                &mut self.core,
            )
            .await
    }

    pub async fn commit(mut self) -> Result<(), LixError> {
        let mut transaction = self.transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        self.engine
            .prepare_transaction_core_for_commit(transaction.as_mut(), &mut self.core)
            .await?;
        stamp_watermark_before_commit(transaction.as_mut()).await?;
        transaction.commit().await?;
        let core = std::mem::replace(
            &mut self.core,
            self.engine
                .new_shared_transaction_core(ExecuteOptions::default())?,
        );
        self.engine.finalize_committed_transaction_core(core).await
    }

    pub async fn rollback(mut self) -> Result<(), LixError> {
        let transaction = self.transaction.take().ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "transaction is no longer active".to_string(),
        })?;
        transaction.rollback().await
    }
}

impl Drop for EngineTransaction<'_> {
    fn drop(&mut self) {
        if self.transaction.is_some() && !std::thread::panicking() {
            panic!("EngineTransaction dropped without commit() or rollback()");
        }
    }
}
