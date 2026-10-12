//! SQL semantics every remote handle shares, checked on the client with the
//! engine's own parser before any SQL reaches the server: `query()` never
//! sends a write, `execute_many()` never runs a statement that returns rows,
//! and buffered reads honour the caller's byte cap.

use std::sync::atomic::Ordering;

use super::{ClientCore, ProtocolExecuteOptions, ProtocolHttp, ProtocolTransaction};
use crate::{ExecuteBatchStatement, ExecuteResult, LixError, QueryResult, Value};

fn check_query(sql: &str) -> Result<(), LixError> {
    if !crate::sql2::classify_statement(sql)?.read_only {
        return Err(crate::session::reject_non_read_error());
    }
    Ok(())
}

fn check_execute_many(sql: &str) -> Result<(), LixError> {
    let class = crate::sql2::classify_statement(sql)?;
    if class.read_only || class.returning {
        return Err(crate::session::execute_many_returns_rows());
    }
    Ok(())
}

/// Applies the caller's byte cap to a result the server buffered under its
/// own default cap. `None` keeps only the server's default.
fn capped_query_result(
    result: ExecuteResult,
    max_bytes: Option<usize>,
) -> Result<QueryResult, LixError> {
    if let Some(max_bytes) = max_bytes {
        crate::session::charge_execute_result(
            &mut crate::common::ReadResultBudget::new(crate::common::ReadLimits::bytes(max_bytes)),
            &result,
        )?;
    }
    Ok(QueryResult::from_execute_result(result))
}

impl<H: ProtocolHttp> ClientCore<H> {
    /// Runs a read on the server. Statements that write fail with
    /// `LIX_ERROR_READ_ONLY` before anything is sent.
    pub async fn query(
        &self,
        sql: &str,
        params: &[Value],
        max_bytes: Option<usize>,
    ) -> Result<QueryResult, LixError> {
        check_query(sql)?;
        capped_query_result(self.execute(sql, params, None).await?, max_bytes)
    }

    /// Runs one statement once per parameter set, atomically, in one request.
    /// Resolves to the total rows affected.
    pub async fn execute_many(
        &self,
        sql: &str,
        param_sets: Vec<Vec<Value>>,
        options: Option<ProtocolExecuteOptions>,
    ) -> Result<u64, LixError> {
        check_execute_many(sql)?;
        if param_sets.is_empty() {
            return Ok(0);
        }
        let statements = param_sets
            .into_iter()
            .map(|params| ExecuteBatchStatement {
                sql: sql.to_owned(),
                params,
                label: None,
            })
            .collect::<Vec<_>>();
        let result = self.execute_batch(&statements, options).await?;
        Ok(result
            .results
            .iter()
            .map(ExecuteResult::rows_affected)
            .sum())
    }
}

impl<H: ProtocolHttp> ProtocolTransaction<H> {
    /// Runs a read inside this transaction; it sees the transaction's writes.
    pub async fn query(
        &self,
        sql: &str,
        params: &[Value],
        max_bytes: Option<usize>,
    ) -> Result<QueryResult, LixError> {
        check_query(sql)?;
        capped_query_result(self.execute(sql, params, None).await?, max_bytes)
    }

    /// Stages one statement once per parameter set. The server has no
    /// statement checkpoint, so a failure after some sets were staged marks
    /// the transaction rollback-only: its commit rolls back and fails.
    pub async fn execute_many(
        &self,
        sql: &str,
        param_sets: Vec<Vec<Value>>,
        options: Option<ProtocolExecuteOptions>,
    ) -> Result<u64, LixError> {
        check_execute_many(sql)?;
        let mut rows_affected = 0;
        for (index, params) in param_sets.into_iter().enumerate() {
            match self.execute(sql, &params, options.clone()).await {
                Ok(result) => rows_affected += result.rows_affected(),
                Err(error) => {
                    if index > 0 {
                        self.rollback_only.store(true, Ordering::Release);
                    }
                    return Err(error);
                }
            }
        }
        Ok(rows_affected)
    }
}
