use crate::sql::execution::contracts::prepared_statement::{PreparedBatch, PreparedStatement};
use crate::{LixBackend, LixError, LixTransaction, QueryResult};

pub(crate) async fn execute_prepared_with_backend(
    backend: &dyn LixBackend,
    statements: &[PreparedStatement],
) -> Result<QueryResult, LixError> {
    let mut last_result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in statements {
        last_result = backend.execute(&statement.sql, &statement.params).await?;
    }
    Ok(last_result)
}

pub(crate) async fn execute_prepared_with_transaction(
    transaction: &mut dyn LixTransaction,
    statements: &[PreparedStatement],
) -> Result<QueryResult, LixError> {
    let mut last_result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in statements {
        last_result = transaction
            .execute(&statement.sql, &statement.params)
            .await?;
    }
    Ok(last_result)
}

pub(crate) async fn execute_prepared_batch_with_transaction(
    transaction: &mut dyn LixTransaction,
    batch: &PreparedBatch,
) -> Result<QueryResult, LixError> {
    transaction.execute(&batch.sql, &batch.params).await
}
