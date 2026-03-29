use crate::backend::prepared::PreparedStatement;
use crate::{LixBackendTransaction, LixError, QueryResult};

pub(crate) async fn execute_prepared_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
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
