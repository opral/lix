use super::super::*;

pub(crate) async fn execute_prepared_with_backend(
    backend: &dyn LixBackend,
    statements: &[crate::sql::PreparedStatement],
) -> Result<QueryResult, LixError> {
    let mut last_result = QueryResult { rows: Vec::new() };
    for statement in statements {
        last_result = backend.execute(&statement.sql, &statement.params).await?;
    }
    Ok(last_result)
}

pub(crate) async fn execute_prepared_with_transaction(
    transaction: &mut dyn LixTransaction,
    statements: &[crate::sql::PreparedStatement],
) -> Result<QueryResult, LixError> {
    let mut last_result = QueryResult { rows: Vec::new() };
    for statement in statements {
        last_result = transaction
            .execute(&statement.sql, &statement.params)
            .await?;
    }
    Ok(last_result)
}
