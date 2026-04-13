use crate::execution::{execute_write, WriteBatch};
use crate::{LixBackendTransaction, LixError, QueryResult};

pub(crate) async fn execute_write_batch_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    write_batch: WriteBatch,
) -> Result<QueryResult, LixError> {
    execute_write(transaction, write_batch).await
}
