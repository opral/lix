use crate::binary_cas::storage;
use crate::binary_cas::store::BinaryCasTransactionRef;
use crate::LixError;

pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
    transaction: BinaryCasTransactionRef<'_>,
) -> Result<(), LixError> {
    storage::garbage_collect_unreachable_in_transaction(transaction).await
}
