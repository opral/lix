pub(crate) use crate::binary_cas::write::{BinaryBlobWriteInput, ResolvedBinaryBlobWrite};

use crate::{LixBackend, LixBackendTransaction, LixError, SqlDialect};

pub(crate) async fn blob_exists(
    backend: &dyn LixBackend,
    blob_hash: &str,
) -> Result<bool, LixError> {
    crate::binary_cas::read::blob_exists(backend, blob_hash).await
}

pub(crate) async fn garbage_collect_unreachable_binary_cas_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    crate::binary_cas::gc::garbage_collect_unreachable_binary_cas_in_transaction(transaction).await
}

pub(crate) async fn persist_resolved_binary_blob_writes_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    writes: &[ResolvedBinaryBlobWrite],
) -> Result<(), LixError> {
    crate::binary_cas::write::persist_resolved_binary_blob_writes_in_transaction(
        transaction,
        writes,
    )
    .await
}

pub(crate) fn build_binary_blob_fastcdc_write_program(
    dialect: SqlDialect,
    payloads: &[BinaryBlobWriteInput<'_>],
) -> Result<crate::backend::program::WriteProgram, LixError> {
    crate::binary_cas::write::build_binary_blob_fastcdc_write_program(dialect, payloads)
}
