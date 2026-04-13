use crate::LixError;

use super::{TransactionWriteDelta, WriteCommand};

pub(crate) trait PreparedWriteStatementStager {
    fn mark_public_surface_registry_refresh_pending(&mut self);

    fn stage_transaction_write_delta(
        &mut self,
        delta: TransactionWriteDelta,
    ) -> Result<(), LixError>;
}

pub(crate) fn stage_prepared_write_statement(
    stager: &mut dyn PreparedWriteStatementStager,
    statement: WriteCommand,
) -> Result<(), LixError> {
    if !statement
        .prepared()
        .public_surface_registry_effect
        .is_none()
    {
        stager.mark_public_surface_registry_refresh_pending();
    }
    let transaction_write_delta =
        statement
            .transaction_write_delta()
            .cloned()
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    "semantic write statement must materialize a transaction write delta",
                )
            })?;
    stager.stage_transaction_write_delta(transaction_write_delta)
}
