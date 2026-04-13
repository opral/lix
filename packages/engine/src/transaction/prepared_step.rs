use crate::execution::step::PreparedWriteExecutionStep;
use crate::LixError;

use super::TransactionWriteDelta;

pub(crate) trait PreparedWriteStepStager {
    fn mark_public_surface_registry_refresh_pending(&mut self);

    fn stage_transaction_write_delta(
        &mut self,
        delta: TransactionWriteDelta,
    ) -> Result<(), LixError>;
}

pub(crate) fn stage_prepared_write_step(
    stager: &mut dyn PreparedWriteStepStager,
    step: PreparedWriteExecutionStep,
) -> Result<(), LixError> {
    if !step.prepared().public_surface_registry_effect.is_none() {
        stager.mark_public_surface_registry_refresh_pending();
    }
    let transaction_write_delta = step.transaction_write_delta().cloned().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "semantic write step must materialize a transaction write delta",
        )
    })?;
    stager.stage_transaction_write_delta(transaction_write_delta)
}
