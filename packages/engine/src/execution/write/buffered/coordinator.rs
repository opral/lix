use crate::contracts::CanonicalCommitReceipt;
use crate::contracts::LiveStateTransactionBridge;
use crate::contracts::{SchemaRegistration, SchemaRegistrationSet};
use crate::{LixBackendTransaction, LixError};

pub(crate) struct TransactionCoordinator<'a> {
    backend_txn: Option<Box<dyn LixBackendTransaction + 'a>>,
    #[cfg(test)]
    registered_schemas: SchemaRegistrationSet,
}

impl<'a> TransactionCoordinator<'a> {
    pub(crate) fn new(backend_txn: Box<dyn LixBackendTransaction + 'a>) -> Self {
        Self {
            backend_txn: Some(backend_txn),
            #[cfg(test)]
            registered_schemas: SchemaRegistrationSet::default(),
        }
    }

    #[cfg(test)]
    pub(crate) fn register_schema(
        &mut self,
        registration: impl Into<SchemaRegistration>,
    ) -> Result<(), LixError> {
        self.ensure_active()?;
        self.registered_schemas.insert(registration);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) async fn register_staged_schemas(&mut self) -> Result<(), LixError> {
        let registrations = self.registered_schemas.clone();
        let transaction = self.backend_transaction_mut()?;
        apply_schema_registrations_in_transaction(transaction, &registrations).await
    }

    pub(crate) async fn advance_live_state_replay_boundary_for_commit(
        &mut self,
        receipt: Option<&CanonicalCommitReceipt>,
    ) -> Result<(), LixError> {
        let Some(receipt) = receipt else {
            return Ok(());
        };
        let transaction = self.backend_transaction_mut()?;
        transaction
            .advance_live_state_replay_boundary(&receipt.replay_cursor)
            .await
    }

    pub(crate) async fn commit(&mut self) -> Result<(), LixError> {
        let transaction = self.backend_txn.take().ok_or_else(inactive_error)?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn rollback(&mut self) -> Result<(), LixError> {
        let transaction = self.backend_txn.take().ok_or_else(inactive_error)?;
        transaction.rollback().await
    }

    pub(crate) fn backend_transaction_mut(
        &mut self,
    ) -> Result<&mut dyn LixBackendTransaction, LixError> {
        self.ensure_active()?;
        Ok(self.backend_txn.as_deref_mut().ok_or_else(inactive_error)?)
    }

    pub(crate) fn ensure_active(&self) -> Result<(), LixError> {
        if self.backend_txn.is_none() {
            return Err(inactive_error());
        }
        Ok(())
    }
}

pub(crate) fn inactive_error() -> LixError {
    LixError::new("LIX_ERROR_UNKNOWN", "transaction is no longer active")
}

pub(crate) async fn apply_schema_registrations_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    registrations: &SchemaRegistrationSet,
) -> Result<(), LixError> {
    if registrations.is_empty() {
        return Ok(());
    }
    for registration in registrations.values().cloned() {
        register_schema_in_transaction(transaction, &registration).await?;
    }
    Ok(())
}

pub(crate) async fn register_schema_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    registration: &SchemaRegistration,
) -> Result<(), LixError> {
    transaction.register_live_state_schema(registration).await
}
