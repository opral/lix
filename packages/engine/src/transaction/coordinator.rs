use std::collections::BTreeMap;

use crate::live_state::{CanonicalWatermark, SchemaRegistration};
use crate::{LixBackendTransaction, LixError};

pub(crate) struct TransactionCoordinator<'a> {
    backend_txn: Option<Box<dyn LixBackendTransaction + 'a>>,
    registered_schemas: BTreeMap<String, SchemaRegistration>,
}

impl<'a> TransactionCoordinator<'a> {
    pub(crate) fn new(backend_txn: Box<dyn LixBackendTransaction + 'a>) -> Self {
        Self {
            backend_txn: Some(backend_txn),
            registered_schemas: BTreeMap::new(),
        }
    }

    pub(crate) fn register_schema(
        &mut self,
        registration: impl Into<SchemaRegistration>,
    ) -> Result<(), LixError> {
        self.ensure_active()?;
        let registration = registration.into();
        self.registered_schemas
            .insert(registration.schema_key().to_string(), registration);
        Ok(())
    }

    pub(crate) async fn register_staged_schemas(&mut self) -> Result<(), LixError> {
        let registrations = self
            .registered_schemas
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let transaction = self.backend_transaction_mut()?;
        for registration in registrations {
            crate::live_state::register_schema_in_transaction(transaction, registration).await?;
        }
        Ok(())
    }

    pub(crate) async fn finalize_live_state(&mut self) -> Result<CanonicalWatermark, LixError> {
        let transaction = self.backend_transaction_mut()?;
        crate::live_state::finalize_commit_in_transaction(transaction).await
    }

    pub(crate) async fn finalize_live_state_allow_missing_watermark(
        &mut self,
    ) -> Result<(), LixError> {
        match self.finalize_live_state().await {
            Ok(_) => Ok(()),
            Err(error)
                if error.description
                    == "live_state::finalize_commit expected a canonical watermark" =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
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
