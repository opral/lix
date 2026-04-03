use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::functions::SharedFunctionProvider;
use crate::runtime::RuntimeHost;
use crate::{LixBackend, LixBackendTransaction, LixError};

#[derive(Clone)]
pub(crate) struct ExecutionRuntimeState {
    settings: DeterministicSettings,
    functions: SharedFunctionProvider<RuntimeFunctionProvider>,
}

impl ExecutionRuntimeState {
    pub(crate) fn from_prepared_parts(
        settings: DeterministicSettings,
        functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    ) -> Self {
        Self {
            settings,
            functions,
        }
    }

    pub(crate) async fn prepare(
        host: &dyn RuntimeHost,
        backend: &dyn LixBackend,
    ) -> Result<Self, LixError> {
        let (settings, functions) = host.prepare_runtime_functions_with_backend(backend).await?;
        Ok(Self::from_prepared_parts(settings, functions))
    }

    pub(crate) fn settings(&self) -> DeterministicSettings {
        self.settings
    }

    pub(crate) fn provider(&self) -> &SharedFunctionProvider<RuntimeFunctionProvider> {
        &self.functions
    }

    pub(crate) async fn ensure_sequence_initialized_in_transaction(
        &self,
        host: &dyn RuntimeHost,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        host.ensure_runtime_sequence_initialized_in_transaction(transaction, &self.functions)
            .await
    }

    pub(crate) async fn flush_in_transaction(
        &self,
        host: &dyn RuntimeHost,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), LixError> {
        host.persist_runtime_sequence_in_transaction(transaction, self.settings, &self.functions)
            .await
    }
}
