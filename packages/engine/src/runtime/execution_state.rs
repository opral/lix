use crate::runtime::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::runtime::functions::SharedFunctionProvider;
use crate::runtime::Runtime;
use crate::{LixBackend, LixError};

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
        runtime: &Runtime,
        backend: &dyn LixBackend,
    ) -> Result<Self, LixError> {
        let (settings, functions) = runtime
            .prepare_runtime_functions_with_backend(backend)
            .await?;
        Ok(Self::from_prepared_parts(settings, functions))
    }

    pub(crate) fn settings(&self) -> DeterministicSettings {
        self.settings
    }

    pub(crate) fn provider(&self) -> &SharedFunctionProvider<RuntimeFunctionProvider> {
        &self.functions
    }
}
