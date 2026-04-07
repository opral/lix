use crate::runtime::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::runtime::functions::SharedFunctionProvider;

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

    pub(crate) fn settings(&self) -> DeterministicSettings {
        self.settings
    }

    pub(crate) fn provider(&self) -> &SharedFunctionProvider<RuntimeFunctionProvider> {
        &self.functions
    }
}
