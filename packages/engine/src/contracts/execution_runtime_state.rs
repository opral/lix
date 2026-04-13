use super::{
    clone_boxed_function_provider, DynFunctionProvider, LixFunctionProvider, SharedFunctionProvider,
};

#[derive(Clone)]
pub(crate) struct ExecutionRuntimeState {
    deterministic_enabled: bool,
    functions: DynFunctionProvider,
}

impl ExecutionRuntimeState {
    pub(crate) fn from_prepared_parts<P>(
        deterministic_enabled: bool,
        functions: &SharedFunctionProvider<P>,
    ) -> Self
    where
        P: LixFunctionProvider + Send + 'static,
    {
        Self {
            deterministic_enabled,
            functions: clone_boxed_function_provider(functions),
        }
    }

    pub(crate) fn deterministic_enabled(&self) -> bool {
        self.deterministic_enabled
    }

    pub(crate) fn provider(&self) -> &DynFunctionProvider {
        &self.functions
    }
}
