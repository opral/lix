use super::{
    clone_boxed_function_provider, DynFunctionProvider, LixFunctionProvider, SharedFunctionProvider,
};

#[derive(Clone)]
pub(crate) struct FunctionBindings {
    deterministic_enabled: bool,
    provider: DynFunctionProvider,
}

impl FunctionBindings {
    pub(crate) fn from_prepared_parts<P>(
        deterministic_enabled: bool,
        provider: &SharedFunctionProvider<P>,
    ) -> Self
    where
        P: LixFunctionProvider + Send + 'static,
    {
        Self {
            deterministic_enabled,
            provider: clone_boxed_function_provider(provider),
        }
    }

    pub(crate) fn deterministic_enabled(&self) -> bool {
        self.deterministic_enabled
    }

    pub(crate) fn provider(&self) -> &DynFunctionProvider {
        &self.provider
    }
}
