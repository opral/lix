//! Transaction-owned compiler-state helpers.
//!
//! `session/*` remains the workflow caller, but the mutable compiler/runtime
//! state used by transaction-time planning and buffered write execution lives
//! under the transaction owner.

#[cfg(test)]
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use crate::catalog::SurfaceRegistry;
use crate::functions::FunctionBindings;
use crate::sql::{RuntimeBindingValues, SessionStateDelta};
#[cfg(test)]
use crate::sql::{StatementTemplate, StatementTemplateCacheKey};
use crate::transaction::BufferedWriteExecutionInput;
use crate::LixError;

pub(crate) type SessionCompilerCacheHandle = Arc<SessionCompilerCache>;

pub(crate) struct SessionCompilerCache {
    public_surface_registry_generation: AtomicU64,
    #[cfg(test)]
    statement_template_cache: Mutex<BTreeMap<StatementTemplateCacheKey, StatementTemplate>>,
}

impl SessionCompilerCache {
    pub(crate) fn new() -> SessionCompilerCacheHandle {
        Arc::new(Self {
            public_surface_registry_generation: AtomicU64::new(0),
            #[cfg(test)]
            statement_template_cache: Mutex::new(BTreeMap::new()),
        })
    }

    pub(crate) fn public_surface_registry_generation(&self) -> u64 {
        self.public_surface_registry_generation
            .load(Ordering::SeqCst)
    }

    pub(crate) fn bump_public_surface_registry_generation(&self) {
        self.public_surface_registry_generation
            .fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn cached_statement_template(
        &self,
        key: &StatementTemplateCacheKey,
    ) -> Option<StatementTemplate> {
        self.statement_template_cache
            .lock()
            .expect("statement template cache lock poisoned")
            .get(key)
            .cloned()
    }

    #[cfg(test)]
    pub(crate) fn cache_statement_template(
        &self,
        key: StatementTemplateCacheKey,
        template: StatementTemplate,
    ) {
        self.statement_template_cache
            .lock()
            .expect("statement template cache lock poisoned")
            .insert(key, template);
    }
}

pub(crate) struct SessionCompilerState {
    pub(crate) origin_key: Option<String>,
    pub(crate) public_surface_registry: SurfaceRegistry,
    compiler_cache: SessionCompilerCacheHandle,
    pub(crate) active_version_id: String,
    pub(crate) active_account_ids: Vec<String>,
    function_bindings: Option<FunctionBindings>,
}

impl SessionCompilerState {
    pub(crate) fn new(
        origin_key: Option<String>,
        public_surface_registry: SurfaceRegistry,
        compiler_cache: SessionCompilerCacheHandle,
        active_version_id: String,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            origin_key,
            public_surface_registry,
            compiler_cache,
            active_version_id,
            active_account_ids,
            function_bindings: None,
        }
    }

    pub(crate) fn bump_public_surface_registry_generation(&mut self) {
        self.compiler_cache
            .bump_public_surface_registry_generation();
    }

    pub(crate) fn public_surface_registry_generation(&self) -> u64 {
        self.compiler_cache.public_surface_registry_generation()
    }

    pub(crate) fn install_public_surface_registry(&mut self, registry: SurfaceRegistry) {
        self.public_surface_registry = registry;
        self.bump_public_surface_registry_generation();
    }

    #[cfg(test)]
    pub(crate) fn cached_statement_template(
        &self,
        key: &StatementTemplateCacheKey,
    ) -> Option<StatementTemplate> {
        self.compiler_cache.cached_statement_template(key)
    }

    #[cfg(test)]
    pub(crate) fn cache_statement_template(
        &self,
        key: StatementTemplateCacheKey,
        template: StatementTemplate,
    ) {
        self.compiler_cache.cache_statement_template(key, template);
    }

    pub(crate) fn function_bindings(&self) -> Option<&FunctionBindings> {
        self.function_bindings.as_ref()
    }

    pub(crate) fn set_function_bindings(&mut self, function_bindings: FunctionBindings) {
        self.function_bindings = Some(function_bindings);
    }

    pub(crate) fn clear_function_bindings(&mut self) {
        self.function_bindings = None;
    }

    pub(crate) fn runtime_binding_values(&self) -> Result<RuntimeBindingValues, LixError> {
        Ok(RuntimeBindingValues {
            active_version_id: self.active_version_id.clone(),
            active_account_ids_json: serde_json::to_string(&self.active_account_ids).map_err(
                |error| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!("active account ids serialization failed: {error}"),
                    )
                },
            )?,
        })
    }

    pub(crate) fn buffered_write_execution_input(&self) -> BufferedWriteExecutionInput {
        BufferedWriteExecutionInput::new(
            self.origin_key.clone(),
            self.active_version_id.clone(),
            self.active_account_ids.clone(),
        )
    }

    pub(crate) fn apply_buffered_write_execution_input(
        &mut self,
        input: &BufferedWriteExecutionInput,
    ) {
        self.active_version_id = input.active_version_id().to_string();
        self.active_account_ids = input.active_account_ids().to_vec();
    }

    pub(crate) fn apply_session_state_delta(&mut self, delta: &SessionStateDelta) {
        if let Some(version_id) = &delta.next_active_version_id {
            self.active_version_id = version_id.clone();
        }
        if let Some(active_account_ids) = &delta.next_active_account_ids {
            self.active_account_ids = active_account_ids.clone();
        }
    }
}
