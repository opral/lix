#[cfg(test)]
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;

use crate::contracts::artifacts::{ExecuteOptions, SessionStateDelta};
use crate::contracts::surface::SurfaceRegistry;
use crate::execution::write::BufferedWriteExecutionInput;
use crate::runtime::execution_state::ExecutionRuntimeState;
use crate::sql::binder::RuntimeBindingValues;
#[cfg(test)]
use crate::sql::prepare::execution_program::{StatementTemplate, StatementTemplateCacheKey};
use crate::LixError;

pub(crate) type SessionExecutionRuntimeHandle = Arc<SessionExecutionRuntime>;

pub(crate) struct SessionExecutionRuntime {
    public_surface_registry_generation: AtomicU64,
    #[cfg(test)]
    statement_template_cache: Mutex<BTreeMap<StatementTemplateCacheKey, StatementTemplate>>,
}

impl SessionExecutionRuntime {
    pub(crate) fn new() -> SessionExecutionRuntimeHandle {
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

pub(crate) struct ExecutionContext {
    pub(crate) options: ExecuteOptions,
    pub(crate) public_surface_registry: SurfaceRegistry,
    session_runtime: SessionExecutionRuntimeHandle,
    pub(crate) active_version_id: String,
    pub(crate) active_account_ids: Vec<String>,
    execution_runtime_state: Option<ExecutionRuntimeState>,
}

impl ExecutionContext {
    pub(crate) fn new(
        options: ExecuteOptions,
        public_surface_registry: SurfaceRegistry,
        session_runtime: SessionExecutionRuntimeHandle,
        active_version_id: String,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            options,
            public_surface_registry,
            session_runtime,
            active_version_id,
            active_account_ids,
            execution_runtime_state: None,
        }
    }

    pub(crate) fn bump_public_surface_registry_generation(&mut self) {
        self.session_runtime
            .bump_public_surface_registry_generation();
    }

    pub(crate) fn public_surface_registry_generation(&self) -> u64 {
        self.session_runtime.public_surface_registry_generation()
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
        self.session_runtime.cached_statement_template(key)
    }

    #[cfg(test)]
    pub(crate) fn cache_statement_template(
        &self,
        key: StatementTemplateCacheKey,
        template: StatementTemplate,
    ) {
        self.session_runtime.cache_statement_template(key, template);
    }

    pub(crate) fn execution_runtime_state(&self) -> Option<&ExecutionRuntimeState> {
        self.execution_runtime_state.as_ref()
    }

    pub(crate) fn set_execution_runtime_state(&mut self, runtime_state: ExecutionRuntimeState) {
        self.execution_runtime_state = Some(runtime_state);
    }

    pub(crate) fn clear_execution_runtime_state(&mut self) {
        self.execution_runtime_state = None;
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
            self.options.writer_key.clone(),
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
