use crate::contracts::{
    CanonicalCommitReceipt, PublicChange, SessionStateDelta, StateCommitStreamChange,
};
use crate::contracts::{LixFunctionProvider, SharedFunctionProvider};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, Default)]
pub struct TransactionCommitOutcome {
    #[serde(default, skip_serializing_if = "SessionStateDelta::is_empty")]
    pub session_delta: SessionStateDelta,
    #[serde(default)]
    pub invalidate_deterministic_settings_cache: bool,
    #[serde(default)]
    pub invalidate_installed_plugins_cache: bool,
    #[serde(default)]
    pub refresh_public_surface_registry: bool,
    #[serde(default)]
    pub state_commit_stream_changes: Vec<StateCommitStreamChange>,
}

impl TransactionCommitOutcome {
    pub fn merge(&mut self, other: TransactionCommitOutcome) {
        self.session_delta.merge(other.session_delta);
        self.invalidate_deterministic_settings_cache |=
            other.invalidate_deterministic_settings_cache;
        self.invalidate_installed_plugins_cache |= other.invalidate_installed_plugins_cache;
        self.refresh_public_surface_registry |= other.refresh_public_surface_registry;
        self.state_commit_stream_changes
            .extend(other.state_commit_stream_changes);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TrackedCommitExecutionOutcome {
    pub receipt: Option<CanonicalCommitReceipt>,
    pub applied_changes: Vec<PublicChange>,
    pub plugin_changes_committed: bool,
    pub next_active_version_id: Option<String>,
}

#[derive(Clone)]
pub struct PreparedWriteRuntimeState {
    deterministic_mode_enabled: bool,
    functions: SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
}

impl std::fmt::Debug for PreparedWriteRuntimeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedWriteRuntimeState")
            .field(
                "deterministic_mode_enabled",
                &self.deterministic_mode_enabled,
            )
            .finish_non_exhaustive()
    }
}

impl PreparedWriteRuntimeState {
    pub fn new(
        deterministic_mode_enabled: bool,
        functions: SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>>,
    ) -> Self {
        Self {
            deterministic_mode_enabled,
            functions,
        }
    }

    pub fn functions(&self) -> &SharedFunctionProvider<Box<dyn LixFunctionProvider + Send>> {
        &self.functions
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BufferedWriteExecutionInput {
    writer_key: Option<String>,
    active_version_id: String,
    active_account_ids: Vec<String>,
}

impl BufferedWriteExecutionInput {
    pub fn new(
        writer_key: Option<String>,
        active_version_id: impl Into<String>,
        active_account_ids: Vec<String>,
    ) -> Self {
        Self {
            writer_key,
            active_version_id: active_version_id.into(),
            active_account_ids,
        }
    }

    pub fn writer_key(&self) -> Option<&str> {
        self.writer_key.as_deref()
    }

    pub fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    pub fn active_account_ids(&self) -> &[String] {
        &self.active_account_ids
    }

    pub fn apply_session_delta(&mut self, delta: &SessionStateDelta) {
        if let Some(version_id) = &delta.next_active_version_id {
            self.active_version_id = version_id.clone();
        }
        if let Some(active_account_ids) = &delta.next_active_account_ids {
            self.active_account_ids = active_account_ids.clone();
        }
    }
}
