use std::sync::Arc;

use crate::GLOBAL_BRANCH_ID;
use crate::LixError;
use crate::branch::BranchRefStoreReader;
use crate::catalog::{CatalogContext, CatalogFingerprint};
use crate::entity_pk::EntityPk;
use crate::forktree::{StateCell, StateKeyRef, encode_state_key};
use crate::init::InitReceipt;
use crate::observe_coordinator::ObserveCoordinator;
use crate::observe_invalidation::ObserveInvalidation;
use crate::plugin::{
    DEFAULT_MAX_LIVE_PLUGIN_STORES, DEFAULT_PLUGIN_MEMORY_BYTES, PluginRuntimeHost,
};
use crate::session::SessionContext;
use crate::sql2::SqlPlanningCache;
use crate::state::ForkTreeStateView;
use crate::storage_adapter::{SharedStorageAdapterRead, StorageReadOptions};
use crate::storage_adapter::{Storage, StorageAdapter};
use crate::telemetry::TelemetrySink;
use crate::transaction::CommitCoordinator;
use crate::plugin::runtime::WasmTransitionCounters;
use crate::plugin::runtime::{UnsupportedWasmRuntime, WasmRuntime};

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct Engine<StorageImpl: Storage + 'static = crate::storage_adapter::Memory> {
    storage: StorageAdapter<StorageImpl>,
    catalog_context: Arc<CatalogContext>,
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
    commit_coordinator: Arc<CommitCoordinator<StorageImpl>>,
    observe_coordinator: Arc<ObserveCoordinator>,
    observe_invalidation: Arc<ObserveInvalidation>,
    plugin_host: PluginRuntimeHost,
    telemetry: Option<Arc<dyn TelemetrySink>>,
}

#[expect(missing_debug_implementations)]
pub struct EngineOptions {
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    plugin_max_memory_bytes: u64,
    plugin_max_live_stores: usize,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            wasm_runtime: None,
            telemetry: None,
            plugin_max_memory_bytes: DEFAULT_PLUGIN_MEMORY_BYTES,
            plugin_max_live_stores: DEFAULT_MAX_LIVE_PLUGIN_STORES,
        }
    }
}

impl EngineOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_wasm_runtime(mut self, wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        self.wasm_runtime = Some(wasm_runtime);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Arc<dyn TelemetrySink>) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Sets the per-Store Wasm linear-memory ceiling and the hard maximum
    /// number of simultaneously live v2 plugin Stores for this engine.
    ///
    /// The Store limit bounds the active working set, not the number of plugin
    /// documents an atomic transaction may open. Transactions retire completed
    /// Stores and reuse their slots while preserving atomic commit.
    /// Defaults are 192 MiB and ten Stores, bounding guest linear memory to
    /// 1.875 GiB before host-side document state. Cached actors, active
    /// existing-document transaction leases, pending publications, cold-open
    /// candidates, and upgrade preflight Stores consume the same
    /// workspace-wide budget. Completed publications may retire their Stores
    /// under pressure and cold-open again after commit.
    pub fn with_plugin_resource_limits(
        mut self,
        max_memory_bytes: u64,
        max_live_stores: usize,
    ) -> Self {
        self.plugin_max_memory_bytes = max_memory_bytes;
        self.plugin_max_live_stores = max_live_stores;
        self
    }
}

impl<StorageImpl> Engine<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Seeds an empty storage with the engine repository bootstrap facts.
    ///
    /// Initialization is a storage lifecycle operation, separate from runtime
    /// construction. Call this before `Engine::new(...)` for a brand-new
    /// storage.
    pub async fn initialize(storage: StorageImpl) -> Result<InitReceipt, LixError> {
        let storage = StorageAdapter::new(storage);

        crate::init::initialize(storage).await
    }

    /// Creates a clean DataFusion-first engine over an initialized storage.
    ///
    /// SessionContext, execution, and transaction overlays are layered below the
    /// instance instead of being hidden behind initialization side effects.
    ///
    /// Deterministic runtime sequencing is serialized within this Engine
    /// context. Independently constructing multiple Engine values over the same
    /// cloned storage is outside that MVP runtime-sharing boundary.
    pub async fn new(storage: StorageImpl) -> Result<Self, LixError> {
        Self::new_with_options(storage, EngineOptions::new()).await
    }

    /// Creates an engine with a WASM component runtime for installed plugins.
    pub async fn new_with_wasm_runtime(
        storage: StorageImpl,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Result<Self, LixError> {
        Self::new_with_options(
            storage,
            EngineOptions::new().with_wasm_runtime(wasm_runtime),
        )
        .await
    }

    pub async fn new_with_options(
        storage: StorageImpl,
        options: EngineOptions,
    ) -> Result<Self, LixError> {
        let storage = StorageAdapter::new(storage);
        let wasm_runtime = options
            .wasm_runtime
            .unwrap_or_else(|| Arc::new(UnsupportedWasmRuntime));
        let plugin_host = PluginRuntimeHost::new_with_limits(
            wasm_runtime,
            options.plugin_max_memory_bytes,
            options.plugin_max_live_stores,
        )?;

        assert_initialized(storage.clone()).await?;

        // SessionContext::execute later projects these stable state contexts into one
        // execution-scoped SQL context, optionally wrapped by a transaction
        // overlay for writes.

        let collaboration_write_gate = Arc::new(tokio::sync::Mutex::new(()));
        let observe_invalidation = Arc::new(ObserveInvalidation::new());
        let commit_coordinator = Arc::new(CommitCoordinator::new(
            Arc::clone(&collaboration_write_gate),
            Arc::clone(&observe_invalidation),
        ));
        Ok(Self {
            storage,
            catalog_context: Arc::new(CatalogContext::new()),
            sql_planning_cache: Arc::new(SqlPlanningCache::default()),
            deterministic_runtime_gate: Arc::new(tokio::sync::Mutex::new(())),
            collaboration_write_gate,
            commit_coordinator,
            observe_coordinator: Arc::new(ObserveCoordinator::new()),
            observe_invalidation,
            plugin_host,
            telemetry: options.telemetry,
        })
    }

    pub(crate) fn storage(&self) -> StorageAdapter<StorageImpl> {
        self.storage.clone()
    }

    /// Loads the current commit head for a branch.
    ///
    /// This is the public engine-level form of the typed `branch_ref` context:
    /// callers should not need to know that branch heads are represented as
    /// untracked `lix_branch_ref` rows in the authenticated ForkTree state.
    pub async fn load_branch_head_commit_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<String>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let result = BranchRefStoreReader::new(read)
            .load_head_commit_id(branch_id)
            .await?
            .map(|commit_id| commit_id.to_string());
        Ok(result)
    }

    pub async fn open_session(
        &self,
        active_branch_id: impl Into<String>,
    ) -> Result<SessionContext<StorageImpl>, LixError> {
        self.open_session_with_account(active_branch_id, crate::ANONYMOUS_ACCOUNT_ID)
            .await
    }

    pub async fn open_session_with_account(
        &self,
        active_branch_id: impl Into<String>,
        active_account_id: impl Into<String>,
    ) -> Result<SessionContext<StorageImpl>, LixError> {
        let active_account_id = active_account_id.into();
        self.validate_active_account(&active_account_id).await?;
        SessionContext::open(
            active_branch_id.into(),
            active_account_id,
            self.storage(),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.sql_planning_cache),
            Arc::clone(&self.deterministic_runtime_gate),
            Arc::clone(&self.collaboration_write_gate),
            Arc::clone(&self.commit_coordinator),
            Arc::clone(&self.observe_coordinator),
            Arc::clone(&self.observe_invalidation),
            self.plugin_host.clone(),
            self.telemetry.clone(),
        )
        .await
    }

    pub async fn open_workspace_session(&self) -> Result<SessionContext<StorageImpl>, LixError> {
        self.open_workspace_session_with_account(crate::ANONYMOUS_ACCOUNT_ID)
            .await
    }

    pub async fn open_workspace_session_with_account(
        &self,
        active_account_id: impl Into<String>,
    ) -> Result<SessionContext<StorageImpl>, LixError> {
        let active_account_id = active_account_id.into();
        self.validate_active_account(&active_account_id).await?;
        SessionContext::open_workspace(
            active_account_id,
            self.storage(),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.sql_planning_cache),
            Arc::clone(&self.deterministic_runtime_gate),
            Arc::clone(&self.collaboration_write_gate),
            Arc::clone(&self.commit_coordinator),
            Arc::clone(&self.observe_coordinator),
            Arc::clone(&self.observe_invalidation),
            self.plugin_host.clone(),
            self.telemetry.clone(),
        )
        .await
    }

    /// Returns process-local work accumulated by completed v2 transitions on
    /// this engine. The snapshot is shared by every session cloned from it.
    #[doc(hidden)]
    pub fn plugin_transition_counters(&self) -> WasmTransitionCounters {
        self.plugin_host.transition_counters()
    }

    /// Resets the process-local v2 transition aggregate used by profiling and
    /// invariant tests. This does not mutate durable workspace state.
    #[doc(hidden)]
    pub fn reset_plugin_transition_counters(&self) {
        self.plugin_host.reset_transition_counters();
    }

    async fn validate_active_account(&self, account_id: &str) -> Result<(), LixError> {
        let account_pk = EntityPk::uuid_from_canonical(account_id).map_err(|_| {
            LixError::new(
                "LIX_INVALID_ACCOUNT_ID",
                format!("active account id '{account_id}' is not a canonical UUID"),
            )
        })?;
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let key = encode_state_key(StateKeyRef {
            schema_key: "lix_account",
            file_id: None,
            entity_pk: &account_pk,
        });
        let view = ForkTreeStateView::from_facade(
            crate::forktree::ForkTreeReadFacade::new(read),
            GLOBAL_BRANCH_ID,
        )
        .await?;
        let row = view
            .points(&[key], false)
            .await?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ACCOUNT_NOT_FOUND",
                    format!("active account '{account_id}' does not exist"),
                )
            })?;
        let snapshot = row
            .seed_logical_snapshot(GLOBAL_BRANCH_ID)?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("account '{account_id}' has no snapshot"),
                )
            })?;
        let snapshot = snapshot.as_str();
        let value: serde_json::Value = serde_json::from_str(snapshot).map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("account '{account_id}' has invalid JSON: {error}"),
            )
        })?;
        if value.get("status").and_then(serde_json::Value::as_str) != Some("active") {
            return Err(LixError::new(
                "LIX_ACCOUNT_DISABLED",
                format!("active account '{account_id}' is disabled"),
            ));
        }
        Ok(())
    }
}

async fn assert_initialized<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let read =
        SharedStorageAdapterRead::new(storage.begin_read(StorageReadOptions::default()).await?);
    match crate::init::repository_protocol_status(&read).await? {
        // The protocol check must precede the live-state read: tracked-head
        // spaces keep their physical IDs across hard layout cuts, so an old
        // group value could otherwise be decoded before we reject it.
        crate::init::RepositoryProtocolStatus::Current => {
            let entity_pk = EntityPk::single("lix_id");
            let key = crate::forktree::encode_state_key(crate::forktree::StateKeyRef {
                schema_key: "lix_key_value",
                file_id: None,
                entity_pk: &entity_pk,
            });
            let view = ForkTreeStateView::from_facade(
                crate::forktree::ForkTreeReadFacade::new(read),
                GLOBAL_BRANCH_ID,
            )
            .await?;
            let initialized = view
                .points(&[key], false)
                .await?
                .into_iter()
                .next()
                .flatten()
                .is_some();
            if initialized {
                Ok(())
            } else {
                Err(not_initialized_error())
            }
        }
        crate::init::RepositoryProtocolStatus::Unsupported => {
            Err(crate::init::unsupported_repository_protocol_error())
        }
        crate::init::RepositoryProtocolStatus::Missing => Err(not_initialized_error()),
    }
}

fn not_initialized_error() -> LixError {
    LixError::new(
        "LIX_ERROR_NOT_INITIALIZED",
        "engine storage is not initialized; call Engine::initialize(...) before Engine::new(...)",
    )
}

#[cfg(test)]
mod predecessor_protocol_tests {
    use super::*;
    use crate::storage_adapter::{Memory, StorageAdapter, StorageWriteOptions};

    #[tokio::test]
    async fn predecessor_protocol_markers_fail_before_forktree_open() {
        // These values are historical protocol markers, not current storage
        // spaces. The public engine boundary must reject them before any old
        // row/codec owner can be consulted.
        let markers = [
            b"live-state.hot.v15".as_slice(),
            b"clustered-packed-history.v20".as_slice(),
            b"file-descriptor-ownership.v21".as_slice(),
            b"file-first-hot-state.v22".as_slice(),
            b"schema-file-membership.v23".as_slice(),
            b"commit-delta-sidecar-zstd.v24".as_slice(),
            b"hot-inline-fingerprint.v25".as_slice(),
            b"selected-payload-reference.v26".as_slice(),
            b"packed-current-base.v27".as_slice(),
            b"checkpoint-owned-hot-baseline.v27".as_slice(),
            b"checkpoint-source-delta.v33".as_slice(),
            b"immutable-physical-commit-state.v61".as_slice(),
        ];

        for marker in markers {
            let storage = Memory::new();
            let adapter = StorageAdapter::new(storage.clone());
            let mut writes = adapter.new_write_set();
            writes.put(
                crate::init::REPOSITORY_PROTOCOL_SPACE,
                crate::init::REPOSITORY_PROTOCOL_KEY,
                marker,
            );
            adapter
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("historical protocol marker should be writable as corruption input");

            let result = Engine::new(storage).await;
            let Err(error) = result else {
                panic!("historical protocol marker must fail closed");
            };
            assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
        }
    }

    #[tokio::test]
    async fn missing_current_protocol_fails_before_any_legacy_state_decode() {
        let storage = Memory::new();
        let result = Engine::new(storage).await;
        let Err(error) = result else {
            panic!("missing protocol must fail closed");
        };
        assert_eq!(error.code, "LIX_ERROR_NOT_INITIALIZED");
    }
}
