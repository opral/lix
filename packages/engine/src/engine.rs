use std::sync::Arc;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{BranchContext, BranchRefReader};
use crate::catalog::CatalogContext;
use crate::commit_graph::CommitGraphContext;
use crate::entity_pk::EntityPk;
use crate::init::InitReceipt;
use crate::live_state::LiveStateContext;
use crate::live_state::LiveStateIndexContext;
use crate::live_state::LiveStateRowRequest;
use crate::observe_coordinator::ObserveCoordinator;
use crate::observe_invalidation::ObserveInvalidation;
use crate::plugin::PluginRuntimeHost;
use crate::session::SessionContext;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{SharedStorageAdapterRead, StorageReadOptions, StorageWriteOptions};
use crate::storage_adapter::{StorageAdapter, StorageWriteSet};
use crate::telemetry::TelemetrySink;
use crate::tracked_state::TrackedStateContext;
use crate::wasm::{UnsupportedWasmRuntime, WasmRuntime};
use crate::{LixError, NullableKeyFilter};

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct Engine<StorageImpl: Storage = crate::storage_adapter::Memory> {
    storage: StorageAdapter<StorageImpl>,
    tracked_state: Arc<TrackedStateContext>,
    live_state: Arc<LiveStateContext>,
    branch_ctx: Arc<BranchContext>,
    binary_cas: Arc<BinaryCasContext>,
    catalog_context: Arc<CatalogContext>,
    deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
    observe_coordinator: Arc<ObserveCoordinator>,
    observe_invalidation: Arc<ObserveInvalidation>,
    plugin_host: PluginRuntimeHost,
    telemetry: Option<Arc<dyn TelemetrySink>>,
}

#[derive(Default)]
#[expect(missing_debug_implementations)]
pub struct EngineOptions {
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
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

        crate::init::initialize(
            storage,
            &TrackedStateContext::new(),
            &LiveStateIndexContext::new(),
        )
        .await
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

        let tracked_state = Arc::new(TrackedStateContext::new());
        let live_index = LiveStateIndexContext::new();
        let commit_graph = CommitGraphContext::new();
        let live_state = Arc::new(LiveStateContext::new(
            tracked_state.as_ref().clone(),
            live_index,
            commit_graph,
        ));
        let branch_ctx = Arc::new(BranchContext::new());
        assert_initialized(storage.clone(), live_state.as_ref()).await?;

        // SessionContext::execute later projects these stable state contexts into one
        // execution-scoped SQL context, optionally wrapped by a transaction
        // overlay for writes.

        Ok(Self {
            binary_cas: Arc::new(BinaryCasContext::new()),
            storage,
            tracked_state,
            live_state,
            branch_ctx,
            catalog_context: Arc::new(CatalogContext::new()),
            deterministic_runtime_gate: Arc::new(tokio::sync::Mutex::new(())),
            collaboration_write_gate: Arc::new(tokio::sync::Mutex::new(())),
            observe_coordinator: Arc::new(ObserveCoordinator::new()),
            observe_invalidation: Arc::new(ObserveInvalidation::new()),
            plugin_host: PluginRuntimeHost::new(wasm_runtime),
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
    /// untracked `lix_branch_ref` rows in live_state.
    pub async fn load_branch_head_commit_id(
        &self,
        branch_id: &str,
    ) -> Result<Option<String>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let result = self
            .branch_ctx
            .ref_reader(read)
            .load_head_commit_id(branch_id)
            .await?
            .map(|commit_id| commit_id.to_string());
        Ok(result)
    }

    pub async fn open_session(
        &self,
        active_branch_id: impl Into<String>,
    ) -> Result<SessionContext<StorageImpl>, LixError> {
        SessionContext::open(
            active_branch_id.into(),
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.deterministic_runtime_gate),
            Arc::clone(&self.collaboration_write_gate),
            Arc::clone(&self.observe_coordinator),
            Arc::clone(&self.observe_invalidation),
            self.plugin_host.clone(),
            self.telemetry.clone(),
        )
        .await
    }

    pub async fn open_workspace_session(&self) -> Result<SessionContext<StorageImpl>, LixError> {
        SessionContext::open_workspace(
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.deterministic_runtime_gate),
            Arc::clone(&self.collaboration_write_gate),
            Arc::clone(&self.observe_coordinator),
            Arc::clone(&self.observe_invalidation),
            self.plugin_host.clone(),
            self.telemetry.clone(),
        )
        .await
    }

    /// Rebuilds the tracked serving commit root for one branch from changelog.
    ///
    /// This is intentionally an engine-level operation: callers should not need
    /// to know which KV namespaces back changelog, commit graph, or tracked
    /// state. The current branch head is read from the live-state facade so
    /// rebuild uses the same moving-ref visibility as normal execution. The
    /// rebuilt root receives the full changelog coverage audit against its
    /// staged chunks before the replacement root is published.
    pub async fn rebuild_tracked_state_for_branch(&self, branch_id: &str) -> Result<(), LixError> {
        let head_commit_id = self
            .load_branch_head_commit_id(branch_id)
            .await?
            .ok_or_else(|| {
                LixError::branch_not_found(
                    branch_id.to_string(),
                    "rebuild_tracked_state_for_branch",
                    "target",
                )
            })?;
        let storage = self.storage();
        let read =
            SharedStorageAdapterRead::new(storage.begin_read(StorageReadOptions::default()).await?);
        let mut writes = StorageWriteSet::new();
        let rebuild_result = self
            .tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at(&head_commit_id)
            .await;
        rebuild_result?;
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map(|_| ())
            .map_err(LixError::from)
    }
}

async fn assert_initialized<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    live_state: &LiveStateContext,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let read =
        SharedStorageAdapterRead::new(storage.begin_read(StorageReadOptions::default()).await?);
    let reader = live_state.reader(read);
    let initialized = reader
        .load_row(&LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            branch_id: GLOBAL_BRANCH_ID.to_string(),
            entity_pk: EntityPk::single("lix_id"),
            file_id: NullableKeyFilter::Null,
        })
        .await?
        .is_some();

    if initialized {
        return Ok(());
    }

    Err(LixError::new(
        "LIX_ERROR_NOT_INITIALIZED",
        "engine storage is not initialized; call Engine::initialize(...) before Engine::new(...)",
    ))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::storage_adapter::{
        Memory, PointReadPlan, StorageGetOptions, StorageKey, StorageProjectedValue, StorageSpace,
        StorageSpaceId, StorageValue,
    };

    #[tokio::test]
    async fn engine_ignores_predecessor_state_bytes_and_leaves_them_untouched() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        let predecessor_spaces = [
            StorageSpace::new(StorageSpaceId(0x0001_0002), "untracked_state.row.v1"),
            StorageSpace::new(
                StorageSpaceId(0x0004_0005),
                "live_state.index.branch_root.v1",
            ),
        ];
        for space in predecessor_spaces {
            writes.put(
                space,
                StorageKey(Bytes::from_static(b"malformed-legacy-key")),
                StorageValue {
                    bytes: Bytes::from_static(b"malformed-legacy-value"),
                },
            );
        }
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("legacy sidecar bytes should commit");

        let engine = Engine::new(storage)
            .await
            .expect("legacy sidecar bytes must not affect engine open");
        assert_eq!(
            engine
                .load_branch_head_commit_id(&receipt.main_branch_id)
                .await
                .expect("branch head should load"),
            Some(receipt.initial_commit_id)
        );
        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("legacy verification read should open");
        for space in predecessor_spaces {
            let value = PointReadPlan::new(
                space,
                &[StorageKey(Bytes::from_static(b"malformed-legacy-key"))],
            )
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("legacy bytes should remain readable")
            .value
            .into_iter()
            .next()
            .flatten();
            assert_eq!(
                value,
                Some(StorageProjectedValue::FullValue(Bytes::from_static(
                    b"malformed-legacy-value"
                )))
            );
        }
    }

    #[tokio::test]
    async fn predecessor_only_repository_is_uninitialized_and_untouched() {
        let storage = Memory::new();
        let storage_adapter = StorageAdapter::new(storage.clone());
        let predecessor_space = StorageSpace::new(
            StorageSpaceId(0x0004_0005),
            "live_state.index.branch_root.v1",
        );
        let predecessor_key = StorageKey(Bytes::from_static(b"legacy-current-root"));
        let predecessor_value = Bytes::from_static(b"legacy-root-bytes");
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            predecessor_space,
            predecessor_key.clone(),
            StorageValue {
                bytes: predecessor_value.clone(),
            },
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("predecessor bytes should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("predecessor-only repository must not open");
        };
        assert_eq!(error.code, "LIX_ERROR_NOT_INITIALIZED");

        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("verification read should open");
        let value = PointReadPlan::new(predecessor_space, &[predecessor_key])
            .materialize(&read, StorageGetOptions::default())
            .await
            .expect("predecessor bytes should remain readable")
            .value
            .into_iter()
            .next()
            .flatten();
        assert_eq!(
            value,
            Some(StorageProjectedValue::FullValue(predecessor_value))
        );
    }
}
