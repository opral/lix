use std::sync::Arc;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{BranchContext, BranchRefReader};
use crate::catalog::{CatalogContext, CatalogFingerprint};
use crate::changelog::COMMIT_SPACE;
use crate::commit_graph::CommitGraphContext;
use crate::entity_pk::EntityPk;
use crate::init::InitReceipt;
use crate::live_state::LiveStateContext;
use crate::live_state::LiveStateRowRequest;
use crate::observe_coordinator::ObserveCoordinator;
use crate::observe_invalidation::ObserveInvalidation;
use crate::plugin::{
    DEFAULT_MAX_LIVE_PLUGIN_STORES, DEFAULT_PLUGIN_MEMORY_BYTES, PluginRuntimeHost,
};
use crate::session::SessionContext;
use crate::sql2::SqlPlanningCache;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    ScanPlan, SharedStorageAdapterRead, StorageCoreProjection, StoragePrefix, StorageReadOptions,
    StorageScanOptions, StorageWriteOptions,
};
use crate::storage_adapter::{StorageAdapter, StorageWriteSet};
use crate::telemetry::TelemetrySink;
use crate::tracked_state::TrackedStateContext;
use crate::transaction::CommitCoordinator;
use crate::wasm::WasmTransitionCounters;
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
    sql_planning_cache: Arc<SqlPlanningCache<CatalogFingerprint>>,
    deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
    collaboration_write_gate: Arc<tokio::sync::Mutex<()>>,
    commit_coordinator: Arc<CommitCoordinator>,
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

        crate::init::initialize(storage, &TrackedStateContext::new()).await
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

        let tracked_state = Arc::new(TrackedStateContext::new());
        let commit_graph = CommitGraphContext::new();
        let live_state = Arc::new(LiveStateContext::new(
            tracked_state.as_ref().clone(),
            commit_graph,
        ));
        let branch_ctx = Arc::new(BranchContext::new());
        assert_initialized(storage.clone(), live_state.as_ref()).await?;

        // SessionContext::execute later projects these stable state contexts into one
        // execution-scoped SQL context, optionally wrapped by a transaction
        // overlay for writes.

        let collaboration_write_gate = Arc::new(tokio::sync::Mutex::new(()));
        let commit_coordinator = Arc::new(CommitCoordinator::new(Arc::clone(
            &collaboration_write_gate,
        )));
        Ok(Self {
            binary_cas: Arc::new(BinaryCasContext::new()),
            storage,
            tracked_state,
            live_state,
            branch_ctx,
            catalog_context: Arc::new(CatalogContext::new()),
            sql_planning_cache: Arc::new(SqlPlanningCache::default()),
            deterministic_runtime_gate: Arc::new(tokio::sync::Mutex::new(())),
            collaboration_write_gate,
            commit_coordinator,
            observe_coordinator: Arc::new(ObserveCoordinator::new()),
            observe_invalidation: Arc::new(ObserveInvalidation::new()),
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
        SessionContext::open_workspace(
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.branch_ctx),
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
        // A healthy rebuild is content-equivalent, but this API also repairs a
        // stale or damaged serving root. Conservatively invalidate transaction
        // opening catalogs so repaired registered-schema facts are never hidden
        // behind a pre-rebuild cache entry.
        crate::catalog::stage_catalog_revision(&mut writes);
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
    match crate::init::repository_protocol_status(&read).await? {
        // The protocol check must precede the live-state read: tracked-head
        // spaces keep their physical IDs across hard layout cuts, so an old
        // group value could otherwise be decoded before we reject it.
        crate::init::RepositoryProtocolStatus::Current => {
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
                Ok(())
            } else {
                Err(not_initialized_error())
            }
        }
        crate::init::RepositoryProtocolStatus::Unsupported => {
            Err(crate::init::unsupported_repository_protocol_error())
        }
        crate::init::RepositoryProtocolStatus::Missing => {
            // A raw changelog key is the initialization sentinel. Unlike a
            // live-state lookup it cannot parse an old tracked-head layout.
            if repository_has_changelog_commit(&read).await? {
                Err(crate::init::unsupported_repository_protocol_error())
            } else {
                Err(not_initialized_error())
            }
        }
    }
}

async fn repository_has_changelog_commit(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
) -> Result<bool, LixError> {
    Ok(!ScanPlan::prefix(
        COMMIT_SPACE,
        StoragePrefix {
            bytes: bytes::Bytes::new(),
        },
    )
    .collect(
        read,
        StorageScanOptions {
            projection: StorageCoreProjection::KeyOnly,
            limit_rows: 1,
            ..StorageScanOptions::default()
        },
    )
    .await?
    .value
    .entries
    .is_empty())
}

fn not_initialized_error() -> LixError {
    LixError::new(
        "LIX_ERROR_NOT_INITIALIZED",
        "engine storage is not initialized; call Engine::initialize(...) before Engine::new(...)",
    )
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use serde_json::json;

    use super::*;
    use crate::storage_adapter::{
        Memory, PointReadPlan, ScanPlan, StorageGetOptions, StorageKey, StoragePrefix,
        StorageProjectedValue, StorageScanOptions, StorageSpace, StorageSpaceId, StorageValue,
    };

    async fn register_json_pointer_schema_in_scope(session: &SessionContext<Memory>, global: bool) {
        let schema = json!({
            "x-lix-key": "json_pointer",
            "x-lix-primary-key": ["/path"],
            "type": "object",
            "required": ["path", "value"],
            "properties": {
                "path": { "type": "string" },
                "value": {
                    "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
                }
            },
            "additionalProperties": false
        });
        assert_eq!(
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), $2, false)",
                    &[
                        crate::Value::Text(schema.to_string()),
                        crate::Value::Boolean(global),
                    ],
                )
                .await
                .expect("register json_pointer schema")
                .rows_affected(),
            1
        );
    }

    async fn register_json_pointer_schema(session: &SessionContext<Memory>) {
        register_json_pointer_schema_in_scope(session, false).await;
    }

    async fn register_global_json_pointer_schema(session: &SessionContext<Memory>) {
        register_json_pointer_schema_in_scope(session, true).await;
    }

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
            .expect("predecessor bytes should commit");

        let engine = Engine::new(storage)
            .await
            .expect("predecessor bytes must not affect engine open");
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

    #[tokio::test]
    async fn initialized_repository_without_protocol_gate_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.delete(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("protocol marker deletion should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("initialized pre-protocol storage must fail closed");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v15_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"live-state.hot.v15"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("legacy protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("v15 repository must fail closed");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_null_file_descriptor_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"clustered-packed-history.v20"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("pre-file-ownership protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("repositories with null-scoped file descriptors must fail closed");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v21_hot_row_order_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"file-descriptor-ownership.v21"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V21 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V21 repositories must fail closed before hot rows are decoded");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v22_file_projection_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"file-first-hot-state.v22"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V22 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V22 repositories must fail closed before file markers are read");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v23_commit_delta_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"schema-file-membership.v23"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V23 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V23 repositories must fail closed before LXCD5 values are decoded");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v24_hot_inline_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"commit-delta-sidecar-zstd.v24"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V24 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V24 repositories must fail closed before HOT rows are decoded");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v25_commit_delta_payload_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"hot-inline-fingerprint.v25"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V25 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V25 repositories must fail closed before LXCD6 payloads are decoded");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v26_selected_payload_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"selected-payload-reference.v26"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V26 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V26 repositories must fail closed before packed bases are read");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v27_partial_current_state_protocols_are_rejected() {
        for protocol in [
            b"packed-current-base.v27".as_slice(),
            b"checkpoint-owned-hot-baseline.v27".as_slice(),
        ] {
            let storage = Memory::new();
            Engine::initialize(storage.clone())
                .await
                .expect("engine should initialize");
            let storage_adapter = StorageAdapter::new(storage.clone());
            let mut writes = storage_adapter.new_write_set();
            writes.put(
                crate::init::REPOSITORY_PROTOCOL_SPACE,
                crate::init::REPOSITORY_PROTOCOL_KEY,
                protocol,
            );
            storage_adapter
                .commit_write_set(writes, StorageWriteOptions::default())
                .await
                .expect("V27 protocol marker should commit");

            let Err(error) = Engine::new(storage).await else {
                panic!("V27 partial protocols must fail closed before HOT state is decoded");
            };
            assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
        }
    }

    #[tokio::test]
    async fn predecessor_v33_commit_delta_coordinates_are_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"checkpoint-source-delta.v33"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V33 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V33 repositories must fail closed before LXCD7 locators are decoded");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn tracked_entity_fast_path_serves_broad_sql_rows() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;
        assert_eq!(
            session
                .execute(
                    "INSERT INTO json_pointer (path, value) VALUES ('/a', lix_json('{\"n\":1}')), ('/b', lix_json('{\"n\":2}')), ('/c', lix_json('{\"n\":3}'))",
                    &[],
                )
                .await
                .expect("write tracked rows")
                .rows_affected(),
            3
        );

        let rows = session
            .execute(
                "SELECT path, value FROM json_pointer ORDER BY path LIMIT 2",
                &[],
            )
            .await
            .expect("broad tracked SQL read should execute");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("tracked row path"))
                .collect::<Vec<_>>(),
            ["/a", "/b"]
        );
        assert_eq!(
            rows.rows()[1]
                .get::<serde_json::Value>("value")
                .expect("tracked row value"),
            json!({"n": 2})
        );
    }

    #[tokio::test]
    async fn tracked_entity_public_fast_path_preserves_canonical_primary_key_order() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;

        // These values exercise the order-preserving tracked-head codec's
        // edge cases: empty strings, embedded NULs, control bytes, and UTF-8.
        // The no-LIMIT single-PK query below is the native public-result
        // shape; the two-key ORDER BY forces the ordinary SQL executor and is
        // the semantic control.
        let paths = ["", "\0", "a", "a\0", "a\u{1}", "z", "é"];
        for (index, path) in paths.iter().enumerate() {
            assert_eq!(
                session
                    .execute(
                        "INSERT INTO json_pointer (path, value) VALUES ($1, lix_json($2))",
                        &[
                            crate::Value::Text((*path).to_string()),
                            crate::Value::Text(json!({"index": index}).to_string()),
                        ],
                    )
                    .await
                    .expect("write ordered tracked row")
                    .rows_affected(),
                1
            );
        }

        let native_shape = session
            .execute("SELECT path, value FROM json_pointer ORDER BY path", &[])
            .await
            .expect("native public tracked read should execute");
        let generic_control = session
            .execute(
                "SELECT path, value FROM json_pointer ORDER BY path, path",
                &[],
            )
            .await
            .expect("generic ordering control should execute");
        let native_values = native_shape
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        let generic_values = generic_control
            .rows()
            .iter()
            .map(|row| row.values().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            native_values, generic_values,
            "the direct result must retain the normal SQL order and values"
        );
        assert_eq!(
            native_shape
                .rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("tracked row path"))
                .collect::<Vec<_>>(),
            paths,
            "the raw tracked-head scan is ordered by the visible string PK"
        );
    }

    #[tokio::test]
    async fn tracked_entity_public_fast_path_falls_back_for_staged_transaction_rows() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) VALUES ('/committed', lix_json('{\"source\":\"tracked\"}'))",
                &[],
            )
            .await
            .expect("write committed tracked row");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .execute(
                "INSERT INTO json_pointer (path, value) VALUES ('/staged', lix_json('{\"source\":\"staged\"}'))",
                &[],
            )
            .await
            .expect("stage tracked row");
        let rows = transaction
            .execute("SELECT path, value FROM json_pointer ORDER BY path", &[])
            .await
            .expect("transaction read must retain its staged overlay");
        assert_eq!(
            rows.rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("row path"))
                .collect::<Vec<_>>(),
            ["/committed", "/staged"],
            "transaction contexts have no raw snapshot capability and must use the generic overlay"
        );
        transaction
            .rollback()
            .await
            .expect("transaction rollback should succeed");
    }

    #[tokio::test]
    async fn current_state_group_serves_mixed_tracked_and_untracked_entity_rows() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/tracked', lix_json('{\"source\":\"tracked\"}'))",
                &[],
            )
            .await
            .expect("write tracked entity row");
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/untracked', lix_json('{\"source\":\"untracked\"}'), true)",
                &[],
            )
            .await
            .expect("write untracked entity row");

        let rows = session
            .execute("SELECT path, value FROM json_pointer ORDER BY path", &[])
            .await
            .expect("mixed tracked/untracked read should execute");
        assert_eq!(
            rows.rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("untracked path"))
                .collect::<Vec<_>>(),
            ["/tracked", "/untracked"],
            "one current-state group must serve both retentions without a separate merge"
        );

        let error = session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/tracked', lix_json('{\"source\":\"collision\"}'), true)",
                &[],
            )
            .await
            .expect_err("an untracked insert must not shadow a tracked identity");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let error = session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/untracked', lix_json('{\"source\":\"collision\"}'))",
                &[],
            )
            .await
            .expect_err("a tracked insert must not shadow an untracked identity");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        assert_eq!(
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                     VALUES ('/tracked', lix_json('{\"source\":\"tracked-upsert\"}'), true) \
                     ON CONFLICT (path) DO UPDATE SET value = excluded.value",
                    &[],
                )
                .await
                .expect("upsert should update the existing tracked row")
                .rows_affected(),
            1
        );
        assert_eq!(
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                     VALUES ('/untracked', lix_json('{\"source\":\"untracked-upsert\"}'), false) \
                     ON CONFLICT (path) DO UPDATE SET value = excluded.value",
                    &[],
                )
                .await
                .expect("upsert should update the existing untracked row")
                .rows_affected(),
            1
        );

        let rows = session
            .execute(
                "SELECT path, value, lixcol_untracked FROM json_pointer ORDER BY path",
                &[],
            )
            .await
            .expect("updated mixed-retention rows should remain readable");
        let tracked = rows
            .rows()
            .iter()
            .find(|row| {
                row.get::<String>("path")
                    .is_ok_and(|path| path == "/tracked")
            })
            .expect("tracked row should remain visible");
        assert!(
            !tracked
                .get::<bool>("lixcol_untracked")
                .expect("tracked retention should be visible")
        );
        assert_eq!(
            tracked
                .get::<serde_json::Value>("value")
                .expect("tracked value should be visible"),
            json!({"source": "tracked-upsert"})
        );
        let untracked = rows
            .rows()
            .iter()
            .find(|row| {
                row.get::<String>("path")
                    .is_ok_and(|path| path == "/untracked")
            })
            .expect("untracked row should remain visible");
        assert!(
            untracked
                .get::<bool>("lixcol_untracked")
                .expect("untracked retention should be visible")
        );
        assert_eq!(
            untracked
                .get::<serde_json::Value>("value")
                .expect("untracked value should be visible"),
            json!({"source": "untracked-upsert"})
        );
    }

    #[tokio::test]
    async fn untracked_public_entity_write_is_history_free_and_diff_invisible() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;

        let head_before = engine
            .load_branch_head_commit_id(&receipt.main_branch_id)
            .await
            .expect("main branch head should load before untracked write");
        let changes_before = session
            .execute("SELECT COUNT(*) AS changes FROM lix_change", &[])
            .await
            .expect("changelog count before untracked write should execute")
            .rows()[0]
            .get::<i64>("changes")
            .expect("changelog count should be numeric");
        let diff_before = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_working_diff \
                 WHERE schema_key = 'json_pointer'",
                &[],
            )
            .await
            .expect("working diff before untracked write should execute")
            .rows()[0]
            .get::<i64>("entries")
            .expect("working diff count should be numeric");
        assert_eq!(
            diff_before, 0,
            "the schema registration itself must not create a json_pointer diff"
        );

        assert_eq!(
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                     VALUES ('/history-free', lix_json('{\"source\":\"untracked\"}'), true)",
                    &[],
                )
                .await
                .expect("untracked public write should execute")
                .rows_affected(),
            1
        );

        let head_after = engine
            .load_branch_head_commit_id(&receipt.main_branch_id)
            .await
            .expect("main branch head should load after untracked write");
        let changes_after = session
            .execute("SELECT COUNT(*) AS changes FROM lix_change", &[])
            .await
            .expect("changelog count after untracked write should execute")
            .rows()[0]
            .get::<i64>("changes")
            .expect("changelog count should be numeric");
        let diff_after = session
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_working_diff \
                 WHERE schema_key = 'json_pointer'",
                &[],
            )
            .await
            .expect("working diff after untracked write should execute")
            .rows()[0]
            .get::<i64>("entries")
            .expect("working diff count should be numeric");

        assert_eq!(
            head_after, head_before,
            "an untracked-only write must not publish a new branch commit"
        );
        assert_eq!(
            changes_after, changes_before,
            "an untracked-only write must not append a changelog change"
        );
        assert_eq!(
            diff_after, diff_before,
            "untracked state must not participate in tracked working diffs"
        );
    }

    #[tokio::test]
    async fn untracked_state_survives_checkpoint_and_next_tracked_write() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/checkpointed', lix_json('{\"source\":\"tracked\"}'))",
                &[],
            )
            .await
            .expect("tracked row should commit before checkpoint");

        session
            .create_checkpoint()
            .await
            .expect("checkpoint should publish a complete hot state");

        // A checkpoint publishes a full hot state immediately. Reads do not
        // replay tracked history and then overlay retained untracked rows.
        let checkpointed_row = session
            .execute(
                "SELECT value FROM json_pointer WHERE path = '/checkpointed'",
                &[],
            )
            .await
            .expect("checkpointed tracked row should read from the hot state");
        assert_eq!(
            checkpointed_row.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("checkpointed value should decode"),
            json!({"source": "tracked"})
        );

        // A checkpointed branch remains writable. This history-free mutation
        // updates the same complete hot generation without advancing history.
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/workspace', lix_json('{\"source\":\"untracked\"}'), true)",
                &[],
            )
            .await
            .expect("untracked row should write against the complete hot state");
        let workspace_row = session
            .execute(
                "SELECT value FROM json_pointer WHERE path = '/workspace'",
                &[],
            )
            .await
            .expect("untracked row should read from the complete hot state");
        assert_eq!(
            workspace_row.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("workspace value should decode"),
            json!({"source": "untracked"})
        );

        // The next tracked child carries the history-free member forward.
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/after-checkpoint', lix_json('{\"source\":\"tracked\"}'))",
                &[],
            )
            .await
            .expect("tracked child should publish the next complete hot state");
        let rows = session
            .execute("SELECT path FROM json_pointer ORDER BY path", &[])
            .await
            .expect("rematerialized current state should read");
        assert_eq!(
            rows.rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("row path"))
                .collect::<Vec<_>>(),
            ["/after-checkpoint", "/checkpointed", "/workspace"]
        );
    }

    #[tokio::test]
    async fn checkpoint_reclaims_the_superseded_physical_working_diff_epoch() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/dirty', lix_json('{\"value\":\"before-checkpoint\"}'))",
                &[],
            )
            .await
            .expect("tracked row should create a working diff");

        let adapter = StorageAdapter::new(storage.clone());
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("working-diff inventory read should open");
        let before_sparse = ScanPlan::prefix(
            crate::live_state::HOT_DIFF_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        )
        .collect(&read, StorageScanOptions::default())
        .await
        .expect("working-diff inventory should scan");
        let before_packed = ScanPlan::prefix(
            crate::live_state::PACKED_CURRENT_BASE_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        )
        .collect(&read, StorageScanOptions::default())
        .await
        .expect("packed working-diff inventory should scan");
        assert!(
            !before_sparse.value.entries.is_empty() || !before_packed.value.entries.is_empty(),
            "tracked mutation must persist a sparse or packed physical dirty epoch"
        );
        drop(read);

        session
            .create_checkpoint()
            .await
            .expect("checkpoint should publish a clean working state");

        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("post-checkpoint inventory read should open");
        let after = ScanPlan::prefix(
            crate::live_state::HOT_DIFF_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        )
        .collect(&read, StorageScanOptions::default())
        .await
        .expect("post-checkpoint working-diff inventory should scan");
        assert!(
            after.value.entries.is_empty(),
            "superseded sparse dirty keys must not remain until repository GC"
        );
        let logical = session
            .execute("SELECT COUNT(*) AS entries FROM lix_working_diff", &[])
            .await
            .expect("post-checkpoint logical diff should execute");
        assert_eq!(
            logical.rows()[0]
                .get::<i64>("entries")
                .expect("working-diff count should be numeric"),
            0
        );
    }

    #[tokio::test]
    async fn tracked_entity_public_fast_path_falls_back_for_global_tracked_rows() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let global_session = engine
            .open_session(GLOBAL_BRANCH_ID)
            .await
            .expect("global session should open");
        register_global_json_pointer_schema(&global_session).await;
        global_session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_global, lixcol_untracked) \
                 VALUES ('/global', lix_json('{\"source\":\"global\"}'), true, false)",
                &[],
            )
            .await
            .expect("write global tracked entity row");

        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");
        register_json_pointer_schema(&session).await;
        let rows = session
            .execute("SELECT path, value FROM json_pointer ORDER BY path", &[])
            .await
            .expect("global-overlaid tracked read should execute");
        assert_eq!(
            rows.rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("global path"))
                .collect::<Vec<_>>(),
            ["/global"],
            "a global tracked overlay must retain the general visibility resolver"
        );
    }

    #[tokio::test]
    async fn tracked_entity_fast_path_keeps_synthesized_branch_refs_generic() {
        let storage = Memory::new();
        let receipt = Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let session = engine
            .open_workspace_session()
            .await
            .expect("workspace session should open");

        let rows = session
            .execute("SELECT id, commit_id FROM lix_branch_ref ORDER BY id", &[])
            .await
            .expect("synthesized branch refs should read through the generic path");
        assert!(
            rows.rows().iter().any(|row| {
                row.get::<String>("id")
                    .is_ok_and(|id| id == receipt.main_branch_id)
                    && row
                        .get::<String>("commit_id")
                        .is_ok_and(|commit_id| commit_id == receipt.initial_commit_id)
            }),
            "the normal branch-ref SQL surface must retain its synthesized control row"
        );
    }

    #[tokio::test]
    async fn predecessor_protocol_is_rejected_before_old_head_bytes_are_decoded() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read initialized hot rows");
        let hot_rows = ScanPlan::prefix(
            crate::live_state::HOT_ROW_SPACE,
            StoragePrefix {
                bytes: Bytes::new(),
            },
        )
        .collect(&read, StorageScanOptions::default())
        .await
        .expect("scan initialized hot rows")
        .value
        .entries;
        assert!(
            !hot_rows.is_empty(),
            "initialized repository must have hot rows"
        );

        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"tracked-direct-plane.v8"[..],
        );
        for hot_row in hot_rows {
            writes.put(
                crate::live_state::HOT_ROW_SPACE,
                hot_row.key,
                StorageValue {
                    bytes: Bytes::from_static(b"predecessor-head-bytes"),
                },
            );
        }
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("predecessor bytes should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("predecessor protocol must fail before head visibility reads");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn initialize_refuses_to_overwrite_existing_repository() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("first initialization should succeed");

        let Err(error) = Engine::initialize(storage).await else {
            panic!("initialization must not overwrite an existing repository");
        };
        assert_eq!(error.code, "LIX_ERROR_ALREADY_INITIALIZED");
    }

    #[tokio::test]
    async fn initialize_refuses_to_overwrite_a_predecessor_protocol() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("first initialization should succeed");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"tracked-direct-plane.v8"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("write predecessor protocol marker");

        let Err(error) = Engine::initialize(storage).await else {
            panic!("initialization must not overwrite a predecessor protocol");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }
}
