use std::sync::Arc;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{BranchContext, BranchRefReader};
use crate::catalog::{CatalogContext, CatalogFingerprint};
use crate::changelog::COMMIT_SPACE;
use crate::commit_graph::CommitGraphContext;
use crate::entity_pk::EntityPk;
use crate::hot_state::HotStateContext;
use crate::hot_state::HotStateRowRequest;
use crate::init::InitReceipt;
use crate::observe_coordinator::ObserveCoordinator;
use crate::observe_invalidation::ObserveInvalidation;
use crate::plugin::runtime::{
    DEFAULT_MAX_LIVE_PLUGIN_STORES, DEFAULT_PLUGIN_MEMORY_BYTES, PluginRuntimeHost,
};
use crate::session::SessionContext;
use crate::sql2::SqlPlanningCache;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageBeginScanOptions, StorageCoreProjection, StoragePrefix,
    StorageReadOptions, StorageWriteOptions,
};
use crate::storage_adapter::{StorageAdapter, StorageWriteSet};
use crate::telemetry::TelemetrySink;
use crate::tracked_state::TrackedStateContext;
use crate::transaction::CommitCoordinator;
use crate::plugin::runtime::WasmTransitionCounters;
use crate::plugin::runtime::{UnsupportedWasmRuntime, WasmRuntime};
use crate::{LixError, NullableKeyFilter};

#[derive(Clone)]
pub(crate) struct Engine<StorageImpl: Storage + 'static = crate::storage_adapter::Memory> {
    storage: StorageAdapter<StorageImpl>,
    tracked_state: Arc<TrackedStateContext>,
    hot_state: Arc<HotStateContext>,
    branch_ctx: Arc<BranchContext>,
    binary_cas: Arc<BinaryCasContext>,
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

pub(crate) struct EngineOptions {
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
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn with_wasm_runtime(mut self, wasm_runtime: Arc<dyn WasmRuntime>) -> Self {
        self.wasm_runtime = Some(wasm_runtime);
        self
    }

    pub(crate) fn with_telemetry(mut self, telemetry: Arc<dyn TelemetrySink>) -> Self {
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
    /// repository-wide budget. Completed publications may retire their Stores
    /// under pressure and cold-open again after commit.
    pub(crate) fn with_plugin_resource_limits(
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
    pub(crate) async fn initialize(storage: StorageImpl) -> Result<InitReceipt, LixError> {
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
    pub(crate) async fn new(storage: StorageImpl) -> Result<Self, LixError> {
        Self::new_with_options(storage, EngineOptions::new()).await
    }

    /// Creates an engine with a WASM component runtime for installed plugins.
    #[allow(dead_code)]
    pub(crate) async fn new_with_wasm_runtime(
        storage: StorageImpl,
        wasm_runtime: Arc<dyn WasmRuntime>,
    ) -> Result<Self, LixError> {
        Self::new_with_options(
            storage,
            EngineOptions::new().with_wasm_runtime(wasm_runtime),
        )
        .await
    }

    pub(crate) async fn new_with_options(
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
        let hot_state = Arc::new(HotStateContext::new(
            tracked_state.as_ref().clone(),
            commit_graph,
        ));
        let branch_ctx = Arc::new(BranchContext::new());
        assert_initialized(storage.clone(), hot_state.as_ref()).await?;

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
            binary_cas: Arc::new(BinaryCasContext::new()),
            storage,
            tracked_state,
            hot_state,
            branch_ctx,
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
    /// untracked `lix_branch_ref` rows in hot_state.
    pub(crate) async fn load_branch_head_commit_id(
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

    /// Point-reads one global, untracked `lix_key_value` row by physical key.
    ///
    /// Client-state rows are ordinary global untracked KV entities, so a single
    /// preference lookup does not need a SQL context. `open_lix` runs before any
    /// statement in the process, where routing through `execute` would pay
    /// catalog construction and DataFusion planning for one key.
    pub(crate) async fn load_untracked_global_key_value(
        &self,
        physical_key: &str,
    ) -> Result<Option<serde_json::Value>, LixError> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await?,
        );
        let Some(row) = self
            .hot_state
            .reader(read)
            .load_row(&HotStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(physical_key),
                file_id: NullableKeyFilter::Null,
            })
            .await?
        else {
            return Ok(None);
        };
        // Client state is only ever written untracked; a tracked row under the
        // same key is not client state and must not answer this read.
        if row.deleted || !row.untracked {
            return Ok(None);
        }
        let Some(snapshot_content) = row.snapshot_content.as_deref() else {
            return Ok(None);
        };
        let snapshot: serde_json::Value =
            serde_json::from_str(snapshot_content).map_err(|error| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("invalid lix_key_value snapshot JSON: {error}"),
                )
            })?;
        Ok(snapshot.get("value").cloned())
    }

    pub(crate) async fn open_session_at(
        &self,
        active_branch_id: impl Into<String>,
    ) -> Result<SessionContext<StorageImpl>, LixError> {
        self.open_session_at_with_account(active_branch_id, crate::ANONYMOUS_ACCOUNT_ID)
            .await
    }

    pub(crate) async fn open_session_at_with_account(
        &self,
        active_branch_id: impl Into<String>,
        active_account_id: impl Into<String>,
    ) -> Result<SessionContext<StorageImpl>, LixError> {
        let active_account_id = active_account_id.into();
        self.validate_active_account(&active_account_id).await?;
        SessionContext::open_at(
            active_branch_id.into(),
            active_account_id,
            self.storage(),
            Arc::clone(&self.hot_state),
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

    pub(crate) async fn open_session(&self) -> Result<SessionContext<StorageImpl>, LixError> {
        self.open_session_with_account(crate::ANONYMOUS_ACCOUNT_ID)
            .await
    }

    pub(crate) async fn open_session_with_account(
        &self,
        active_account_id: impl Into<String>,
    ) -> Result<SessionContext<StorageImpl>, LixError> {
        let active_account_id = active_account_id.into();
        self.validate_active_account(&active_account_id).await?;
        SessionContext::open_default(
            active_account_id,
            self.storage(),
            Arc::clone(&self.hot_state),
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
    #[allow(dead_code)]
    pub(crate) fn plugin_transition_counters(&self) -> WasmTransitionCounters {
        self.plugin_host.transition_counters()
    }

    /// Resets the process-local v2 transition aggregate used by profiling and
    /// invariant tests. This does not mutate durable repository state.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub(crate) fn reset_plugin_transition_counters(&self) {
        self.plugin_host.reset_transition_counters();
    }

    /// Rebuilds the tracked serving commit root for one branch from changelog.
    ///
    /// This is intentionally an engine-level operation: callers should not need
    /// to know which KV namespaces back changelog, commit graph, or tracked
    /// state. The current branch head is read from the live-state facade so
    /// rebuild uses the same moving-ref visibility as normal execution. Rooted
    /// heads restore content-addressed chunks only after their immutable root
    /// metadata passes a full changelog coverage audit. Rootless heads receive
    /// the same audit transiently and remain bounded-replay layouts.
    pub(crate) async fn rebuild_tracked_state_for_branch(
        &self,
        branch_id: &str,
    ) -> Result<(), LixError> {
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
        let typed_head_commit_id = crate::changelog::CommitId::parse_lix(
            &head_commit_id,
            "tracked-state branch rebuild authority",
        )?;
        let manifest = crate::tracked_state::load_commit_state_manifest(
            &read,
            typed_head_commit_id,
        )
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "cannot rebuild tracked_state root for commit '{head_commit_id}' without its commit-state manifest"
                ),
            )
        })?;
        let mut writes = StorageWriteSet::new();
        let rebuild_result = self
            .tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at(&head_commit_id)
            .await;
        rebuild_result?;
        if manifest.snapshot_root.is_none() {
            // Rootless heads are audited transiently and remain replay-only;
            // there is no serving-root publication or cache state to commit.
            return Ok(());
        }
        // A healthy rebuild is content-equivalent, but this API also repairs
        // missing or damaged serving chunks. Conservatively invalidate
        // transaction opening catalogs so restored registered-schema facts are
        // never hidden behind a pre-rebuild cache entry.
        crate::catalog::stage_catalog_revision(&mut writes);
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .map(|_| ())
            .map_err(LixError::from)
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
        let row = self
            .hot_state
            .reader(read)
            .load_row(&HotStateRowRequest {
                schema_key: "lix_account".to_string(),
                branch_id: GLOBAL_BRANCH_ID.to_string(),
                entity_pk: account_pk,
                file_id: NullableKeyFilter::Null,
            })
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ACCOUNT_NOT_FOUND",
                    format!("active account '{account_id}' does not exist"),
                )
            })?;
        let snapshot = row.snapshot_content.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("account '{account_id}' has no snapshot"),
            )
        })?;
        let value: serde_json::Value = serde_json::from_str(&snapshot).map_err(|error| {
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
    hot_state: &HotStateContext,
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
            let reader = hot_state.reader(read);
            let initialized = reader
                .load_row(&HotStateRowRequest {
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
    let range = StoragePrefix {
        bytes: bytes::Bytes::new(),
    }
    .to_range()?;
    let mut cursor = read
        .begin_scan(
            COMMIT_SPACE,
            range,
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..StorageBeginScanOptions::default()
            },
        )
        .await?;
    Ok(!cursor.next_page(1).await?.is_empty())
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
        Memory, PointReadPlan, StorageBeginScanOptions, StorageGetOptions, StorageKey,
        StoragePrefix, StorageProjectedValue, StorageSpace, StorageSpaceId, StorageValue,
    };

    async fn scan_test_space(
        read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
        space: StorageSpace,
    ) -> Vec<crate::storage_adapter::StorageReadEntry> {
        let range = StoragePrefix {
            bytes: Bytes::new(),
        }
        .to_range()
        .expect("valid empty prefix");
        let mut cursor = read
            .begin_scan(space, range, StorageBeginScanOptions::default())
            .await
            .expect("begin test scan");
        cursor.collect_all().await.expect("read test scan page")
    }

    async fn register_json_pointer_schema_in_scope(session: &SessionContext<Memory>, global: bool) {
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "json_pointer",
            "columns": [
                { "name": "path", "type": "text", "nullable": false },
                { "name": "value", "type": "jsonb", "nullable": false },
            ],
            "primary_key": ["path"],
        });
        assert_eq!(
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (CAST($1 AS JSONB), $2, false)",
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
            StorageSpace::mutable(StorageSpaceId(0x0001_0002), "untracked_state.row.v1"),
            StorageSpace::mutable(
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
        let predecessor_space = StorageSpace::mutable(
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
    async fn predecessor_v61_checkpoint_marker_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"immutable-physical-commit-state.v61"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V61 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V61 checkpoint-marker repositories must fail closed");
        };
        assert_eq!(error.code, "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT");
    }

    #[tokio::test]
    async fn predecessor_v64_direct_change_id_leaf_protocol_is_rejected() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let storage_adapter = StorageAdapter::new(storage.clone());
        let mut writes = storage_adapter.new_write_set();
        writes.put(
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            crate::init::REPOSITORY_PROTOCOL_KEY,
            &b"myers-first-parent-jump.v64"[..],
        );
        storage_adapter
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("V64 protocol marker should commit");

        let Err(error) = Engine::new(storage).await else {
            panic!("V64 packed-history repositories must fail closed");
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
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;
        assert_eq!(
            session
                .execute(
                    "INSERT INTO json_pointer (path, value) VALUES ('/a', CAST('{\"n\":1}' AS JSONB)), ('/b', CAST('{\"n\":2}' AS JSONB)), ('/c', CAST('{\"n\":3}' AS JSONB))",
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
    async fn tracked_entity_provider_preserves_canonical_primary_key_order() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage)
            .await
            .expect("initialized engine should open");
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;

        // These values exercise the order-preserving tracked-head codec's
        // edge cases: empty strings, embedded NULs, control bytes, and UTF-8.
        // Compare equivalent orderings to exercise provider projection and
        // DataFusion ordering over the tracked-head primary-key codec.
        let paths = ["", "a", "a\u{1}", "z", "é"];
        for (index, path) in paths.iter().enumerate() {
            assert_eq!(
                session
                    .execute(
                        "INSERT INTO json_pointer (path, value) VALUES ($1, CAST($2 AS JSONB))",
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

        let primary_order = session
            .execute("SELECT path, value FROM json_pointer ORDER BY path", &[])
            .await
            .expect("tracked read should execute");
        let generic_control = session
            .execute(
                "SELECT path, value FROM json_pointer ORDER BY path, path",
                &[],
            )
            .await
            .expect("generic ordering control should execute");
        let primary_values = primary_order
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
            primary_values, generic_values,
            "equivalent DataFusion orderings must retain the same values"
        );
        assert_eq!(
            primary_order
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
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) VALUES ('/committed', CAST('{\"source\":\"tracked\"}' AS JSONB))",
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
                "INSERT INTO json_pointer (path, value) VALUES ('/staged', CAST('{\"source\":\"staged\"}' AS JSONB))",
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
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/tracked', CAST('{\"source\":\"tracked\"}' AS JSONB))",
                &[],
            )
            .await
            .expect("write tracked entity row");
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/untracked', CAST('{\"source\":\"untracked\"}' AS JSONB), true)",
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
                 VALUES ('/tracked', CAST('{\"source\":\"collision\"}' AS JSONB), true)",
                &[],
            )
            .await
            .expect_err("an untracked insert must not shadow a tracked identity");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        let error = session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/untracked', CAST('{\"source\":\"collision\"}' AS JSONB))",
                &[],
            )
            .await
            .expect_err("a tracked insert must not shadow an untracked identity");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        assert_eq!(
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                     VALUES ('/tracked', CAST('{\"source\":\"tracked-upsert\"}' AS JSONB), true) \
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
                     VALUES ('/untracked', CAST('{\"source\":\"untracked-upsert\"}' AS JSONB), false) \
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
        let session = engine.open_session().await.expect("session should open");
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
                     VALUES ('/history-free', CAST('{\"source\":\"untracked\"}' AS JSONB), true)",
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

    /// The two working-diff read paths in `hot_working_diff_entries` must
    /// answer identically for every row state a branch can actually reach.
    ///
    /// The broad path enumerates the sparse `HOT_DIFF` index and then loads
    /// each dirty identity's primary row; the finite `schema_key + entity_pk`
    /// path skips that index entirely and reads the primary rows directly.
    /// They treat an untracked or absent primary row differently — the broad
    /// path fails closed to canonical replay, the finite path skips the row —
    /// so this walks every reachable shape (clean, modified, removed,
    /// added, added-then-removed, untracked, untracked-recycled-as-tracked,
    /// never-existed) and asserts the finite answer equals the broad answer
    /// restricted to that identity.
    #[tokio::test]
    async fn working_diff_finite_bypass_and_index_scan_agree_on_every_row_state() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("initialized engine should open");
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;

        for path in ["/clean", "/modified", "/removed", "/recycled-source"] {
            session
                .execute(
                    "INSERT INTO json_pointer (path, value) VALUES ($1, CAST('{\"v\":0}' AS JSONB))",
                    &[crate::Value::Text(path.to_string())],
                )
                .await
                .expect("pre-checkpoint tracked row should commit");
        }
        session
            .execute(
                "DELETE FROM json_pointer WHERE path = '/recycled-source'",
                &[],
            )
            .await
            .expect("pre-checkpoint delete should commit");
        session
            .create_checkpoint()
            .await
            .expect("checkpoint should publish a clean working state");

        session
            .execute(
                "UPDATE json_pointer SET value = CAST('{\"v\":1}' AS JSONB) WHERE path = '/modified'",
                &[],
            )
            .await
            .expect("modify should dirty the row");
        session
            .execute("DELETE FROM json_pointer WHERE path = '/removed'", &[])
            .await
            .expect("delete should dirty the row");
        session
            .execute(
                "INSERT INTO json_pointer (path, value) VALUES ('/added', CAST('{\"v\":1}' AS JSONB))",
                &[],
            )
            .await
            .expect("insert should dirty a new identity");
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/added-then-removed', CAST('{\"v\":1}' AS JSONB))",
                &[],
            )
            .await
            .expect("insert should dirty a new identity");
        session
            .execute(
                "DELETE FROM json_pointer WHERE path = '/added-then-removed'",
                &[],
            )
            .await
            .expect("delete of a post-checkpoint insert should commit");
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/untracked', CAST('{\"v\":1}' AS JSONB), true)",
                &[],
            )
            .await
            .expect("untracked insert should commit");

        // Physically removing a HOT primary row is only reachable through an
        // untracked delete (`CurrentStateDelta::physically_deletes` is
        // `untracked && deleted`). Exercise that, then re-insert the identity
        // as tracked so a dirty row exists whose HOT slot was previously
        // vacated.
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/recycled', CAST('{\"v\":1}' AS JSONB), true)",
                &[],
            )
            .await
            .expect("untracked insert should commit");
        session
            .execute("DELETE FROM json_pointer WHERE path = '/recycled'", &[])
            .await
            .expect("untracked delete should physically remove the hot row");
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/recycled', CAST('{\"v\":2}' AS JSONB))",
                &[],
            )
            .await
            .expect("tracked insert should reuse the vacated identity");

        // The invariant that makes the two paths equivalent: retention is
        // immutable while a physical member exists, so a dirty tracked row can
        // never become untracked or vanish under its own `HOT_DIFF` entry.
        let retention_flip = session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/modified', CAST('{\"v\":9}' AS JSONB), true)",
                &[],
            )
            .await;
        assert!(
            retention_flip.is_err(),
            "an untracked write must not take over a dirty tracked identity"
        );

        use std::sync::atomic::Ordering;
        let hits = &crate::hot_state::WORKING_DIFF_PATH_HITS;
        let index_before = hits.index_scan.load(Ordering::Relaxed);
        let broad = session
            .execute(
                "SELECT entity_pk, diff_type FROM lix_working_diff \
                 WHERE schema_key = 'json_pointer' ORDER BY entity_pk",
                &[],
            )
            .await
            .expect("broad working-diff read should execute");
        assert!(
            hits.index_scan.load(Ordering::Relaxed) > index_before,
            "the schema-only working-diff read must take the HOT_DIFF index path"
        );
        let broad_rows = broad
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<serde_json::Value>("entity_pk")
                        .expect("entity_pk should decode")
                        .to_string(),
                    row.get::<String>("diff_type")
                        .expect("diff_type should decode"),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            broad_rows,
            vec![
                ("[\"/added\"]".to_string(), "added".to_string()),
                ("[\"/modified\"]".to_string(), "modified".to_string()),
                ("[\"/recycled\"]".to_string(), "added".to_string()),
                ("[\"/removed\"]".to_string(), "removed".to_string()),
            ],
            "the index-driven working diff must classify every reachable shape"
        );

        for path in [
            "/clean",
            "/modified",
            "/removed",
            "/added",
            "/added-then-removed",
            "/untracked",
            "/recycled",
            "/recycled-source",
            "/never-existed",
        ] {
            let entity_pk = format!("[\"{path}\"]");
            let bypass_before = hits.finite_bypass.load(Ordering::Relaxed);
            let finite = session
                .execute(
                    "SELECT entity_pk, diff_type FROM lix_working_diff \
                     WHERE schema_key = 'json_pointer' AND entity_pk = CAST($1 AS JSONB) \
                     ORDER BY entity_pk",
                    &[crate::Value::Text(entity_pk.clone())],
                )
                .await
                .expect("finite working-diff read should execute");
            assert!(
                hits.finite_bypass.load(Ordering::Relaxed) > bypass_before,
                "the finite working-diff read for {path} must take the primary-row bypass"
            );
            let finite_rows = finite
                .rows()
                .iter()
                .map(|row| {
                    (
                        row.get::<serde_json::Value>("entity_pk")
                            .expect("entity_pk should decode")
                            .to_string(),
                        row.get::<String>("diff_type")
                            .expect("diff_type should decode"),
                    )
                })
                .collect::<Vec<_>>();
            let expected = broad_rows
                .iter()
                .filter(|(row_pk, _)| row_pk == &entity_pk)
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                finite_rows, expected,
                "the finite working-diff bypass disagrees with the index scan for {path}"
            );
        }
    }

    /// A file-scoped working-diff read skips the `HOT_DIFF` index and its
    /// scope-global coverage proof, enumerating primary `HOT_ROW` rows through
    /// the file-first prefix seek instead. That is only sound if the primary
    /// rows are the complete authority for every dirty identity in the file, so
    /// this asserts the file-scoped answer equals the unfiltered index-driven
    /// answer restricted to that file — across every reachable row state, and
    /// for both the bare `file_id` shape (whose schema domain is resolved from
    /// the `FILE_SPACE` markers) and the `file_id + schema_key` shape.
    #[tokio::test]
    async fn file_scoped_working_diff_bypass_matches_the_index_scan() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("initialized engine should open");
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;

        let files = [
            "66696c65-0000-8000-8000-000000000000",
            "66696c65-0001-8000-8000-000000000001",
        ];
        for (index, file) in files.iter().enumerate() {
            session
                .execute(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ($1, $2, CAST($3 AS BYTEA))",
                    &[
                        crate::Value::Text((*file).to_string()),
                        crate::Value::Text(format!("/f{index}.txt")),
                        crate::Value::Text("seed".to_string()),
                    ],
                )
                .await
                .expect("file should insert");
        }

        // Pre-checkpoint rows: one per reachable post-checkpoint fate, spread
        // over both files and the null-file bucket.
        for (path, file) in [
            ("/clean", Some(files[0])),
            ("/modified", Some(files[0])),
            ("/removed", Some(files[0])),
            ("/clean-b", Some(files[1])),
            ("/modified-b", Some(files[1])),
            ("/modified-none", None),
        ] {
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_file_id) \
                     VALUES ($1, CAST('{\"v\":0}' AS JSONB), $2)",
                    &[
                        crate::Value::Text(path.to_string()),
                        file.map_or(crate::Value::Null, |file| {
                            crate::Value::Text(file.to_string())
                        }),
                    ],
                )
                .await
                .expect("pre-checkpoint row should insert");
        }
        session
            .create_checkpoint()
            .await
            .expect("checkpoint should publish a clean working state");

        session
            .execute(
                "UPDATE json_pointer SET value = CAST('{\"v\":1}' AS JSONB) \
                 WHERE path IN ('/modified', '/modified-b', '/modified-none')",
                &[],
            )
            .await
            .expect("modify should dirty the rows");
        session
            .execute("DELETE FROM json_pointer WHERE path = '/removed'", &[])
            .await
            .expect("delete should dirty the row");
        for (path, file) in [("/added", Some(files[0])), ("/added-b", Some(files[1]))] {
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_file_id) \
                     VALUES ($1, CAST('{\"v\":1}' AS JSONB), $2)",
                    &[
                        crate::Value::Text(path.to_string()),
                        file.map_or(crate::Value::Null, |file| {
                            crate::Value::Text(file.to_string())
                        }),
                    ],
                )
                .await
                .expect("post-checkpoint insert should dirty a new identity");
        }
        // Dirty the file itself, not just rows inside it. This puts a row of a
        // *different* schema into file 0's diff, which is the case the bare
        // `file_id` shape must cover and the `file_id + schema_key` shape must
        // not. Without it both shapes would return the same json_pointer rows
        // and the `FILE_SPACE` schema-domain resolution would go unexercised.
        session
            .execute(
                "UPDATE lix_file SET content = CAST($1 AS BYTEA) WHERE id = $2",
                &[
                    crate::Value::Text("changed".to_string()),
                    crate::Value::Text(files[0].to_string()),
                ],
            )
            .await
            .expect("file content update should dirty the file descriptor");

        // An untracked row cannot live inside a tracked file — file ownership
        // validation requires a row and its owning file to share one lane — so
        // the untracked case is only reachable in the null-file bucket. It is
        // still worth carrying: the bypass must classify it as "no diff entry"
        // and the unfiltered index scan must agree.
        session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_untracked) \
                 VALUES ('/untracked', CAST('{\"v\":1}' AS JSONB), true)",
                &[],
            )
            .await
            .expect("untracked insert should commit");

        type DiffRow = (String, String, Option<String>, String);
        fn collect(result: &crate::ExecuteResult) -> Vec<DiffRow> {
            let mut rows = result
                .rows()
                .iter()
                .map(|row| {
                    (
                        row.get::<String>("schema_key")
                            .expect("schema_key should decode"),
                        row.get::<serde_json::Value>("entity_pk")
                            .expect("entity_pk should decode")
                            .to_string(),
                        // `file_id` is nullable; a NULL surfaces as a decode
                        // error through the typed accessor.
                        row.get::<String>("file_id").ok(),
                        row.get::<String>("diff_type")
                            .expect("diff_type should decode"),
                    )
                })
                .collect::<Vec<_>>();
            rows.sort();
            rows
        }
        const COLUMNS: &str = "SELECT entity_pk, schema_key, file_id, diff_type \
                               FROM lix_working_diff";

        use std::sync::atomic::Ordering;
        let hits = &crate::hot_state::WORKING_DIFF_PATH_HITS;

        let index_before = hits.index_scan.load(Ordering::Relaxed);
        let broad = collect(
            &session
                .execute(COLUMNS, &[])
                .await
                .expect("unfiltered working-diff read should execute"),
        );
        assert!(
            hits.index_scan.load(Ordering::Relaxed) > index_before,
            "the unfiltered working-diff read must take the HOT_DIFF index path"
        );
        assert!(
            broad
                .iter()
                .any(|(schema, _, file, kind)| schema == "json_pointer"
                    && file.as_deref() == Some(files[0])
                    && kind == "removed"),
            "fixture should produce a removed row inside the probed file"
        );

        for file in files {
            let bypass_before = hits.finite_bypass.load(Ordering::Relaxed);
            let scoped = collect(
                &session
                    .execute(
                        &format!("{COLUMNS} WHERE file_id = $1"),
                        &[crate::Value::Text(file.to_string())],
                    )
                    .await
                    .expect("file-scoped working-diff read should execute"),
            );
            assert!(
                hits.finite_bypass.load(Ordering::Relaxed) > bypass_before,
                "the file-scoped working-diff read for {file} must take the primary-row bypass"
            );
            let want = broad
                .iter()
                .filter(|(_, _, row_file, _)| row_file.as_deref() == Some(file))
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                scoped, want,
                "the file-scoped working-diff bypass disagrees with the index scan for {file}"
            );

            let bypass_before = hits.finite_bypass.load(Ordering::Relaxed);
            let scoped_schema = collect(
                &session
                    .execute(
                        &format!("{COLUMNS} WHERE file_id = $1 AND schema_key = $2"),
                        &[
                            crate::Value::Text(file.to_string()),
                            crate::Value::Text("json_pointer".to_string()),
                        ],
                    )
                    .await
                    .expect("file+schema working-diff read should execute"),
            );
            assert!(
                hits.finite_bypass.load(Ordering::Relaxed) > bypass_before,
                "the file+schema working-diff read for {file} must take the primary-row bypass"
            );
            let want_schema = want
                .iter()
                .filter(|(schema, _, _, _)| schema == "json_pointer")
                .cloned()
                .collect::<Vec<_>>();
            assert_eq!(
                scoped_schema, want_schema,
                "the file+schema working-diff bypass disagrees with the index scan for {file}"
            );
            if file == files[0] {
                // The dirtied file descriptor proves the bare `file_id` shape
                // resolved a schema domain wider than the one predicate-named
                // schema, rather than silently answering json_pointer only.
                assert!(
                    want.len() > want_schema.len(),
                    "fixture should put a non-json_pointer schema in {file}'s diff, \
                     otherwise the FILE_SPACE schema-domain resolution is untested"
                );
            }
        }
    }

    /// `WHERE lixcol_file_id = $1` is now an exact provider constraint, so
    /// DataFusion drops its residual filter and the answer rests entirely on
    /// `HotStateFilter::file_ids`. Rows can live in four authorities — the
    /// branch-local `HOT_ROW` overlay, a packed current base, a certified
    /// entity batch, and the root current base — and a file-scoped seek is
    /// only sound if every one of them applies the same filter. The checkpoint
    /// in the middle republishes the generation, so rows written before and
    /// after it reach the read through different authorities.
    #[tokio::test]
    async fn file_scoped_entity_read_matches_the_unfiltered_scan() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("initialized engine should open");
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;

        let files = [
            "66696c65-0000-8000-8000-000000000000",
            "66696c65-0001-8000-8000-000000000001",
        ];
        for (index, file) in files.iter().enumerate() {
            session
                .execute(
                    "INSERT INTO lix_file (id, path, content) \
                     VALUES ($1, $2, CAST($3 AS BYTEA))",
                    &[
                        crate::Value::Text((*file).to_string()),
                        crate::Value::Text(format!("/f{index}.txt")),
                        crate::Value::Text("seed".to_string()),
                    ],
                )
                .await
                .expect("file should insert");
        }

        let mut expected: Vec<(String, Option<String>)> = Vec::new();
        for (path, file) in [
            ("/a0", Some(files[0])),
            ("/a1", Some(files[0])),
            ("/b0", Some(files[1])),
            ("/none", None),
        ] {
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_file_id) \
                     VALUES ($1, CAST('{\"v\":0}' AS JSONB), $2)",
                    &[
                        crate::Value::Text(path.to_string()),
                        file.map_or(crate::Value::Null, |file| {
                            crate::Value::Text(file.to_string())
                        }),
                    ],
                )
                .await
                .expect("pre-checkpoint row should insert");
            expected.push((path.to_string(), file.map(str::to_string)));
        }

        session
            .create_checkpoint()
            .await
            .expect("checkpoint should republish the generation");

        for (path, file) in [
            ("/a2", Some(files[0])),
            ("/b1", Some(files[1])),
            ("/none2", None),
        ] {
            session
                .execute(
                    "INSERT INTO json_pointer (path, value, lixcol_file_id) \
                     VALUES ($1, CAST('{\"v\":1}' AS JSONB), $2)",
                    &[
                        crate::Value::Text(path.to_string()),
                        file.map_or(crate::Value::Null, |file| {
                            crate::Value::Text(file.to_string())
                        }),
                    ],
                )
                .await
                .expect("post-checkpoint row should insert");
            expected.push((path.to_string(), file.map(str::to_string)));
        }
        expected.sort();

        let all = session
            .execute("SELECT path, lixcol_file_id FROM json_pointer", &[])
            .await
            .expect("unfiltered scan should execute");
        let mut all_rows = all
            .rows()
            .iter()
            .map(|row| {
                (
                    row.get::<String>("path").expect("path should decode"),
                    // `lixcol_file_id` is nullable; the typed accessor has no
                    // Option impl, so a NULL surfaces as a decode error here.
                    row.get::<String>("lixcol_file_id").ok(),
                )
            })
            .collect::<Vec<_>>();
        all_rows.sort();
        assert_eq!(all_rows, expected, "fixture should read back unfiltered");

        for file in files {
            let filtered = session
                .execute(
                    "SELECT path FROM json_pointer WHERE lixcol_file_id = $1 ORDER BY path",
                    &[crate::Value::Text(file.to_string())],
                )
                .await
                .expect("file-scoped read should execute");
            let filtered_rows = filtered
                .rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("path should decode"))
                .collect::<Vec<_>>();
            let mut want = expected
                .iter()
                .filter(|(_, row_file)| row_file.as_deref() == Some(file))
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>();
            want.sort();
            assert_eq!(
                filtered_rows, want,
                "file-scoped entity read must return exactly the rows in {file}"
            );
        }

        let in_list = session
            .execute(
                "SELECT path FROM json_pointer \
                 WHERE lixcol_file_id IN ($1, $2) ORDER BY path",
                &[
                    crate::Value::Text(files[0].to_string()),
                    crate::Value::Text(files[1].to_string()),
                ],
            )
            .await
            .expect("file-scoped IN read should execute");
        let mut want_in = expected
            .iter()
            .filter(|(_, row_file)| row_file.is_some())
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        want_in.sort();
        assert_eq!(
            in_list
                .rows()
                .iter()
                .map(|row| row.get::<String>("path").expect("path should decode"))
                .collect::<Vec<_>>(),
            want_in
        );

        let point = session
            .execute(
                "SELECT path FROM json_pointer WHERE lixcol_file_id = $1 AND path = '/a2'",
                &[crate::Value::Text(files[0].to_string())],
            )
            .await
            .expect("file + primary key read should execute");
        assert_eq!(point.rows().len(), 1);

        let contradiction = session
            .execute(
                "SELECT path FROM json_pointer WHERE lixcol_file_id = $1 AND path = '/b0'",
                &[crate::Value::Text(files[0].to_string())],
            )
            .await
            .expect("contradictory file + primary key read should execute");
        assert!(
            contradiction.rows().is_empty(),
            "a row must not be visible through another file's scope"
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
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/checkpointed', CAST('{\"source\":\"tracked\"}' AS JSONB))",
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
                 VALUES ('/repository', CAST('{\"source\":\"untracked\"}' AS JSONB), true)",
                &[],
            )
            .await
            .expect("untracked row should write against the complete hot state");
        let repository_row = session
            .execute(
                "SELECT value FROM json_pointer WHERE path = '/repository'",
                &[],
            )
            .await
            .expect("untracked row should read from the complete hot state");
        assert_eq!(
            repository_row.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("repository value should decode"),
            json!({"source": "untracked"})
        );

        // The next tracked child carries the history-free member forward.
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/after-checkpoint', CAST('{\"source\":\"tracked\"}' AS JSONB))",
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
            ["/after-checkpoint", "/checkpointed", "/repository"]
        );
    }

    #[tokio::test]
    async fn checkpoint_reclaims_working_diff_epochs_and_retains_checkpoint_entities() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("initialized engine should open");
        let session = engine.open_session().await.expect("session should open");
        register_json_pointer_schema(&session).await;
        session
            .execute(
                "INSERT INTO json_pointer (path, value) \
                 VALUES ('/dirty', CAST('{\"value\":\"before-checkpoint\"}' AS JSONB))",
                &[],
            )
            .await
            .expect("tracked row should create a working diff");

        let adapter = StorageAdapter::new(storage.clone());
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("working-diff inventory read should open");
        let before_sparse = scan_test_space(&read, crate::hot_state::DIFF_SPACE).await;
        let before_packed =
            scan_test_space(&read, crate::hot_state::PACKED_CURRENT_BASE_SPACE).await;
        assert!(
            !before_sparse.is_empty() || !before_packed.is_empty(),
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
        let after = scan_test_space(&read, crate::hot_state::DIFF_SPACE).await;
        assert_eq!(
            after.len(),
            1,
            "the superseded branch epoch must be reclaimed; only the repository-global checkpoint entity may remain dirty"
        );
        drop(read);
        session
            .create_checkpoint()
            .await
            .expect("a second checkpoint should remain bounded");
        let read = adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("second checkpoint inventory read should open");
        let after_second = scan_test_space(&read, crate::hot_state::DIFF_SPACE).await;
        assert_eq!(
            after_second.len(),
            2,
            "the second immutable checkpoint entity remains while the superseded branch epoch is reclaimed"
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
            .open_session_at(GLOBAL_BRANCH_ID)
            .await
            .expect("global session should open");
        register_global_json_pointer_schema(&global_session).await;
        global_session
            .execute(
                "INSERT INTO json_pointer (path, value, lixcol_global, lixcol_untracked) \
                 VALUES ('/global', CAST('{\"source\":\"global\"}' AS JSONB), true, false)",
                &[],
            )
            .await
            .expect("write global tracked entity row");

        let session = engine.open_session().await.expect("session should open");
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
        let session = engine.open_session().await.expect("session should open");

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
        let hot_rows = scan_test_space(&read, crate::hot_state::ROW_SPACE).await;
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
                crate::hot_state::ROW_SPACE,
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

    /// The hot index plane's write half: a schema registration publishes a
    /// witness for each declared column, and ordinary inserts publish one entry
    /// per row per declared column.
    ///
    /// Also pins the shape of what is *not* indexed. `locale` is declared by
    /// nothing, so it gets no entries and the collection scan keeps serving it.
    #[tokio::test]
    async fn declared_columns_publish_index_entries_and_a_witness() {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("engine should open");
        let session = engine.open_session().await.expect("session should open");
        for schema in [
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "index_probe_parent",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            }),
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": "index_probe_child",
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "parent_id", "type": "text", "nullable": false },
                    { "name": "locale", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
                "foreign_keys": [{
                    "columns": ["parent_id"],
                    "references": { "schema_key": "index_probe_parent", "columns": ["id"] }
                }],
            }),
        ] {
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                    &[crate::Value::Text(schema.to_string())],
                )
                .await
                .expect("schema should register");
        }
        session
            .execute(
                "INSERT INTO index_probe_parent (id) VALUES ('parent-0')",
                &[],
            )
            .await
            .expect("parent should insert");
        for index in 0..3 {
            session
                .execute(
                    r#"INSERT INTO index_probe_child (id, "parent_id", locale) VALUES ($1, 'parent-0', 'en')"#,
                    &[crate::Value::Text(format!("child-{index}"))],
                )
                .await
                .expect("child should insert");
        }

        assert_eq!(
            hot_index_record_counts(&storage).await,
            (1, 3),
            "expected one witness for the one declared column and one entry per child row"
        );
    }

    /// The two schemas the index tests drive: a parent keyed only by its
    /// primary key, and a child declaring a foreign key onto it. The foreign
    /// key is what makes `parent_id` an indexed column.
    fn index_probe_schemas(parent: &str, child: &str) -> [serde_json::Value; 2] {
        [
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": parent,
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
            }),
            json!({
                "$schema": "https://lix.dev/schema-v1.json",
                "key": child,
                "columns": [
                    { "name": "id", "type": "text", "nullable": false },
                    { "name": "parent_id", "type": "text", "nullable": false },
                    { "name": "locale", "type": "text", "nullable": false },
                ],
                "primary_key": ["id"],
                "foreign_keys": [{
                    "columns": ["parent_id"],
                    "references": { "schema_key": parent, "columns": ["id"] }
                }],
            }),
        ]
    }

    async fn open_index_probe_session() -> (Memory, SessionContext<Memory>) {
        let storage = Memory::new();
        Engine::initialize(storage.clone())
            .await
            .expect("engine should initialize");
        let engine = Engine::new(storage.clone())
            .await
            .expect("engine should open");
        let session = engine.open_session().await.expect("session should open");
        (storage, session)
    }

    async fn hot_index_record_counts(storage: &Memory) -> (usize, usize) {
        let storage_adapter = StorageAdapter::new(storage.clone());
        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read the index plane");
        let entries = scan_test_space(&read, crate::hot_state::INDEX_SPACE).await;
        let witnesses = entries
            .iter()
            .filter(|entry| match &entry.value {
                crate::storage::ProjectedValue::FullValue(bytes) => !bytes.starts_with(b"["),
                crate::storage::ProjectedValue::KeyOnly => true,
            })
            .count();
        (witnesses, entries.len() - witnesses)
    }

    /// The witness carries how many entries the plane has published, which is
    /// what sizes the degradation budget. It must count every published entry
    /// in the generation, not just this commit's.
    async fn hot_index_published_count(storage: &Memory) -> u64 {
        let storage_adapter = StorageAdapter::new(storage.clone());
        let read = storage_adapter
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read the index plane");
        let entries = scan_test_space(&read, crate::hot_state::INDEX_SPACE).await;
        let mut total = 0;
        for entry in &entries {
            let crate::storage::ProjectedValue::FullValue(bytes) = &entry.value else {
                continue;
            };
            if bytes.starts_with(b"[") {
                continue;
            }
            let count: [u8; 8] = bytes.as_ref().try_into().expect("witness carries a u64");
            total += u64::from_be_bytes(count);
        }
        total
    }

    #[tokio::test]
    async fn the_index_witness_accumulates_its_published_entry_count() {
        let (storage, session) = open_index_probe_session().await;
        for schema in index_probe_schemas("counted_parent", "counted_child") {
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                    &[crate::Value::Text(schema.to_string())],
                )
                .await
                .expect("schema should register");
        }
        session
            .execute("INSERT INTO counted_parent (id) VALUES ('parent-0')", &[])
            .await
            .expect("parent should insert");
        assert_eq!(hot_index_published_count(&storage).await, 0);
        for index in 0..4 {
            session
                .execute(
                    r#"INSERT INTO counted_child (id, "parent_id", locale) VALUES ($1, 'parent-0', 'en')"#,
                    &[crate::Value::Text(format!("child-{index}"))],
                )
                .await
                .expect("child should insert");
        }
        assert_eq!(
            hot_index_published_count(&storage).await,
            4,
            "the count must span commits, not restart at each one"
        );
        // A delete publishes no entry — the plane is put-only — so the count
        // stands still while the collection shrinks. That divergence is
        // exactly what the budget measures.
        session
            .execute("DELETE FROM counted_child WHERE id = 'child-0'", &[])
            .await
            .expect("child should delete");
        assert_eq!(hot_index_published_count(&storage).await, 4);
    }

    /// Past the budget the lookup abandons the index and the caller's ordinary
    /// scan serves instead. The route is deliberately invisible in the result,
    /// so what this pins is that it stays invisible: the same rows, through a
    /// bucket far past the budget, with live rows, superseded rows and deleted
    /// rows all present.
    #[tokio::test]
    async fn a_bucket_past_the_budget_still_answers_exactly() {
        let (storage, session) = open_index_probe_session().await;
        for schema in index_probe_schemas("degraded_parent", "degraded_child") {
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                    &[crate::Value::Text(schema.to_string())],
                )
                .await
                .expect("schema should register");
        }
        for parent in ["parent-0", "parent-1"] {
            session
                .execute(
                    "INSERT INTO degraded_parent (id) VALUES ($1)",
                    &[crate::Value::Text(parent.into())],
                )
                .await
                .expect("parent should insert");
        }
        const ROWS: usize = 200;
        let values = (0..ROWS)
            .map(|index| format!("('child-{index}', 'parent-0', 'en')"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!(r#"INSERT INTO degraded_child (id, "parent_id", locale) VALUES {values}"#),
                &[],
            )
            .await
            .expect("children should insert");
        // Move half off `parent-0` and delete a quarter, so the `parent-0`
        // bucket holds every identity while only a quarter still match.
        let moved = (0..ROWS / 2)
            .map(|index| format!("'child-{index}'"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!(
                    r#"UPDATE degraded_child SET "parent_id" = 'parent-1' WHERE id IN ({moved})"#
                ),
                &[],
            )
            .await
            .expect("children should move");
        let deleted = (ROWS / 2..ROWS * 3 / 4)
            .map(|index| format!("'child-{index}'"))
            .collect::<Vec<_>>()
            .join(",");
        session
            .execute(
                &format!("DELETE FROM degraded_child WHERE id IN ({deleted})"),
                &[],
            )
            .await
            .expect("children should delete");

        let published = hot_index_published_count(&storage).await;
        assert!(
            published > 64,
            "the fixture must push the plane past the budget floor, published {published}"
        );

        async fn ids(session: &SessionContext<Memory>, parent: &str) -> Vec<String> {
            let rows = session
                .execute(
                    r#"SELECT id FROM degraded_child WHERE "parent_id" = $1 ORDER BY id"#,
                    &[crate::Value::Text(parent.into())],
                )
                .await
                .expect("declared-column read should succeed");
            rows.rows()
                .iter()
                .map(|row| match &row.values()[0] {
                    crate::Value::Text(id) => id.clone(),
                    other => panic!("unexpected id value {other:?}"),
                })
                .collect()
        }

        let mut expected_zero = (ROWS * 3 / 4..ROWS)
            .map(|index| format!("child-{index}"))
            .collect::<Vec<_>>();
        expected_zero.sort();
        assert_eq!(ids(&session, "parent-0").await, expected_zero);
        let mut expected_one = (0..ROWS / 2)
            .map(|index| format!("child-{index}"))
            .collect::<Vec<_>>();
        expected_one.sort();
        assert_eq!(ids(&session, "parent-1").await, expected_one);
    }

    /// Entries are candidates, never answers. A row whose indexed value moves
    /// leaves its old entry behind, and the caller's own predicate is what
    /// rejects it — so the moved row must disappear from the old value's
    /// result and appear under the new one.
    #[tokio::test]
    async fn superseded_index_entries_are_rejected_and_no_match_is_ever_lost() {
        let (_storage, session) = open_index_probe_session().await;
        for schema in index_probe_schemas("stale_parent", "stale_child") {
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                    &[crate::Value::Text(schema.to_string())],
                )
                .await
                .expect("schema should register");
        }
        for parent in ["parent-0", "parent-1"] {
            session
                .execute(
                    "INSERT INTO stale_parent (id) VALUES ($1)",
                    &[crate::Value::Text(parent.into())],
                )
                .await
                .expect("parent should insert");
        }
        for index in 0..3 {
            session
                .execute(
                    r#"INSERT INTO stale_child (id, "parent_id", locale) VALUES ($1, 'parent-0', 'en')"#,
                    &[crate::Value::Text(format!("child-{index}"))],
                )
                .await
                .expect("child should insert");
        }

        async fn count(session: &SessionContext<Memory>, parent: &str) -> usize {
            session
                .execute(
                    r#"SELECT id FROM stale_child WHERE "parent_id" = $1"#,
                    &[crate::Value::Text(parent.into())],
                )
                .await
                .expect("declared-column read should succeed")
                .len()
        }

        assert_eq!(count(&session, "parent-0").await, 3);
        assert_eq!(count(&session, "parent-1").await, 0);

        session
            .execute(
                r#"UPDATE stale_child SET "parent_id" = 'parent-1' WHERE id = 'child-1'"#,
                &[],
            )
            .await
            .expect("child should move to the other parent");

        assert_eq!(
            count(&session, "parent-0").await,
            2,
            "the superseded entry under the old value must be rejected on read"
        );
        assert_eq!(
            count(&session, "parent-1").await,
            1,
            "the moved row must be found under its new value"
        );

        session
            .execute("DELETE FROM stale_child WHERE id = 'child-0'", &[])
            .await
            .expect("child should delete");
        assert_eq!(
            count(&session, "parent-0").await,
            1,
            "a deleted row leaves its entry behind and must not resurface"
        );
    }

    /// A checkpoint publication reuses its branch's serving generation, so the
    /// index plane — which is keyed by that generation — survives it intact.
    ///
    /// This is the explicit choice, not an accident: republishing the whole
    /// index at checkpoint time would make checkpoints O(collection). Because
    /// the generation is reused, there is nothing to copy. The test exists so
    /// that a future change minting a fresh generation at checkpoint time
    /// fails here instead of silently returning no rows.
    #[tokio::test]
    async fn a_checkpoint_keeps_the_declared_column_index_serving() {
        let (_storage, session) = open_index_probe_session().await;
        for schema in index_probe_schemas("ckpt_parent", "ckpt_child") {
            session
                .execute(
                    "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                    &[crate::Value::Text(schema.to_string())],
                )
                .await
                .expect("schema should register");
        }
        session
            .execute("INSERT INTO ckpt_parent (id) VALUES ('parent-0')", &[])
            .await
            .expect("parent should insert");
        for index in 0..3 {
            session
                .execute(
                    r#"INSERT INTO ckpt_child (id, "parent_id", locale) VALUES ($1, 'parent-0', 'en')"#,
                    &[crate::Value::Text(format!("child-{index}"))],
                )
                .await
                .expect("child should insert");
        }
        session
            .create_checkpoint()
            .await
            .expect("checkpoint should publish");

        let rows = session
            .execute(
                r#"SELECT id FROM ckpt_child WHERE "parent_id" = 'parent-0'"#,
                &[],
            )
            .await
            .expect("declared-column read should succeed");
        assert_eq!(
            rows.len(),
            3,
            "the index must keep serving across a checkpoint publication"
        );

        session
            .execute(
                r#"INSERT INTO ckpt_child (id, "parent_id", locale) VALUES ('child-3', 'parent-0', 'en')"#,
                &[],
            )
            .await
            .expect("post-checkpoint child should insert");
        let rows = session
            .execute(
                r#"SELECT id FROM ckpt_child WHERE "parent_id" = 'parent-0'"#,
                &[],
            )
            .await
            .expect("declared-column read should succeed");
        assert_eq!(
            rows.len(),
            4,
            "rows written after the checkpoint must join the same index"
        );
    }

    /// The unique validator now probes the index instead of scanning the
    /// collection. The probe must reject a duplicate exactly as the scan did,
    /// and must keep accepting a value that only the staged row holds.
    #[tokio::test]
    async fn the_unique_probe_still_rejects_committed_duplicates() {
        let (_storage, session) = open_index_probe_session().await;
        session
            .execute(
                "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
                &[crate::Value::Text(
                    json!({
                        "$schema": "https://lix.dev/schema-v1.json",
                        "key": "probe_unique",
                        "columns": [
                            { "name": "id", "type": "text", "nullable": false },
                            { "name": "slug", "type": "text", "nullable": false },
                        ],
                        "primary_key": ["id"],
                        "unique": [["slug"]],
                    })
                    .to_string(),
                )],
            )
            .await
            .expect("schema should register");
        for index in 0..8 {
            session
                .execute(
                    "INSERT INTO probe_unique (id, slug) VALUES ($1, $2)",
                    &[
                        crate::Value::Text(format!("row-{index}")),
                        crate::Value::Text(format!("slug-{index}")),
                    ],
                )
                .await
                .expect("row should insert");
        }

        let error = session
            .execute(
                "INSERT INTO probe_unique (id, slug) VALUES ('row-dup', 'slug-3')",
                &[],
            )
            .await
            .expect_err("a committed duplicate must still be rejected");
        assert_eq!(error.code, LixError::CODE_UNIQUE);

        session
            .execute(
                "INSERT INTO probe_unique (id, slug) VALUES ('row-8', 'slug-8')",
                &[],
            )
            .await
            .expect("a fresh value must still be accepted");
        assert_eq!(
            session
                .execute("SELECT id FROM probe_unique", &[])
                .await
                .expect("read")
                .len(),
            9
        );
    }
}
