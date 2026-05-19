use std::sync::Arc;

use crate::binary_cas::BinaryCasContext;
use crate::catalog::CatalogContext;
use crate::commit_graph::CommitGraphContext;
use crate::entity_identity::EntityIdentity;
use crate::init::InitReceipt;
use crate::live_state::LiveStateContext;
use crate::live_state::LiveStateRowRequest;
use crate::session::SessionContext;
use crate::storage::{DurableWriteLock, StorageBackend, StorageReadOptions, StorageWriteOptions};
use crate::storage::{StorageContext, StorageWriteSet};
use crate::tracked_state::TrackedStateContext;
use crate::untracked_state::UntrackedStateContext;
use crate::version::{VersionContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter};

#[derive(Clone)]
pub struct Engine<B: StorageBackend = crate::storage::InMemoryStorageBackend> {
    storage: StorageContext<B>,
    tracked_state: Arc<TrackedStateContext>,
    live_state: Arc<LiveStateContext>,
    version_ctx: Arc<VersionContext>,
    binary_cas: Arc<BinaryCasContext>,
    catalog_context: Arc<CatalogContext>,
    write_lock: DurableWriteLock,
}

impl<B> Engine<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    /// Seeds an empty backend with the engine repository bootstrap facts.
    ///
    /// Initialization is a storage lifecycle operation, separate from runtime
    /// construction. Call this before `Engine::new(...)` for a brand-new
    /// backend.
    pub async fn initialize(backend: B) -> Result<InitReceipt, LixError> {
        let storage = StorageContext::new(backend);
        let _write_guard = storage.durable_write_lock().lock_owned().await;

        crate::init::initialize(
            storage,
            &TrackedStateContext::new(),
            &UntrackedStateContext::new(),
        )
        .await
    }

    /// Creates a clean DataFusion-first engine over an initialized backend.
    ///
    /// SessionContext, execution, and transaction overlays are layered below the
    /// instance instead of being hidden behind a legacy boot path.
    pub async fn new(backend: B) -> Result<Self, LixError> {
        let storage = StorageContext::new(backend);

        let tracked_state = Arc::new(TrackedStateContext::new());
        let untracked_state = Arc::new(UntrackedStateContext::new());
        let commit_graph = CommitGraphContext::new();
        let live_state = Arc::new(LiveStateContext::new(
            tracked_state.as_ref().clone(),
            *untracked_state,
            commit_graph,
        ));
        let version_ctx = Arc::new(VersionContext::new(Arc::clone(&untracked_state)));
        assert_initialized(storage.clone(), live_state.as_ref()).await?;

        // SessionContext::execute later projects these stable state contexts into one
        // execution-scoped SQL context, optionally wrapped by a transaction
        // overlay for writes.

        Ok(Self {
            binary_cas: Arc::new(BinaryCasContext::new()),
            write_lock: storage.durable_write_lock(),
            storage,
            tracked_state,
            live_state,
            version_ctx,
            catalog_context: Arc::new(CatalogContext::new()),
        })
    }

    pub(crate) fn storage(&self) -> StorageContext<B> {
        self.storage.clone()
    }

    /// Loads the current commit head for a version.
    ///
    /// This is the public engine-level form of the typed `version_ref` context:
    /// callers should not need to know that version heads are represented as
    /// untracked `lix_version_ref` rows in live_state.
    pub async fn load_version_head_commit_id(
        &self,
        version_id: &str,
    ) -> Result<Option<String>, LixError> {
        let read = self.storage.begin_read(StorageReadOptions::default())?;
        let result = self
            .version_ctx
            .ref_reader(&read)
            .load_head_commit_id(version_id)
            .await;
        result
    }

    pub async fn open_session(
        &self,
        active_version_id: impl Into<String>,
    ) -> Result<SessionContext<B>, LixError> {
        SessionContext::open(
            active_version_id.into(),
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
            self.write_lock.clone(),
        )
        .await
    }

    pub async fn open_workspace_session(&self) -> Result<SessionContext<B>, LixError> {
        SessionContext::open_workspace(
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
            self.write_lock.clone(),
        )
        .await
    }

    /// Rebuilds the tracked serving projection root for one version from changelog.
    ///
    /// This is intentionally an engine-level operation: callers should not need
    /// to know which KV namespaces back changelog, commit graph, or tracked
    /// state. The current version head is read from the live-state facade so
    /// rebuild uses the same moving-ref visibility as normal execution.
    pub async fn rebuild_tracked_state_for_version(
        &self,
        version_id: &str,
    ) -> Result<(), LixError> {
        let _write_guard = self.write_lock.lock_owned().await;
        let head_commit_id = self
            .load_version_head_commit_id(version_id)
            .await?
            .ok_or_else(|| {
                LixError::version_not_found(
                    version_id.to_string(),
                    "rebuild_tracked_state_for_version",
                    "target",
                )
            })?;
        let storage = self.storage();
        let read = storage.begin_read(StorageReadOptions::default())?;
        let mut writes = StorageWriteSet::new();
        let rebuild_result = self
            .tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_projection_root_at(&head_commit_id)
            .await;
        if let Err(error) = rebuild_result {
            return Err(error);
        }
        storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .map(|_| ())
            .map_err(Into::into)
    }
}

async fn assert_initialized<B>(
    storage: StorageContext<B>,
    live_state: &LiveStateContext,
) -> Result<(), LixError>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    let read = storage.begin_read(StorageReadOptions::default())?;
    let reader = live_state.reader(&read);
    let initialized = reader
        .load_row(&LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: EntityIdentity::single("lix_id"),
            file_id: NullableKeyFilter::Null,
        })
        .await?
        .is_some();

    if initialized {
        return Ok(());
    }

    Err(LixError::new(
        "LIX_ERROR_NOT_INITIALIZED",
        "engine backend is not initialized; call Engine::initialize(...) before Engine::new(...)",
    ))
}
