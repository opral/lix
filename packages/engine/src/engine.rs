use std::sync::Arc;

use crate::binary_cas::BinaryCasContext;
use crate::catalog::CatalogContext;
use crate::commit_graph::CommitGraphContext;
use crate::commit_store::CommitStoreContext;
use crate::entity_identity::EntityIdentity;
use crate::init::InitReceipt;
use crate::live_state::LiveStateContext;
use crate::live_state::LiveStateRowRequest;
use crate::session::SessionContext;
use crate::storage::{StorageBackend, StorageReadOptions, StorageWriteOptions};
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
    commit_store: Arc<CommitStoreContext>,
    catalog_context: Arc<CatalogContext>,
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
        let commit_store = CommitStoreContext::new();

        crate::init::initialize(
            storage,
            &commit_store,
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
        let commit_store = Arc::new(CommitStoreContext::new());
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
            commit_store,
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
            Arc::clone(&self.commit_store),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
        )
        .await
    }

    pub async fn open_workspace_session(&self) -> Result<SessionContext<B>, LixError> {
        SessionContext::open_workspace(
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.commit_store),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
        )
        .await
    }

    /// Materializes the tracked serving projection root for one version from commit_store.
    ///
    /// This is intentionally an engine-level operation: callers should not need
    /// to know which KV namespaces back changelog, commit graph, or tracked
    /// state. The current version head is read from the live-state facade so
    /// materialization uses the same moving-ref visibility as normal execution.
    pub async fn rebuild_tracked_state_for_version(
        &self,
        version_id: &str,
    ) -> Result<(), LixError> {
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
        let materialize_result = self
            .tracked_state
            .materializer(&read, &mut writes, self.commit_store.as_ref())
            .materialize_root_at(&head_commit_id)
            .await;
        if let Err(error) = materialize_result {
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
