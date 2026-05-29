use std::sync::Arc;

use crate::GLOBAL_BRANCH_ID;
use crate::binary_cas::BinaryCasContext;
use crate::branch::{BranchContext, BranchRefReader};
use crate::catalog::CatalogContext;
use crate::commit_graph::CommitGraphContext;
use crate::entity_pk::EntityPk;
use crate::init::InitReceipt;
use crate::live_state::LiveStateContext;
use crate::live_state::LiveStateRowRequest;
use crate::session::SessionContext;
use crate::storage::{StorageBackend, StorageReadOptions, StorageWriteOptions};
use crate::storage::{StorageContext, StorageWriteSet};
use crate::tracked_state::TrackedStateContext;
use crate::untracked_state::UntrackedStateContext;
use crate::{LixError, NullableKeyFilter};

#[derive(Clone)]
#[expect(missing_debug_implementations)]
pub struct Engine<B: StorageBackend = crate::storage::InMemoryStorageBackend> {
    storage: StorageContext<B>,
    tracked_state: Arc<TrackedStateContext>,
    live_state: Arc<LiveStateContext>,
    branch_ctx: Arc<BranchContext>,
    binary_cas: Arc<BinaryCasContext>,
    catalog_context: Arc<CatalogContext>,
    deterministic_runtime_gate: Arc<tokio::sync::Mutex<()>>,
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
    ///
    /// Deterministic runtime sequencing is serialized within this Engine
    /// context. Independently constructing multiple Engine values over the same
    /// cloned backend is outside that MVP runtime-sharing boundary.
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
        let branch_ctx = Arc::new(BranchContext::new(Arc::clone(&untracked_state)));
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
        })
    }

    pub(crate) fn storage(&self) -> StorageContext<B> {
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
        let read = self.storage.begin_read(StorageReadOptions::default())?;
        let result = self
            .branch_ctx
            .ref_reader(&read)
            .load_head_commit_id(branch_id)
            .await?
            .map(|commit_id| commit_id.to_string());
        Ok(result)
    }

    pub async fn open_session(
        &self,
        active_branch_id: impl Into<String>,
    ) -> Result<SessionContext<B>, LixError> {
        SessionContext::open(
            active_branch_id.into(),
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.deterministic_runtime_gate),
        )
        .await
    }

    pub async fn open_workspace_session(&self) -> Result<SessionContext<B>, LixError> {
        SessionContext::open_workspace(
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
            Arc::clone(&self.deterministic_runtime_gate),
        )
        .await
    }

    /// Rebuilds the tracked serving commit root for one branch from changelog.
    ///
    /// This is intentionally an engine-level operation: callers should not need
    /// to know which KV namespaces back changelog, commit graph, or tracked
    /// state. The current branch head is read from the live-state facade so
    /// rebuild uses the same moving-ref visibility as normal execution.
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
        let read = storage.begin_read(StorageReadOptions::default())?;
        let mut writes = StorageWriteSet::new();
        let rebuild_result = self
            .tracked_state
            .root_rebuilder(&read, &mut writes)
            .rebuild_commit_root_at(&head_commit_id)
            .await;
        rebuild_result?;
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
        "engine backend is not initialized; call Engine::initialize(...) before Engine::new(...)",
    ))
}
