use std::sync::Arc;

use crate::binary_cas::BinaryCasContext;
use crate::commit_graph::CommitGraphContext;
use crate::commit_store::CommitStoreContext;
use crate::entity_identity::EntityIdentity;
use crate::init::InitReceipt;
use crate::live_state::LiveStateContext;
use crate::live_state::LiveStateRowRequest;
use crate::schema_catalog::SchemaCatalogSource;
use crate::session::SessionContext;
use crate::storage::{StorageContext, StorageWriteSet};
use crate::tracked_state::TrackedStateContext;
use crate::untracked_state::UntrackedStateContext;
use crate::version::{VersionContext, VersionRefReader};
use crate::GLOBAL_VERSION_ID;
use crate::{Backend, LixError, NullableKeyFilter};

#[derive(Clone)]
pub struct Engine {
    storage: StorageContext,
    tracked_state: Arc<TrackedStateContext>,
    live_state: Arc<LiveStateContext>,
    version_ctx: Arc<VersionContext>,
    binary_cas: Arc<BinaryCasContext>,
    commit_store: Arc<CommitStoreContext>,
    schema_catalog_source: Arc<SchemaCatalogSource>,
}

impl Engine {
    /// Seeds an empty backend with the engine repository bootstrap facts.
    ///
    /// Initialization is a storage lifecycle operation, separate from runtime
    /// construction. Call this before `Engine::new(...)` for a brand-new
    /// backend.
    pub async fn initialize(
        backend: Box<dyn Backend + Send + Sync>,
    ) -> Result<InitReceipt, LixError> {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::from(backend);
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
    pub async fn new(backend: Box<dyn Backend + Send + Sync>) -> Result<Self, LixError> {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::from(backend);
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
            schema_catalog_source: Arc::new(SchemaCatalogSource::new()),
        })
    }

    pub(crate) fn storage(&self) -> StorageContext {
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
        let mut transaction = self.storage.begin_read_transaction().await?;
        let result = self
            .version_ctx
            .ref_reader(transaction.as_mut())
            .load_head_commit_id(version_id)
            .await;
        match result {
            Ok(result) => {
                transaction.rollback().await?;
                Ok(result)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn open_session(
        &self,
        active_version_id: impl Into<String>,
    ) -> Result<SessionContext, LixError> {
        SessionContext::open(
            active_version_id.into(),
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.commit_store),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.schema_catalog_source),
        )
        .await
    }

    pub async fn open_workspace_session(&self) -> Result<SessionContext, LixError> {
        SessionContext::open_workspace(
            self.storage(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.commit_store),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.schema_catalog_source),
        )
        .await
    }

    /// Rebuilds the tracked serving projection for one version from changelog.
    ///
    /// This is intentionally an engine-level operation: callers should not need
    /// to know which KV namespaces back changelog, commit graph, or tracked
    /// state. The current version head is read from the live-state facade so
    /// rebuild uses the same moving-ref visibility as normal execution.
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
        let commit_graph = CommitGraphContext::new();
        let storage = self.storage();
        let mut read_transaction = storage.begin_read_transaction().await?;
        let mut transaction = storage.begin_write_transaction().await?;
        let mut writes = StorageWriteSet::new();
        let rebuild_result = self
            .tracked_state
            .rebuild_state_at_commit(
                &commit_graph,
                read_transaction.as_mut(),
                transaction.as_mut(),
                &mut writes,
                &head_commit_id,
            )
            .await;
        if let Err(error) = rebuild_result {
            let _ = read_transaction.rollback().await;
            let _ = transaction.rollback().await;
            return Err(error);
        }
        if let Err(error) = read_transaction.rollback().await {
            let _ = transaction.rollback().await;
            return Err(error);
        }
        if let Err(error) = writes.apply(&mut transaction.as_mut()).await {
            let _ = transaction.rollback().await;
            return Err(error);
        }
        transaction.commit().await
    }
}

async fn assert_initialized(
    storage: StorageContext,
    live_state: &LiveStateContext,
) -> Result<(), LixError> {
    let mut transaction = storage.begin_read_transaction().await?;
    let reader = live_state.reader(transaction.as_mut());
    let result = reader
        .load_row(&LiveStateRowRequest {
            schema_key: "lix_key_value".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: EntityIdentity::single("lix_id"),
            file_id: NullableKeyFilter::Null,
        })
        .await;
    let initialized = match result {
        Ok(row) => {
            transaction.rollback().await?;
            row.is_some()
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            return Err(error);
        }
    };

    if initialized {
        return Ok(());
    }

    Err(LixError::new(
        "LIX_ERROR_NOT_INITIALIZED",
        "engine backend is not initialized; call Engine::initialize(...) before Engine::new(...)",
    ))
}
