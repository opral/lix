use std::sync::Arc;

use crate::binary_cas::BinaryCasContext;
use crate::changelog::ChangelogContext;
use crate::commit_graph::CommitGraphContext;
use crate::entity_identity::EntityIdentity;
use crate::init::InitReceipt;
use crate::live_state::LiveStateContext;
use crate::live_state::LiveStateRowRequest;
use crate::schema_registry::SchemaRegistry;
use crate::session::SessionContext;
use crate::tracked_state::TrackedStateContext;
use crate::untracked_state::UntrackedStateContext;
use crate::version::GLOBAL_VERSION_ID;
use crate::version_ref::VersionRefContext;
use crate::{LixBackend, LixError, NullableKeyFilter, TransactionBeginMode};

#[derive(Clone)]
pub struct Engine {
    backend: Arc<dyn LixBackend + Send + Sync>,
    tracked_state: Arc<TrackedStateContext>,
    live_state: Arc<LiveStateContext>,
    version_ref: Arc<VersionRefContext>,
    binary_cas: Arc<BinaryCasContext>,
    changelog: Arc<ChangelogContext>,
    schema_registry: Arc<SchemaRegistry>,
}

impl Engine {
    /// Seeds an empty backend with the engine2 repository bootstrap facts.
    ///
    /// Initialization is a storage lifecycle operation, separate from runtime
    /// construction. Call this before `Engine::new(...)` for a brand-new
    /// backend.
    pub async fn initialize(
        backend: Box<dyn LixBackend + Send + Sync>,
    ) -> Result<InitReceipt, LixError> {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::from(backend);
        let changelog = ChangelogContext::new();
        let commit_graph = CommitGraphContext::new(changelog);
        let tracked_state = TrackedStateContext::new();
        let untracked_state = UntrackedStateContext::new();
        let live_state = LiveStateContext::new(tracked_state, untracked_state, commit_graph);

        crate::init::initialize(backend, &changelog, &live_state).await
    }

    /// Creates a clean DataFusion-first engine over an initialized backend.
    ///
    /// SessionContext, execution, and transaction overlays are layered below the
    /// instance instead of being hidden behind a legacy boot path.
    pub async fn new(backend: Box<dyn LixBackend + Send + Sync>) -> Result<Self, LixError> {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::from(backend);

        let tracked_state = Arc::new(TrackedStateContext::new());
        let untracked_state = Arc::new(UntrackedStateContext::new());
        let changelog = Arc::new(ChangelogContext::new());
        let commit_graph = CommitGraphContext::new(changelog.as_ref().clone());
        let live_state = Arc::new(LiveStateContext::new(
            tracked_state.as_ref().clone(),
            *untracked_state,
            commit_graph,
        ));
        let version_ref = Arc::new(VersionRefContext::new(Arc::clone(&untracked_state)));
        assert_initialized(Arc::clone(&backend), live_state.as_ref()).await?;

        // SessionContext::execute later projects these stable state contexts into one
        // execution-scoped SQL context, optionally wrapped by a transaction
        // overlay for writes.

        Ok(Self {
            binary_cas: Arc::new(BinaryCasContext::new()),
            changelog,
            backend,
            tracked_state,
            live_state,
            version_ref,
            schema_registry: Arc::new(SchemaRegistry::new()),
        })
    }

    pub(crate) fn backend(&self) -> Arc<dyn LixBackend + Send + Sync> {
        Arc::clone(&self.backend)
    }

    #[cfg(test)]
    pub(crate) fn tracked_state(&self) -> Arc<TrackedStateContext> {
        Arc::clone(&self.tracked_state)
    }

    #[cfg(test)]
    pub(crate) fn version_ref(&self) -> Arc<VersionRefContext> {
        Arc::clone(&self.version_ref)
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
        self.version_ref
            .reader(self.backend())
            .load_head_commit_id(version_id)
            .await
    }

    pub async fn open_session(
        &self,
        active_version_id: impl Into<String>,
    ) -> Result<SessionContext, LixError> {
        SessionContext::open(
            active_version_id.into(),
            self.backend(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
        )
        .await
    }

    pub async fn open_workspace_session(&self) -> Result<SessionContext, LixError> {
        SessionContext::open_workspace(
            self.backend(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.version_ref),
            Arc::clone(&self.schema_registry),
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
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("missing version ref for version '{version_id}'"),
                )
            })?;
        let commit_graph = CommitGraphContext::new(ChangelogContext::new());
        let mut transaction = self
            .backend
            .begin_transaction(TransactionBeginMode::Write)
            .await?;
        self.tracked_state
            .rebuild_state_at_commit(
                &commit_graph,
                self.backend(),
                transaction.as_mut(),
                &head_commit_id,
            )
            .await?;
        transaction.commit().await
    }
}

async fn assert_initialized(
    backend: Arc<dyn LixBackend + Send + Sync>,
    live_state: &LiveStateContext,
) -> Result<(), LixError> {
    let reader = live_state.reader(backend);
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
        "engine2 backend is not initialized; call Engine::initialize(...) before Engine::new(...)",
    ))
}
