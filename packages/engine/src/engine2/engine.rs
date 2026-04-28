use std::sync::Arc;

use crate::binary_cas::BinaryCasContext;
use crate::engine2::changelog::ChangelogContext;
use crate::engine2::init::InitReceipt;
use crate::engine2::live_state::LiveStateContext;
use crate::engine2::live_state::LiveStateRowRequest;
use crate::engine2::schema_registry::SchemaRegistry;
use crate::engine2::session::SessionContext;
use crate::engine2::tracked_state::TrackedStateContext;
use crate::engine2::untracked_state::UntrackedStateContext;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, NullableKeyFilter};

#[derive(Clone)]
pub struct Engine {
    backend: Arc<dyn LixBackend + Send + Sync>,
    tracked_state: Arc<TrackedStateContext>,
    untracked_state: Arc<UntrackedStateContext>,
    live_state: Arc<LiveStateContext>,
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
        let tracked_state = TrackedStateContext::new();
        let untracked_state = UntrackedStateContext::new();
        let live_state = LiveStateContext::new(tracked_state, untracked_state);

        crate::engine2::init::initialize(backend, &changelog, &live_state).await
    }

    /// Creates a clean DataFusion-first engine over an initialized backend.
    ///
    /// SessionContext, execution, and transaction overlays are layered below the
    /// instance instead of being hidden behind a legacy boot path.
    pub async fn new(backend: Box<dyn LixBackend + Send + Sync>) -> Result<Self, LixError> {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::from(backend);

        // The engine is constructed bottom-up from the storage DAG:
        //
        // let canonical_state = Arc::new(CanonicalStateContext::new(Arc::clone(&backend)));

        let tracked_state = Arc::new(TrackedStateContext::new());
        let untracked_state = Arc::new(UntrackedStateContext::new());
        let live_state = Arc::new(LiveStateContext::new(*tracked_state, *untracked_state));
        assert_initialized(Arc::clone(&backend), live_state.as_ref()).await?;

        // let history_state = Arc::new(HistoryStateContext::new(
        //     Arc::clone(&canonical_state),
        //     Arc::clone(&backend),
        // ));

        // SessionContext::execute later projects these stable state contexts into one
        // execution-scoped SQL context, optionally wrapped by a transaction
        // overlay for writes.

        Ok(Self {
            binary_cas: Arc::new(BinaryCasContext::new()),
            changelog: Arc::new(ChangelogContext::new()),
            backend,
            tracked_state,
            untracked_state,
            live_state,
            schema_registry: Arc::new(SchemaRegistry::new()),
        })
    }

    pub(crate) fn backend(&self) -> Arc<dyn LixBackend + Send + Sync> {
        Arc::clone(&self.backend)
    }

    pub(crate) fn tracked_state(&self) -> Arc<TrackedStateContext> {
        Arc::clone(&self.tracked_state)
    }

    pub async fn open_session(
        &self,
        active_version_id: impl Into<String>,
    ) -> Result<SessionContext, LixError> {
        SessionContext::open(
            active_version_id.into(),
            self.backend(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.schema_registry),
        )
        .await
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
            entity_id: "lix_id".to_string(),
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
