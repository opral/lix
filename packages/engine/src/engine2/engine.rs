use std::sync::Arc;

use crate::binary_cas::BinaryCasContext;
use crate::engine2::changelog::ChangelogContext;
use crate::engine2::live_state::CommittedLiveStateContext;
use crate::engine2::schema_registry::SchemaRegistry;
use crate::engine2::session::SessionContext;
use crate::{LixBackend, LixError};

#[derive(Clone)]
pub struct Engine {
    backend: Arc<dyn LixBackend + Send + Sync>,
    committed_live_state: Arc<CommittedLiveStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    changelog: Arc<ChangelogContext>,
    schema_registry: Arc<SchemaRegistry>,
}

impl Engine {
    /// Creates a clean DataFusion-first engine over an initialized backend.
    ///
    /// SessionContext, execution, and transaction overlays are layered below the
    /// instance instead of being hidden behind a legacy boot path.
    pub async fn new(backend: Box<dyn LixBackend + Send + Sync>) -> Result<Self, LixError> {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::from(backend);

        // TODO(engine2): assert that the backend has been initialized through an
        // engine-owned storage readiness check. This should not ask live_state,
        // because live_state is only one lower subsystem.

        // The engine is constructed bottom-up from the storage DAG:
        //
        // let canonical_state = Arc::new(CanonicalStateContext::new(Arc::clone(&backend)));

        let committed_live_state = Arc::new(CommittedLiveStateContext::new());

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
            committed_live_state,
            schema_registry: Arc::new(SchemaRegistry::new()),
        })
    }

    pub(crate) fn backend(&self) -> Arc<dyn LixBackend + Send + Sync> {
        Arc::clone(&self.backend)
    }

    pub async fn open_session(
        &self,
        active_version_id: impl Into<String>,
    ) -> Result<SessionContext, LixError> {
        SessionContext::open(
            active_version_id.into(),
            self.backend(),
            Arc::clone(&self.committed_live_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.changelog),
            Arc::clone(&self.schema_registry),
        )
        .await
    }
}
