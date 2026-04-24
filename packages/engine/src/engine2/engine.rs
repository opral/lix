use std::sync::Arc;

use crate::engine2::session::Session;
use crate::engine2::write_services::WriteServices;
use crate::live_state::CommittedLiveStateContext;
use crate::{LixBackend, LixError};

#[derive(Clone)]
pub struct Engine {
    backend: Arc<dyn LixBackend + Send + Sync>,
    committed_live_state: Arc<CommittedLiveStateContext>,
    // Engine-owned services used by write transactions at commit time.
    //
    // Keep this concrete. It is not meant to become another kitchen-sink
    // execution context; the `WriteExecutionContext` trait implementation is a
    // compatibility seam for the existing buffered transaction pipeline.
    write_services: Arc<WriteServices>,
}

impl Engine {
    /// Creates a clean DataFusion-first engine over an initialized backend.
    ///
    /// Session, execution, and transaction overlays are layered below the
    /// instance instead of being hidden behind a legacy boot path.
    pub async fn new(backend: Box<dyn LixBackend + Send + Sync>) -> Result<Self, LixError> {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::from(backend);
        // Construct lower write services once at engine creation, then thread
        // them down through Session -> Transaction. This mirrors the desired
        // DAG instead of creating ad hoc commit contexts inside transactions.
        let write_services = Arc::new(WriteServices::new(backend.dialect()));

        // TODO(engine2): assert that the backend has been initialized through an
        // engine-owned storage readiness check. This should not ask live_state,
        // because live_state is only one lower subsystem.

        // The engine is constructed bottom-up from the storage DAG:
        //
        // let canonical_state = Arc::new(CanonicalStateContext::new(Arc::clone(&backend)));

        let committed_live_state = Arc::new(
            CommittedLiveStateContext::new(Arc::clone(&backend)),
            // TODO pass canonical_state as argument. any canonical query shouldn't go through raw sql in live_state
        );

        // let history_state = Arc::new(HistoryStateContext::new(
        //     Arc::clone(&canonical_state),
        //     Arc::clone(&backend),
        // ));

        // let binary_cas_state = Arc::new(BinaryCasStateContext::new(Arc::clone(&backend)));
        //
        // Session::execute later projects these stable state contexts into one
        // execution-scoped SQL context, optionally wrapped by a transaction
        // overlay for writes.

        Ok(Self {
            backend,
            committed_live_state,
            write_services,
        })
    }

    pub(crate) fn backend(&self) -> Arc<dyn LixBackend + Send + Sync> {
        Arc::clone(&self.backend)
    }

    pub async fn open_session(&self) -> Result<Session, LixError> {
        // TODO(engine2): load this from an engine2-owned workspace/session
        // selector context instead of depending on the legacy session module.
        let active_version_id = "global".to_string();

        Ok(Session::new(
            active_version_id,
            self.backend(),
            Arc::clone(&self.committed_live_state),
            Arc::clone(&self.write_services),
        ))
    }
}
