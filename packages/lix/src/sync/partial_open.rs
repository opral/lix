//! Opening orchestration for a partial replica with on-demand sync.

use super::platform::HttpSyncTransport;
use super::{PartialReplicaState, SyncRuntime, SyncTransport};
use crate::engine::Engine;
use crate::storage_adapter::{Storage, StorageAdapter};
use crate::{LixError, ServerOptions};
use futures_util::{FutureExt, select_biased};
use std::sync::Arc;
use std::time::Duration;

pub(crate) struct PreparedPartialOpen<S> {
    pub(crate) adapter: StorageAdapter<S>,
    pub(crate) state: Arc<PartialReplicaState>,
    pub(crate) initialized: bool,
    server: Option<ServerOptions>,
    transport: Option<HttpSyncTransport>,
}

async fn close_bounded(transport: &HttpSyncTransport) {
    let close = transport.close_session().fuse();
    let deadline = super::platform::sleep(Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(close, deadline);
    select_biased! { _ = close => {}, _ = deadline => {} }
}

impl<S: Storage + Clone + Send + Sync + 'static> PreparedPartialOpen<S> {
    /// Binding is captured by each transaction before it can author state.
    /// It is distinct from the full-replica certification capability.
    pub(crate) fn bind_engine(&self, engine: &Engine<S>) -> Result<(), LixError> {
        if engine.lix_id() != self.state.repository_id() {
            return Err(LixError::new(
                "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                "partial engine repository disagrees with prepared admission",
            ));
        }
        engine.sync_mode().admit_partial_replica(
            self.state.clone(),
            super::partial_replica_write_capability(),
        );
        Ok(())
    }

    pub(crate) async fn start_runtime(
        &mut self,
        changes: tokio::sync::watch::Receiver<u64>,
        engine: Arc<Engine<S>>,
    ) -> Result<Arc<SyncRuntime>, LixError> {
        // Keep an owner-local close handle until worker creation succeeds.
        // A transport clone does not create another authenticated session.
        let cleanup = self.transport.clone();
        let result = super::partial_runtime::start_partial_runtime_with_engine(
            self.adapter.clone(),
            self.state.clone(),
            self.server.clone(),
            self.transport.take(),
            Some(changes),
            Some(engine),
        )
        .await;
        if result.is_err() {
            if let Some(transport) = &cleanup {
                close_bounded(transport).await;
            }
        }
        result
    }

    pub(crate) async fn close_after_error(&mut self) {
        if let Some(transport) = self.transport.take() {
            close_bounded(&transport).await;
        }
    }
}

pub(crate) async fn prepare_partial_open<S>(
    storage: S,
    server: Option<ServerOptions>,
) -> Result<PreparedPartialOpen<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let server = server
        .map(|mut server| {
            server.url = super::normalize_sync_locator(&server.url)?.locator;
            Ok::<_, LixError>(server)
        })
        .transpose()?;
    match crate::migration::admit_partial_epoch(&storage).await {
        Ok(admitted) => {
            if server
                .as_ref()
                .is_some_and(|server| server.url != admitted.state.remote_id())
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                    "configured authority differs from durable partial admission",
                ));
            }
            return Ok(PreparedPartialOpen {
                adapter: admitted.adapter,
                state: Arc::new(admitted.state),
                initialized: false,
                server,
                transport: None,
            });
        }
        Err(error) => {
            if error.code != "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED"
                || !crate::migration::partial_epoch_has_no_markers(&storage).await?
            {
                return Err(error);
            }
        }
    }
    let server = server.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "fresh partial replica requires an authenticated authority",
        )
    })?;
    let transport = HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let installed = async {
        let descriptor = transport.partial_replica_descriptor(None).await?.wire;
        let state = PartialReplicaState::from_leased(
            server.url.clone(),
            transport.active_account_id().to_owned(),
            uuid::Uuid::now_v7().to_string(),
            descriptor,
        )?;
        let admission = crate::migration::install_fresh_partial_epoch(storage, &state).await?;
        Ok::<_, LixError>(admission)
    }
    .await;
    match installed {
        Ok(admitted) => Ok(PreparedPartialOpen {
            adapter: admitted.adapter,
            state: Arc::new(admitted.state),
            initialized: true,
            server: Some(server),
            transport: Some(transport),
        }),
        Err(error) => {
            close_bounded(&transport).await;
            Err(error)
        }
    }
}

/// Grants only the partial writer fence after the backing engine has validated
/// the same durable admission and inherited its owner's live mode binding.
pub(crate) fn admit_partial_storage_session<S>(
    engine: &Engine<S>,
    expected: &PartialReplicaState,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mode = engine.sync_mode();
    if mode.role() != super::SyncRole::PartialReplica
        || mode.partial_admission().as_deref() != Some(expected)
        || engine.lix_id() != expected.repository_id()
    {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "partial storage session binding changed during admission",
        ));
    }
    engine
        .storage()
        .admit_partial_replica_writer(super::partial_replica_write_capability());
    Ok(())
}

/// An authority-derived descriptor for explicit cache conversion. Fields stay
/// sync-owned so migration cannot substitute unauthenticated coordinates.
pub(crate) struct FinalizedPartialConversion {
    state: PartialReplicaState,
    deadline: super::http::CandidateBaselineDeadline,
}
impl FinalizedPartialConversion {
    pub(crate) fn state(&self) -> &PartialReplicaState {
        &self.state
    }
    pub(crate) fn check_deadline(&self) -> Result<(), LixError> {
        self.deadline.check(&self.state.baseline_lease().lease_id)
    }
    #[cfg(test)]
    pub(crate) fn for_test(state: PartialReplicaState, duration: std::time::Duration) -> Self {
        let deadline = super::http::CandidateBaselineDeadline::for_test(
            &state.baseline_lease().lease_id,
            duration,
        );
        Self { state, deadline }
    }
}

pub(crate) struct AuthenticatedPartialConversion {
    state: PartialReplicaState,
    server: ServerOptions,
    // Explicit source selection is resolved only after source migration creates
    // its native authority ref. It never changes authenticated descriptor facts.
    requested_branch: Option<String>,
}
impl AuthenticatedPartialConversion {
    /// Renew exactly the authenticated baseline near durable conversion
    /// publication. Expiry aborts conversion; this never silently rebases.
    pub(crate) async fn finalize_state(&self) -> Result<FinalizedPartialConversion, LixError> {
        let transport = HttpSyncTransport::connect(&self.server.url, &self.server.headers).await?;
        let result = async {
            if transport.lix_id() != self.state.repository_id()
                || transport.active_account_id() != self.state.active_account_id()
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                    "conversion authority identity changed during lease finalization",
                ));
            }
            transport.bind_native_baseline_lease(self.state.baseline_lease())?;
            let (lease, deadline) = transport.renew_native_baseline_lease_timed().await?;
            let state = self.state.with_renewed_baseline_lease(lease)?;
            Ok(FinalizedPartialConversion { state, deadline })
        }
        .await;
        close_bounded(&transport).await;
        result
    }
    pub(crate) fn state(&self) -> &PartialReplicaState {
        &self.state
    }
    pub(crate) fn requested_branch(&self) -> &str {
        self.requested_branch
            .as_deref()
            .unwrap_or(&self.state.descriptor().selected_branch.branch_id)
    }
    pub(crate) fn server(&self) -> &ServerOptions {
        &self.server
    }
}
pub(crate) async fn authenticate_partial_conversion(
    mut server: ServerOptions,
    branch_id: Option<&str>,
) -> Result<AuthenticatedPartialConversion, LixError> {
    server.url = super::normalize_sync_locator(&server.url)?.locator;
    let transport = HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let result = async {
        let descriptor = transport.partial_replica_descriptor(branch_id).await?.wire;
        let state = PartialReplicaState::from_leased(
            server.url.clone(),
            transport.active_account_id().to_owned(),
            uuid::Uuid::now_v7().to_string(),
            descriptor,
        )?;
        Ok(AuthenticatedPartialConversion {
            state,
            server,
            requested_branch: None,
        })
    }
    .await;
    close_bounded(&transport).await;
    result
}

/// Repository authentication precedes explicit full-source classification. A
/// requested locally new branch must not be looked up on authority until its
/// exact native global outcome has published that ref.
pub(crate) async fn authenticate_partial_source_conversion(
    server: ServerOptions,
    requested_branch: Option<&str>,
) -> Result<AuthenticatedPartialConversion, LixError> {
    if let Some(branch) = requested_branch {
        crate::storage_codec::id_string::uuid_bytes_from_canonical(branch).ok_or_else(|| {
            LixError::new(
                "LIX_PARTIAL_CONVERSION_INVALID_BRANCH",
                "conversion requires a canonical branch ID",
            )
        })?;
    }
    let mut authenticated = authenticate_partial_conversion(server, None).await?;
    authenticated.requested_branch = requested_branch.map(str::to_owned);
    Ok(authenticated)
}
