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
    pub(crate) migration: Option<crate::OpenMigrationReport>,
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
    progress: Option<&Arc<dyn crate::OpenProgressSink>>,
) -> Result<PreparedPartialOpen<S>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    loop {
        match Box::pin(prepare_partial_open_once(storage.clone(), server.clone(), progress)).await {
            Err(error)
                if error.code == "LIX_PARTIAL_OPEN_RETRY"
                    || error.code == LixError::CODE_STORAGE_FENCED =>
            {
                super::platform::sleep(Duration::from_millis(1)).await;
            }
            result => return result,
        }
    }
}

async fn prepare_partial_open_once<S>(
    storage: S,
    server: Option<ServerOptions>,
    progress: Option<&Arc<dyn crate::OpenProgressSink>>,
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
    let replacement;
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
            let transport = connect_existing_authority(&admitted.state, server.as_ref(), progress).await?;
            return Ok(PreparedPartialOpen {
                adapter: admitted.adapter,
                state: Arc::new(admitted.state),
                initialized: false,
                migration: None,
                server,
                transport,
            });
        }
        Err(error) => {
            if error.code != "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED" {
                return Err(error);
            }
            replacement = if crate::migration::partial_epoch_has_no_markers(&storage).await? {
                None
            } else {
                Some(crate::migration::inspect_partial_replacement(&storage).await?)
            };
        }
    }
    if let Some(source) = replacement {
        if let (Some(server), Some(remote)) = (&server, source.remote_id()) {
            if server.url != remote {
                return Err(LixError::new(
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                    "configured authority differs from durable partial admission",
                ));
            }
        }
        source.require_preserving_upgrade(&storage).await?;
        let from_format = source.format();
        let admitted = if source.is_partial() {
            // Sparse upgrades retain both the authenticated admission and every
            // resident record, including pending work, without contacting a server.
            crate::migration::admit_repository_with_server(&storage, progress, None).await?;
            crate::migration::admit_partial_epoch(&storage).await?
        } else {
            let configured = server.clone().ok_or_else(|| {
                LixError::new(
                    "LIX_PARTIAL_REPLICA_CONVERSION_RECOVERY_REQUIRED",
                    "full replica conversion requires its authenticated authority; source retained",
                )
            })?;
            let authenticated =
                authenticate_partial_source_conversion_with_progress(configured, None, progress)
                    .await?;
            crate::migration::convert_clean_replica_to_partial(&storage, &authenticated, progress)
                .await?
        };
        if server
            .as_ref()
            .is_some_and(|server| server.url != admitted.state.remote_id())
        {
            return Err(LixError::new(
                "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                "configured authority differs from durable partial admission",
            ));
        }
        let transport =
            connect_existing_authority(&admitted.state, server.as_ref(), progress).await?;
        return Ok(PreparedPartialOpen {
            adapter: admitted.adapter,
            state: Arc::new(admitted.state),
            initialized: false,
            migration: Some(crate::OpenMigrationReport {
                from_format,
                to_format: crate::init::CURRENT_FORMAT_VERSION,
            }),
            server,
            transport,
        });
    }
    let server = server.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INVALID_PARAM,
            "fresh partial replica requires an authenticated authority",
        )
    })?;
    let transport =
        HttpSyncTransport::connect_with_progress(&server.url, &server.headers, progress).await?;
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
            migration: None,
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

/// A configured authority is part of opening. Omitting it preserves offline
/// admission of an existing replica from its durable authenticated metadata.
async fn connect_existing_authority(
    state: &PartialReplicaState,
    server: Option<&ServerOptions>,
    progress: Option<&Arc<dyn crate::OpenProgressSink>>,
) -> Result<Option<HttpSyncTransport>, LixError> {
    let Some(server) = server else {
        return Ok(None);
    };
    let transport = match HttpSyncTransport::connect_with_progress(
        &server.url,
        &server.headers,
        progress,
    )
    .await
    {
        Ok(transport) => transport,
        // Durable admission permits offline work. Authentication, protocol,
        // identity and malformed-response failures are never offline fallbacks.
        Err(error)
            if matches!(
                error.code.as_str(),
                "LIX_TRANSPORT_NETWORK" | "LIX_VERIFIED_OFFLINE_ADMISSION"
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    if transport.lix_id() != state.repository_id()
        || transport.active_account_id() != state.active_account_id()
    {
        close_bounded(&transport).await;
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "configured authority identity differs from durable partial admission",
        ));
    }
    Ok(Some(transport))
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
    pub(crate) fn for_test(state: PartialReplicaState, duration: Duration) -> Self {
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
    server: ServerOptions,
    branch_id: Option<&str>,
) -> Result<AuthenticatedPartialConversion, LixError> {
    authenticate_partial_conversion_with_progress(server, branch_id, None).await
}

pub(crate) async fn authenticate_partial_conversion_with_progress(
    mut server: ServerOptions,
    branch_id: Option<&str>,
    progress: Option<&Arc<dyn crate::OpenProgressSink>>,
) -> Result<AuthenticatedPartialConversion, LixError> {
    server.url = super::normalize_sync_locator(&server.url)?.locator;
    let transport =
        HttpSyncTransport::connect_with_progress(&server.url, &server.headers, progress).await?;
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
    authenticate_partial_source_conversion_with_progress(server, requested_branch, None).await
}

pub(crate) async fn authenticate_partial_source_conversion_with_progress(
    server: ServerOptions,
    requested_branch: Option<&str>,
    progress: Option<&Arc<dyn crate::OpenProgressSink>>,
) -> Result<AuthenticatedPartialConversion, LixError> {
    if let Some(branch) = requested_branch {
        crate::storage_codec::id_string::uuid_bytes_from_canonical(branch).ok_or_else(|| {
            LixError::new(
                "LIX_PARTIAL_CONVERSION_INVALID_BRANCH",
                "conversion requires a canonical branch ID",
            )
        })?;
    }
    let mut authenticated =
        authenticate_partial_conversion_with_progress(server, None, progress).await?;
    authenticated.requested_branch = requested_branch.map(str::to_owned);
    Ok(authenticated)
}

#[cfg(all(test, not(target_family = "wasm")))]
mod opening_tests {
    use super::*;
    use std::io::{Read, Write};

    #[tokio::test]
    async fn current_replica_waits_for_configured_authority_upgrade() {
        let authority = crate::open_lix().await.unwrap();
        let repository_id = authority.lix_id().to_owned();
        let account_id = authority.active_account_id().to_owned();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        authority.close().await.unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "http://{}/lix/{repository_id}",
            listener.local_addr().unwrap()
        );
        let state = PartialReplicaState::new(
            url.clone(),
            account_id.clone(),
            "00000000-0000-7000-8000-000000000811".into(),
            descriptor,
        )
        .unwrap();
        let storage = crate::storage_adapter::StorageSession::acquire(
            super::super::durable_memory_for_test(crate::Memory::new()),
        )
        .await
        .unwrap();
        crate::migration::install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        let serving = std::thread::spawn(move || {
            for migrating in [true, false] {
                let (mut connection, _) = listener.accept().unwrap();
                connection
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    connection.read_exact(&mut byte).unwrap();
                    headers.push(byte[0]);
                    assert!(headers.len() < 16 * 1024);
                }
                let (status, body) = if migrating {
                    (
                        "503 Service Unavailable",
                        serde_json::json!({"error": {
                            "code": "LIX_REPOSITORY_MIGRATING", "message": "upgrading",
                            "details": {"fromVersion":80,"toVersion":81}
                        }}),
                    )
                } else {
                    (
                        "200 OK",
                        serde_json::json!({
                            "protocolVersion":crate::SERVER_PROTOCOL_VERSION,
                            "syncProtocolVersion":super::super::SYNC_PROTOCOL_VERSION,
                            "lixId":repository_id,"sessionId":"ready-replica-session",
                            "activeAccountId":account_id
                        }),
                    )
                };
                let body = serde_json::to_vec(&body).unwrap();
                write!(connection, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
                connection.write_all(&body).unwrap();
            }
        });
        let events = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = events.clone();
        let progress: Arc<dyn crate::OpenProgressSink> =
            Arc::new(crate::CallbackOpenProgressSink::new(move |event| {
                observed.lock().unwrap().push(event)
            }));
        let mut prepared = prepare_partial_open(
            storage.clone(),
            Some(ServerOptions::new(url)),
            Some(&progress),
        )
        .await
        .unwrap();
        assert!(
            prepared.transport.is_some(),
            "opening must authenticate before returning"
        );
        assert!(!prepared.initialized);
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.scope == crate::OpenScope::Authority
                    && event.phase == crate::OpenPhase::Migrating
                    && event.from_format == Some(80))
        );
        serving.join().unwrap();
        prepared.close_after_error().await;
        let offline = prepare_partial_open(storage, None, None).await.unwrap();
        assert!(offline.transport.is_none());
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod existing_authority_tests {
    use super::*;
    use std::io::{Read, Write};

    #[tokio::test]
    async fn existing_authority_rejects_authentication_identity_and_malformed_responses() {
        let authority = crate::open_lix().await.unwrap();
        for case in ["identity", "authentication", "malformed"] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!(
                "http://{}/lix/{}",
                listener.local_addr().unwrap(),
                authority.lix_id()
            );
            let state = PartialReplicaState::new(
                url.clone(),
                authority.active_account_id().to_owned(),
                uuid::Uuid::now_v7().to_string(),
                authority.partial_replica_descriptor(None).await.unwrap(),
            )
            .unwrap();
            let lix_id = authority.lix_id().to_owned();
            let thread = std::thread::spawn(move || {
                for index in 0..if case == "identity" { 2 } else { 1 } {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        stream.read_exact(&mut byte).unwrap();
                        headers.push(byte[0]);
                    }
                    let (status, body) = if index == 1 {
                        ("200 OK", "{}".to_owned())
                    } else if case == "authentication" {
                        (
                            "401 Unauthorized",
                            r#"{"error":{"code":"LIX_UNAUTHORIZED","message":"expired"}}"#
                                .to_owned(),
                        )
                    } else if case == "malformed" {
                        ("200 OK", "not JSON".to_owned())
                    } else {
                        (
                            "200 OK",
                            serde_json::json!({
                                "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                                "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                                "lixId": lix_id, "sessionId": "identity-test",
                                "activeAccountId": "00000000-0000-7000-8000-000000000599",
                            })
                            .to_string(),
                        )
                    };
                    write!(stream, "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
                }
            });
            let error = connect_existing_authority(&state, Some(&ServerOptions::new(url)), None)
                .await
                .err()
                .expect("non-network errors must not open offline");
            if case == "identity" {
                assert_eq!(error.code, "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH");
            }
            assert_ne!(error.code, "LIX_TRANSPORT_NETWORK");
            thread.join().unwrap();
        }
        authority.close().await.unwrap();
    }
}
