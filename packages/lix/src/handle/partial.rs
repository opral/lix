//! Public sync-mode construction for a partial replica with on-demand sync.

use super::*;

pub(crate) async fn open_partial_lix<StorageImpl>(
    storage: StorageSession<StorageImpl>,
    wasm_runtime: Option<Arc<dyn WasmRuntime>>,
    telemetry: Option<Arc<dyn TelemetrySink>>,
    server: Option<ServerOptions>,
) -> Result<Lix<StorageImpl>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let owner = storage
        .acquire_partial_replica_owner(storage.token())
        .await?;
    let owner = crate::engine::PartialOwnerLifetime::install(owner);
    let mut prepared = crate::sync::prepare_partial_open(storage, server.clone()).await?;
    let result = async {
        #[cfg(feature = "default_wasm_runtime")]
        let wasm_runtime = match wasm_runtime {
            Some(runtime) => Some(runtime),
            None => Some(crate::plugin::runtime::default::runtime()?),
        };
        let mut options = EngineOptions::new();
        if let Some(runtime) = wasm_runtime {
            options = options.with_wasm_runtime(runtime);
        }
        if let Some(telemetry) = telemetry {
            options = options.with_telemetry(telemetry);
        }
        let (mut engine, session) =
            Engine::new_partial_replica(prepared.adapter.clone(), options, &prepared.state).await?;
        engine.install_partial_owner(owner.clone());
        let engine = Arc::new(engine);
        prepared.bind_engine(&engine)?;
        let runtime = prepared
            .start_runtime(engine.sync_mode().change_watcher(), engine.clone())
            .await?;
        let lix = Lix {
            engine,
            session: Arc::new(session),
            transaction_lifecycle: Arc::default(),
            primary_switch_gate: Some(Arc::new(tokio::sync::Mutex::new(()))),
            sync_demand_tx: Some(runtime.demand_tx.clone()),
            sync_lease: Some(SyncSessionLease::root_with_owner(runtime, owner.clone())),
            server,
            open_report: Arc::new(OpenReport {
                format: crate::init::CURRENT_FORMAT_VERSION,
                initialized: prepared.initialized,
                migration: None,
            }),
        };
        lix.bind_session();
        Ok(lix)
    }
    .await;
    if result.is_err() {
        prepared.close_after_error().await;
    }
    result
}

pub(super) async fn open_partial_storage_session<Source, Backing>(
    source: &Lix<Source>,
    storage: Backing,
) -> Result<Lix<Backing>, LixError>
where
    Source: Storage + Clone + Send + Sync + 'static,
    Backing: Storage + Clone + Send + Sync + 'static,
{
    let expected = source
        .engine
        .sync_mode()
        .partial_admission()
        .ok_or_else(|| {
            LixError::new(
                "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                "partial storage session lacks authenticated admission",
            )
        })?;
    if source.sync_demand_tx.is_none() || source.sync_lease.is_none() {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "partial storage session requires a live owning demand runtime",
        ));
    }
    let storage = StorageSession::acquire(storage).await?;
    let admitted = crate::migration::admit_partial_epoch(&storage).await?;
    if &admitted.state != expected.as_ref() {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "storage session must use the identical durable repository, authority, account and epoch",
        ));
    }
    let mut options = EngineOptions::new();
    if let Some(telemetry) = source.engine.telemetry() {
        options = options.with_telemetry(telemetry.clone());
    }
    let (mut engine, initial_session) =
        Engine::new_partial_replica(admitted.adapter, options, &expected).await?;
    engine.inherit_partial_storage_runtime(&source.engine);
    engine.inherit_sync_mode(source.engine.sync_mode());
    crate::sync::admit_partial_storage_session(&engine, &expected)?;
    let session = engine
        .open_session_at_with_account(
            source.active_branch_id().await?,
            source.active_account_id().to_owned(),
        )
        .await?;
    initial_session.close().await?;
    let lix = Lix {
        engine: Arc::new(engine),
        session: Arc::new(session),
        transaction_lifecycle: Arc::default(),
        primary_switch_gate: None,
        sync_demand_tx: source.sync_demand_tx.clone(),
        sync_lease: source.sync_lease.as_ref().map(|lease| lease.child()),
        server: source.server.clone(),
        open_report: source.open_report.clone(),
    };
    lix.bind_session();
    Ok(lix)
}

// Keep explicit migration's large owned future out of ordinary caller poll
// frames, including SQL performed before the migration itself is awaited.
/// Normal partial opening never invokes this explicit conversion operation.
pub(crate) fn convert_full_replica_for_partial_open<S>(
    storage: S,
    server: ServerOptions,
    branch_id: Option<&str>,
) -> crate::sync::SyncTransportFuture<'static, ()>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let operation = convert_full_replica_owned(storage, server, branch_id.map(str::to_owned));
    #[cfg(not(target_family = "wasm"))]
    {
        // SAFETY: this exact operation owns storage, server configuration and
        // branch text. StorageSession/owner lease and native storage handles
        // are Send by their owner contracts; references retained by migration
        // point only to Sync state. No borrowed caller input crosses an await.
        // `conversion_send_tests` checks the complete raw Memory future plus
        // universally quantified borrowing-adapter and named-pointee obligations.
        // This is the same higher-ranked GAT obstruction as owned opening;
        // do not move the assertion to a generic migration/SQL helper.
        Box::pin(unsafe { crate::session::AssumeSendFuture::new(operation) })
    }
    #[cfg(target_family = "wasm")]
    {
        Box::pin(operation)
    }
}

// Kept separate so the compile-time safety proof inspects the raw operation,
// not an already-asserted Send wrapper.
async fn convert_full_replica_owned<S>(
    storage: S,
    server: ServerOptions,
    branch_id: Option<String>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageSession::acquire(storage).await?;
    let _owner = storage
        .acquire_partial_replica_owner(storage.token())
        .await?;
    let authenticated =
        crate::sync::authenticate_partial_source_conversion(server, branch_id.as_deref()).await?;
    crate::migration::convert_clean_replica_to_partial(&storage, &authenticated, None).await?;
    Ok(())
}

#[cfg(all(test, not(target_family = "wasm")))]
mod conversion_send_tests;

#[cfg(all(test, feature = "server-protocol", not(target_family = "wasm")))]
mod profile;

#[cfg(all(test, feature = "server-protocol", not(target_family = "wasm")))]
mod browser_profile_authority;

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::atomic::AtomicUsize;

    #[tokio::test]
    async fn partial_handle_rejects_full_cache_before_connecting() {
        let backing = crate::sync::durable_memory_for_test(Memory::new());
        let full = open_lix().with_storage(backing.clone()).await.unwrap();
        let repository_id = full.lix_id().to_owned();
        full.close().await.unwrap();
        drop(full);
        let error = open_partial_lix(
            StorageSession::acquire(backing).await.unwrap(),
            None,
            None,
            Some(ServerOptions::new(format!(
                "http://127.0.0.1:9/lix/{repository_id}"
            ))),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.code, "LIX_PARTIAL_REPLICA_MIGRATION_REQUIRED");
    }

    #[tokio::test]
    async fn public_partial_handle_opens_bounded_hydrates_sql_and_reopens_offline() {
        let authority = open_lix().await.unwrap();
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('partial-handle', 'warm')",
                &[],
            )
            .await
            .unwrap();
        let repository_id = authority.lix_id().to_owned();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let locator = format!(
            "http://{}/lix/{repository_id}",
            listener.local_addr().unwrap()
        );
        let requests = Arc::new(AtomicUsize::new(0));
        let received = requests.clone();
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            'connections: loop {
                let (mut connection, _) = listener.accept().unwrap();
                connection
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    let mut byte = [0u8];
                    if connection.read_exact(&mut byte).is_err() {
                        continue 'connections;
                    }
                    headers.push(byte[0]);
                    assert!(headers.len() < 16 * 1024);
                }
                let headers = String::from_utf8(headers).unwrap();
                assert!(
                    headers
                        .to_ascii_lowercase()
                        .contains("authorization: bearer partial-test\r\n")
                );
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                assert!(length <= 16 * 1024);
                let mut bytes = vec![0; length];
                connection.read_exact(&mut bytes).unwrap();
                let first = headers.lines().next().unwrap();
                let path = first.split_whitespace().nth(1).unwrap();
                let route = path.split('?').next().unwrap();
                let background = route.ends_with("/sync/descriptor") && path.contains('?');
                let closing = first.starts_with("DELETE ");
                let body = if closing {
                    serde_json::json!({})
                } else if route.ends_with("/sync/descriptor") {
                    serde_json::to_value(crate::sync::LeasedPartialReplicaDescriptor::for_test(
                        descriptor.clone(),
                        authority.active_account_id(),
                    ))
                    .unwrap()
                } else if path.ends_with("/sync/native-metadata") {
                    let request: crate::sync::NativeMetadataRequest =
                        serde_json::from_slice(&bytes).unwrap();
                    serde_json::to_value(
                        runtime
                            .block_on(authority.read_sync_native_metadata(&request))
                            .unwrap(),
                    )
                    .unwrap()
                } else if path.ends_with("/sync/native-object-range") {
                    let request: crate::sync::NativeObjectRangeRequest =
                        serde_json::from_slice(&bytes).unwrap();
                    serde_json::to_value(
                        runtime
                            .block_on(authority.read_sync_native_object_range(&request))
                            .unwrap(),
                    )
                    .unwrap()
                } else {
                    assert_eq!(
                        path.trim_end_matches('/'),
                        format!("/lix/v1/{repository_id}")
                    );
                    serde_json::json!({"protocolVersion": crate::SERVER_PROTOCOL_VERSION, "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION, "lixId":repository_id, "sessionId":"partial-handle-test", "activeAccountId":authority.active_account_id()})
                };
                if !background {
                    received.fetch_add(1, Ordering::SeqCst);
                }
                let body = serde_json::to_vec(&body).unwrap();
                let _ = write!(
                    connection,
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = connection.write_all(&body);
                if closing {
                    break;
                }
            }
        });
        let backing = crate::sync::durable_memory_for_test(Memory::new());
        let server = ServerOptions::new(locator)
            .with_headers([("Authorization".to_owned(), "Bearer partial-test".to_owned())]);
        let lix = open_lix()
            .with_storage(backing.clone())
            .with_server(server)
            .await
            .unwrap();
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "foreground opening must only handshake and fetch its descriptor"
        );
        assert!(lix.open_report().initialized);
        let backing_session = lix.open_storage_session(backing.clone()).await.unwrap();
        assert_eq!(backing_session.active_account_id(), lix.active_account_id());
        assert!(
            lix.open_storage_session(crate::sync::durable_memory_for_test(Memory::new()))
                .await
                .is_err()
        );
        let child = lix.open_another_session().await.unwrap();
        assert_eq!(child.active_account_id(), lix.active_account_id());
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "same-account session opening must not hydrate account rows"
        );
        let sql = "SELECT value FROM lix_key_value WHERE key = $1";
        let params = [Value::Text("partial-handle".into())];
        assert_eq!(lix.execute(sql, &params).await.unwrap().rows().len(), 1);
        let warm = requests.load(Ordering::SeqCst);
        assert!(warm > 2, "cold SQL must demand missing native inputs");
        assert_eq!(lix.execute(sql, &params).await.unwrap().rows().len(), 1);
        assert_eq!(requests.load(Ordering::SeqCst), warm);
        lix.close().await.unwrap();
        let contender = StorageSession::acquire(backing.clone()).await.unwrap();
        assert!(
            contender
                .acquire_partial_replica_owner(contender.token())
                .await
                .is_err(),
            "root close must retain ownership through live child sessions"
        );
        assert_eq!(child.execute(sql, &params).await.unwrap().rows().len(), 1);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            warm,
            "child lease keeps the shared worker alive without another request"
        );
        child.close().await.unwrap();
        assert!(
            contender
                .acquire_partial_replica_owner(contender.token())
                .await
                .is_err()
        );
        assert_eq!(
            backing_session
                .execute(sql, &params)
                .await
                .unwrap()
                .rows()
                .len(),
            1
        );
        assert_eq!(requests.load(Ordering::SeqCst), warm);
        backing_session.close().await.unwrap();
        // All closed handles remain allocated across the successful reopen.
        thread.join().unwrap();
        let offline = open_lix().with_storage(backing.clone()).await.unwrap();
        assert!(!offline.open_report().initialized);
        let mut snapshot = Vec::new();
        assert!(
            offline
                .export_snapshot()
                .write_to(&mut snapshot)
                .await
                .is_err()
        );
        assert!(
            snapshot.is_empty(),
            "partial storage must not emit a full snapshot header"
        );
        assert_eq!(offline.execute(sql, &params).await.unwrap().rows().len(), 1);
        assert_eq!(
            requests.load(Ordering::SeqCst),
            warm + 1,
            "offline reopen must not contact the stopped authority"
        );
        assert!(
            offline
                .open_another_session()
                .with_account(crate::SYSTEM_ACCOUNT_ID)
                .await
                .is_err()
        );
        offline.close().await.unwrap();
    }
}

#[cfg(all(test, feature = "server-protocol", not(target_family = "wasm")))]
mod browser_file_profile_authority;

/// Explicitly retry retained native migration pins. Storage must be closed;
/// success never changes the published serving baseline or cached working set.
pub(crate) async fn retry_partial_migration_cleanup<S>(
    storage: S,
    server: ServerOptions,
) -> Result<usize, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageSession::acquire(storage).await?;
    let _owner = storage
        .acquire_partial_replica_owner(storage.token())
        .await?;
    let admitted = crate::migration::admit_partial_epoch(&storage).await?;
    let selected = admitted
        .state
        .descriptor()
        .selected_branch
        .branch_id
        .clone();
    let authenticated =
        crate::sync::authenticate_partial_conversion(server, Some(&selected)).await?;
    crate::migration::retry_published_conversion_cleanup(&storage, &authenticated).await
}
