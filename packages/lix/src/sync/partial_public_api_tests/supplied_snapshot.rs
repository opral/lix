use super::*;

#[derive(Clone)]
struct SnapshotClient {
    server: LixServerProtocol<Memory>,
    account_id: String,
    requests: Arc<AtomicUsize>,
}
impl RawHttpClient for SnapshotClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            if request.url.contains("/sync/native-") || request.url.contains("/sync/chunk") {
                self.requests.fetch_add(1, Ordering::SeqCst);
            }
            // Keep the captured remote identity for admission, but route its
            // deployment-specific prefix to this in-memory protocol authority.
            let base = format!("/lix/v1/{}", self.server.lix_id());
            let path = match request.url.split_once("/sync/") {
                Some((_, suffix)) => format!("{base}/sync/{suffix}"),
                None => base, // Protocol handshake at the deployment's base URL.
            };
            let mut builder = http::Request::builder().method(request.method).uri(path);
            for (key, value) in request.headers {
                builder = builder.header(key, value);
            }
            let response = self
                .server
                .handle(
                    builder
                        .body(ServerProtocolBody::from(request.body.unwrap_or_default()))
                        .unwrap(),
                    ServerProtocolContext {
                        principal: crate::server_protocol::ServerProtocolPrincipal::Authenticated {
                            account_id: self.account_id.clone(),
                            idempotency_scope: "snapshot-reproduction".into(),
                        },
                        durable_terminal_storage_notifier: None,
                    },
                )
                .await;
            let status = response.status();
            Ok(RawHttpResponse {
                status: status.as_u16(),
                status_text: status.to_string(),
                body: response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .to_vec(),
            })
        })
    }
}

fn snapshot_bytes(name: &str) -> Vec<u8> {
    let directory = std::env::var("LIX_REPRODUCTION_DIR")
        .expect("set LIX_REPRODUCTION_DIR to the supplied reproduction directory");
    std::fs::read(std::path::Path::new(&directory).join(format!("{name}.lixsnap"))).unwrap()
}

#[tokio::test]
#[ignore = "requires user-supplied reproduction snapshots"]
async fn reconcile_supplied_missing_file_snapshots() {
    let remote_bytes = snapshot_bytes("remote");
    let remote = Memory::new();
    let authority = open_lix()
        .with_storage(remote.clone())
        .from_snapshot(futures_lite::io::Cursor::new(remote_bytes))
        .await
        .unwrap();
    let expected = authority
        .execute("SELECT path,name FROM lix_file ORDER BY path", &[])
        .await
        .unwrap();
    authority.close().await.unwrap();
    let server = open_lix()
        .with_storage(remote)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let local_bytes = snapshot_bytes("local");
    let restored = open_lix()
        .with_storage(crate::sync::durable_memory_for_test(Memory::new()))
        .from_snapshot(futures_lite::io::Cursor::new(local_bytes))
        .await
        .unwrap();
    let storage = restored.storage_adapter();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (old, _) = crate::sync::partial_state::load_partial_replica_state(&read)
        .await
        .unwrap()
        .unwrap();
    drop(read);
    let old = Arc::new(old);
    let requests = Arc::new(AtomicUsize::new(0));
    let client = SnapshotClient {
        server,
        account_id: old.active_account_id().to_owned(),
        requests: requests.clone(),
    };
    let transport = HttpSyncTransport::connect_with(client, old.remote_id())
        .await
        .unwrap();
    transport
        .bind_native_baseline_lease(old.baseline_lease())
        .unwrap();
    let (engine, session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &old)
            .await
            .unwrap();
    let engine = Arc::new(engine);
    engine
        .sync_mode()
        .admit_partial_replica(old.clone(), crate::sync::partial_replica_write_capability());
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    let registry = engine.sync_mode().read_interests().unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    crate::sync::partial_interest_journal::restore_candidate_read_interests(&read, &old, &registry)
        .await
        .unwrap();
    drop(read);
    // Exercise the worker's descriptor wait and publication loop, without a
    // foreground query or manual candidate publication to drive convergence.
    let (shutdown, shutdown_rx) =
        tokio::sync::watch::channel(crate::sync::runtime::SyncShutdown::Running);
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let lix = Lix::from_partial_engine_for_test(engine.clone(), session, sender);
    let worker = crate::sync::partial_runtime::run_partial_worker_with_engine(
        storage,
        old.clone(),
        Some(transport),
        || Box::pin(async { Err(LixError::unknown("unexpected reconnect")) }),
        shutdown_rx,
        receiver,
        None,
        Some(engine.clone()),
    );
    let caller = async {
        loop {
            let admitted = engine.sync_mode().partial_admission().unwrap();
            if admitted.descriptor().cursor > old.descriptor().cursor {
                break;
            }
            crate::sync::platform::sleep(std::time::Duration::from_millis(2)).await;
        }
        let sql = "SELECT path,name FROM lix_file ORDER BY path";
        assert_eq!(lix.execute(sql, &[]).await.unwrap().rows(), expected.rows());
        eprintln!(
            "supplied snapshot converged: cursor {} -> {}, {} file rows, {} retained interests",
            old.descriptor().cursor,
            engine
                .sync_mode()
                .partial_admission()
                .unwrap()
                .descriptor()
                .cursor,
            expected.rows().len(),
            registry.snapshot().unwrap().interests.len()
        );
        let before = requests.load(Ordering::SeqCst);
        for _ in 0..3 {
            assert_eq!(lix.execute(sql, &[]).await.unwrap().rows(), expected.rows());
        }
        assert_eq!(
            requests.load(Ordering::SeqCst),
            before,
            "warm reads remain local"
        );
        shutdown.send_replace(crate::sync::runtime::SyncShutdown::Stop);
    };
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        let (result, ()) = futures_util::join!(Box::pin(worker), Box::pin(caller));
        result.unwrap();
    })
    .await
    .expect("supplied local snapshot must converge through background sync");
}
