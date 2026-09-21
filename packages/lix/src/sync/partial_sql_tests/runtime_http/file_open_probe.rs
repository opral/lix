//! Network profiles use the real HTTP dispatcher. The optional external snapshot
//! never becomes a checked-in fixture; only counts and timings are reported.
use super::*;

// Mutate through the server's engine; a separate authority handle over the
// same backing storage retains stale full-repository revision caches.
pub(super) async fn authority_execute(
    server: &LixServerProtocol<Memory>,
    repository: &str,
    sql: &str,
    params: &[Value],
) -> ExecuteResult {
    let base = format!("https://example.test/lix/v1/{repository}");
    let response = server
        .handle(
            http::Request::builder()
                .method("GET")
                .uri(&base)
                .header(
                    "lix-server-protocol-version",
                    crate::SERVER_PROTOCOL_VERSION.to_string(),
                )
                .body(ServerProtocolBody::from(Vec::new()))
                .unwrap(),
            ServerProtocolContext::anonymous(),
        )
        .await;
    assert!(response.status().is_success());
    let handshake: serde_json::Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let response = server.handle(
        http::Request::builder().method("POST").uri(format!("{base}/execute"))
            .header("lix-server-protocol-version", crate::SERVER_PROTOCOL_VERSION.to_string())
            .header("lix-session-id", handshake["sessionId"].as_str().unwrap())
            .header("idempotency-key", uuid::Uuid::now_v7().to_string())
            .header("content-type", "application/json")
            .body(ServerProtocolBody::from(serde_json::to_vec(&serde_json::json!({
                "sql": sql,
                "params": crate::authority_client::wire::encode_engine_values(params).unwrap(),
            })).unwrap())).unwrap(),
        ServerProtocolContext::anonymous(),
    ).await;
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert!(status.is_success(), "{}", String::from_utf8_lossy(&bytes));
    serde_json::from_slice::<crate::authority_client::wire::ExecuteResponseBody>(&bytes)
        .unwrap()
        .into_execute_result()
        .unwrap()
}

#[derive(Clone)]
pub(super) struct TimedClient {
    pub(super) inner: Client,
    pub(super) log: Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    pub(super) delay: u64,
}
impl RawHttpClient for TimedClient {
    fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
        Box::pin(async move {
            let operation = request
                .url
                .split("/sync/")
                .last()
                .unwrap()
                .split('?')
                .next()
                .unwrap()
                .to_owned();
            let request_bytes = request.body.as_ref().map_or(0, Vec::len);
            let started = Instant::now();
            if self.delay > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(self.delay)).await;
            }
            let server_started = Instant::now();
            let result = self.inner.send(request).await;
            let server_ms = server_started.elapsed().as_secs_f64() * 1000.;
            let (response_bytes, inputs, discovery) = result
                .as_ref()
                .map(|response| {
                    let json: serde_json::Value =
                        serde_json::from_slice(&response.body).unwrap_or_default();
                    (
                        response.body.len(),
                        json.get("inputs")
                            .and_then(|v| v.as_array())
                            .map_or(0, Vec::len),
                        json.get("profile").cloned(),
                    )
                })
                .unwrap_or_default();
            self.log.lock().unwrap().push(serde_json::json!({"operation":operation,"request_bytes":request_bytes,"response_bytes":response_bytes,"inputs":inputs,"discovery":discovery,"server_ms":server_ms,"elapsed_ms":started.elapsed().as_secs_f64()*1000.}));
            result
        })
    }
}

#[tokio::test]
#[ignore = "external snapshot profile; set LIX_PROBE_SNAPSHOT and LIX_PROBE_FILE_ID"]
async fn attached_snapshot_file_open_probe() {
    let backing = Memory::new();
    let bytes = std::fs::read(std::env::var("LIX_PROBE_SNAPSHOT").unwrap()).unwrap();
    let file_id = std::env::var("LIX_PROBE_FILE_ID").unwrap();
    uuid::Uuid::parse_str(&file_id).unwrap();
    let authority = open_lix()
        .with_storage(backing.clone())
        .from_snapshot(futures_lite::io::Cursor::new(bytes))
        .await
        .unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let sql = format!("SELECT content FROM lix_file WHERE id = '{file_id}'");
    let expected = authority.execute(&sql, &[]).await.unwrap().rows()[0]
        .get::<Vec<u8>>("content")
        .unwrap();
    for mode in ["object", "operation"] {
        for delay in [0, 100] {
            for run in 0..3 {
                let log = Arc::new(std::sync::Mutex::new(Vec::new()));
                let client = TimedClient {
                    inner: Client {
                        server: server.clone(),
                        lose_body: Arc::new(AtomicBool::new(false)),
                    },
                    log: log.clone(),
                    delay,
                };
                let transport = HttpSyncTransport::connect_with(
                    client,
                    &format!("https://example.test/lix/{}", authority.lix_id()),
                )
                .await
                .unwrap();
                let leased = transport.partial_replica_descriptor(None).await.unwrap();
                let state = Arc::new(
                    PartialReplicaState::from_leased(
                        transport.protocol_url().into(),
                        authority.active_account_id().into(),
                        uuid::Uuid::now_v7().to_string(),
                        leased.wire,
                    )
                    .unwrap(),
                );
                transport
                    .bind_native_baseline_lease(state.baseline_lease())
                    .unwrap();
                let storage = StorageAdapter::new(Memory::new());
                let read = storage.begin_read(Default::default()).await.unwrap();
                let mut writes = storage.new_write_set();
                let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
                crate::init::stage_partial_repository_protocol(&mut writes);
                drop(read);
                storage
                    .commit_write_set(
                        writes,
                        StorageWriteOptions {
                            preconditions,
                            await_durable: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .unwrap();
                let (engine, session) =
                    Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
                        .await
                        .unwrap();
                engine.sync_mode().admit_partial_replica(
                    state.clone(),
                    crate::sync::partial_replica_write_capability(),
                );
                storage
                    .admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
                for heat in ["cold", "warm"] {
                    log.lock().unwrap().clear();
                    let start = Instant::now();
                    let mut attempts = 0;
                    let mut seen = BTreeSet::new();
                    loop {
                        attempts += 1;
                        assert!(attempts < 300);
                        match session.execute(&sql, &[]).await {
                            Ok(result) => {
                                assert_eq!(
                                    result.rows()[0].get::<Vec<u8>>("content").unwrap(),
                                    expected
                                );
                                break;
                            }
                            Err(mut error) => {
                                if mode == "operation" {
                                    let diagnostic = format!("{}:{:?}", error.code, error.details);
                                    assert!(
                                        seen.insert(diagnostic),
                                        "operation repeated missing input: {error:?}"
                                    );
                                    if attempts <= 64 {
                                        eprintln!(
                                            "READ_DISCOVERY_MISS attempt={attempts} code={} details={:?}",
                                            error.code, error.details
                                        );
                                    }
                                }
                                // Controlled A/B comparator only. Production reads never
                                // fall back from failed operation discovery to object RPCs.
                                if mode == "object" {
                                    if let Some(details) =
                                        error.details.as_mut().and_then(|v| v.as_object_mut())
                                    {
                                        details.remove("readFulfillment");
                                    }
                                }
                                let demand =
                                    crate::sync::runtime::native_sync_demand_request_for_error(
                                        &error,
                                    )
                                    .unwrap()
                                    .unwrap_or_else(|| panic!("unexpected {error:?}"));
                                crate::sync::partial_runtime::hydrate_demand(
                                    &storage, &state, &transport, demand,
                                )
                                .await
                                .unwrap_or_else(|failure| panic!("hydration failed at attempt {attempts}: {failure:?}; original: {error:?}; requests: {:?}", log.lock().unwrap()));
                            }
                        }
                    }
                    let requests = log.lock().unwrap().clone();
                    eprintln!(
                        "FILE_OPEN_PROBE {}",
                        serde_json::json!({"mode":mode,"delay_ms":delay,"run":run,"heat":heat,"elapsed_ms":start.elapsed().as_secs_f64()*1000.,"attempts":attempts,"content_bytes":expected.len(),"request_count":requests.len(),"response_bytes":requests.iter().map(|v|v["response_bytes"].as_u64().unwrap()).sum::<u64>(),"requests":requests})
                    );
                }
                session.close().await.unwrap();
            }
        }
    }
    authority.close().await.unwrap();
}

#[tokio::test]
async fn file_read_fulfillment_uses_public_retry_and_preserves_local_edits() {
    use crate::storage_adapter::StorageSession;
    let backing = Memory::new();
    let authority = open_lix().with_storage(backing.clone()).await.unwrap();
    authority
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/target.bin', $1), ('/other.bin', $2)",
            &[
                Value::Blob(vec![17; 140_000].into()),
                Value::Blob(vec![23; 1024 * 1024].into()),
            ],
        )
        .await
        .unwrap();
    // Keep the cold fixture's plugin dependency explicit and in a separate
    // commit. The empty registry is valid reserved-row JSON and exercises the
    // exact registry/owner reads performed while preparing returned file rows.
    let branch_id = authority.active_branch_id().await.unwrap();
    let registry = crate::plugin::runtime::PluginRegistry::empty();
    let mut registry_seed = authority.begin_transaction().await.unwrap();
    registry_seed
        .stage_test_row(registry.write_row(&branch_id).unwrap())
        .await
        .unwrap();
    registry_seed.commit().await.unwrap();
    let server = open_lix()
        .with_storage(backing)
        .serve()
        .with_embedded_lix_id()
        .await
        .unwrap();
    let log = Arc::new(std::sync::Mutex::new(Vec::new()));
    let transport = HttpSyncTransport::connect_with(
        TimedClient {
            inner: Client {
                server: server.clone(),
                lose_body: Arc::new(AtomicBool::new(false)),
            },
            log: log.clone(),
            delay: 0,
        },
        &format!("https://example.test/lix/{}", authority.lix_id()),
    )
    .await
    .unwrap();
    let leased = transport.partial_replica_descriptor(None).await.unwrap();
    let state = Arc::new(
        PartialReplicaState::from_leased(
            transport.protocol_url().into(),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            leased.wire,
        )
        .unwrap(),
    );
    transport
        .bind_native_baseline_lease(state.baseline_lease())
        .unwrap();
    let storage = StorageAdapter::new(StorageSession::acquire(Memory::new()).await.unwrap());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut writes = storage.new_write_set();
    let preconditions = stage_partial_bootstrap(&read, &mut writes, &state).unwrap();
    crate::init::stage_partial_repository_protocol(&mut writes);
    drop(read);
    storage
        .commit_write_set(
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (engine, session) =
        Engine::new_partial_replica(storage.clone(), EngineOptions::new(), &state)
            .await
            .unwrap();
    engine.sync_mode().admit_partial_replica(
        state.clone(),
        crate::sync::partial_replica_write_capability(),
    );
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<crate::sync::SyncDemand>(16);
    let replica = Lix::from_partial_engine_for_test(Arc::new(engine), session, sender);
    let worker = tokio::spawn(async move {
        while let Some(demand) = receiver.recv().await {
            let result = crate::sync::partial_runtime::hydrate_demand_with_receipt(
                &storage,
                &state,
                &transport,
                demand.request,
            )
            .await;
            let _ = demand.response.send(result);
        }
    });
    log.lock().unwrap().clear();
    let sql = "SELECT content FROM lix_file WHERE path = '/target.bin'";
    assert_eq!(
        replica.execute(sql, &[]).await.unwrap().rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        vec![17; 140_000]
    );
    let cold = log.lock().unwrap().clone();
    eprintln!("PUBLIC_FILE_FULFILLMENT {}", serde_json::json!(cold));
    assert!(
        cold.len() == 1,
        "cold point read must discover its complete dependency closure in one request: {cold:?}"
    );
    assert!(
        cold.iter().all(|r| r["operation"] == "read-fulfillment"),
        "ordinary reads must not fall back to pointer hydration"
    );
    assert!(
        cold.iter()
            .map(|r| r["response_bytes"].as_u64().unwrap())
            .sum::<u64>()
            < 1024 * 1024,
        "opening one file must not transfer the unrelated file"
    );
    log.lock().unwrap().clear();
    assert_eq!(
        replica.execute(sql, &[]).await.unwrap().rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        vec![17; 140_000]
    );
    assert!(
        log.lock().unwrap().is_empty(),
        "warm read must work without network"
    );
    replica
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/target.bin'",
            &[Value::Blob(b"local pending edit".to_vec().into())],
        )
        .await
        .unwrap();
    authority_execute(
        &server,
        authority.lix_id(),
        "UPDATE lix_file SET content = $1 WHERE path = '/target.bin'",
        &[Value::Blob(b"new authority value".to_vec().into())],
    )
    .await;
    assert!(
        replica
            .execute("SELECT id FROM lix_file WHERE path = '/absent.bin'", &[])
            .await
            .unwrap()
            .rows()
            .is_empty()
    );
    assert_eq!(
        replica.execute(sql, &[]).await.unwrap().rows()[0]
            .get::<Vec<u8>>("content")
            .unwrap(),
        b"local pending edit"
    );
    replica.close().await.unwrap();
    worker.abort();
    authority.close().await.unwrap();
}
